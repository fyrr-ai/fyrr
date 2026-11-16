use super::*;

struct LexicalFixture {
    dataset: Dataset,
    schema: Arc<arrow_schema::Schema>,
    root: std::path::PathBuf,
}
async fn lexical_mask_fixture() -> LexicalFixture {
    use arrow_array::RecordBatchIterator;
    use arrow_schema::{Field, Schema};
    use lance::index::DatasetIndexExt;
    use lance_index::scalar::InvertedIndexParams;
    let schema = Arc::new(Schema::new(
        ["source_id", "root_id", "record_id", "searchable_text"]
            .into_iter()
            .map(|name| Field::new(name, arrow_schema::DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["source"; 64])),
            Arc::new(StringArray::from(
                (0..64).map(|i| format!("root-{i:02}")).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["record"; 64])),
            Arc::new(StringArray::from(
                (0..64)
                    .map(|i| {
                        if i < 60 {
                            "needle"
                        } else {
                            "needle filler filler filler"
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lance-pin")
        .join(format!(
            "lexical-mask-{}-{}",
            std::process::id(),
            fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
        ));
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
        root.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["searchable_text"],
            IndexType::Inverted,
            Some("fts_idx".into()),
            &InvertedIndexParams::default().with_position(false),
            false,
        )
        .await
        .unwrap();
    LexicalFixture {
        dataset,
        schema,
        root,
    }
}

#[tokio::test]
async fn lexical_request_mask_precedes_topk_and_preserves_sql_scores() {
    use arrow_array::RecordBatchIterator;
    use lance::index::DatasetIndexExt;
    let LexicalFixture {
        mut dataset,
        schema,
        root,
    } = lexical_mask_fixture().await;
    let mut denied_scan = dataset.scan();
    denied_scan.project(&["root_id"]).unwrap();
    denied_scan
        .full_text_search(
            lance_index::scalar::FullTextSearchQuery::new("needle".into())
                .with_column("searchable_text".into())
                .unwrap()
                .limit(Some(2)),
        )
        .unwrap();
    let global = denied_scan.try_into_batch().await.unwrap();
    assert!(
        strings(&global, "root_id")
            .unwrap()
            .iter()
            .all(|r| r.unwrap() < "root-60"),
        "denied rows outrank every admitted row"
    );
    // The two-row mask also models a request filter narrowing four granted rows.
    let eligible = RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([62, 63]));
    assert!(!PreparedReader::fts_requires_sql_prefilter(
        &dataset,
        &dataset.load_indices().await.unwrap(),
    ));
    let (masked, _, _) = PreparedReader::lexical(&dataset, &eligible, None, "needle", 2)
        .await
        .unwrap();
    assert_eq!(
        masked.iter().map(|r| r.root.as_str()).collect::<Vec<_>>(),
        ["root-62", "root-63"]
    );
    let mut legacy = dataset.scan();
    legacy
        .project(&["source_id", "root_id", "record_id"])
        .unwrap();
    legacy
        .filter("root_id >= 'root-62'")
        .unwrap()
        .prefilter(true)
        .with_row_id();
    legacy
        .full_text_search(
            lance_index::scalar::FullTextSearchQuery::new("needle".into())
                .with_column("searchable_text".into())
                .unwrap()
                .limit(Some(3)),
        )
        .unwrap();
    let batch = legacy.try_into_batch().await.unwrap();
    let sql = PreparedReader::lane_rows(&batch, "_score", false, &eligible).unwrap();
    let observed = |rows: &[Candidate]| {
        rows.iter()
            .map(|r| (r.row_id, r.root.clone(), r.score))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        observed(&masked),
        observed(&sql),
        "same BM25 scores and stable cutoff order"
    );
    assert!(
        PreparedReader::lexical(&dataset, &RowAddrMask::allow_nothing(), None, "needle", 2)
            .await
            .unwrap()
            .0
            .is_empty()
    );
    // Append after indexing: one readable row and fifty denied/filter-excluded
    // matches must not change the readable row's flat BM25 statistics.
    let tail = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["source"; 51])),
            Arc::new(StringArray::from(
                (0..51).map(|i| format!("tail-{i:02}")).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["record"; 51])),
            Arc::new(StringArray::from(vec!["needle"; 51])),
        ],
    )
    .unwrap();
    dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(tail)], schema),
        root.to_str().unwrap(),
        Some(lance::dataset::WriteParams {
            mode: lance::dataset::WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert!(PreparedReader::fts_requires_sql_prefilter(
        &dataset,
        &dataset.load_indices().await.unwrap(),
    ));
    let tail_id = dataset.manifest().fragments.last().unwrap().id << 32;
    let eligible = RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([tail_id]));
    let predicate = "root_id = 'tail-00'";
    let (masked, _, _) = PreparedReader::lexical(&dataset, &eligible, Some(predicate), "needle", 1)
        .await
        .unwrap();
    let mut legacy = dataset.scan();
    legacy
        .project(&["source_id", "root_id", "record_id"])
        .unwrap();
    legacy
        .filter(predicate)
        .unwrap()
        .prefilter(true)
        .with_row_id();
    legacy
        .full_text_search(
            lance_index::scalar::FullTextSearchQuery::new("needle".into())
                .with_column("searchable_text".into())
                .unwrap()
                .limit(Some(2)),
        )
        .unwrap();
    let sql = PreparedReader::lane_rows(
        &legacy.try_into_batch().await.unwrap(),
        "_score",
        false,
        &eligible,
    )
    .unwrap();
    assert_eq!(masked.len(), 1);
    assert_eq!(
        observed(&masked),
        observed(&sql),
        "tail scores preserve SQL admission"
    );
    let (unfiltered, _, _) = PreparedReader::lexical(&dataset, &eligible, None, "needle", 1)
        .await
        .unwrap();
    assert_ne!(
        masked[0].score, unfiltered[0].score,
        "regression fixture exercises denied tail statistics"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn recall_at_10_accepts_authorized_boundary_ties_in_opposite_lane_order() {
    use arrow_array::{BooleanArray, FixedSizeListArray, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    let vectors = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        1024,
        Arc::new(Float32Array::from(vec![0.125; 40 * 1024])),
        None,
    );
    let arrays: Vec<Arc<dyn Array>> = vec![
        Arc::new(vectors),
        Arc::new(BooleanArray::from(
            (0..40).map(|i| i < 20).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec!["allowed"; 40])),
        Arc::new(StringArray::from(
            (0..40)
                .map(|i| format!("{:02}", 39 - i))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec!["record"; 40])),
    ];
    let schema = Arc::new(Schema::new(
        ["vector", "authorized", "source_id", "root_id", "record_id"]
            .into_iter()
            .zip(&arrays)
            .map(|(name, a)| Field::new(name, a.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lance-pin")
        .join(format!(
            "recall-ties-{}-{}",
            std::process::id(),
            fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
        ));
    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        path.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let query = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        1024,
        Arc::new(Float32Array::from(vec![0.125; 1024])),
        None,
    );
    let eligible = RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(0..20));
    let mut scan = dataset.scan();
    scan.filter("authorized = true")
        .unwrap()
        .prefilter(true)
        .with_row_id_prefilter(eligible.clone());
    scan.nearest("vector", &query, 75)
        .unwrap()
        .use_index(false)
        .distance_metric(lance_linalg::distance::DistanceType::Cosine)
        .with_row_id();
    scan.project(&["source_id", "root_id", "record_id", "_rowid", "_distance"])
        .unwrap();
    let batch = scan.try_into_batch().await.unwrap();
    let reference = ids(&batch).unwrap();
    assert_eq!(reference.values().as_ref(), &(0..20).collect::<Vec<u64>>());
    let lane = PreparedReader::lane_rows(&batch, "_distance", true, &eligible).unwrap();
    let rows = lane.iter().map(|row| row.row_id).collect::<Vec<_>>();
    assert_eq!(rows, (0..20).rev().collect::<Vec<u64>>());
    let distances = batch
        .column_by_name("_distance")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert!(distances.values().iter().all(|d| *d == distances.value(0)));
    assert_eq!(recall_at_k(&rows, reference, distances, 10), 1.0);
}

#[test]
fn pruning_old_admissions_releases_only_unheld_reservations() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n",
    )
    .unwrap()
    .config;
    let manifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    let capacity = 128 * 1024;
    let pool = Arc::new(tokio::sync::Semaphore::new(capacity));
    let held = PreparationReservation::acquire(&config, &manifest, 1, pool.clone()).unwrap();
    let remaining = pool.available_permits();
    assert!(remaining < capacity);
    let mut cache = AuthorizationCache::default();
    let key = AuthorizationCacheKey {
        namespace: "budget".into(),
        subject: "subject".into(),
        identity: "identity".into(),
        identity_generation: 1,
        policy: "policy".into(),
        policy_generation: 1,
        content: 1,
        hidden: Vec::new(),
    };
    let claim = cache.reserve(&key, 4096, 1).unwrap();
    cache.insert(
        key.clone(),
        CachedAdmission {
            _cache_claim: claim,
            eligible: RowAddrMask::allow_nothing(),
            maps: None,
            unindexed: BTreeSet::new(),
            reported_hidden_sources: Vec::new(),
            fallback_probes: 1,
            _reservation: held.clone(),
            _native_session: NativeSession::acquire(
                crate::generation::MemoryPool {
                    cap: capacity as u64 * 4096,
                    permits: pool.clone(),
                },
                (0, 0),
                0,
                None,
            )
            .unwrap(),
        },
        1,
    );
    let cached = cache.get(&key).unwrap();
    cache.retain_content(2);
    assert_eq!(cache.entries.capacity(), 0);
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES - 4096
    );
    assert_eq!(pool.available_permits(), remaining);
    drop(held);
    assert_eq!(pool.available_permits(), remaining);
    drop(cached);
    assert_eq!(pool.available_permits(), capacity);
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES
    );
}

#[test]
fn pinned_publication_serves_only_its_own_namespace_incarnation() {
    let fixture: fyrr_core::publication::ServingHead = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/serving-head/head.json"
    ))
    .unwrap();
    let head = |incarnation: &str, content: Option<(&str, u64)>| {
        let mut head = fixture.clone();
        head.incarnation = incarnation.to_owned();
        head.content = content.map(|(id, sequence)| fyrr_core::publication::ContentPointer {
            publication_id: id.into(),
            sequence,
            ..fixture.content.clone().unwrap()
        });
        head
    };
    let pinned = |head: &fyrr_core::publication::ServingHead, incarnation: &str| {
        PreparedReader::same_lineage(head, "pub-a", 5, incarnation)
    };
    // The pinned publication and later ones of the same incarnation.
    assert!(pinned(&head("one", Some(("pub-a", 5))), "one"));
    assert!(pinned(&head("one", Some(("pub-b", 6))), "one"));
    // An earlier publication of the same incarnation is not the pin's lineage.
    assert!(!pinned(&head("one", Some(("pub-b", 4))), "one"));
    // Another incarnation, even at a higher sequence or the same publication ID.
    assert!(!pinned(&head("two", Some(("pub-b", 6))), "one"));
    assert!(!pinned(&head("two", Some(("pub-a", 5))), "one"));
    // Before a recreated namespace's first commit.
    assert!(!pinned(&head("two", None), "one"));
}
