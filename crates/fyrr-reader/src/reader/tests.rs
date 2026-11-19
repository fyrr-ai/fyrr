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
    let session = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: capacity as u64 * 4096,
            permits: pool.clone(),
        },
        (0, 0),
        0,
        None,
    )
    .unwrap();
    let claim = cache.reserve(&key, 4096, 1).unwrap();
    cache.insert(
        key.clone(),
        CachedAdmission {
            _cache_claim: claim,
            admission: Arc::new(Admission::nothing()),
        },
        1,
    );
    let cached = cache.get(&key).unwrap();
    // An entry owns no generation memory: the old generation's reservation
    // and native session are free as soon as its readers go, even while the
    // entry is cached and held.
    drop(held);
    drop(session);
    assert_eq!(pool.available_permits(), capacity);
    cache.retain_content(2);
    assert_eq!(cache.entries.capacity(), 0);
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES - 4096
    );
    drop(cached);
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES
    );
}

fn cache_key(subject: &str) -> AuthorizationCacheKey {
    AuthorizationCacheKey {
        namespace: "budget".into(),
        subject: subject.into(),
        identity: "identity".into(),
        identity_generation: 1,
        policy: "policy".into(),
        policy_generation: 1,
        content: 1,
        hidden: Vec::new(),
    }
}

#[test]
fn cache_hits_share_one_immutable_admission() {
    let mut cache = AuthorizationCache::default();
    let key = cache_key("subject");
    let mut rows = RowAddrTreeMap::new();
    rows.insert(7);
    let admission = Arc::new(Admission {
        eligible: RowAddrMask::from_allowed(rows),
        ..Admission::nothing()
    });
    let claim = cache
        .reserve(&key, key.memory_bound() + admission.memory_bound(), 4)
        .unwrap();
    cache.insert(
        key.clone(),
        CachedAdmission {
            _cache_claim: claim,
            admission: admission.clone(),
        },
        4,
    );
    let first = cache.get(&key).unwrap().admission.clone();
    let second = cache.get(&key).unwrap().admission.clone();
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &admission));
    assert!(first.eligible.selected(7) && !first.eligible.selected(8));
}

#[test]
fn broad_admission_beyond_the_old_row_floor_is_charged_by_size_and_cached() {
    let mut cache = AuthorizationCache::default();
    let key = cache_key("broad");
    // 5 M allowed rows, built as one run: the old 32 B per row floor charged
    // 160 MB and never cached it. The real size is a few bytes.
    let mut rows = RowAddrTreeMap::new();
    rows.insert_range(0..5_000_000);
    rows.optimize();
    let admission = Arc::new(Admission {
        eligible: RowAddrMask::from_allowed(rows),
        ..Admission::nothing()
    });
    let allowed = usize::try_from(admission.eligible.max_len().unwrap()).unwrap();
    assert!(allowed * 32 > AUTHORIZATION_CACHE_MAX_BYTES);
    let incoming = key.memory_bound() + admission.memory_bound();
    assert!(incoming < 64 * 1024, "{incoming}");
    let claim = cache.reserve(&key, incoming, 1).unwrap();
    cache.insert(
        key.clone(),
        CachedAdmission {
            _cache_claim: claim,
            admission,
        },
        1,
    );
    let hit = cache.get(&key).unwrap();
    assert_eq!(hit.admission.eligible.max_len(), Some(allowed as u64));
    assert_eq!(
        cache.budget.available_permits(),
        AUTHORIZATION_CACHE_MAX_BYTES - incoming
    );
}

#[test]
fn compacted_mask_selects_exactly_the_scanned_rows() {
    // Scattered rows plus a long run in one fragment, and a second fragment.
    let ids: Vec<u64> = (0..70_000u64)
        .filter(|row| row % 7 != 3)
        .chain((1u64 << 32) + 5..(1u64 << 32) + 50_000)
        .collect();
    let mut plain = RowAddrTreeMap::new();
    for id in &ids {
        plain.insert(*id);
    }
    let mut compact = plain.clone();
    compact.optimize();
    assert!(compact.serialized_size() < plain.serialized_size());
    let (plain, compact) = (
        RowAddrMask::from_allowed(plain),
        RowAddrMask::from_allowed(compact),
    );
    assert_eq!(plain.max_len(), compact.max_len());
    assert!(
        plain
            .iter_addrs()
            .unwrap()
            .eq(compact.iter_addrs().unwrap())
    );
    for probe in (0..80_000u64).chain((1u64 << 32)..(1u64 << 32) + 60_000) {
        assert_eq!(plain.selected(probe), compact.selected(probe));
    }
}

/// The reader's former construction: every eligible row filtered by coverage.
fn unindexed_by_row(eligible: &RowAddrMask, covered: &BTreeSet<u32>) -> BTreeSet<u64> {
    eligible
        .iter_addrs()
        .unwrap()
        .map(u64::from)
        .filter(|id| !covered.contains(&((id >> 32) as u32)))
        .collect()
}

#[test]
fn unindexed_fragment_bitmap_equals_the_per_row_tail() {
    // Deterministic pseudo-random fixtures: fragments 0..12 with uneven sizes,
    // deleted offsets (absent from the scan), and several coverage sets that
    // include uncovered fragments, an empty coverage and full coverage.
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for round in 0..40 {
        let mut eligible = RowAddrTreeMap::new();
        for fragment in 0..12u64 {
            if next() % 4 == 0 {
                continue; // fragment without eligible rows
            }
            let size = next() % 150_000;
            let density = 1 + next() % 9;
            for offset in 0..size {
                let deleted = next() % 11 == 0;
                if !deleted && next() % 10 < density {
                    eligible.insert((fragment << 32) | offset);
                }
            }
        }
        if round % 2 == 0 {
            eligible.optimize();
        }
        let covered: BTreeSet<u32> = match round % 5 {
            0 => BTreeSet::new(),
            1 => (0..12).collect(),
            _ => (0..12).filter(|_| next() % 2 == 0).collect(),
        };
        let mask = RowAddrMask::from_allowed(eligible.clone());
        let tail = PreparedReader::unindexed_rows(&eligible, |f| covered.contains(&f)).unwrap();
        let expected = unindexed_by_row(&mask, &covered);
        let actual: BTreeSet<u64> = tail
            .row_addrs()
            .into_iter()
            .flatten()
            .map(u64::from)
            .collect();
        assert_eq!(actual, expected, "round {round}");
        // The same order as the old BTreeSet iteration.
        assert!(
            tail.row_addrs()
                .into_iter()
                .flatten()
                .map(u64::from)
                .eq(expected.iter().copied())
        );
    }
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

#[test]
fn filtered_index_search_needs_many_expected_hits_per_candidate() {
    use super::dense::filtered_index_pays;
    // Every partition is probed: the rows seen are the selected rows, and
    // the bar is 64 rescore pools of them.
    let all = || [16usize].into_iter();
    assert!(!filtered_index_pays(64 * 40 - 1, all(), 16, 40));
    assert!(filtered_index_pays(64 * 40, all(), 16, 40));
    assert!(filtered_index_pays(64 * 40, all(), 1 << 16, 40));
    // Probing a quarter of the partitions sees a quarter of the selection.
    assert!(!filtered_index_pays(64 * 40 * 4 - 1, all(), 4, 40));
    assert!(filtered_index_pays(64 * 40 * 4, all(), 4, 40));
    // A deeper rescore pool raises the bar in proportion.
    assert!(!filtered_index_pays(64 * 40, all(), 16, 300));
    assert!(filtered_index_pays(64 * 300, all(), 16, 300));
    // Segments add up: 4 of 8 partitions probed in total.
    let two = || [4usize, 4].into_iter();
    assert!(!filtered_index_pays(64 * 40 * 2 - 1, two(), 2, 40));
    assert!(filtered_index_pays(64 * 40 * 2, two(), 2, 40));
    // No admitted partition or no selection never takes the index path.
    assert!(!filtered_index_pays(u64::MAX, [0usize].into_iter(), 4, 40));
    assert!(!filtered_index_pays(0, all(), 16, 40));
    // The product stays exact beyond 64 bits.
    assert!(filtered_index_pays(
        u64::MAX,
        [usize::MAX].into_iter(),
        usize::MAX,
        usize::MAX >> 8
    ));
}
