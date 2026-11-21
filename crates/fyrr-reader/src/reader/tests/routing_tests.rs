use super::*;

fn assert_fallback(result: SearchOutput, exact: &[SearchHit], reason: RoutingFallback) {
    assert_eq!(result.hits, exact);
    assert_eq!(
        result.metadata.telemetry.routing_strategy,
        Some(RoutingMethod::Exact)
    );
    assert_eq!(result.metadata.telemetry.routing_fallback, Some(reason));
}

#[tokio::test]
#[ignore = "real publication supplied by the dense_path routing fixture"]
async fn routing_fallbacks_on_storage() {
    let path = std::env::var_os("FYRR_TEST_ROUTING_CONFIG").expect("routing fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    config.reader.routing = RoutingStrategy::Exact;
    let args: SearchInput =
        serde_json::from_value(serde_json::json!({"query":"routing-only query","limit":10}))
            .unwrap();
    let mut reader = PreparedReader::open(&config).await.unwrap();
    let exact = reader.search_input(&args, None, None).await.unwrap().hits;
    // An exact generation owns no router. Exercise the absent lookup on
    // that real pin/admission, without injecting a fake router state.
    reader.config.reader.routing = RoutingStrategy::Rabitq;
    assert!(reader.centroid_routers.get().is_none());
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::Absent,
    );
    // Existing claims stay paid; exhaust only the remaining shared pool.
    let pool = reader.preparation_budget.permits.clone();
    let held = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(pool.available_permits()).unwrap())
        .unwrap();
    reader.prepare_routing().await.unwrap();
    assert!(
        reader
            .centroid_routers
            .get()
            .unwrap()
            .iter()
            .all(|r| matches!(r, Err(RoutingFallback::MemoryBudget)))
    );
    // Routing failure is retained by the OnceCell. Release the artificial
    // pressure before search reserves its own native execution memory.
    drop(held);
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::MemoryBudget,
    );
    drop(reader);

    config.reader.routing = RoutingStrategy::Rabitq;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    assert!(
        reader
            .centroid_routers
            .get()
            .unwrap()
            .iter()
            .all(Result::is_ok)
    );
    // A filter that matches every row is planned by bytes read, like any
    // other filter: here the index reads fewer than the exact scan of all
    // rows, so it routes as the unfiltered request does. Equal masks do not
    // change the admission and do not leave a filter selection behind.
    let all_sources: Vec<_> = config.sources.iter().map(|source| &source.id).collect();
    let filtered: SearchInput = serde_json::from_value(serde_json::json!({
        "query":"routing-only query", "sources":all_sources, "limit":10
    }))
    .unwrap();
    let base_rows = reader.admission.eligible.max_len();
    let unfiltered = reader.search_input(&args, None, None).await.unwrap();
    let selected = reader.search_input(&filtered, None, None).await.unwrap();
    // Both probe every partition of the same index, so they rank alike.
    assert_eq!(selected.hits, unfiltered.hits);
    assert!(searched_index(&unfiltered.metadata.telemetry));
    assert_eq!(
        selected.metadata.telemetry.routing_strategy,
        unfiltered.metadata.telemetry.routing_strategy
    );
    assert_eq!(
        selected.metadata.telemetry.routing_fallback,
        unfiltered.metadata.telemetry.routing_fallback
    );
    assert_eq!(reader.admission.eligible.max_len(), base_rows);
    assert!(reader.filter_selection.is_none());

    reader.config.reader.nprobes = 65536;
    let mut reference = config.clone();
    reference.reader.routing = RoutingStrategy::Exact;
    reference.reader.nprobes = 65536;
    let mut exact_reader = PreparedReader::open(&reference).await.unwrap();
    let exact = exact_reader
        .search_input(&args, None, None)
        .await
        .unwrap()
        .hits;
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::EligibleShortlist,
    );
    // The only injected state is failure at the optional build boundary;
    // roots, index executor, admission and storage are still the real ones.
    reader.centroid_routers = Arc::new(tokio::sync::OnceCell::new());
    reader
        .centroid_routers
        .set(
            reader
                .vector_roots
                .get()
                .unwrap()
                .iter()
                .map(|_| Err(RoutingFallback::BuildFailed))
                .collect(),
        )
        .unwrap();
    assert_fallback(
        reader.search_input(&args, None, None).await.unwrap(),
        &exact,
        RoutingFallback::BuildFailed,
    );
}

/// Dense candidates under a request filter on `sources`, as `search` runs
/// them with `k` candidates and `nprobes`.
async fn filtered_dense(
    reader: &mut PreparedReader,
    sources: &[&str],
    k: usize,
    nprobes: usize,
) -> (Vec<Candidate>, Telemetry) {
    reader.config.reader.nprobes = nprobes;
    reader.config.namespace.retrieval.dense_candidates = k;
    let args: SearchInput = serde_json::from_value(serde_json::json!({
        "query":"routing-only query", "sources":sources, "limit":10
    }))
    .unwrap();
    let (_, _, mut telemetry, _pin) = reader.request_prologue().await.unwrap();
    reader.apply_filters(&args).await.unwrap();
    // The fixture's query vector is its first document's vector.
    let vector: Vec<f32> = (0..1024).map(|d| ((d + 7) as f32).sin()).collect();
    let rows = reader.dense(&vector, k, &mut telemetry).await.unwrap();
    for row in &rows {
        // Nothing outside the authorization and the filter is returned.
        assert!(reader.selected_rows().selected(row.row_id));
        assert!(sources.contains(&row.source.as_str()));
    }
    (rows, telemetry)
}

/// The exact scan reports exact routing without a fallback; a search of the
/// index reports its routing method or the reason it could not use one.
fn searched_index(telemetry: &Telemetry) -> bool {
    telemetry.routing_strategy != Some(RoutingMethod::Exact) || telemetry.routing_fallback.is_some()
}

const ALL: &[&str] = &["allowed", "denied"];

#[tokio::test]
#[ignore = "real publication supplied by the dense_path routing fixture"]
async fn filtered_ann_on_storage() {
    let path = std::env::var_os("FYRR_TEST_ROUTING_CONFIG").expect("routing fixture config");
    let mut config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    // Index searches report a routing method or a fallback reason; the exact
    // scan reports neither, so telemetry tells the paths apart.
    config.reader.routing = RoutingStrategy::Rabitq;
    let mut reader = PreparedReader::open(&config).await.unwrap();
    // Each segment has 8 partitions. A filter takes the index when probing
    // reads fewer bytes than the exact scan of its selection: a 1-bit RaBitQ
    // code (144 B) per probed row plus 4 * dense_candidates stored vectors
    // (2 KiB) against one stored vector per selected row. Every selection
    // below holds at least 400 rows at k <= 10, so each one takes the index;
    // the exact choice is covered by
    // `filtered_index_search_reads_fewer_bytes_than_the_exact_scan`.
    match reader.admission.eligible.max_len() {
        // Every grant: 2000 allowed, 2000 denied, 401 correlated rows.
        Some(4401) => {
            // (a) 2000 selected rows use the index at any nprobes and return
            // only filtered, authorized rows.
            for nprobes in [1, 65536] {
                let (rows, t) = filtered_dense(&mut reader, &["allowed"], 10, nprobes).await;
                assert!(searched_index(&t), "{t:?}");
                assert_eq!(rows.len(), 10);
            }
            // (b) 4000 selected rows likewise.
            let (rows, t) = filtered_dense(&mut reader, ALL, 10, 65536).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 10);
            let (rows, t) = filtered_dense(&mut reader, ALL, 1, 1).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 1);
            // (c) 400 correlated rows sit in one partition. With 6 of 8
            // partitions probed a row comes back. Which one is not asserted:
            // the quantizer's rotation is random per index build and a
            // one-row request rescores only four candidates.
            let (rows, t) = filtered_dense(&mut reader, &["correlated"], 1, 6).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 1);
        }
        // (d) A subject who reads only the allowed source: a filter on both
        // sources returns the intersection.
        Some(2000) => {
            let (rows, t) = filtered_dense(&mut reader, ALL, 10, 65536).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 10);
            assert!(rows.iter().all(|row| row.source == "allowed"));
            let (rows, t) = filtered_dense(&mut reader, ALL, 1, 65536).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 1);
            assert!(rows.iter().all(|row| row.source == "allowed"));
            let (rows, _) = filtered_dense(&mut reader, &["denied"], 1, 65536).await;
            assert!(rows.is_empty());
        }
        // (e) A subject who reads only the correlated source.
        Some(401) => {
            let (rows, _) = filtered_dense(&mut reader, ALL, 10, 65536).await;
            assert!(rows.is_empty());
            // One admitted partition per segment is always probed.
            let (rows, t) = filtered_dense(&mut reader, &["correlated"], 10, 1).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 10);
            let (rows, t) = filtered_dense(&mut reader, &["correlated"], 1, 1).await;
            assert!(searched_index(&t), "{t:?}");
            assert_eq!(rows.len(), 1);
        }
        other => panic!("unexpected fixture grant {other:?}"),
    }
}
