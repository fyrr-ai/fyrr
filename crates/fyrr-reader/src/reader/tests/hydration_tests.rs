use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
#[ignore = "real publication supplied by tools_hydration_projection fixture"]
async fn hydration_projection_reads_only_text_bearing_rows_on_real_s3() {
    let path = std::env::var_os("FYRR_TEST_HYDRATION_CONFIG").expect("hydration fixture config");
    let config = fyrr_core::config::load(std::path::Path::new(&path))
        .unwrap()
        .config;
    let template = PreparedReader::open(&config).await.unwrap();
    let mut reader = template.worker().await.unwrap();
    // Check selective locator coverage against a flat reference.
    for predicate in [
        "root_id='r0'",
        "root_id='r0' OR root_id='denied'",
        "record_id='r0:0'",
        "root_id='r0' AND record_id='r0:0'",
    ] {
        let mut outputs = Vec::new();
        for indexed in [false, true] {
            let mut scan = reader.dataset.scan();
            scan.empty_project().unwrap();
            scan.filter(predicate)
                .unwrap()
                .with_row_id()
                .use_scalar_index(indexed)
                .batch_size(4096)
                .batch_readahead(1);
            let mut stream = scan.try_into_stream().await.unwrap();
            let mut found = BTreeSet::new();
            while let Some(batch) = stream.try_next().await.unwrap() {
                found.extend(ids(&batch).unwrap().values().iter().copied());
            }
            outputs.push(found);
        }
        assert!(outputs[0] == outputs[1], "locator coverage differs");
        assert!(!outputs[0].is_empty(), "locator fixture must match rows");
    }
    // Even an internal caller with a real row ID must not hydrate denied
    // content. Resolve only identity here, then prove refusal before I/O.
    let mut denied = reader.dataset.scan();
    denied.project(&["record_id"]).unwrap();
    denied
        .filter("source_id='allowed' AND root_id='denied'")
        .unwrap()
        .with_row_id();
    let denied = denied.try_into_batch().await.unwrap();
    assert!(denied.num_rows() > 0);
    let before = reader.mirror.counters();
    assert_eq!(
        reader
            .rows_by_id(
                ids(&denied).unwrap().values(),
                &["searchable_text"],
                ReaderError::PublicationInvalid
            )
            .await
            .unwrap_err(),
        ReaderError::AuthorizationUnavailable
    );
    assert_eq!(reader.mirror.counters(), before);
    let full_args: SearchInput = serde_json::from_value(serde_json::json!({
        "query":"fixture", "limit":4, "output":"content", "text_hits":4
    }))
    .unwrap();
    let full = reader.search_input(&full_args, None, None).await.unwrap();
    assert_eq!(
        full.hits.len(),
        4,
        "eligible={:?}, result={full:?}",
        reader.eligible.max_len()
    );
    assert_eq!(
        reader
            .test_hooks
            .hydration_text_rows
            .load(Ordering::Relaxed),
        4
    );
    let mut metadata_args = full_args.clone();
    metadata_args.text_hits = Some(0);
    reader
        .test_hooks
        .hydration_text_rows
        .store(0, Ordering::Relaxed);
    let metadata = reader
        .search_input(&metadata_args, None, None)
        .await
        .unwrap();
    let mut expected = full.hits.clone();
    let mut bytes = full.metadata.omitted_bytes;
    for hit in &mut expected {
        bytes += hit.content.take().unwrap().len() as u64;
    }
    assert_eq!(metadata.hits, expected);
    assert_eq!(metadata.metadata.omitted_bytes, bytes);
    // Private per-record metadata answers a metadata-only search without text.
    assert_eq!(
        reader
            .test_hooks
            .hydration_text_rows
            .load(Ordering::Relaxed),
        0
    );
    metadata_args.text_hits = Some(1);
    reader
        .test_hooks
        .hydration_text_rows
        .store(0, Ordering::Relaxed);
    let partial = reader
        .search_input(&metadata_args, None, None)
        .await
        .unwrap();
    assert_eq!(
        reader
            .test_hooks
            .hydration_text_rows
            .load(Ordering::Relaxed),
        1
    );
    assert!(partial.hits[0].content.is_some());
    assert!(partial.hits[1..].iter().all(|hit| hit.content.is_none()));
    for (full, partial) in full.hits.iter().zip(partial.hits) {
        assert_eq!(full.revision, partial.revision);
    }
}
