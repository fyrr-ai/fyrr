use super::*;
use crate::ReaderError;

#[test]
fn process_query_keys_include_the_profile_and_exact_utf8() {
    let mut config = fyrr_core::config::parse(
        "[namespace]\nname='query-key'\n[storage]\nuri='s3://query-key'\n[embedding]\n\
            endpoint='https://fake.invalid'\nquery_cache_max_bytes=0\n",
    )
    .unwrap()
    .config
    .embedding;
    let pool = Arc::new(Semaphore::new(128));
    let cache = QueryEmbeddings::new(&config, pool.clone()).unwrap();
    let baseline = cache.prepare(&["é", "e\u{301}"]).unwrap();
    assert_ne!(baseline.keys[0], baseline.keys[1]);
    assert_ne!(baseline.keys[0], digest("é".as_bytes()));
    config.prefix = "query:".into();
    let prefixed = QueryEmbeddings::new(&config, pool).unwrap();
    let changed = prefixed.prepare(&["é"]).unwrap();
    assert_ne!(baseline.keys[0], changed.keys[0]);
}

#[test]
fn cache_only_query_copies_use_existing_generation_scratch() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='cache-only-budget'\n[storage]\nuri='s3://cache-only-budget'\n\
            [embedding]\nendpoint='https://fake.invalid'\nquery_cache_max_bytes=0\n",
    )
    .unwrap()
    .config
    .embedding;
    let pool = Arc::new(Semaphore::new(0));
    let cache = QueryEmbeddings::new(&config, pool.clone()).unwrap();
    let work = cache.prepare(&["cache-only query"]).unwrap();
    assert!(work._copies.is_none());
    assert_eq!(pool.available_permits(), 0);
    assert_eq!(work.vectors, [Err(ReaderError::EmbeddingUnavailable)]);
}

#[test]
fn query_cache_charges_memory_and_capacity_and_evicts_without_leaks() {
    let mut config = fyrr_core::config::parse(
        "[namespace]\nname='query-cache'\n[storage]\nuri='s3://query-cache'\n[embedding]\n\
            endpoint='https://fake.invalid'\n",
    )
    .unwrap()
    .config
    .embedding;
    config.dimensions = 512;
    config.query_cache_max_bytes = 8192;
    config.reader_live = true;
    config.spend = Some(fyrr_core::config::SpendConfig {
        limit_microusd: 1000,
        max_call_microusd: 10,
    });
    let pool = Arc::new(Semaphore::new(128));
    let cache = QueryEmbeddings::new(&config, pool.clone()).unwrap();
    let vector = vec![0.125; 512];
    cache.remember("one", &vector);
    cache.remember("two", &vector);
    cache.remember("three", &vector);
    assert_eq!(pool.available_permits(), 126);
    assert_eq!(cache.capacity.available_permits(), 0);
    {
        let state = cache.cache.lock().unwrap();
        assert_eq!(state.entries.len(), 2);
        assert!(!state.entries.contains_key("one"));
    }
    drop(cache);
    assert_eq!(pool.available_permits(), 128);
    config.query_cache_max_bytes = 0;
    let cache = QueryEmbeddings::new(&config, pool.clone()).unwrap();
    cache.remember("disabled", &vector);
    assert_eq!(pool.available_permits(), 128);
    let work = cache.prepare(&["one", "two"]).unwrap();
    assert_eq!(
        pool.available_permits(),
        123,
        "request copies are charged separately"
    );
    drop(work);
    assert_eq!(pool.available_permits(), 128);
    let exhausted = QueryEmbeddings::new(&config, Arc::new(Semaphore::new(0))).unwrap();
    assert!(matches!(
        exhausted.prepare(&["busy"]),
        Err(ReaderError::Busy)
    ));
}
