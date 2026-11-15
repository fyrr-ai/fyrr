use super::*;
#[test]
fn readiness_wire_preserves_states_degradation_and_nulls() {
    for (state, label, warm) in [
        (GenerationState::Cold, "cold", false),
        (GenerationState::Serving, "serving", false),
        (GenerationState::WarmReady, "warm-ready", true),
    ] {
        for cache in [false, true] {
            let mut observed = Readiness {
                ready: true,
                state,
                publication_id: "p".into(),
                lance_version: 7,
                io: IoSnapshot {
                    requests: 3,
                    bytes: 20,
                    hits: 1,
                },
                preparation_reserved_bytes: 32,
                minimum_memory_bytes: 64,
                native_cache_needed_bytes: 128,
                native_cache_available_bytes: 96,
                cache_degraded: cache,
                rss_bytes: None,
            };
            let mut legacy = json!({
                "ready":true, "state":label, "warm_ready":warm,
                "publication_id":"p", "lance_version":7,
                "s3_requests":3, "s3_bytes":20, "mirror_hits":1,
                "s3_scope":"reader-storage", "preparation_reserved_bytes":32,
                "minimum_memory_bytes":64, "native_cache_needed_bytes":128,
                "native_cache_available_bytes":96,
                "native_cache_degradation":cache.then_some("cache_budget"), "rss_bytes":null,
            });
            assert_eq!(
                serde_json::to_vec(&observed.clone().into_wire()).unwrap(),
                serde_json::to_vec(&legacy).unwrap()
            );
            observed.ready = false;
            observed.rss_bytes = Some(512);
            legacy["ready"] = false.into();
            legacy["rss_bytes"] = 512.into();
            assert_eq!(
                serde_json::to_vec(&observed.into_wire()).unwrap(),
                serde_json::to_vec(&legacy).unwrap()
            );
        }
    }
}
