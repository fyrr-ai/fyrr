//! An observed serving state. Rendering never takes another resource sample.
use crate::generation::GenerationState;
use crate::mirror::IoSnapshot;
use serde_json::{Value, json};

#[derive(Debug, Clone)]
pub(crate) struct Readiness {
    pub(crate) ready: bool,
    pub(crate) state: GenerationState,
    pub(crate) publication_id: String,
    pub(crate) lance_version: u64,
    pub(crate) io: IoSnapshot,
    pub(crate) preparation_reserved_bytes: u64,
    pub(crate) minimum_memory_bytes: u64,
    pub(crate) native_cache_needed_bytes: u64,
    pub(crate) native_cache_available_bytes: u64,
    pub(crate) cache_degraded: bool,
    pub(crate) rss_bytes: Option<u64>,
}
impl Readiness {
    pub(crate) fn into_wire(self) -> Value {
        // Move the owned publication ID into the response. Keeping the original
        // insertion order also preserves builds that enable serde's ordered maps.
        let mut wire = serde_json::Map::new();
        wire.insert("ready".into(), self.ready.into());
        wire.insert("state".into(), json!(self.state));
        wire.insert(
            "warm_ready".into(),
            (self.state == GenerationState::WarmReady).into(),
        );
        wire.insert("publication_id".into(), self.publication_id.into());
        wire.insert("lance_version".into(), self.lance_version.into());
        wire.insert("s3_requests".into(), self.io.requests.into());
        wire.insert("s3_bytes".into(), self.io.bytes.into());
        wire.insert("mirror_hits".into(), self.io.hits.into());
        wire.insert("s3_scope".into(), "reader-storage".into());
        wire.insert(
            "preparation_reserved_bytes".into(),
            self.preparation_reserved_bytes.into(),
        );
        wire.insert(
            "minimum_memory_bytes".into(),
            self.minimum_memory_bytes.into(),
        );
        wire.insert(
            "native_cache_needed_bytes".into(),
            self.native_cache_needed_bytes.into(),
        );
        wire.insert(
            "native_cache_available_bytes".into(),
            self.native_cache_available_bytes.into(),
        );
        wire.insert(
            "native_cache_degradation".into(),
            json!(self.cache_degraded.then_some("cache_budget")),
        );
        wire.insert("rss_bytes".into(), json!(self.rss_bytes));
        Value::Object(wire)
    }
}

#[cfg(test)]
#[path = "readiness/tests.rs"]
mod tests;
