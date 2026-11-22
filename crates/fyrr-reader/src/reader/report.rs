use super::*;
use fyrr_core::http_api::{NamespaceReport, Unindexed};

impl PreparedReader {
    /// What the reader holds of the namespace it serves, with no storage
    /// read: the published manifest and the settings of its policy. The
    /// numbers are not filtered by row permissions.
    pub(crate) fn report(&self) -> Result<serde_json::Value> {
        let manifest = &self.manifest;
        let stats = manifest.stats.as_ref();
        serde_json::to_value(NamespaceReport {
            namespace: manifest.namespace.clone(),
            publication_id: manifest.publication_id.clone(),
            schema: manifest.attributes.clone(),
            settings: self.policy.settings.clone(),
            rows: stats.map(|stats| stats.rows),
            logical_bytes: stats.map(|stats| stats.logical_bytes),
            first_published_at: stats.and_then(|stats| stats.first_published_at),
            // `verify_indices` refuses a version that an index does not cover
            // in full, so a served version has nothing unindexed.
            unindexed: Unindexed {
                rows: 0,
                logical_bytes: 0,
            },
            last_write_at: manifest.committed_at,
        })
        .map_err(|_| StorageUnavailable)
    }
}
