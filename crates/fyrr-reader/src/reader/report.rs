use super::*;
use serde_json::json;

impl PreparedReader {
    /// What the reader holds of the namespace it serves, with no storage
    /// read: the published manifest and the settings of its policy. The
    /// numbers are not filtered by row permissions.
    pub(crate) fn report(&self) -> Result<serde_json::Value> {
        let manifest = &self.manifest;
        let stats = manifest.stats.as_ref();
        // The policy may be newer than the manifest, when an authorization
        // refresh took a later generation before the reader moved to the later
        // publication. The schema is one generation: the manifest's.
        let mut settings = self.policy.settings.clone();
        settings.attributes.clone_from(&manifest.attributes);
        settings
            .attribute_indexes
            .retain(|name, _| manifest.attributes.contains_key(name));
        Ok(json!({
            "namespace": manifest.namespace,
            "publication_id": manifest.publication_id,
            "schema": manifest.attributes,
            "settings": serde_json::to_value(&settings).map_err(|_| StorageUnavailable)?,
            "rows": stats.map(|stats| stats.rows),
            "logical_bytes": stats.map(|stats| stats.logical_bytes),
            "first_published_at": stats.and_then(|stats| stats.first_published_at),
            // `verify_indices` refuses a version that an index does not cover
            // in full, so a served version has nothing unindexed.
            "unindexed": {"rows": 0, "logical_bytes": 0},
            "last_write_at": manifest.committed_at,
        }))
    }
}
