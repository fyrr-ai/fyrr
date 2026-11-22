use super::*;
use serde_json::json;

impl PreparedReader {
    /// What the reader holds of the namespace it serves, with no storage
    /// read: the published manifest, the settings of its policy and the Lance
    /// index metadata. The numbers are not filtered by row permissions.
    pub(crate) fn report(&self) -> Result<serde_json::Value> {
        let manifest = &self.manifest;
        let sizes = crate::storage::file_sizes(&manifest.files);
        // A fragment is covered when every index of the version covers it.
        let covered: Vec<_> = manifest
            .indexes
            .iter()
            .map(|index| index.name.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|name| crate::storage::coverage(&self.indexes, name))
            .collect();
        let (mut rows, mut bytes) = (0, 0);
        for fragment in self.dataset.manifest().fragments.iter() {
            if !covered
                .iter()
                .all(|covered| covered.contains(&coverage_fragment(fragment.id)))
            {
                let (fragment_rows, fragment_bytes) =
                    crate::storage::fragment_extent(fragment, &sizes);
                rows += fragment_rows;
                bytes += fragment_bytes;
            }
        }
        let stats = manifest.stats.as_ref();
        Ok(json!({
            "namespace": manifest.namespace,
            "publication_id": manifest.publication_id,
            "schema": manifest.attributes,
            "settings": serde_json::to_value(&self.policy.settings)
                .map_err(|_| StorageUnavailable)?,
            "rows": stats.map(|stats| stats.rows),
            "logical_bytes": stats.map(|stats| stats.logical_bytes),
            "created_at": stats.map(|stats| stats.created_at),
            "last_write_at": manifest.committed_at,
            "unindexed": {"rows": rows, "logical_bytes": bytes},
        }))
    }
}
