use super::*;
use fyrr_core::http_api::{NamespaceReport, Unindexed};

impl PreparedReader {
    /// What the reader holds of the namespace it serves: the published
    /// manifest and the settings of its policy, with no storage read of its
    /// own. Only `read_only` is the current head's: a switch is published
    /// without a new version, so it is read from the head the authorization
    /// checks share (a read when that is old) and a policy of a later
    /// generation, which is kept for the next report. The numbers are not
    /// filtered by row permissions.
    pub(crate) async fn report(&self) -> Result<serde_json::Value> {
        let current: ServingHead = self
            .head_cache
            .read(&self.control_store, &self.control_root)
            .await
            .map_err(|_| StorageUnavailable)?;
        let read_only = if current.policy == self.head.policy {
            self.policy.read_only
        } else {
            let mut cached = self.policy_cache.lock().await;
            if cached
                .as_ref()
                .is_none_or(|(pointer, _)| pointer != &current.policy)
            {
                let policy = artifact(
                    self.control_store.as_ref(),
                    &self.control_root,
                    &current.policy,
                )
                .await
                .map_err(|_| StorageUnavailable)?;
                *cached = Some((current.policy.clone(), Arc::new(policy)));
            }
            cached.as_ref().is_some_and(|(_, policy)| policy.read_only)
        };
        let manifest = &self.manifest;
        let stats = &manifest.stats;
        // The policy may be newer than the manifest, when an authorization
        // refresh took a later generation before the reader moved to the later
        // publication. The schema is one generation: the manifest's.
        let mut settings = self.policy.settings.clone();
        settings
            .namespace
            .attributes
            .clone_from(&manifest.settings.namespace.attributes);
        settings
            .namespace
            .attribute_indexes
            .retain(|name, _| manifest.settings.namespace.attributes.contains_key(name));
        serde_json::to_value(NamespaceReport {
            namespace: manifest.namespace.clone(),
            publication_id: manifest.publication_id.clone(),
            schema: manifest.settings.namespace.attributes.clone(),
            settings,
            dense_metric: manifest.settings.namespace.dense_metric,
            dense_element: manifest.settings.namespace.dense_element,
            read_only,
            rows: stats.rows,
            logical_bytes: stats.logical_bytes,
            first_published_at: stats.first_published_at,
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
