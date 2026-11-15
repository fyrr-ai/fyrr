//! File identity, never fragment ID, is evidence of unchanged immutable rows.
use super::*;
type Key = (Vec<String>, Option<(u64, u64)>);

fn keys(dataset: &Dataset) -> BTreeMap<u64, Key> {
    dataset
        .manifest()
        .fragments
        .iter()
        .map(|fragment| {
            let mut files: Vec<_> = fragment.files.iter().map(|f| f.path.clone()).collect();
            files.sort();
            (
                fragment.id,
                (
                    files,
                    fragment
                        .deletion_file
                        .as_ref()
                        .map(|f| (f.id, f.read_version)),
                ),
            )
        })
        .collect()
}

pub(super) struct Difference {
    pub current: BTreeSet<u64>,
    pub historical: BTreeSet<u64>,
    shared: BTreeSet<Key>,
    pub fallback: Option<&'static str>,
}
impl Difference {
    pub(super) fn new(current: &Dataset, historical: &Dataset) -> Self {
        let old = keys(historical);
        let new = keys(current);
        let old_keys: BTreeSet<_> = old.values().cloned().collect();
        let new_keys: BTreeSet<_> = new.values().cloned().collect();
        let shared: BTreeSet<_> = old_keys.intersection(&new_keys).cloned().collect();
        let fallback = if [current, historical].iter().any(|d| {
            d.manifest()
                .fragments
                .iter()
                .any(|f| !f.overlays.is_empty())
        }) {
            Some("native_data_overlays")
        } else if old_keys == new_keys && current.version().version != historical.version().version
        {
            // Index-only commits do not rewrite whole roots. Keep the full path
            // for this precise exception to the mutation premise.
            Some("metadata_only_version_change")
        } else {
            None
        };
        Self {
            current: new
                .into_iter()
                .filter(|(_, key)| fallback.is_some() || !shared.contains(key))
                .map(|(id, _)| id)
                .collect(),
            historical: old
                .into_iter()
                .filter(|(_, key)| fallback.is_some() || !shared.contains(key))
                .map(|(id, _)| id)
                .collect(),
            shared,
            fallback,
        }
    }
    pub(super) fn rows(&self, current: &Dataset, historical: &Dataset) -> Result<u64> {
        [(current, &self.current), (historical, &self.historical)]
            .into_iter()
            .flat_map(|(dataset, selected)| {
                dataset
                    .manifest()
                    .fragments
                    .iter()
                    .filter(move |f| selected.contains(&f.id))
            })
            .try_fold(0u64, |total, fragment| {
                total
                    .checked_add(
                        fragment
                            .physical_rows
                            .ok_or(WriterError::RestoreWorkingSpaceExhausted)?
                            as u64,
                    )
                    .ok_or(WriterError::RestoreWorkingSpaceExhausted)
            })
    }
    pub(super) fn restored(&mut self, current: &Dataset, target: &Dataset, restored: &Dataset) {
        if keys(target).values().collect::<BTreeSet<_>>()
            != keys(restored).values().collect::<BTreeSet<_>>()
        {
            self.fallback = Some("native_restore_changed_fragment_metadata");
            self.current = keys(current).into_keys().collect();
            self.historical = keys(target).into_keys().collect();
        }
    }
    pub(super) fn verification(&self, dataset: &Dataset) -> BTreeSet<u64> {
        keys(dataset)
            .into_iter()
            .filter(|(_, key)| self.fallback.is_some() || !self.shared.contains(key))
            .map(|(id, _)| id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reused_ids_cannot_prove_file_identity_and_restore_mismatch_falls_back() {
        let batch =
            apply::marker_row(1, "source", "root", &apply::DocumentState::default()).unwrap();
        let dataset = Dataset::write(
            arrow_array::RecordBatchIterator::new([Ok(batch.clone())], batch.schema()),
            &format!(
                "memory://fragment-identity-{}",
                Utc::now().timestamp_nanos_opt().unwrap()
            ),
            None,
        )
        .await
        .unwrap();
        let mut changed = dataset.clone();
        let manifest = std::sync::Arc::make_mut(&mut changed.manifest);
        let fragments = std::sync::Arc::make_mut(&mut manifest.fragments);
        fragments[0].files[0].path = "different-uuid.lance".into();
        let id = fragments[0].id;
        let mut difference = Difference::new(&changed, &dataset);
        assert_eq!(difference.current, BTreeSet::from([id]));
        assert_eq!(difference.historical, difference.current);
        // A runtime Restore that unexpectedly changes target files must select
        // the original authority and the complete immutable target instead.
        difference.restored(&changed, &dataset, &changed);
        assert_eq!(
            difference.fallback,
            Some("native_restore_changed_fragment_metadata")
        );
        assert_eq!(difference.verification(&dataset), difference.historical);
        let mut renumbered = dataset.clone();
        std::sync::Arc::make_mut(
            &mut std::sync::Arc::make_mut(&mut renumbered.manifest).fragments,
        )[0]
        .id += 100;
        let difference = Difference::new(&renumbered, &dataset);
        assert!(difference.current.is_empty() && difference.historical.is_empty());
    }
}
