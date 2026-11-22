//! A schema change on a namespace that exists: attributes are added and the
//! indexes of attributes are switched on or off. It is one publication, so a
//! reader keeps the version it serves until the head holds the new schema.

use super::*;
use fyrr_core::attributes::possible_indexes;

/// Whether `next` is `current` with attributes added and attribute indexes
/// set: nothing else of the settings changes, and an attribute is neither
/// removed nor given another type, which would change what its stored values mean.
pub(crate) fn check_schema_change(
    current: &NamespaceSettings,
    next: &NamespaceSettings,
) -> Result<()> {
    let mut rest = next.clone();
    rest.attributes.clone_from(&current.attributes);
    rest.attribute_indexes
        .clone_from(&current.attribute_indexes);
    if rest != *current {
        return Err(WriterError::NamespaceSettingsFixed);
    }
    for (name, kind) in &current.attributes {
        match next.attributes.get(name) {
            None => return Err(WriterError::AttributeRemoval),
            Some(other) if other != kind => return Err(WriterError::AttributeTypeChange),
            Some(_) => {}
        }
    }
    Ok(())
}

/// Publishes the namespace with the settings `next` under the lease, judged
/// against the settings it has then. A new attribute is a column that reads null
/// for the rows already stored. An index switched off is dropped with the
/// publication; one switched on is built by the next maintenance, and a filter
/// on its attribute is a scan until then.
pub(crate) async fn change_schema(config: &Config, next: NamespaceSettings) -> Result<()> {
    LeaseGuard::with(config, |lease| async move {
        let (store, root) = namespace_store(config)?;
        let mut head = lease.head().await?;
        let current: PolicyArtifact =
            fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
        check_schema_change(&current.settings, &next)?;
        if current.settings == next {
            return Ok(());
        }
        let config = &config
            .with_settings(&next)
            .ok_or(WriterError::ConfigurationInvalid)?;
        let mut policy = current;
        policy.settings = next;
        policy.schema_version = policy.current_version();
        policy.generation = head
            .policy
            .generation
            .checked_add(1)
            .ok_or(WriterError::PolicyGenerationOverflow)?;
        head.policy = crate::prepare::put_generation(
            store.as_ref(),
            &root,
            "policy",
            policy.generation,
            &policy,
        )
        .await?;
        if head.content.is_none() {
            // The first publication creates the columns and the indexes.
            head.updated_at = fyrr_core::clock::now();
            return lease.publish(head).await;
        }
        let mut work = native::ContentWork::open(config, &lease, &head, false).await?;
        let dataset = work
            .dataset
            .as_mut()
            .ok_or(WriterError::PublicationInvalid)?;
        Box::pin(add_attribute_columns(
            config,
            &lease,
            dataset,
            &mut work.lineage,
        ))
        .await?;
        drop_attribute_indexes(config, &lease, dataset, &mut work.lineage).await?;
        publish(
            config,
            &lease,
            dataset,
            head,
            work.lineage,
            Vec::new(),
            false,
            Vec::new(),
            false,
        )
        .await?;
        Ok(())
    })
    .await
}

/// Drops the indexes of the attributes that the settings no longer ask for.
async fn drop_attribute_indexes(
    config: &Config,
    lease: &LeaseGuard,
    dataset: &mut Dataset,
    lineage: &mut CommitLineage,
) -> Result<()> {
    let wanted: BTreeSet<_> = fyrr_reader::search::attribute_indexes(
        &config.namespace.attributes,
        &config.namespace.attribute_indexes,
    )
    .map(|(_, name, _)| name)
    .collect();
    let built: BTreeSet<_> = dataset
        .load_indices()
        .await?
        .iter()
        .map(|index| index.name.clone())
        .collect();
    for (_, _, _, name) in possible_indexes(&config.namespace.attributes) {
        if built.contains(&name) && !wanted.contains(&name) {
            let before = dataset.version().version;
            native_step(
                lease,
                dataset.drop_index(&name),
                WriterError::IndexBuildFailed,
            )
            .await?;
            record_transaction(dataset, lineage, before).await?;
        }
    }
    Ok(())
}
