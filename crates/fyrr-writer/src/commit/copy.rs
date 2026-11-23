use super::namespace::{new_head, remove};
use super::publication::write_manifest;
use super::*;

/// What a copy writes before its head exists. Whatever else lies under the
/// destination's prefix, a deletion's fence included, is not its debris.
const DEBRIS: [&str; 4] = ["dataset.lance/", "identity/", "policy/", "publications/"];

/// Copies the namespace `from` as the new namespace `to`, which may be in
/// another bucket or region.
///
/// The copy is the version the source's head serves when the command starts:
/// its manifest pins the policy and identity, and Lance copies that version's
/// files. The source is only read. The head of the copy is written last, so a
/// copy that stops earlier leaves no namespace, and the next run clears what
/// it left and starts over.
pub async fn copy(from: &Config, to: &Config) -> Result<serde_json::Value> {
    let started = std::time::Instant::now();
    let (source_store, source_root) = namespace_store(from)?;
    let head = fyrr_reader::storage::optional_head(from)
        .await?
        .ok_or(WriterError::NamespaceUnavailable)?;
    let pointer = head
        .content
        .as_ref()
        .ok_or(WriterError::NamespaceNotPublished)?;
    let manifest =
        fyrr_reader::storage::read_manifest(source_store.as_ref(), &source_root, &pointer.manifest)
            .await?;
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(source_store.as_ref(), &source_root, &manifest.policy)
            .await?;
    let identity: IdentityArtifact =
        fyrr_reader::storage::artifact(source_store.as_ref(), &source_root, &manifest.identity)
            .await?;
    // The snapshots an identity is built from are sealed for the namespace's
    // name, so the next generation of a copy could not read them.
    if !identity.inputs.is_empty() {
        return Err(WriterError::CopyInputsUnsupported);
    }

    // The copy hashes under the name the source's hashes were made under,
    // which is only the same hashes with the same key.
    let source = from
        .with_settings(&policy.settings)
        .ok_or(WriterError::ConfigurationInvalid)?;
    let scope = source.namespace.hash_scope().to_owned();
    let mut destination = source.clone();
    destination.storage = to.storage.clone();
    destination.identity.hashing_key = to.identity.hashing_key.clone();
    destination.namespace.name = to.namespace.name.clone();
    destination.namespace.hash_name = Some(scope.clone());
    if hash_principal(&source, "key", "")? != hash_principal(&destination, "key", "")? {
        return Err(WriterError::HashingKeyMismatch);
    }
    destination.namespace.hash_name = (scope != destination.namespace.name).then_some(scope);

    let (store, root) = namespace_store(&destination)?;
    if fyrr_reader::storage::optional_head(&destination)
        .await?
        .is_some()
    {
        return Err(WriterError::NamespaceExists);
    }
    let mut stale = Vec::new();
    let mut present = store.list(Some(&root));
    while let Some(object) = present.try_next().await? {
        let relative = fyrr_reader::storage::relative_path(&object.location, &root);
        if !DEBRIS.iter().any(|dir| relative.starts_with(dir)) {
            return Err(WriterError::NamespaceDeleting);
        }
        stale.push(object.location);
    }
    drop(present);
    crate::io::bounded(&stale, |path| remove(store.as_ref(), path)).await?;

    let mut pinned = fyrr_reader::storage::pinned(&source, &head).await?;
    let cloned = pinned
        .deep_clone(
            &dataset_uri(&destination),
            manifest.lance_version,
            Some(store_params(&destination)?),
        )
        .await
        .map_err(|_| WriterError::StorageUnavailable)?;

    let mut settings = policy.settings.clone();
    settings
        .hash_name
        .clone_from(&destination.namespace.hash_name);
    let mut new_policy = PolicyArtifact {
        // The ACL change of the source's history is not the copy's.
        row_acl_sequence: None,
        read_only: false,
        namespace: destination.namespace.name.clone(),
        generation: 1,
        settings,
        ..policy
    };
    new_policy.schema_version = new_policy.current_version();
    let new_identity = IdentityArtifact {
        namespace: destination.namespace.name.clone(),
        generation: 1,
        ..identity
    };
    let mut created = new_head(
        &destination,
        store.as_ref(),
        &root,
        &new_identity,
        &new_policy,
    )
    .await?;
    let lineage = CommitLineage {
        holder: format!("copy-{}", created.incarnation),
        epoch: 1,
        base_version: cloned.version().version,
        recovery_version: None,
        transactions: Vec::new(),
    };
    let copied = write_manifest(
        &destination,
        &cloned,
        &mut created,
        &lineage,
        Vec::new(),
        false,
        Vec::new(),
        false,
        Some(&manifest.attribute_indexes),
    )
    .await?;
    put_immutable(store.as_ref(), &root.clone().join("head.json"), &created).await?;
    Ok(serde_json::json!({
        "copied": true,
        "namespace": destination.namespace.name,
        "lance_version": copied.lance_version,
        "files": copied.files.len(),
        "bytes": copied.files.iter().map(|file| file.size_bytes).sum::<u64>(),
        "seconds": started.elapsed().as_secs_f64(),
    }))
}
