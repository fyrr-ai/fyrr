use super::namespace::{new_head, remove};
use super::publication::write_manifest;
use super::*;
use fyrr_core::connector::CursorState;

/// What a copy writes before its head exists. Whatever else lies under the
/// destination's prefix, a deletion's fence included, is not its debris.
const DEBRIS: [&str; 7] = [
    "dataset.lance/",
    "identity/",
    "policy/",
    "publications/",
    "evidence/",
    "cursors/",
    "document-metadata/",
];

/// Copies the namespace `from` into the storage of `to`, under the same name:
/// the hashes in its rows are keyed by the name, so no other name would find
/// them.
///
/// The copy is the version the source's head serves when the command starts,
/// with the policy and identity the head names. Lance copies that version's
/// files. The source is only read. The copy's head is written last, so a copy
/// that stops earlier leaves no namespace, and the next run clears what it left
/// and starts over.
pub async fn copy(from: &Config, to: &Config) -> Result<serde_json::Value> {
    // The location only: the key does not make another storage.
    if (&from.storage.uri, &from.storage.endpoint) == (&to.storage.uri, &to.storage.endpoint) {
        return Err(WriterError::CopySameStorage);
    }
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
    // What the head serves, not what the manifest was published with: a source
    // retired or a directory published since must stay in force. The copy is
    // writable whether or not the source is frozen.
    let policy: PolicyArtifact =
        fyrr_reader::storage::artifact(source_store.as_ref(), &source_root, &head.policy).await?;
    let identity: IdentityArtifact =
        fyrr_reader::storage::artifact(source_store.as_ref(), &source_root, &head.identity).await?;
    // The snapshots an identity is built from are sealed for the namespace's
    // storage and path, so the next generation of a copy could not read them.
    if !identity.inputs.is_empty() {
        return Err(WriterError::CopyInputsUnsupported);
    }

    let source = from
        .with_settings(&policy.settings)
        .ok_or(WriterError::ConfigurationInvalid)?;
    let mut destination = source.clone();
    destination.storage = to.storage.clone();
    destination.identity.hashing_key = to.identity.hashing_key.clone();
    // The rows hold keyed hashes: another key could not match any of them.
    if hash_principal(&source, "key", "")? != hash_principal(&destination, "key", "")? {
        return Err(WriterError::HashingKeyMismatch);
    }

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
    // The source may have removed a file of this version while Lance listed
    // the others; a version that is not whole is never published.
    let indexes: BTreeSet<_> = manifest.indexes.iter().map(|i| i.uuid.as_str()).collect();
    let dataset_root = root.clone().join("dataset.lance");
    let whole = |path: &str| {
        path.starts_with("data/")
            || path.starts_with("_deletions/")
            || path
                .strip_prefix("_indices/")
                .and_then(|path| path.split_once('/'))
                .is_some_and(|(uuid, _)| indexes.contains(uuid))
    };
    crate::io::bounded(
        manifest.files.iter().filter(|file| whole(&file.path)),
        |file| {
            let location = Path::from(format!("{dataset_root}/{}", file.path));
            let store = &store;
            async move {
                match store.head(&location).await {
                    Ok(meta) if meta.size == file.size_bytes => Ok(()),
                    _ => Err(WriterError::CopyIncomplete),
                }
            }
        },
    )
    .await?;

    // The connector maps that rows refer to by digest, which a reader loads
    // beside the dataset: all of them, whole.
    let maps: Vec<_> = source_store
        .list(Some(&source_root.clone().join("document-metadata")))
        .try_collect()
        .await?;
    {
        let (source_store, source_root, store, root) = (&source_store, &source_root, &store, &root);
        crate::io::bounded(
            maps.iter()
                .filter(|object| object.location.as_ref().ends_with(".json")),
            |object| async move {
                let bytes = source_store.get(&object.location).await?.bytes().await?;
                let target = Path::from(format!(
                    "{root}/{}",
                    fyrr_reader::storage::relative_path(&object.location, source_root)
                ));
                store
                    .put_opts(&target, bytes.into(), object_store::PutMode::Create.into())
                    .await?;
                match store.head(&target).await {
                    Ok(meta) if meta.size == object.size => Ok(()),
                    _ => Err(WriterError::CopyIncomplete),
                }
            },
        )
        .await?;
    }

    let new_policy = PolicyArtifact {
        generation: 1,
        read_only: false,
        ..policy
    };
    let new_identity = IdentityArtifact {
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
    // The copy continues the source's publication sequence: its rows' state
    // is compared with it.
    let copied = write_manifest(
        &destination,
        &cloned,
        &mut created,
        &lineage,
        manifest.sequence,
        Vec::new(),
        Default::default(),
        false,
        Vec::new(),
        false,
        Some(&manifest.attribute_indexes),
    )
    .await?;

    // The cursors keep their sequences, which the revisions and the envelope
    // IDs of the next writes must exceed, read after the version was pinned so
    // that they are not behind it. Their positions are dropped: a source reads
    // again from the start, never past what the copy does not hold.
    let mut cursors = source_store.list(Some(&source_root.clone().join("cursors")));
    while let Some(object) = cursors.try_next().await? {
        // A cursor that is being written has a temporary name of its own.
        if !object.location.as_ref().ends_with(".json") {
            continue;
        }
        let bytes = source_store.get(&object.location).await?.bytes().await?;
        let mut state: CursorState =
            fyrr_core::parse_contract(&bytes).map_err(|_| WriterError::CursorInvalid)?;
        state.cursor = None;
        // The envelopes are not copied: no chain, and nothing applied, in the copy.
        state.last = None;
        state.lease = None;
        state.updated_at = chrono::DateTime::UNIX_EPOCH;
        let target = Path::from(format!(
            "{root}/{}",
            fyrr_reader::storage::relative_path(&object.location, &source_root)
        ));
        store
            .put_opts(
                &target,
                write_contract(&state).into(),
                object_store::PutMode::Create.into(),
            )
            .await?;
    }
    drop(cursors);

    // A namespace deleted and created again meanwhile is another one.
    if fyrr_reader::storage::optional_head(from)
        .await?
        .is_none_or(|now| now.incarnation != head.incarnation)
    {
        return Err(WriterError::CopySourceChanged);
    }
    put_immutable(store.as_ref(), &root.clone().join("head.json"), &created).await?;
    Ok(serde_json::json!({
        "copied": true,
        "namespace": destination.namespace.name,
        "publication_sequence": copied.sequence,
        "lance_version": copied.lance_version,
        "files": copied.files.len(),
        "bytes": copied.files.iter().map(|file| file.size_bytes).sum::<u64>(),
        "seconds": started.elapsed().as_secs_f64(),
    }))
}
