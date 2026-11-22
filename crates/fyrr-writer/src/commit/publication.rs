use super::*;

/// Publish native work through the same immutable manifest and fenced head CAS.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish(
    config: &Config,
    lease: &LeaseGuard,
    dataset: &Dataset,
    mut head: ServingHead,
    lineage: CommitLineage,
    receipts: Vec<String>,
    acl_changed: bool,
    mut cursor_rewinds: Vec<String>,
    maintenance_completed: bool,
) -> Result<u64> {
    let (store, root) = namespace_store(config)?;
    let previous_manifest = previous_manifest(store.as_ref(), &root, &head).await?;
    if cursor_rewinds.is_empty() {
        cursor_rewinds = previous_manifest
            .as_ref()
            .map(|m| m.cursor_rewinds.clone())
            .unwrap_or_default();
    }
    let actual = dataset.load_indices().await?;
    let mut indexes = Vec::new();
    for index in actual.iter() {
        let Some(&(column, _, _, kind)) =
            INDEXES.iter().find(|(_, name, _, _)| *name == index.name)
        else {
            continue;
        };
        indexes.push(IndexIdentity {
            name: index.name.clone(),
            column: column.into(),
            kind: kind.into(),
            uuid: index.uuid.to_string(),
        });
    }
    let reader_coverage = fyrr_reader::storage::coverage(&actual, "readers_idx");
    if dataset
        .manifest()
        .fragments
        .iter()
        .any(|f| !reader_coverage.contains(&(crate::narrow_u32(f.id))))
    {
        return Err(WriterError::ReadersIndexMissing);
    }
    let files = file_inventory(
        store.as_ref(),
        &root,
        dataset,
        previous_manifest.as_ref(),
        &indexes,
    )
    .await?;
    let durable: BTreeSet<_> = previous_manifest
        .iter()
        .flat_map(|manifest| manifest.files.iter())
        .map(|file| file.path.as_str())
        .collect();
    fyrr_reader::storage::persist_native_files(
        config,
        files
            .iter()
            .filter(|file| !durable.contains(file.path.as_str()))
            .cloned()
            .collect(),
    )
    .await?;
    let sequence = head.content.as_ref().map_or(Ok(1), |p| {
        p.sequence
            .checked_add(1)
            .ok_or(WriterError::PublicationSequenceOverflow)
    })?;
    let publication_id = format!(
        "pub-{}",
        &digest(
            serde_json::json!([
                config.namespace.name,
                sequence,
                dataset.version().version,
                receipts,
                lineage
            ])
            .to_string()
            .as_bytes()
        )[7..39]
    );
    if acl_changed {
        let mut policy: PolicyArtifact =
            fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
        policy.schema_version = <PolicyArtifact as fyrr_core::Contract>::VERSION;
        policy.generation = head
            .policy
            .generation
            .checked_add(1)
            .ok_or(WriterError::PolicyGenerationOverflow)?;
        policy.row_acl_sequence = Some(sequence);
        head.policy = crate::prepare::put_generation(
            store.as_ref(),
            &root,
            "policy",
            policy.generation,
            &policy,
        )
        .await?;
    }
    let evidence = envelope_evidence(
        config,
        store.as_ref(),
        &root,
        &head,
        previous_manifest.as_ref(),
        &receipts,
        sequence,
    )
    .await?;
    let manifest = ContentManifest {
        schema_version: PUBLICATION_CONTRACT_VERSION,
        namespace: config.namespace.name.clone(),
        publication_id: publication_id.clone(),
        sequence,
        previous_publication_id: head.content.as_ref().map(|p| p.publication_id.clone()),
        lance_version: dataset.version().version,
        committed_at: fyrr_core::clock::now(),
        maintenance_completed_at: if maintenance_completed {
            Some(fyrr_core::clock::now())
        } else {
            previous_manifest
                .as_ref()
                .and_then(|m| m.maintenance_completed_at)
        },
        embedding: identity(&config.embedding),
        chunk_profile: chunk_identity(),
        fts: config.namespace.fts_settings(),
        dense_rq_bits: config.namespace.dense_rq_bits,
        dense_partition_rows: config.namespace.dense_partition_rows,
        retrieval_policy_digest: config.namespace.retrieval_digest(),
        indexes,
        files,
        envelope_ids: receipts,
        lineage: lineage.clone(),
        identity: head.identity.clone(),
        policy: head.policy.clone(),
        rejected_envelope_ids: head.rejected_envelope_ids.clone(),
        cursor_rewinds,
        incarnation: head.incarnation.clone(),
        envelope_evidence: Some(evidence),
    };
    let manifest_path = format!("publications/{publication_id}.json");
    put_immutable(
        store.as_ref(),
        &Path::from(format!("{root}/{manifest_path}")),
        &manifest,
    )
    .await?;
    head.rejected_envelope_ids.clear();
    head.content = Some(ContentPointer {
        publication_id,
        sequence,
        lance_version: manifest.lance_version,
        manifest: manifest_path,
        committed_at: manifest.committed_at,
    });
    head.updated_at = fyrr_core::clock::now();
    verify_lineage(dataset, &lineage).await?;
    #[cfg(test)]
    crate::maintenance::restore_test_publication();
    lease.publish(head).await?;
    Ok(manifest.lance_version)
}

pub(super) async fn previous_manifest(
    store: &dyn ObjectStore,
    root: &Path,
    head: &ServingHead,
) -> Result<Option<ContentManifest>> {
    let previous_manifest = if let Some(pointer) = &head.content {
        Some(
            fyrr_reader::storage::read_contract::<ContentManifest>(
                store,
                &Path::from(format!("{root}/{}", pointer.manifest)),
            )
            .await?,
        )
    } else {
        None
    };
    if previous_manifest
        .as_ref()
        .is_some_and(|manifest| manifest.chunk_profile != chunk_identity())
    {
        return Err(WriterError::ChunkProfileMismatch);
    }
    Ok(previous_manifest)
}

async fn file_inventory(
    store: &dyn ObjectStore,
    root: &Path,
    dataset: &Dataset,
    previous_manifest: Option<&ContentManifest>,
    indexes: &[IndexIdentity],
) -> Result<Vec<FileRef>> {
    // Carry immutable file sizes forward. Incremental publications inspect only
    // new data/deletion files and new native index UUIDs, not the whole S3 corpus.
    let dataset_root = root.clone().join("dataset.lance");
    let mut inventory: std::collections::BTreeMap<String, u64> = previous_manifest
        .map(|m| {
            m.files
                .iter()
                .map(|f| (f.path.clone(), f.size_bytes))
                .collect()
        })
        .unwrap_or_default();
    if previous_manifest.is_none() {
        let objects: Vec<_> = store
            .list(Some(&dataset_root))
            .try_collect()
            .await
            .map_err(|_| WriterError::FileInventoryUnavailable)?;
        for object in objects {
            if object.location.prefix_match(&dataset_root).is_some() {
                inventory.insert(
                    fyrr_reader::storage::relative_path(&object.location, &dataset_root),
                    object.size,
                );
            }
        }
    } else {
        let mut paths = fyrr_reader::storage::data_paths(dataset);
        let manifest_path = &dataset.manifest_location().path;
        let relative = fyrr_reader::storage::relative_path(manifest_path, &dataset_root);
        paths.insert(relative);
        if let Some(transaction) = &dataset.manifest().transaction_file {
            paths.insert(format!("_transactions/{transaction}"));
        }
        let new_paths = paths
            .into_iter()
            .filter(|path| !inventory.contains_key(path))
            .collect::<Vec<_>>();
        let native_root = &dataset_root;
        for (path, size) in crate::io::bounded(new_paths, |path| async move {
            let metadata = store
                .head(&Path::from(format!("{native_root}/{path}")))
                .await
                .map_err(|_| WriterError::FileInventoryUnavailable)?;
            Ok((path, metadata.size))
        })
        .await?
        {
            inventory.insert(path, size);
        }
        let old_uuids: BTreeSet<_> = previous_manifest
            .iter()
            .flat_map(|m| &m.indexes)
            .map(|i| i.uuid.as_str())
            .collect();
        for index in indexes {
            if !old_uuids.contains(index.uuid.as_str()) {
                let prefix = dataset_root
                    .clone()
                    .join("_indices")
                    .join(index.uuid.as_str());
                let objects: Vec<_> = store
                    .list(Some(&prefix))
                    .try_collect()
                    .await
                    .map_err(|_| WriterError::FileInventoryUnavailable)?;
                for object in objects {
                    if object.location.prefix_match(&dataset_root).is_some() {
                        inventory.insert(
                            fyrr_reader::storage::relative_path(&object.location, &dataset_root),
                            object.size,
                        );
                    }
                }
            }
        }
    }
    let referenced = fyrr_reader::storage::data_paths(dataset);
    let manifest_path = dataset.manifest_location().path.to_string();
    let index_uuids: HashSet<_> = indexes.iter().map(|index| index.uuid.as_str()).collect();
    inventory.retain(|path, _| {
        referenced.contains(path)
            || manifest_path.ends_with(&format!("/{path}"))
            || dataset
                .manifest()
                .transaction_file
                .as_ref()
                .is_some_and(|tx| path == &format!("_transactions/{tx}"))
            || path
                .strip_prefix("_indices/")
                .and_then(|path| path.split_once('/'))
                .is_some_and(|(uuid, _)| index_uuids.contains(uuid))
    });
    let files = inventory
        .into_iter()
        .map(|(path, size_bytes)| FileRef { path, size_bytes })
        .collect();
    Ok(files)
}

/// A checkpoint is rewritten when this many ids have gathered in the manifests.
pub(super) const CHECKPOINT_IDS: usize = 256;

/// What the chain ending at this publication records: the checkpoint of its
/// older part and the few ids since. Receipts live as long as the namespace, so
/// the checkpoint grows with it; the manifests, which readers fetch, stay small.
// Each rewrite leaves the older checkpoints to `writer maintain`, which deletes
// those no retained manifest names.
pub(super) async fn envelope_evidence(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &ServingHead,
    previous: Option<&ContentManifest>,
    receipts: &[String],
    sequence: u64,
) -> Result<EnvelopeEvidence> {
    if let Some(old) = previous.and_then(|manifest| manifest.envelope_evidence.as_ref()) {
        let applied_since: BTreeSet<_> =
            old.applied_since.iter().chain(receipts).cloned().collect();
        let rejected_since: BTreeSet<_> = old
            .rejected_since
            .iter()
            .chain(&head.rejected_envelope_ids)
            .cloned()
            .collect();
        if applied_since.len() + rejected_since.len() < CHECKPOINT_IDS {
            return Ok(EnvelopeEvidence {
                checkpoint: old.checkpoint.clone(),
                applied_since: applied_since.into_iter().collect(),
                rejected_since: rejected_since.into_iter().collect(),
            });
        }
    }
    // The chain up to the previous publication, whose manifest may already
    // carry evidence, or may be the first that does.
    let chain = published_evidence(config, head).await?;
    let checkpoint = EnvelopeCheckpoint {
        schema_version: <EnvelopeCheckpoint as fyrr_core::Contract>::VERSION,
        namespace: config.namespace.name.clone(),
        applied_envelope_ids: chain
            .applied
            .into_iter()
            .chain(receipts.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        rejected_envelope_ids: chain.rejected.into_iter().collect(),
    };
    Ok(EnvelopeEvidence {
        checkpoint: crate::prepare::put_generation(store, root, "evidence", sequence, &checkpoint)
            .await?,
        applied_since: Vec::new(),
        rejected_since: Vec::new(),
    })
}
