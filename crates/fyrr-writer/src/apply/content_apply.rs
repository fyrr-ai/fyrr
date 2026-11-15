use super::*;

/// Stage one bounded envelope against the actual currently published/locally rebased pin.
/// Old fragments are not copied; native delete vectors and new fragments share one Update.
pub async fn content(
    config: &Config,
    dataset: &Dataset,
    envelope: &PreparedEnvelope,
    sequence: u64,
) -> Result<(Vec<String>, Vec<serde_json::Value>, bool)> {
    let EnvelopePayload::Content(payload) = &envelope.payload else {
        return Err(WriterError::EnvelopeInvalid);
    };
    if payload.embedding != fyrr_reader::embedding::identity(&config.embedding)
        || payload.chunk_profile != crate::prepare::chunk_identity()
        || payload.table_schema_version.major != 1
    {
        reject(config, envelope, "envelope_profile_mismatch").await?;
        return Ok((Vec::new(), Vec::new(), false));
    }
    let staged = match verified_staged(config, dataset, envelope).await {
        Ok(rows) => rows,
        Err(reason) => {
            reject(config, envelope, reason.as_str()).await?;
            return Ok((Vec::new(), Vec::new(), false));
        }
    };
    let roots = payload
        .documents
        .iter()
        .map(|d| d.root_id.clone())
        .collect::<Vec<_>>();
    let (previous, mut readers) = content_state(dataset, &envelope.source_id, &roots).await?;
    let fixed_readers = config
        .sources
        .iter()
        .find(|s| s.id == envelope.source_id)
        .filter(|s| s.permissions == fyrr_core::config::PermissionsMode::Fixed)
        .map(|s| permission_readers(config, &s.id, &s.readers, &[]))
        .transpose()?;
    let mut acl_changed = false;
    let mut changed = Vec::new();
    let mut output = Vec::new();
    for document in &payload.documents {
        let old = previous.get(&document.root_id);
        if let Some(old) = old {
            if document.intent == DocumentIntent::Replace
                && old.deleted
                && !old
                    .delete_sequence
                    .is_some_and(|deleted| envelope.sequence > deleted)
                && envelope
                    .base_head
                    .as_ref()
                    .is_none_or(|b| b.content_sequence < old.publication_sequence)
            {
                reject(config, envelope, "revision_conflict").await?;
                continue;
            }
            if document.intent == DocumentIntent::Replace
                && !old.deleted
                && !old.content_purged
                && document.source_revision == old.revision
            {
                continue;
            }
            if let (Some(incoming), Some(applied)) = (document.revision_order, old.revision_order) {
                if incoming < applied || (incoming == applied && !old.content_purged) {
                    continue;
                }
            } else if document.expected_prior_revision
                != if old.deleted || old.content_purged {
                    None
                } else {
                    old.revision.clone()
                }
            {
                reject(config, envelope, "revision_conflict").await?;
                continue;
            }
        } else if document.expected_prior_revision.is_some() {
            reject(config, envelope, "revision_conflict").await?;
            continue;
        }
        changed.push(keys_filter(
            &envelope.source_id,
            std::slice::from_ref(&document.root_id),
        ));
        let mut next = old.cloned().unwrap_or_default();
        next.publication_sequence = sequence;
        next.revision_order = document.revision_order.or(next.revision_order);
        if document.intent == DocumentIntent::Delete {
            acl_changed = true;
            next.deleted = true;
            next.delete_sequence = Some(envelope.sequence);
            next.revision = document.source_revision.clone().or(next.revision);
            output.push(marker_row(
                config.embedding.dimensions,
                &envelope.source_id,
                &document.root_id,
                &next,
            )?);
            continue;
        }
        next.deleted = false;
        next.content_purged = false;
        next.delete_sequence = None;
        next.revision.clone_from(&document.source_revision);
        if old.is_some_and(|s| s.content_purged)
            && fixed_readers.as_ref().is_none_or(|r| r.is_empty())
            && next
                .permission
                .as_ref()
                .is_none_or(|p| p.readers.is_empty())
        {
            next.content_purged = true;
            output.push(marker_row(
                config.embedding.dimensions,
                &envelope.source_id,
                &document.root_id,
                &next,
            )?);
            continue;
        }
        for row in 0..staged.num_rows() {
            if strings(&staged, "root_id")?.value(row) != document.root_id {
                continue;
            }
            let (batch, narrowed) = stage_row(
                &staged,
                fixed_readers.as_deref(),
                document,
                &mut next,
                row,
                &mut readers,
            )?;
            acl_changed |= narrowed;
            output.push(batch);
        }
    }
    acl_changed |= readers.iter().any(|((root, _), readers)| {
        !readers.is_empty()
            && changed.iter().any(|filter| {
                filter == &keys_filter(&envelope.source_id, std::slice::from_ref(root))
            })
    });
    Ok((changed, write_rows(config, output).await?, acl_changed))
}

type RecordReaders = BTreeMap<(String, String), Vec<String>>;
async fn content_state(
    dataset: &Dataset,
    source: &str,
    roots: &[String],
) -> Result<(BTreeMap<String, DocumentState>, RecordReaders)> {
    let mut previous = BTreeMap::new();
    let mut previous_readers = BTreeMap::new();
    for page in roots.chunks(256) {
        let batch = dataset
            .scan()
            .project(&[
                "root_id",
                "record_id",
                "readers",
                "source_revision",
                "metadata_json",
            ])
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .filter(&keys_filter(source, page))
            .map_err(|_| WriterError::ChangedRowsInvalid)?
            .try_into_batch()
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?;
        for row in 0..batch.num_rows() {
            let root = strings(&batch, "root_id")?.value(row).to_owned();
            let value = state(&batch, row)?;
            if previous.get(&root).is_some_and(|old| old != &value) {
                return Err(WriterError::DocumentStateInvalid);
            }
            previous.insert(root, value);
            previous_readers.insert(
                (
                    strings(&batch, "root_id")?.value(row).to_owned(),
                    strings(&batch, "record_id")?.value(row).to_owned(),
                ),
                tokens(&batch, row)?,
            );
        }
    }

    Ok((previous, previous_readers))
}
fn stage_row(
    staged: &RecordBatch,
    fixed_readers: Option<&[String]>,
    document: &EnvelopeDocument,
    next: &mut DocumentState,
    row: usize,
    previous_readers: &mut RecordReaders,
) -> Result<(RecordBatch, bool)> {
    let row_batch = staged.slice(row, 1);
    let incoming = state(staged, row)?;
    if let Some(readers) = fixed_readers {
        next.permission = Some(PermissionState {
            revision: None,
            readers: readers.to_vec(),
            containers: Vec::new(),
        });
        let batch = with_state(&row_batch, next, readers)?;
        previous_readers.remove(&(
            document.root_id.clone(),
            strings(staged, "record_id")?.value(row).to_owned(),
        ));
        return Ok((batch, false));
    }
    // A change staged without a permission statement narrows nothing.
    // Any other narrows the applied state to its readers, and
    // an empty set means nobody.
    let mut acl_changed = false;
    let statement = incoming.permission.as_ref();
    let current = next.permission.get_or_insert_with(|| {
        incoming.permission.clone().unwrap_or(PermissionState {
            revision: None,
            readers: Vec::new(),
            containers: Vec::new(),
        })
    });
    if let Some(statement) = statement {
        let prior = current.readers.clone();
        current
            .readers
            .retain(|token| statement.readers.contains(token));
        if current.readers != prior {
            acl_changed = true;
            // The narrowed state is no longer what its permission
            // revision described: the next authoritative envelope
            // applies even at that same revision.
            current.revision = None;
        }
    }
    let tokens = effective_readers(own_readers(staged, row)?.as_deref(), &current.readers);
    if let Some(old_readers) = previous_readers.remove(&(
        document.root_id.clone(),
        strings(staged, "record_id")?.value(row).to_owned(),
    )) {
        acl_changed |= old_readers.iter().any(|token| !tokens.contains(token));
    }
    let batch = with_state(&row_batch, next, &tokens)?;
    Ok((batch, acl_changed))
}
