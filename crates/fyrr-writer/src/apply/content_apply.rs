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
    super::check_table(payload, Some(dataset))?;
    if payload.embedding != fyrr_reader::embedding::identity(&config.embedding)
        || payload.chunk_profile != crate::prepare::chunk_identity()
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
    // The one-row slices of an API envelope leave out the attribute columns,
    // which are taken whole at the end.
    let typed = staged.clone();
    let staged = if envelope.source_id == fyrr_core::connector::API_SOURCE_ID {
        let built_in =
            crate::prepare::schema(config.embedding.dimensions, config.namespace.dense_element)
                .fields()
                .len();
        staged
            .project(&(0..built_in).collect::<Vec<_>>())
            .map_err(|_| WriterError::ChangedRowsInvalid)?
    } else {
        staged
    };
    // The filters are evaluated here, once, against the state this envelope
    // is applied to: what the envelopes before it left.
    let Some(documents) = with_filter_writes(config, dataset, envelope, payload).await? else {
        return Ok((Vec::new(), Vec::new(), false));
    };
    let roots = documents
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
    // An API envelope is one request: it is applied whole or refused whole,
    // and a change that would not advance a document is a refusal, not a skip.
    let api = envelope.source_id == fyrr_core::connector::API_SOURCE_ID;
    let refused = (Vec::new(), Vec::new(), false);
    let mut acl_changed = false;
    let mut changed = Vec::new();
    let mut output = Vec::new();
    let mut rows = Vec::new();
    let mut taken = Vec::new();
    // The rows and bytes of the stored rows a patch loads, which are held until
    // `write_rows` checks the rows written.
    let mut held = (0u64, 0u64);
    for document in documents.iter() {
        let old = previous.get(&document.root_id);
        // A patch never creates a document: one that is not there is skipped.
        if document.intent == DocumentIntent::Patch
            && old.is_none_or(|old| old.deleted || old.content_purged)
        {
            continue;
        }
        if let Some(condition) = &document.condition {
            let live = old.is_some_and(|old| !old.deleted && !old.content_purged);
            match condition_met(config, dataset, envelope, document, condition, live).await? {
                None => {
                    reject(config, envelope, "document_invalid").await?;
                    return Ok(refused);
                }
                Some(false) => continue,
                Some(true) => {}
            }
        }
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
                if api {
                    return Ok(refused);
                }
                continue;
            }
            if document.intent == DocumentIntent::Replace
                && !old.deleted
                && !old.content_purged
                && document.source_revision == old.revision
            {
                if api {
                    reject(config, envelope, "revision_conflict").await?;
                    return Ok(refused);
                }
                continue;
            }
            if let (Some(incoming), Some(applied)) = (document.revision_order, old.revision_order) {
                if incoming < applied || (incoming == applied && !old.content_purged) {
                    if api {
                        reject(config, envelope, "revision_conflict").await?;
                        return Ok(refused);
                    }
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
                if api {
                    return Ok(refused);
                }
                continue;
            }
        } else if document.expected_prior_revision.is_some() {
            reject(config, envelope, "revision_conflict").await?;
            if api {
                return Ok(refused);
            }
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
                config.namespace.dense_element,
                &envelope.source_id,
                &document.root_id,
                &next,
            )?);
            continue;
        }
        next.revision.clone_from(&document.source_revision);
        if let Some(patch) = &document.patch {
            // The stored records are rewritten with the patch's values; their
            // vectors, and every column it does not name, are copied as stored.
            if let Some(stated) = &patch.readers {
                next.permission = Some(PermissionState {
                    revision: None,
                    readers: stated.clone(),
                    containers: Vec::new(),
                });
            }
            let stored = dataset
                .scan()
                .filter(&rows_filter(
                    &envelope.source_id,
                    std::slice::from_ref(&document.root_id),
                ))
                .map_err(|_| WriterError::ChangedRowsInvalid)?
                .try_into_batch()
                .await
                .map_err(|_| WriterError::ChangedRowScanFailed)?;
            match hold_rows(config, &mut held, &stored) {
                Ok(()) => {}
                // A patch writes whole rows of what is stored, a size its request
                // could not know: the envelope is refused, as `write_rows` would.
                Err(WriterError::EnvelopeBoundsInvalid) => {
                    reject(config, envelope, "envelope_bounds_exceeded").await?;
                    return Ok(refused);
                }
                Err(error) => return Err(error),
            }
            let revision = document
                .source_revision
                .as_deref()
                .ok_or(WriterError::DocumentRevisionMissing)?;
            for row in 0..stored.num_rows() {
                let kept = match &next.permission {
                    Some(permission) => effective_readers(
                        own_readers(&stored, row)?.as_deref(),
                        &permission.readers,
                    ),
                    None => tokens(&stored, row)?,
                };
                if let Some(before) = readers.remove(&(
                    document.root_id.clone(),
                    strings(&stored, "record_id")?.value(row).to_owned(),
                )) {
                    acl_changed |= before.iter().any(|token| !kept.contains(token));
                }
                output.push(with_patch(
                    &with_state(&stored.slice(row, 1), &next, &kept)?,
                    patch,
                    revision,
                    &config.namespace.attributes,
                    config.namespace.dense_element,
                )?);
            }
            continue;
        }
        next.deleted = false;
        next.content_purged = false;
        next.delete_sequence = None;
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
                config.namespace.dense_element,
                &envelope.source_id,
                &document.root_id,
                &next,
            )?);
            continue;
        }
        if api {
            // The request states the document's readers whole; it narrows nothing.
            next.permission = None;
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
            if api {
                rows.push(batch);
                taken.push(crate::narrow_u32(row as u64));
            } else {
                output.push(batch);
            }
        }
    }
    if !rows.is_empty() {
        output.push(with_typed(
            &rows,
            &taken,
            &typed,
            &config.namespace.attributes,
            &config.namespace.vectors,
        )?);
    }
    acl_changed |= readers.iter().any(|((root, _), readers)| {
        !readers.is_empty()
            && changed.iter().any(|filter| {
                filter == &keys_filter(&envelope.source_id, std::slice::from_ref(root))
            })
    });
    let fragments = match write_rows(config, output).await {
        // A patch writes whole rows of what is stored, a size its request could
        // not know: such an envelope is refused, not left to fail its group.
        Err(WriterError::EnvelopeBoundsInvalid)
            if payload.filter_writes.is_some()
                || documents.iter().any(|d| d.intent == DocumentIntent::Patch) =>
        {
            reject(config, envelope, "envelope_bounds_exceeded").await?;
            return Ok(refused);
        }
        written => written?,
    };
    Ok((changed, fragments, acl_changed))
}

/// Whether the stored document meets the condition of its change, tested
/// against the document as the envelopes before this one left it: a document
/// that is not there meets it for a replacement only. `None` when the condition
/// does not hold for the namespace's schema.
async fn condition_met(
    config: &Config,
    dataset: &Dataset,
    envelope: &PreparedEnvelope,
    document: &EnvelopeDocument,
    condition: &fyrr_core::mcp::Filter,
    live: bool,
) -> Result<Option<bool>> {
    if !live {
        return Ok(Some(document.intent == DocumentIntent::Replace));
    }
    let Some(sql) = fyrr_reader::filter_sql(condition, &config.namespace.attributes) else {
        return Ok(None);
    };
    let count = dataset
        .count_rows(Some(format!(
            "{} AND ({sql})",
            rows_filter(&envelope.source_id, std::slice::from_ref(&document.root_id))
        )))
        .await?;
    Ok(Some(count > 0))
}

/// The documents of `payload` and those its filters match in `dataset`; `None`
/// once the envelope is refused: a filter that matches more than
/// `MAX_FILTER_WRITE_DOCUMENTS` documents changes nothing. Only the documents of
/// the API's source are matched, whatever their readers. The deletes come
/// first: a patch by id of a document they delete finds it deleted. A document
/// the envelope names is written as it says, a patch by id over the patch by
/// filter.
async fn with_filter_writes<'a>(
    config: &Config,
    dataset: &Dataset,
    envelope: &PreparedEnvelope,
    payload: &'a ContentPayload,
) -> Result<Option<std::borrow::Cow<'a, [EnvelopeDocument]>>> {
    let Some(writes) = &payload.filter_writes else {
        return Ok(Some(std::borrow::Cow::Borrowed(&payload.documents)));
    };
    // A change of the envelope whose condition is not met writes nothing, and
    // leaves the filters to write on their own. One that is met needs no test
    // again.
    let roots: Vec<String> = payload
        .documents
        .iter()
        .filter(|d| d.condition.is_some())
        .map(|d| d.root_id.clone())
        .collect();
    let (previous, _) = content_state(dataset, &envelope.source_id, &roots).await?;
    let mut documents = Vec::with_capacity(payload.documents.len());
    for mut document in payload.documents.iter().cloned() {
        if let Some(condition) = document.condition.take() {
            let live = previous
                .get(&document.root_id)
                .is_some_and(|old| !old.deleted && !old.content_purged);
            match condition_met(config, dataset, envelope, &document, &condition, live).await? {
                None => {
                    reject(config, envelope, "document_invalid").await?;
                    return Ok(None);
                }
                Some(false) => continue,
                Some(true) => {}
            }
        }
        documents.push(document);
    }
    let named: BTreeMap<String, usize> = documents
        .iter()
        .enumerate()
        .map(|(position, d)| (d.root_id.clone(), position))
        .collect();
    let document = |root_id: &String, intent, patch| EnvelopeDocument {
        root_id: root_id.clone(),
        intent,
        source_revision: Some(writes.source_revision.clone()),
        revision_order: Some(writes.revision_order),
        expected_prior_revision: None,
        permission_revision: None,
        records: 0,
        patch,
        condition: None,
    };
    let mut deleted = BTreeSet::new();
    if let Some(filter) = &writes.delete {
        deleted = match matching(config, dataset, filter).await? {
            Ok(roots) => roots,
            Err(reason) => {
                reject(config, envelope, reason.as_str()).await?;
                return Ok(None);
            }
        };
        for root in &deleted {
            match named.get(root) {
                None => documents.push(document(root, DocumentIntent::Delete, None)),
                Some(&at) if documents[at].intent == DocumentIntent::Patch => {
                    documents[at].intent = DocumentIntent::Delete;
                    documents[at].patch = None;
                }
                Some(_) => {}
            }
        }
    }
    if let Some(patch) = &writes.patch {
        let matched = match matching(config, dataset, &patch.filter).await? {
            Ok(roots) => roots,
            Err(reason) => {
                reject(config, envelope, reason.as_str()).await?;
                return Ok(None);
            }
        };
        for root in matched.difference(&deleted) {
            match named.get(root) {
                None => documents.push(document(
                    root,
                    DocumentIntent::Patch,
                    Some(patch.values.clone()),
                )),
                Some(&at) => {
                    if let Some(explicit) = documents[at].patch.as_mut() {
                        let mut values = (*patch.values).clone();
                        values.readers = explicit.readers.take().or(values.readers);
                        values.attributes.append(&mut explicit.attributes);
                        values.text = explicit.text.take();
                        values.vector = explicit.vector.take();
                        **explicit = values;
                    }
                }
            }
        }
    }
    Ok(Some(std::borrow::Cow::Owned(documents)))
}

/// The roots of the documents of the API's source that pass `filter`, or why
/// the filter is refused.
async fn matching(
    config: &Config,
    dataset: &Dataset,
    filter: &fyrr_core::mcp::Filter,
) -> Result<std::result::Result<BTreeSet<String>, WriterError>> {
    use fyrr_core::limits::write::MAX_FILTER_WRITE_DOCUMENTS;
    let Some(filter) = fyrr_reader::filter_sql(filter, &config.namespace.attributes) else {
        return Ok(Err(WriterError::FilterInvalid));
    };
    // One document of the API is one record: a row past the bound is a document past it.
    let batch = dataset
        .scan()
        .project(&["root_id"])
        .map_err(|_| WriterError::ChangedRowsInvalid)?
        .filter(&format!(
            "source_id = {} AND ({filter})",
            sql(fyrr_core::connector::API_SOURCE_ID)
        ))
        .map_err(|_| WriterError::ChangedRowsInvalid)?
        .limit(i64::try_from(MAX_FILTER_WRITE_DOCUMENTS + 1).ok(), None)
        .map_err(|_| WriterError::ChangedRowsInvalid)?
        .try_into_batch()
        .await
        .map_err(|_| WriterError::ChangedRowScanFailed)?;
    if batch.num_rows() > MAX_FILTER_WRITE_DOCUMENTS {
        return Ok(Err(WriterError::FilterMatchesTooMany));
    }
    let roots = strings(&batch, "root_id")?;
    Ok(Ok((0..batch.num_rows())
        .map(|row| roots.value(row).to_owned())
        .collect()))
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
