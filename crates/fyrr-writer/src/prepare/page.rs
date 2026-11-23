use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn content_page(
    config: &Config,
    source: &fyrr_core::config::SourceConfig,
    batch: &ConnectorBatch,
    changes: Vec<ContentChange>,
    state: &mut CursorState,
    lease: &crate::sync_lease::CursorLease,
    base_head: Option<BaseHead>,
    cache: &mut WriterEmbedding,
    advances_cursor: bool,
    permission_stream: bool,
    attributes: &[(String, ArrayRef)],
) -> Result<u64> {
    let (store, root) = namespace_store(config)?;
    let sequence = state
        .sequence
        .checked_add(1)
        .ok_or(WriterError::CursorSequenceOverflow)?;
    let id = envelope_id(
        &config.namespace.name,
        &source.id,
        SyncKind::Content,
        sequence,
        state.rejection_generation,
        state.cursor.as_deref(),
    );
    let envelope_path = root.clone().join("envelopes").join(format!("{id}.json"));
    lease.state().await?;
    for change in &changes {
        let ContentChange::Write(change, _) = change else {
            continue;
        };
        if source.provider == "local-files"
            && (source.readers.is_empty()
                || change.readers != source.readers
                || change
                    .sections
                    .iter()
                    .any(|s| s.readers.as_ref().is_some_and(|r| r != &source.readers)))
            && change.op == ChangeOp::Upsert
        {
            return Err(WriterError::LocalFilesReadersInvalid);
        }
    }
    let embedding_texts = changes
        .iter()
        .filter_map(|change| match change {
            ContentChange::Write(change, _) => Some(change),
            ContentChange::Present(_) | ContentChange::Where(_) => None,
        })
        .filter(|change| {
            change.op == ChangeOp::Upsert && quarantine_reason(config, source, change).is_none()
        })
        .flat_map(|change| change.sections.iter())
        .filter(|section| section.vector.is_none())
        .flat_map(|section| crate::chunk::chunks(&section.text))
        .collect::<Vec<_>>();
    let mut embedded = cache.embed(&embedding_texts).await?.into_iter();
    let filled;
    let attributes = if config.namespace.vectors.values().any(|v| v.from.is_some()) {
        filled = embed_sources(config, cache, attributes).await?;
        &filled[..]
    } else {
        attributes
    };
    let mut maps = std::collections::BTreeMap::new();
    for change in &changes {
        if let ContentChange::Write(change, _) = change
            && quarantine_reason(config, source, change).is_none()
            && let Some(map) = change
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.connector_map.as_ref())
        {
            let bytes = serde_json::to_vec(map).map_err(|_| WriterError::DocumentInvalid)?;
            maps.entry(digest(&bytes)).or_insert(bytes);
        }
    }
    let metadata_store = store.as_ref();
    let metadata_root = &root;
    crate::io::bounded(maps, |(reference, bytes)| async move {
        lease.state().await?;
        let path = metadata_root
            .clone()
            .join("document-metadata")
            .join(format!("{}.json", &reference[7..]));
        match metadata_store
            .put_opts(
                &path,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_)
            | Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => Ok(()),
            Err(_) => Err(WriterError::StorageUnavailable),
        }
    })
    .await?;
    let page = Page {
        config,
        source,
        store: &store,
        root: &root,
        base_head: &base_head,
        permission_stream,
        attributes,
    };
    let mut rows = PageRows::default();
    for change in changes {
        rows.append(&page, change, &mut embedded)?;
    }
    let fragments = rows.columns.fragments(&page).await?;
    let quarantined_count = rows.quarantined.len() as u64;
    // Only a committer that knows conditions and filter writes may read an
    // envelope with one.
    let conditional =
        rows.filter_writes.is_some() || rows.documents.iter().any(|d| d.condition.is_some());
    let envelope = PreparedEnvelope {
        schema_version: if conditional {
            fyrr_core::envelope::CONDITIONAL_ENVELOPE_CONTRACT_VERSION
        } else {
            ENVELOPE_CONTRACT_VERSION
        },
        envelope_id: id,
        namespace: config.namespace.name.clone(),
        source_id: source.id.clone(),
        kind: SyncKind::Content,
        sequence,
        rejection_generation: state.rejection_generation,
        restore_sequence: state.restore_sequence,
        cursor: batch.cursor.clone(),
        advances_cursor,
        base_head,
        created_at: fyrr_core::clock::now(),
        payload: EnvelopePayload::Content(ContentPayload {
            embedding: identity(&config.embedding),
            chunk_profile: chunk_identity(),
            table_schema_version: SchemaVersion::new(1, 2),
            documents: rows.documents,
            fragments,
            quarantined: rows.quarantined,
            filter_writes: rows.filter_writes,
        }),
    };
    if let EnvelopePayload::Content(payload) = &envelope.payload {
        fyrr_reader::storage::persist_native_files(
            config,
            payload
                .fragments
                .iter()
                .flat_map(|fragment| fragment.files.iter().cloned())
                .collect(),
        )
        .await?;
    }
    lease.state().await?;
    lease.persist_observed().await?;
    crate::commit::ensure_writable(config).await?;
    put_immutable(store.as_ref(), &envelope_path, &envelope).await?;
    if advances_cursor {
        state.cursor = Some(batch.cursor.end.clone());
    }
    state.sequence = sequence;
    lease.write(state.clone()).await?;
    Ok(quarantined_count)
}

struct Page<'a> {
    config: &'a Config,
    source: &'a fyrr_core::config::SourceConfig,
    store: &'a Arc<dyn ObjectStore>,
    root: &'a Path,
    base_head: &'a Option<BaseHead>,
    permission_stream: bool,
    /// The typed attribute columns of the rows, in the order of the table's
    /// columns: a fragment names its columns by position, so another order
    /// would store each value under another attribute.
    attributes: &'a [(String, ArrayRef)],
}
#[derive(Default)]
struct PageRows {
    documents: Vec<EnvelopeDocument>,
    quarantined: Vec<QuarantineEntry>,
    filter_writes: Option<Box<fyrr_core::envelope::FilterWrites>>,
    columns: RowColumns,
}
#[derive(Default)]
struct RowColumns {
    sources: Vec<String>,
    roots: Vec<String>,
    records: Vec<String>,
    revisions: Vec<String>,
    readers: Vec<Vec<String>>,
    vectors: Vec<Vec<f32>>,
    texts: Vec<String>,
    metadata: Vec<String>,
    record_orders: Vec<u64>,
    map_refs: Vec<Option<String>>,
    uris: Vec<String>,
    types: Vec<String>,
    projects: Vec<Option<String>>,
    authors: Vec<Option<String>>,
    updated: Vec<Option<i64>>,
    attributes: Vec<Vec<String>>,
    /// The typed columns of a connector's rows; an API request brings its own.
    typed: Option<AttributeColumns>,
}
/// `attributes` with each vector attribute that names a source attribute given,
/// for every row that has none and a source value, the embedding of the value.
async fn embed_sources(
    config: &Config,
    cache: &mut WriterEmbedding,
    attributes: &[(String, ArrayRef)],
) -> Result<Vec<(String, ArrayRef)>> {
    use arrow_array::Array as _;
    let mut filled = attributes.to_vec();
    for (name, spec) in &config.namespace.vectors {
        let Some(source) = &spec.from else {
            continue;
        };
        let find = |wanted: &str| attributes.iter().position(|(n, _)| n == wanted);
        let (Some(at), Some(from)) = (find(name), find(source)) else {
            continue;
        };
        let (Some(texts), Some(current)) = (
            filled[from].1.as_any().downcast_ref::<StringArray>(),
            filled[at].1.as_any().downcast_ref::<FixedSizeListArray>(),
        ) else {
            return Err(WriterError::PreparedRowsInvalid);
        };
        let missing: Vec<usize> = (0..current.len())
            .filter(|&row| {
                current.is_null(row) && texts.is_valid(row) && !texts.value(row).is_empty()
            })
            .collect();
        if missing.is_empty() {
            continue;
        }
        let inputs: Vec<&str> = missing.iter().map(|&row| texts.value(row)).collect();
        let vectors = cache.embed(&inputs).await?;
        if vectors.len() != missing.len() {
            return Err(WriterError::EmbeddingBatchInvalid);
        }
        let mut rows: Vec<Option<Vec<f32>>> = (0..current.len())
            .map(|row| {
                (!current.is_null(row))
                    .then(|| fyrr_reader::embedding::stored_values(current.value(row).as_ref()))
                    .flatten()
            })
            .collect();
        for (row, vector) in missing.into_iter().zip(vectors) {
            rows[row] = Some(vector);
        }
        let column = nullable_vector_column(
            config.namespace.dense_element,
            spec.dimensions,
            rows.iter().map(Option::as_deref),
        );
        filled[at].1 = column;
    }
    Ok(filled)
}

struct DocumentRows<'a> {
    change: &'a DocumentChange,
    common: Option<fyrr_core::connector::DocumentMetadata>,
    /// A connector document's metadata fields that are typed columns, which
    /// its stored metadata no longer holds.
    typed: serde_json::Map<String, serde_json::Value>,
    map_ref: Option<String>,
    section_ids: Vec<String>,
    silent: bool,
}

impl PageRows {
    fn append(
        &mut self,
        page: &Page<'_>,
        change: ContentChange,
        embedded: &mut impl Iterator<Item = Vec<f32>>,
    ) -> Result<()> {
        let config = page.config;
        let source = page.source;
        let (change, condition) = match change {
            ContentChange::Write(change, condition) => (*change, condition),
            ContentChange::Present(document) => {
                self.documents.push(document);
                return Ok(());
            }
            ContentChange::Where(writes) => {
                self.filter_writes = Some(writes);
                return Ok(());
            }
        };
        if let Some(reason) = quarantine_reason(config, source, &change) {
            self.quarantined.push(QuarantineEntry {
                root_digest: digest(change.root_id.as_bytes()),
                revision_digest: digest(change.source_revision.as_deref().unwrap_or("").as_bytes()),
                reason: reason.into(),
            });
            return Ok(());
        }
        let rows = DocumentRows::new(page, &change)?;
        let count = self.columns.append(page, &rows, embedded)?;
        self.documents.push(EnvelopeDocument {
            root_id: change.root_id,
            intent: if change.op == ChangeOp::Delete {
                DocumentIntent::Delete
            } else {
                DocumentIntent::Replace
            },
            source_revision: change.source_revision,
            revision_order: change.revision_order,
            expected_prior_revision: change.expected_prior_revision,
            permission_revision: change.permission_revision,
            records: count,
            patch: None,
            condition,
        });
        Ok(())
    }
}
impl<'a> DocumentRows<'a> {
    fn new(page: &Page<'_>, change: &'a DocumentChange) -> Result<Self> {
        let source = page.source;
        let permission_stream = page.permission_stream;
        let mut common = change.metadata.clone();
        let map = common.as_mut().and_then(|m| m.connector_map.take());
        let mut typed = serde_json::Map::new();
        if page.config.namespace.typed_connector_fields()
            && let Some(metadata) = &mut common
        {
            typed.insert("title".into(), metadata.title.take().into());
            typed.insert("team".into(), metadata.team.take().into());
            typed.insert("status".into(), metadata.status.take().into());
            typed.insert("labels".into(), std::mem::take(&mut metadata.labels).into());
            typed.insert(
                "created_at".into(),
                metadata
                    .created_at
                    .take()
                    .map(|at| at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                    .into(),
            );
        }
        let map_ref = if let Some(map) = map {
            let bytes = serde_json::to_vec(&map).map_err(|_| WriterError::DocumentInvalid)?;
            let reference = digest(&bytes);
            Some(reference)
        } else {
            None
        };
        // A connector location identifies its own section, never a sibling's position.
        // An unlocated whole unit keeps the frozen v5 root:chunk identity.
        let section_ids = change
            .sections
            .iter()
            .map(|section| {
                section.location.as_ref().map_or_else(
                    || change.root_id.clone(),
                    |location| {
                        digest(
                            serde_json::json!([change.root_id, location])
                                .to_string()
                                .as_bytes(),
                        )
                    },
                )
            })
            .collect::<Vec<_>>();
        if section_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != section_ids.len()
        {
            // Ambiguous locations cannot supply stable, distinct record identities.
            return Err(WriterError::DocumentInvalid);
        }
        // Where the provider has a permission stream, a sync change without a
        // permission revision, readers or containers makes no permission
        // statement: it is staged without one and without row
        // readers, and its rows take the applied state at commit. Staged
        // that way it is also what a first commit may publish as it is.
        // Without that stream content alone states the readers, and none
        // means nobody.
        let silent = permission_stream
            && source.permissions == PermissionsMode::Sync
            && change.permission_revision.is_none()
            && change.readers.is_empty()
            && change.containers.is_empty();
        Ok(Self {
            change,
            common,
            typed,
            map_ref,
            section_ids,
            silent,
        })
    }
    fn metadata(
        &self,
        page: &Page<'_>,
        records: &[String],
        text: &str,
        tokens: &[String],
        own: bool,
    ) -> Result<String> {
        let config = page.config;
        let source = page.source;
        let base_head = page.base_head;
        let change = self.change;
        let silent = self.silent;
        let mut value =
            serde_json::to_value(&self.common).map_err(|_| WriterError::DocumentInvalid)?;
        value["_fyrr"] = serde_json::to_value(crate::apply::DocumentState {
            revision: change.source_revision.clone(),
            revision_order: change.revision_order,
            deleted: false,
            content_purged: false,
            delete_sequence: None,
            publication_sequence: base_head.as_ref().map_or(1, |b| b.content_sequence + 1),
            permission: if silent {
                None
            } else {
                Some(crate::apply::PermissionState {
                    revision: change.permission_revision.clone(),
                    readers: crate::apply::permission_readers(
                        config,
                        &source.id,
                        &change.readers,
                        &change.containers,
                    )?,
                    containers: change.containers.clone(),
                })
            },
        })
        .map_err(|_| WriterError::DocumentInvalid)?;
        value["_fyrr_record_revision"] = fyrr_core::limits::record_revision(
            &source.id,
            &change.root_id,
            records.last().ok_or(WriterError::DocumentInvalid)?,
            text,
        )
        .into();
        value["_fyrr_text_utf8_bytes"] = (text.len() as u64).into();
        if source.permissions != PermissionsMode::Fixed && own {
            // The section's own statement, kept so that its row stays
            // within it whenever the document's readers change.
            value[crate::apply::OWN_READERS] = serde_json::json!(tokens);
        }
        Ok(value.to_string())
    }
}
impl RowColumns {
    fn append(
        &mut self,
        page: &Page<'_>,
        rows: &DocumentRows<'_>,
        embedded: &mut impl Iterator<Item = Vec<f32>>,
    ) -> Result<u32> {
        let config = page.config;
        let source = page.source;
        let change = rows.change;
        let common = &rows.common;
        let map_ref = &rows.map_ref;
        let section_ids = &rows.section_ids;
        let silent = rows.silent;
        let mut count = 0;
        for (section_number, section) in change.sections.iter().enumerate() {
            let section_readers = if source.permissions == PermissionsMode::Fixed {
                &source.readers
            } else {
                section.readers.as_ref().unwrap_or(&change.readers)
            };
            let mut tokens = section_readers
                .iter()
                .map(|token| crate::apply::reader_token(config, &source.id, token))
                .collect::<Result<Vec<_>, _>>()?;
            if section.readers.is_none() {
                for container in &change.containers {
                    tokens.push(crate::apply::container_token(
                        config, &source.id, container,
                    )?);
                }
            }
            for (chunk_number, text) in crate::chunk::chunks(&section.text).into_iter().enumerate()
            {
                let vector = section
                    .vector
                    .clone()
                    .or_else(|| embedded.next())
                    .ok_or(WriterError::EmbeddingBatchInvalid)?;
                self.sources.push(source.id.clone());
                self.roots.push(change.root_id.clone());
                let record = format!("{}:{chunk_number}", section_ids[section_number]);
                self.records
                    .push(if record.len() > fyrr_core::limits::MAX_ID_BYTES {
                        digest(record.as_bytes())
                    } else {
                        record
                    });
                self.revisions.push(
                    change
                        .source_revision
                        .clone()
                        .ok_or(WriterError::DocumentRevisionMissing)?,
                );
                self.readers
                    .push(if silent { Vec::new() } else { tokens.clone() });
                self.vectors.push(vector);
                self.texts.push(text.to_owned());
                self.metadata.push(rows.metadata(
                    page,
                    &self.records,
                    text,
                    &tokens,
                    section.readers.is_some(),
                )?);
                let common = common.as_ref().ok_or(WriterError::DocumentInvalid)?;
                self.record_orders
                    .push(((section_number as u64) << 32) | chunk_number as u64);
                self.map_refs.push(map_ref.clone());
                self.uris.push(common.source_uri.clone());
                self.types.push(common.source_type.clone());
                self.projects.push(common.project.clone());
                self.authors.push(common.author.clone());
                self.updated
                    .push(common.updated_at.map(|t| t.timestamp_micros()));
                self.attributes.push(change.attributes.clone());
                if config.namespace.typed_connector_fields() {
                    // Connector namespaces never change their schema: the table's
                    // columns are in name order.
                    self.typed
                        .get_or_insert_with(|| {
                            let schema = &config.namespace.attributes;
                            AttributeColumns::new(schema, schema.keys().cloned().collect())
                        })
                        .push(&rows.typed)
                        .map_err(|_| WriterError::DocumentInvalid)?;
                }
                count += 1;
            }
        }
        Ok(count)
    }
    async fn fragments(self, page: &Page<'_>) -> Result<Vec<PreparedFragment>> {
        let config = page.config;
        let store = page.store;
        let root = page.root;
        let mut fragments = Vec::new();
        if !self.records.is_empty() {
            let reader_array = string_lists(self.readers.iter().map(Vec::as_slice));
            let vector_array = vector_column(
                config.namespace.dense_element,
                config.embedding.dimensions,
                self.vectors.iter().map(Vec::as_slice),
            );
            let attribute_array = string_lists(self.attributes.iter().map(Vec::as_slice));
            // An API request brings its attribute columns; a connector's rows
            // take theirs from the metadata of their documents.
            let typed = self
                .typed
                .map_or_else(|| page.attributes.to_vec(), AttributeColumns::finish);
            let mut columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(self.sources)),
                Arc::new(StringArray::from(self.roots)),
                Arc::new(StringArray::from(self.records)),
                Arc::new(StringArray::from(self.revisions)),
                Arc::new(reader_array),
                vector_array,
                Arc::new(StringArray::from(self.texts)),
                Arc::new(StringArray::from(self.metadata)),
                Arc::new(UInt64Array::from(self.record_orders)),
                Arc::new(StringArray::from(self.map_refs)),
                Arc::new(StringArray::from(self.uris)),
                Arc::new(StringArray::from(self.types)),
                Arc::new(StringArray::from(self.projects)),
                Arc::new(StringArray::from(self.authors)),
                Arc::new(TimestampMicrosecondArray::from(self.updated).with_timezone("UTC")),
                Arc::new(attribute_array),
            ];
            columns.extend(typed.iter().map(|(_, column)| column.clone()));
            let schema = schema_with(
                config.embedding.dimensions,
                config.namespace.dense_element,
                typed
                    .iter()
                    .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
                    .collect(),
            );
            let rows = RecordBatch::try_new(schema.clone(), columns)
                .map_err(|_| WriterError::PreparedRowsInvalid)?;
            let params = WriteParams {
                store_params: Some(store_params(config)?),
                ..Default::default()
            };
            let uri = dataset_uri(config);
            for fragment in FragmentCreateBuilder::new(&uri)
                .write_params(&params)
                .write_fragments(RecordBatchIterator::new(vec![Ok(rows)], schema))
                .await
                .map_err(|_| WriterError::FragmentWriteFailed)?
            {
                let paths: Vec<_> = fragment
                    .files
                    .iter()
                    .map(|file| format!("data/{}", file.path))
                    .collect();
                let (files, binding) = bind_files(store.as_ref(), root, &paths).await?;
                fragments.push(PreparedFragment {
                    digest: binding,
                    rows: fragment.physical_rows.unwrap_or(0) as u64,
                    files,
                    lance_fragment: serde_json::to_value(fragment)
                        .map_err(|_| WriterError::PreparedFragmentInvalid)?,
                });
            }
        }
        Ok(fragments)
    }
}
