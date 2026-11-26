use super::*;

#[derive(Debug)]
pub(super) struct ContentWork<'a> {
    pub(super) config: &'a Config,
    pub(super) lease: &'a LeaseGuard,
    pub(super) dataset: Option<Dataset>,
    pub(super) lineage: CommitLineage,
    pub(super) acl_changed: bool,
}

/// The published version of the dataset and the latest version of any
/// commit, published or not. None before the first publication.
pub(super) async fn published(
    config: &Config,
    head: &ServingHead,
) -> Result<Option<(Dataset, u64)>> {
    if head.content.is_none() {
        return Ok(None);
    }
    let pinned = fyrr_reader::storage::pinned(config, head).await?;
    let latest = latest_version(&pinned).await?;
    Ok(Some((pinned, latest)))
}

/// A commit was interrupted after it wrote versions the head never published.
pub(super) fn interrupted(head: &ServingHead, latest: u64) -> bool {
    head.content
        .as_ref()
        .is_some_and(|pointer| pointer.lance_version != latest)
}

pub(super) async fn fts_merge_needed(config: &Config, dataset: &Dataset) -> Result<bool> {
    Ok(config.writer.commit.merge_fts_segments
        && dataset
            .load_indices()
            .await?
            .iter()
            .filter(|i| i.name == "fts_idx")
            .count()
            > 1)
}

impl<'a> ContentWork<'a> {
    pub(super) async fn open(
        config: &'a Config,
        lease: &'a LeaseGuard,
        head: &ServingHead,
        acl_changed: bool,
    ) -> Result<Self> {
        let mut lineage = crate::maintenance::lineage(lease, head);
        let dataset = if let Some((mut pinned, latest)) = published(config, head).await? {
            // Discard unpublished intermediate versions from an interrupted commit.
            if interrupted(head, latest) {
                native_step(lease, pinned.restore(), WriterError::StorageUnavailable).await?;
                record_transaction(&pinned, &mut lineage, latest).await?;
                lineage.recovery_version = Some(pinned.version().version);
            }
            Some(pinned)
        } else {
            None
        };
        Ok(Self {
            config,
            lease,
            dataset,
            lineage,
            acl_changed,
        })
    }
    pub(super) async fn fts_merge_needed(&self) -> Result<bool> {
        match &self.dataset {
            Some(dataset) => fts_merge_needed(self.config, dataset).await,
            None => Ok(false),
        }
    }

    pub(super) async fn apply_groups(
        &mut self,
        non_content: &[&PreparedEnvelope],
        groups: Vec<Vec<&PreparedEnvelope>>,
        sequence: u64,
    ) -> Result<()> {
        let config = self.config;
        let lease = self.lease;
        // Permissions before content whenever a serving version already exists.
        if let Some(ds) = &mut self.dataset {
            for envelope in non_content {
                if matches!(envelope.payload, EnvelopePayload::Permissions(_)) {
                    let (keys, fragments) = crate::apply::permissions(config, ds, envelope).await?;
                    apply_staged(config, ds, lease, &mut self.lineage, keys, fragments).await?;
                }
            }
        }
        let mut created = false;
        for group in groups {
            if group.is_empty() {
                continue;
            }
            if self.dataset.is_none() && config.namespace.write_api.is_some() {
                // Rows of the API reach their typed columns through the commit
                // path of any later publication: start from the empty table.
                self.overwrite(Vec::new()).await?;
                created = true;
            }
            if let Some(ds) = &mut self.dataset {
                for envelope in group {
                    let (keys, fragments, narrowed) =
                        crate::apply::content(config, ds, envelope, sequence).await?;
                    self.acl_changed |= narrowed;
                    apply_staged(config, ds, lease, &mut self.lineage, keys, fragments).await?;
                }
                continue;
            }

            let mut fragments = Vec::new();
            for envelope in group {
                let EnvelopePayload::Content(payload) = &envelope.payload else {
                    unreachable!()
                };
                for fragment in crate::apply::initial_deletes(config, envelope, sequence).await? {
                    fragments.push(
                        serde_json::from_value(fragment)
                            .map_err(|_| WriterError::PreparedFragmentInvalid)?,
                    );
                }
                for fragment in &payload.fragments {
                    fragments.push(
                        serde_json::from_value(fragment.lance_fragment.clone())
                            .map_err(|_| WriterError::PreparedFragmentInvalid)?,
                    );
                }
            }
            if !fragments.is_empty() {
                self.overwrite(fragments).await?;
            }
        }
        // Every request of a first publication was refused: nothing exists to
        // publish, as when a first bulk is rejected.
        if created
            && let Some(ds) = &self.dataset
            && ds.count_rows(None).await? == 0
        {
            self.dataset = None;
        }
        Ok(())
    }

    /// The table as a first publication creates it: `fragments` over the
    /// built-in columns and the namespace's attribute columns.
    async fn overwrite(&mut self, fragments: Vec<lance::table::format::Fragment>) -> Result<()> {
        let config = self.config;
        let lease = self.lease;
        let uri = dataset_uri(config);
        lease.head().await?;
        // A failed first bulk has no serving version to Restore. Overwrite
        // from its latest version explicitly discards all unpublished rows.
        let before = {
            match lance::dataset::builder::DatasetBuilder::from_uri(&uri)
                .with_storage_options(
                    config
                        .storage
                        .object_store_options(&ProcessEnvironment)
                        .map_err(|_| WriterError::StorageConfigurationInvalid)?
                        .into_map()
                        .into_iter()
                        .collect(),
                )
                .load()
                .await
            {
                Ok(orphan) => orphan.latest_version_id().await?,
                Err(lance::Error::DatasetNotFound { .. }) => 0,
                Err(_) => return Err(WriterError::StorageUnavailable),
            }
        };
        self.lineage.base_version = before;
        let operation = {
            Operation::Overwrite {
                fragments,
                schema: lance::datatypes::Schema::try_from(
                    crate::prepare::schema_with(
                        config.embedding.dimensions,
                        config.namespace.dense_element,
                        config
                            .namespace
                            .attributes
                            .iter()
                            .map(|(name, kind)| {
                                arrow_schema::Field::new(name, kind.arrow_type(), true)
                            })
                            // The vector attributes come after the typed ones, in
                            // name order, and are fixed with the table.
                            .chain(config.namespace.vectors.iter().map(|(name, vector)| {
                                arrow_schema::Field::new(
                                    name,
                                    crate::prepare::named_vector_type(
                                        config.namespace.dense_element,
                                        vector,
                                    ),
                                    true,
                                )
                            }))
                            .collect(),
                    )
                    .as_ref(),
                )
                .map_err(|_| WriterError::PreparedSchemaInvalid)?,
                config_upsert_values: Some(HashMap::new()),
                initial_bases: None,
            }
        };
        let dataset = self.dataset.insert(
            native_step(
                lease,
                Dataset::commit(
                    uri.as_str(),
                    operation,
                    Some(before),
                    Some(store_params(config)?),
                    None,
                    fyrr_reader::storage::writer_session(),
                    false,
                ),
                WriterError::StorageUnavailable,
            )
            .await?,
        );
        record_transaction(dataset, &mut self.lineage, before).await?;
        Ok(())
    }

    pub(super) async fn apply_non_content(
        &mut self,
        head: &mut ServingHead,
        non_content: Vec<&PreparedEnvelope>,
        receipts: &mut Vec<String>,
        sequence: u64,
    ) -> Result<()> {
        let config = self.config;
        let lease = self.lease;
        for envelope in non_content {
            match &envelope.payload {
                EnvelopePayload::Permissions(_) => {
                    if head.content.is_none()
                        && let Some(ds) = &mut self.dataset
                    {
                        let (keys, fragments) =
                            crate::apply::permissions(config, ds, envelope).await?;
                        apply_staged(config, ds, lease, &mut self.lineage, keys, fragments).await?;
                    }
                }
                EnvelopePayload::Reconcile(_) => {
                    if let Some(ds) = &mut self.dataset {
                        let (keys, fragments, finished) =
                            crate::apply::reconcile(config, ds, envelope, sequence, head).await?;
                        self.acl_changed |= !keys.is_empty();
                        apply_staged(config, ds, lease, &mut self.lineage, keys, fragments).await?;
                        if !finished {
                            receipts.retain(|id| id != &envelope.envelope_id);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
