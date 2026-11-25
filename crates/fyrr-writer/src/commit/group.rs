use super::publication::previous_manifest;
use super::*;
use fyrr_core::attributes::{AttributeIndexes, AttributeType, stale_analyzers};
use lance::dataset::NewColumnTransform;
use lance_index::scalar::inverted::DocumentGranularity;

mod native;
mod schema;

pub(crate) use schema::{change_schema, check_schema_change};

#[allow(clippy::print_stderr)]
pub(crate) async fn commit_one(
    config: &Config,
    stage_timings: bool,
    lease: &LeaseGuard,
    envelopes: Vec<PreparedEnvelope>,
    mut history: Option<&mut RestoreHistory>,
) -> Result<u64> {
    let preparation_start = std::time::Instant::now();
    let (store, root) = namespace_store(config)?;
    let mut head = lease.head().await?;
    if head.namespace != config.namespace.name {
        return Err(WriterError::PublicationNamespaceInvalid);
    }
    let previous_manifest = previous_manifest(store.as_ref(), &root, &head).await?;
    let markers = if let Some(h) = history.as_deref_mut() {
        h.refresh(config, &head).await?;
        h.fences(config, &envelopes).await?
    } else {
        let mut fences = history::RejectionFences::new(&envelopes)?;
        for marker in rejection_markers(config, &head).await? {
            fences.observe(&marker)?;
        }
        fences
    };
    let current_policy: PolicyArtifact =
        fyrr_reader::storage::artifact(store.as_ref(), &root, &head.policy).await?;
    // The namespace's settings change only by a schema request: a configuration
    // that does not carry the ones the policy holds now judges no batch, rather
    // than rejecting it for good.
    if current_policy.settings != namespace_settings(config) {
        return Err(WriterError::NamespaceSettingsFixed);
    }
    let (accepted, mut rejected) =
        accept_envelopes(config, &head, &current_policy, &markers, envelopes).await?;
    let mut rejection_head = head.clone();
    rejection_head.rejected_envelope_ids = rejected.clone();
    let blocked = if let Some(h) = history.as_deref_mut() {
        h.refresh(config, &rejection_head).await?;
        blocked_sources(&h.unconsumed_rejections(config, &accepted).await?)
    } else {
        blocked_sources(&unconsumed_rejections(config, &rejection_head, &accepted).await?)
    };
    let envelopes: Vec<_> = accepted
        .into_iter()
        .filter(|e| {
            e.kind != fyrr_core::connector::SyncKind::Content || !blocked.contains(&e.source_id)
        })
        .collect();
    stage(stage_timings, "envelopes", preparation_start);
    let identity_changed = refresh_identity(
        config,
        store.as_ref(),
        &root,
        &mut head,
        &envelopes,
        history.as_deref(),
    )
    .await?;
    let (policy_changed, acl_changed) = refresh_policy(
        config,
        store.as_ref(),
        &root,
        &mut head,
        &current_policy,
        &envelopes,
    )
    .await?;
    let data_start = std::time::Instant::now();
    let mut work = native::ContentWork::open(config, lease, &head, acl_changed).await?;
    let fts_merge_needed = work.fts_merge_needed().await?;
    if envelopes.is_empty()
        && work.lineage.transactions.is_empty()
        && rejected == head.rejected_envelope_ids
        && (!policy_changed || head.content.is_none())
        && !fts_merge_needed
    {
        // An idle namespace publishes nothing.
        if identity_changed || policy_changed {
            head.updated_at = fyrr_core::clock::now();
            lease.publish(head.clone()).await?;
        }
        return Ok(head.content.map_or(0, |p| p.lance_version));
    }
    let sequence = head.content.as_ref().map_or(Ok(1), |p| {
        p.sequence
            .checked_add(1)
            .ok_or(WriterError::PublicationSequenceOverflow)
    })?;
    let groups = content_groups(&envelopes);
    let non_content: Vec<_> = envelopes
        .iter()
        .filter(|e| !matches!(e.payload, EnvelopePayload::Content(_)))
        .collect();
    let mut receipts = envelopes.iter().map(|e| e.envelope_id.clone()).collect();
    work.apply_groups(&non_content, groups, sequence).await?;
    work.apply_non_content(&mut head, non_content, &mut receipts, sequence)
        .await?;
    for envelope in &envelopes {
        if crate::apply::rejected(config, &envelope.envelope_id).await? {
            rejected.push(envelope.envelope_id.clone());
        }
    }
    rejected.sort();
    rejected.dedup();
    stage(stage_timings, "data_commit", data_start);
    let native::ContentWork {
        dataset,
        mut lineage,
        acl_changed,
        ..
    } = work;
    let Some(mut dataset) = dataset else {
        head.updated_at = fyrr_core::clock::now();
        head.rejected_envelope_ids = rejected;
        lease.publish(head).await?;
        return Ok(0);
    };
    if !verify_content(config, &dataset, &head, &envelopes, &mut rejected, sequence).await? {
        head.rejected_envelope_ids = rejected;
        head.updated_at = fyrr_core::clock::now();
        lease.publish(head).await?;
        return Ok(0);
    }
    build_indexes(
        config,
        lease,
        &mut dataset,
        &mut lineage,
        head.content.is_none(),
        previous_manifest.as_ref(),
        stage_timings,
    )
    .await?;
    head.rejected_envelope_ids = rejected;
    let publication_start = std::time::Instant::now();
    let version = publish(
        config,
        lease,
        &dataset,
        head,
        lineage,
        receipts,
        acl_changed,
        Vec::new(),
        false,
        None,
    )
    .await?;
    stage(stage_timings, "publication", publication_start);
    Ok(version)
}

fn content_groups(envelopes: &[PreparedEnvelope]) -> Vec<Vec<&PreparedEnvelope>> {
    // Batch disjoint keys, but flush before any key repeats. Revisions may cycle
    // A->B->A; only envelope order determines which physical rows survive.
    let mut groups: Vec<Vec<&PreparedEnvelope>> = Vec::with_capacity(1);
    let mut group = Vec::new();
    let mut keys = BTreeSet::new();
    for envelope in envelopes {
        let EnvelopePayload::Content(payload) = &envelope.payload else {
            continue;
        };
        if payload
            .documents
            .iter()
            .any(|d| keys.contains(&(envelope.source_id.clone(), d.root_id.clone())))
        {
            groups.push(std::mem::take(&mut group));
            keys.clear();
        }
        keys.extend(
            payload
                .documents
                .iter()
                .map(|d| (envelope.source_id.clone(), d.root_id.clone())),
        );
        group.push(envelope);
    }
    groups.push(group);
    groups
}

async fn verify_content(
    config: &Config,
    dataset: &Dataset,
    head: &ServingHead,
    envelopes: &[PreparedEnvelope],
    rejected: &mut Vec<String>,
    sequence: u64,
) -> Result<bool> {
    let mut verification = Ok(());
    if head.content.is_none() {
        for envelope in envelopes {
            if matches!(envelope.payload, EnvelopePayload::Content(_)) {
                verification = crate::apply::verified_staged(config, dataset, envelope)
                    .await
                    .map(|_| ());
                if verification.is_err() {
                    break;
                }
            }
        }
    }
    if verification.is_ok() {
        verification = crate::apply::verify_publication(config, dataset, envelopes, sequence).await;
    }
    if let Err(reason) = verification {
        if head.content.is_some() {
            return Err(reason);
        }
        // A failed genesis bulk is still invisible. Reject and rewind its source
        // inputs under the head CAS; the next first bulk overwrites the orphan.
        for envelope in envelopes {
            if matches!(envelope.payload, EnvelopePayload::Content(_)) {
                crate::apply::reject(config, envelope, reason.as_str()).await?;
                rejected.push(envelope.envelope_id.clone());
            }
        }
        rejected.sort();
        rejected.dedup();
        return Ok(false);
    }
    Ok(true)
}

async fn accept_envelopes(
    config: &Config,
    head: &ServingHead,
    current_policy: &PolicyArtifact,
    markers: &history::RejectionFences,
    envelopes: Vec<PreparedEnvelope>,
) -> Result<(Vec<PreparedEnvelope>, Vec<String>)> {
    let mut rejected = head.rejected_envelope_ids.clone();
    let mut accepted = Vec::new();
    for envelope in envelopes {
        // A retired source's commits are fenced.
        let reason = if current_policy
            .retired_sources
            .contains_key(&envelope.source_id)
        {
            Some(WriterError::SourceRetired.as_str())
        } else if current_policy.settings.write_api.is_some()
            != (envelope.source_id == fyrr_core::connector::API_SOURCE_ID)
        {
            // A namespace takes documents from its connectors or from the API,
            // as it was created; the policy under the lease decides.
            Some(if current_policy.settings.write_api.is_some() {
                "namespace_takes_api"
            } else {
                "namespace_takes_connectors"
            })
        } else if let Some(reason) = markers.refusal(&envelope) {
            Some(reason)
        } else if (fyrr_core::clock::now() - envelope.created_at)
            .to_std()
            .is_ok_and(|age| age > config.writer.max_envelope_age.as_duration())
        {
            Some("envelope_expired")
        } else {
            let (work, bytes) = envelope_work(config, head, &envelope).await?;
            if head.content.is_some()
                && (work > config.writer.commit.group_max_rows
                    || bytes > config.writer.commit.group_max_bytes.as_u64())
            {
                Some("envelope_bounds_exceeded")
            } else if let EnvelopePayload::Content(p) = &envelope.payload {
                if p.embedding != identity(&config.embedding) || p.chunk_profile != chunk_identity()
                {
                    Some("envelope_profile_mismatch")
                } else {
                    crate::apply::verify_files(config, p)
                        .await
                        .err()
                        .map(WriterError::as_str)
                }
            } else {
                None
            }
        };
        if let Some(reason) = reason {
            crate::apply::reject(config, &envelope, reason).await?;
            rejected.push(envelope.envelope_id.clone());
        } else {
            accepted.push(envelope);
        }
    }
    Ok((accepted, rejected))
}

/// The identity the configuration and the inputs give, and whether the head
/// must point at a new generation of it.
async fn next_identity(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &ServingHead,
    envelopes: &[PreparedEnvelope],
    history: Option<&RestoreHistory>,
) -> Result<(IdentityArtifact, bool)> {
    let current_identity: IdentityArtifact =
        fyrr_reader::storage::artifact(store, root, &head.identity).await?;
    let identity_generation = head
        .identity
        .generation
        .checked_add(1)
        .ok_or(WriterError::IdentityGenerationOverflow)?;
    let configured = configured_identity(config, identity_generation)?;
    let member_paths = history
        .map(|h| {
            h.envelopes
                .iter()
                .filter(|(path, (_, summary, _))| {
                    h.listed.contains(path)
                        && envelopes.iter().any(|e| {
                            let EnvelopePayload::Members(p) = &e.payload else {
                                return false;
                            };
                            summary.kind == e.kind
                                && summary.source_id == e.source_id
                                && summary.rejection_generation == e.rejection_generation
                                && summary.sequence >= e.sequence.saturating_sub(p.part as u64)
                                && summary.sequence <= e.sequence
                        })
                })
                .map(|(path, (_, summary, _))| {
                    Ok((
                        path.clone(),
                        h.identity(&summary.envelope_id)?
                            .ok_or(WriterError::PublicationInvalid)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let mut next_identity = crate::membership::build(
        config,
        &current_identity,
        head,
        envelopes,
        configured,
        member_paths.as_deref(),
    )
    .await?;
    let identity_changed = next_identity.principals != current_identity.principals
        || next_identity.tokens != current_identity.tokens
        || next_identity.subjects != current_identity.subjects;
    if !identity_changed {
        next_identity.generation = head.identity.generation;
    }
    let pointer_changed = identity_changed || next_identity.inputs != current_identity.inputs;
    Ok((next_identity, pointer_changed))
}

async fn refresh_identity(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &mut ServingHead,
    envelopes: &[PreparedEnvelope],
    history: Option<&RestoreHistory>,
) -> Result<bool> {
    let (next_identity, pointer_changed) =
        next_identity(config, store, root, head, envelopes, history).await?;
    if pointer_changed {
        head.identity = crate::prepare::put_generation(
            store,
            root,
            "identity",
            next_identity.generation,
            &next_identity,
        )
        .await?;
    }
    Ok(pointer_changed)
}

/// The configured policy, when it differs from the one published.
fn configured_policy(
    config: &Config,
    current_policy: &PolicyArtifact,
) -> Result<Option<PolicyArtifact>> {
    let source_grants = fixed_grants(config)?;
    if current_policy.fixed_grants == source_grants {
        return Ok(None);
    }
    let mut policy = current_policy.clone();
    policy.fixed_grants = source_grants;
    Ok(Some(policy))
}

async fn refresh_policy(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &mut ServingHead,
    current_policy: &PolicyArtifact,
    envelopes: &[PreparedEnvelope],
) -> Result<(bool, bool)> {
    let acl_changed = envelopes
        .iter()
        .any(|e| matches!(&e.payload,EnvelopePayload::Permissions(p) if !p.documents.is_empty()));
    let policy_changed = if let Some(mut policy) = configured_policy(config, current_policy)? {
        policy.generation = head
            .policy
            .generation
            .checked_add(1)
            .ok_or(WriterError::PolicyGenerationOverflow)?;
        head.policy =
            crate::prepare::put_generation(store, root, "policy", policy.generation, &policy)
                .await?;
        true
    } else {
        false
    };
    Ok((policy_changed, acl_changed))
}

/// Whether a tick has nothing to commit, found by reads alone: no envelope
/// waits, no source was purged, and the policy, the identity, the dataset
/// and its indexes need nothing from the writer. `commit_one` takes these
/// decisions again under the lease, so a stale answer costs a lease, not
/// correctness.
pub(crate) async fn idle(
    config: &Config,
    head: &ServingHead,
    policy: &PolicyArtifact,
) -> Result<bool> {
    let (store, root) = namespace_store(config)?;
    if !crate::envelopes::purged_envelopes(config, head, policy)
        .await?
        .is_empty()
        || !pending(config, head).await?.is_empty()
        || configured_policy(config, policy)?.is_some()
        || next_identity(config, store.as_ref(), &root, head, &[], None)
            .await?
            .1
    {
        return Ok(false);
    }
    Ok(match native::published(config, head).await? {
        Some((dataset, latest)) => {
            !native::interrupted(head, latest)
                && !native::fts_merge_needed(config, &dataset).await?
        }
        None => true,
    })
}

fn vector_index_params(
    rows: usize,
    rq_bits: u8,
    partition_rows: u32,
    metric: fyrr_core::config::DenseMetric,
) -> VectorIndexParams {
    VectorIndexParams::with_ivf_rq_params(
        fyrr_reader::embedding::distance_type(metric),
        IvfBuildParams {
            num_partitions: Some(rows.div_ceil(partition_rows as usize).max(1)),
            target_partition_size: Some(partition_rows as usize),
            sample_rate: 256,
            ..Default::default()
        },
        RQBuildParams::new(rq_bits),
    )
}

#[allow(clippy::print_stderr)]
async fn build_indexes(
    config: &Config,
    lease: &LeaseGuard,
    dataset: &mut Dataset,
    lineage: &mut CommitLineage,
    first: bool,
    previous_manifest: Option<&ContentManifest>,
    stage_timings: bool,
) -> Result<()> {
    if !first {
        let before = dataset.version().version;
        native_step(
            lease,
            dataset.optimize_indices(&OptimizeOptions::append()),
            WriterError::IndexBuildFailed,
        )
        .await?;
        record_transaction(dataset, lineage, before).await?;
    }
    let fts_changed = previous_manifest.is_none_or(|m| m.fts != config.namespace.fts_settings());
    let count = if first {
        dataset.count_rows(None).await?
    } else {
        0
    };
    if first {
        // Boxed: the build's future is already at the compiler's depth limit.
        Box::pin(add_attribute_columns(config, lease, dataset, lineage)).await?;
    }
    let vector = vector_index_params(
        count,
        config.namespace.dense_rq_bits,
        config.namespace.dense_partition_rows,
        config.namespace.dense_metric,
    );
    let scalar = ScalarIndexParams::default();
    let fts = fts_params(config)?;
    for &(column, name, kind, _) in INDEXES {
        if (name == "fts_idx" && !fts_changed) || (name != "fts_idx" && !first) {
            continue;
        }
        let params: &dyn lance_index::IndexParams = match name {
            "vector_idx" => &vector,
            "fts_idx" => &fts,
            _ => &scalar,
        };
        let start = std::time::Instant::now();
        let before = dataset.version().version;
        native_step(
            lease,
            dataset.create_index(&[column], kind, Some(name.into()), params, true),
            WriterError::IndexBuildFailed,
        )
        .await
        .inspect_err(|_error| {
            if stage_timings {
                eprintln!("writer_stage stage={name} status=failed");
            }
        })?;
        record_transaction(dataset, lineage, before).await?;
        stage(stage_timings, name, start);
    }
    // Boxed for the same reason.
    Box::pin(build_vector_indexes(config, lease, dataset, lineage)).await?;
    if first {
        // Boxed for the same reason.
        Box::pin(build_attribute_indexes(
            config,
            lease,
            dataset,
            lineage,
            &AttributeIndexes::new(),
        ))
        .await?;
    }
    if config.writer.commit.merge_fts_segments {
        let start = std::time::Instant::now();
        let before = dataset.version().version;
        let mut options = OptimizeOptions::merge(usize::MAX);
        options.index_names = Some(vec!["fts_idx".into()]);
        native_step(
            lease,
            dataset.optimize_indices(&options),
            WriterError::IndexMergeFailed,
        )
        .await?;
        record_transaction(dataset, lineage, before).await?;
        stage(stage_timings, "fts_merge", start);
    }
    Ok(())
}

/// Builds the vector index of each named vector attribute that has none and
/// whose column holds a vector. An attribute that no row has a vector for yet
/// has no index, and a search of it scans: the next commit that brings one
/// builds it, and `optimize_indices` indexes every later row.
async fn build_vector_indexes(
    config: &Config,
    lease: &LeaseGuard,
    dataset: &mut Dataset,
    lineage: &mut CommitLineage,
) -> Result<()> {
    if config.namespace.vectors.is_empty() {
        return Ok(());
    }
    let present: BTreeSet<_> = dataset
        .load_indices()
        .await?
        .iter()
        .map(|index| index.name.clone())
        .collect();
    for name in config.namespace.vectors.keys() {
        let index = fyrr_core::attributes::vector_index_name(name);
        if present.contains(&index) {
            continue;
        }
        let rows = dataset
            // Backticks make the name a column whatever it spells (`null`, `order`).
            .count_rows(Some(format!("`{name}` IS NOT NULL")))
            .await?;
        if rows == 0 {
            continue;
        }
        // shortcut: a multi-vector attribute sizes its partitions by the rows
        // that hold vectors, not by the vectors, so it has fewer partitions than
        // its vectors would fill (Lance caps them by the vectors it counts);
        // size them by the vectors if a search of such a column proves slow.
        let params = vector_index_params(
            rows,
            config.namespace.dense_rq_bits,
            config.namespace.dense_partition_rows,
            config.namespace.dense_metric,
        );
        let before = dataset.version().version;
        native_step(
            lease,
            dataset.create_index(
                &[name.as_str()],
                IndexType::Vector,
                Some(index),
                &params,
                true,
            ),
            WriterError::IndexBuildFailed,
        )
        .await?;
        record_transaction(dataset, lineage, before).await?;
    }
    Ok(())
}

/// Builds the indexes the attributes are to have that the table does not have
/// yet: all of them in a first publication, the ones switched on since in
/// maintenance. A full-text index that `built` (the options its manifest
/// records) shows built with another analyzer is built again: the table keeps
/// the old index until this work is published, and a reader serves that
/// publication's version, so the old one serves meanwhile. Until an index is
/// built, a filter on its attribute is a scan. Returns the attributes whose
/// full-text index is built again.
pub(crate) async fn build_attribute_indexes(
    config: &Config,
    lease: &LeaseGuard,
    dataset: &mut Dataset,
    lineage: &mut CommitLineage,
    built: &AttributeIndexes,
) -> Result<Vec<String>> {
    let present: BTreeSet<_> = dataset
        .load_indices()
        .await?
        .iter()
        .map(|index| index.name.clone())
        .collect();
    let stale: BTreeSet<_> = stale_analyzers(&config.namespace.attribute_indexes, built).collect();
    let scalar = ScalarIndexParams::default();
    let mut rebuilt = Vec::new();
    for (column, name, kind) in fyrr_reader::search::attribute_indexes(
        &config.namespace.attributes,
        &config.namespace.attribute_indexes,
    ) {
        let again = kind == IndexType::Inverted && stale.contains(column.as_str());
        if present.contains(&name) && !again {
            continue;
        }
        let mut fts;
        let params: &dyn lance_index::IndexParams = if kind == IndexType::Inverted {
            // Without an analyzer of its own the index takes the namespace's
            // settings, but not its positions: a phrase reads `searchable_text` only.
            fts = match config
                .namespace
                .attribute_indexes
                .get(&column)
                .and_then(|options| options.analyzer.as_ref())
            {
                Some(analyzer) => with_workers(config, fyrr_reader::analyzer_params(analyzer)?),
                None => fts_params(config)?.with_position(false),
            };
            // A string array has one document per element: a match never spans two.
            if matches!(
                config.namespace.attributes.get(&column),
                Some(AttributeType::Array(_))
            ) {
                fts = fts.document_granularity(DocumentGranularity::ListElement);
            }
            &fts
        } else {
            &scalar
        };
        let before = dataset.version().version;
        native_step(
            lease,
            dataset.create_index(&[column.as_str()], kind, Some(name), params, true),
            WriterError::IndexBuildFailed,
        )
        .await?;
        record_transaction(dataset, lineage, before).await?;
        if again {
            rebuilt.push(column);
        }
    }
    Ok(rebuilt)
}

/// Adds a nullable column for each declared attribute. Lance records it in the
/// table's metadata: no data file is written, and rows read as null until a
/// value is written.
pub(crate) async fn add_attribute_columns(
    config: &Config,
    lease: &LeaseGuard,
    dataset: &mut Dataset,
    lineage: &mut CommitLineage,
) -> Result<()> {
    // A namespace written through the API created them with its table.
    let fields: Vec<_> = config
        .namespace
        .attributes
        .iter()
        .filter(|(name, _)| dataset.schema().field(name).is_none())
        .map(|(name, kind)| arrow_schema::Field::new(name, kind.arrow_type(), true))
        .collect();
    if fields.is_empty() {
        return Ok(());
    }
    let before = dataset.version().version;
    native_step(
        lease,
        dataset.add_columns(
            NewColumnTransform::AllNulls(Arc::new(arrow_schema::Schema::new(fields))),
            None,
            None,
        ),
        WriterError::IndexBuildFailed,
    )
    .await?;
    record_transaction(dataset, lineage, before).await
}

/// Parameters of the full-text index. The partition size
/// (`writer.fts_partition_memory_mb`) is not set here: Lance reads it from the
/// environment (see `threads::apply`) for builds and for index merges alike,
/// which keeps the partition geometry through both.
fn fts_params(config: &Config) -> Result<lance_index::scalar::InvertedIndexParams> {
    Ok(with_workers(
        config,
        fyrr_reader::fts_params(&config.namespace)?,
    ))
}

fn with_workers(
    config: &Config,
    fts: lance_index::scalar::InvertedIndexParams,
) -> lance_index::scalar::InvertedIndexParams {
    match config.writer.threads {
        Some(threads) => fts.num_workers(crate::threads::pools(threads).fts_workers),
        None => fts,
    }
}

#[cfg(test)]
#[path = "group/fts_partition_tests.rs"]
mod fts_partition_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::types::{Float16Type, Float32Type};
    use arrow_array::{ArrayRef, FixedSizeListArray, RecordBatch, RecordBatchIterator};
    use arrow_schema::{Field, Schema};
    use fyrr_core::config::{DenseElement, DenseMetric};

    const DIM: i32 = 32;
    const ROWS: usize = 600;

    async fn index_stats(tag: &str, rq_bits: u8, partition_rows: u32) -> serde_json::Value {
        index_stats_of(
            tag,
            rq_bits,
            partition_rows,
            DenseMetric::Cosine,
            DenseElement::F16,
        )
        .await
    }

    async fn index_stats_of(
        tag: &str,
        rq_bits: u8,
        partition_rows: u32,
        metric: DenseMetric,
        element: DenseElement,
    ) -> serde_json::Value {
        let number = |row: usize, d: usize| ((row * 7 + d * 3) % 17) as f32 - 8.0;
        let vectors: ArrayRef = match element {
            DenseElement::F16 => Arc::new(FixedSizeListArray::from_iter_primitive::<
                Float16Type,
                _,
                _,
            >(
                (0..ROWS).map(|row| {
                    Some((0..DIM as usize).map(move |d| Some(half::f16::from_f32(number(row, d)))))
                }),
                DIM,
            )),
            DenseElement::F32 => Arc::new(FixedSizeListArray::from_iter_primitive::<
                Float32Type,
                _,
                _,
            >(
                (0..ROWS).map(|row| Some((0..DIM as usize).map(move |d| Some(number(row, d))))),
                DIM,
            )),
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            vectors.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/lance-pin")
            .join(format!("{tag}-{}", std::process::id()));
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            root.to_str().unwrap(),
            None,
        )
        .await
        .unwrap();
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("vector_idx".into()),
                &vector_index_params(ROWS, rq_bits, partition_rows, metric),
                true,
            )
            .await
            .unwrap();
        let stats =
            serde_json::from_str(&dataset.index_statistics("vector_idx").await.unwrap()).unwrap();
        std::fs::remove_dir_all(root).unwrap();
        stats
    }

    #[tokio::test]
    async fn vector_index_uses_the_configured_rabitq_bits() {
        for bits in [1u8, 3] {
            let stats = index_stats(&format!("rq-bits-{bits}"), bits, 550).await;
            assert_eq!(
                stats["indices"][0]["sub_index"]["num_bits"], bits,
                "{stats}"
            );
        }
    }

    /// Every metric builds an IVF_RQ index over both element types in the
    /// pinned Lance, and the index records the metric it was built with.
    #[tokio::test]
    async fn vector_index_uses_the_configured_metric_on_both_element_types() {
        for (metric, name) in [
            (DenseMetric::Cosine, "cosine"),
            (DenseMetric::SquaredL2, "l2"),
            (DenseMetric::Dot, "dot"),
        ] {
            for element in [DenseElement::F16, DenseElement::F32] {
                let stats = index_stats_of(
                    &format!("metric-{name}-{element:?}"),
                    1,
                    550,
                    metric,
                    element,
                )
                .await;
                assert_eq!(stats["indices"][0]["metric_type"], name, "{stats}");
            }
        }
    }

    #[tokio::test]
    async fn vector_index_uses_the_configured_partition_rows() {
        for (partition_rows, partitions) in [(550u32, 2), (100, 6), (64, 10)] {
            let stats = index_stats(
                &format!("partition-rows-{partition_rows}"),
                1,
                partition_rows,
            )
            .await;
            assert_eq!(stats["indices"][0]["num_partitions"], partitions, "{stats}");
        }
    }
}
