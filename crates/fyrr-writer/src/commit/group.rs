use super::publication::previous_manifest;
use super::*;

mod native;

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
    let previous_manifest = previous_manifest(config, store.as_ref(), &root, &head).await?;
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
    let next_identity = refresh_identity(
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
    if !next_identity
        .inputs
        .iter()
        .any(|input| input.kind == IdentityInputKind::Directory)
    {
        head.freshness.directory = None;
    }
    let data_start = std::time::Instant::now();
    let mut work = native::ContentWork::open(config, lease, &head, acl_changed).await?;
    let fts_merge_needed = work.fts_merge_needed().await?;
    if envelopes.is_empty()
        && work.lineage.transactions.is_empty()
        && rejected == head.rejected_envelope_ids
        && (!policy_changed || head.content.is_none())
        && !fts_merge_needed
    {
        head.updated_at = Utc::now();
        lease.publish(head.clone()).await?;
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
    work.apply_non_content(&mut head, &blocked, non_content, &mut receipts, sequence)
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
        head.updated_at = Utc::now();
        head.rejected_envelope_ids = rejected;
        lease.publish(head).await?;
        return Ok(0);
    };
    if !verify_content(config, &dataset, &head, &envelopes, &mut rejected, sequence).await? {
        head.rejected_envelope_ids = rejected;
        head.updated_at = Utc::now();
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
        } else if let Some(reason) = markers.refusal(&envelope) {
            Some(reason)
        } else if (Utc::now() - envelope.created_at)
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

async fn refresh_identity(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &mut ServingHead,
    envelopes: &[PreparedEnvelope],
    history: Option<&RestoreHistory>,
) -> Result<IdentityArtifact> {
    let current_identity: IdentityArtifact =
        fyrr_reader::storage::artifact(store, root, &head.identity).await?;
    let identity_generation = head
        .identity
        .generation
        .checked_add(1)
        .ok_or(WriterError::IdentityGenerationOverflow)?;
    let configured = configured_identity(config, identity_generation, Utc::now())?;
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
    if identity_changed || next_identity.inputs != current_identity.inputs {
        head.identity = crate::prepare::put_generation(
            store,
            root,
            "identity",
            next_identity.generation,
            &next_identity,
        )
        .await?;
    }
    Ok(next_identity)
}

async fn refresh_policy(
    config: &Config,
    store: &dyn ObjectStore,
    root: &Path,
    head: &mut ServingHead,
    current_policy: &PolicyArtifact,
    envelopes: &[PreparedEnvelope],
) -> Result<(bool, bool)> {
    let source_grants = fixed_grants(config)?;
    let policy_changed = current_policy.retrieval_policy_digest
        != config.namespace.retrieval_digest()
        || current_policy.fixed_grants != source_grants;
    let acl_changed = envelopes
        .iter()
        .any(|e| matches!(&e.payload,EnvelopePayload::Permissions(p) if !p.documents.is_empty()));
    if policy_changed {
        let mut policy = current_policy.clone();
        policy.schema_version = <PolicyArtifact as fyrr_core::Contract>::VERSION;
        policy.fixed_grants = source_grants;
        policy.generation = head
            .policy
            .generation
            .checked_add(1)
            .ok_or(WriterError::PolicyGenerationOverflow)?;
        policy.retrieval_policy_digest = config.namespace.retrieval_digest();
        head.policy =
            crate::prepare::put_generation(store, root, "policy", policy.generation, &policy)
                .await?;
    }
    Ok((policy_changed, acl_changed))
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
    let vector = VectorIndexParams::with_ivf_rq_params(
        DistanceType::Cosine,
        IvfBuildParams {
            num_partitions: Some(count.div_ceil(550).max(1)),
            target_partition_size: Some(550),
            sample_rate: 256,
            ..Default::default()
        },
        RQBuildParams::new(1),
    );
    let scalar = ScalarIndexParams::default();
    let fts = lance_index::scalar::InvertedIndexParams::default()
        .base_tokenizer("simple".into())
        .with_position(false)
        .max_token_length(Some(config.namespace.fts_max_token_length as usize))
        .lower_case(true)
        .stem(config.namespace.fts_stem)
        .remove_stop_words(config.namespace.fts_remove_stop_words)
        .ascii_folding(config.namespace.fts_ascii_folding);
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
