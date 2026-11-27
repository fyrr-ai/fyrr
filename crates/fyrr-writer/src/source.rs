//! Retiring and purging a source, under the namespace
//! lease. Retirement is one entry in the policy artifact: readers deny the
//! source on their next request, the committer rejects its envelopes, a
//! worker refuses to sync it, and a restore cannot bring its rows back. A
//! purge retires the source and removes its rows, envelopes and cursors; old
//! Lance versions leave within the purge window like any deleted content.
use crate::{Result, WriterError};
use crate::{apply, commit, lease::LeaseGuard, maintenance};
use fyrr_core::config::Config;
use fyrr_core::connector::SyncKind;
use fyrr_core::publication::{MAX_RETIRED_SOURCES, PolicyArtifact, RetiredSource, ServingHead};
use fyrr_reader::storage::{artifact, namespace_store, optional_head, pinned, sql};
use object_store::ObjectStoreExt;

const KINDS: [SyncKind; 4] = [
    SyncKind::Content,
    SyncKind::Permissions,
    SyncKind::Members,
    SyncKind::Reconcile,
];

/// Whether the published policy retires `source`.
pub async fn retired(config: &Config, source: &str) -> Result<bool> {
    let Some(head) = optional_head(config).await? else {
        return Ok(false);
    };
    let (store, root) = namespace_store(config)?;
    let policy: PolicyArtifact = artifact(store.as_ref(), &root, &head.policy).await?;
    Ok(policy.retired_sources.contains_key(source))
}

/// Every row of the source: its content and its document-state markers.
pub(crate) fn rows_filter(source: &str) -> String {
    format!(
        "source_id IN ({}, {})",
        sql(source),
        sql(&apply::state_source(source))
    )
}

fn cursor_path(
    root: &object_store::path::Path,
    source: &str,
    kind: SyncKind,
) -> object_store::path::Path {
    root.clone()
        .join("cursors")
        .join(format!("{source}-{}.json", kind.as_str()))
}

/// What the namespace holds of the source: rows, envelope paths, cursor paths.
async fn holdings(
    config: &Config,
    head: &ServingHead,
    source: &str,
) -> Result<
    (
        usize,
        Vec<object_store::path::Path>,
        Vec<object_store::path::Path>,
    ),
    WriterError,
> {
    let (store, root) = namespace_store(config)?;
    let rows = if head.content.is_some() {
        pinned(config, head)
            .await?
            .count_rows(Some(rows_filter(source)))
            .await
            .map_err(|_| WriterError::ChangedRowScanFailed)?
    } else {
        0
    };
    let (paths, _) = commit::envelope_paths(config, head).await?;
    let envelopes = commit::read_envelopes(config, &paths)
        .await?
        .0
        .into_iter()
        .filter(|(_, envelope)| envelope.source_id == source)
        .map(|(path, _)| path)
        .collect();
    let mut cursors = Vec::new();
    for kind in KINDS {
        let path = cursor_path(&root, source, kind);
        match store.head(&path).await {
            Ok(_) => cursors.push(path),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(_) => return Err(WriterError::StorageUnavailable),
        }
    }
    Ok((rows, envelopes, cursors))
}

/// Write the source's policy entry and move the head to it. Returns whether
/// anything changed: a repeated call is a no-op.
async fn mark(config: &Config, lease: &LeaseGuard, source: &str, purged: bool) -> Result<bool> {
    let (store, root) = namespace_store(config)?;
    let head = lease.head().await?;
    let mut policy: PolicyArtifact = artifact(store.as_ref(), &root, &head.policy).await?;
    let known = policy.retired_sources.contains_key(source);
    if !known && policy.retired_sources.len() >= MAX_RETIRED_SOURCES {
        return Err(WriterError::RetiredSourcesFull);
    }
    let now = fyrr_core::clock::now();
    let entry = policy
        .retired_sources
        .entry(source.to_owned())
        .or_insert(RetiredSource {
            retired_at: now,
            purged_at: None,
        });
    let purge = purged && entry.purged_at.is_none();
    if purge {
        entry.purged_at = Some(now);
    }
    if known && !purge {
        return Ok(false);
    }
    commit::publish_policy(config, lease, head, policy).await?;
    Ok(true)
}

/// A source this namespace knows: configured, or with rows, envelopes or
/// cursors. A mistyped ID must not take one of the retirement entries.
async fn known(config: &Config, head: &ServingHead, source: &str) -> Result<()> {
    fyrr_core::limits::check_id("source", source).map_err(|_| WriterError::SourceInvalid)?;
    let (rows, envelopes, cursors) = holdings(config, head, source).await?;
    if config.sources.iter().any(|s| s.id == source)
        || rows > 0
        || !envelopes.is_empty()
        || !cursors.is_empty()
        || retired(config, source).await?
    {
        Ok(())
    } else {
        Err(WriterError::SourceUnknown)
    }
}

pub async fn retire(config: &Config, source: &str) -> Result<serde_json::Value> {
    LeaseGuard::with(config, |lease| async move {
        known(config, &lease.head().await?, source).await?;
        let changed = mark(config, &lease, source, false).await?;
        Ok(serde_json::json!({"source_id":source,"retired":true,"changed":changed}))
    })
    .await
}

/// Without `confirm`: the plan, and nothing changes. With it: retire, delete
/// the rows in one publication, then the envelopes and cursors, then record
/// the purge. Every step is a no-op once done, so a purge that was cut short
/// is simply run again.
pub async fn purge(config: &Config, source: &str, confirm: bool) -> Result<serde_json::Value> {
    LeaseGuard::with(config, |lease| async move {
        let head = lease.head().await?;
        known(config, &head, source).await?;
        let (rows, envelopes, cursors) = holdings(config, &head, source).await?;
        // Staging files cannot be attributed to one source. Report namespace-wide
        // abandoned uploads explicitly, rather than omitting unlisted document text.
        let staging = maintenance::local_staging::plan(config).await?;
        let plan = serde_json::json!({"source_id":source,"rows":rows,"envelopes":envelopes.len(),"cursors":cursors.len(),"namespace_staging_files":staging.len()});
        if !confirm {
            let mut plan = plan;
            plan["dry_run"] = true.into();
            return Ok(plan);
        }
        mark(config, &lease, source, false).await?;
        // Recover unpublished native versions through the committer before
        // starting a new delete lineage from the serving head.
        commit::commit_one(config, false, &lease, Vec::new(), None).await?;
        if rows > 0 {
            let head = lease.head().await?;
            let mut dataset = pinned(config, &head).await?;
            let mut work = maintenance::lineage(&lease, &head);
            commit::apply_staged(
                config,
                &mut dataset,
                &lease,
                &mut work,
                vec![rows_filter(source)],
                Vec::new(),
            )
            .await?;
            commit::publish(
                config,
                &lease,
                &dataset,
                head,
                work,
                Vec::new(),
                Default::default(),
                true,
                Vec::new(),
                false,
                None,
            )
            .await?;
        }
        let (store, _) = namespace_store(config)?;
        let deletion_store = store.as_ref();
        crate::io::bounded(envelopes.iter().chain(&cursors), |path| {
            let lease = &lease;
            async move {
                lease.head().await?;
                match deletion_store.delete(path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(_) => Err(WriterError::StorageUnavailable),
                }
            }
        }).await?;
        // The identity is rebuilt without the source's principal snapshot.
        commit::commit_one(config, false, &lease, Vec::new(), None).await?;
        mark(config, &lease, source, true).await?;
        let mut plan = plan;
        plan["deleted_staging_files"] =
            maintenance::local_staging::remove(config, &lease, staging).await?.into();
        plan["purged"] = true.into();
        Ok(plan)
    })
    .await
}
