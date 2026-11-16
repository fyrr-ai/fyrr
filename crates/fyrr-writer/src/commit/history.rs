use super::*;

// Per-command evidence only: never retain envelope bodies or trust cursor caches.
// Bound resident active-operation summaries; lifetime evidence lives on disk.
const RESTORE_HISTORY_LIMIT: usize = 65_536;

// Callers separately prove that the replacement is a complete, authoritative read.
pub(super) fn replacement_matches(
    rejected: &RejectionMarker,
    source_id: &str,
    kind: fyrr_core::connector::SyncKind,
    rejection_generation: u32,
    restore_sequence: u64,
) -> bool {
    source_id == rejected.source_id
        && kind == rejected.kind
        && rejection_generation > rejected.rejection_generation
        && restore_sequence >= rejected.restore_sequence
}

mod index;
use index::{Evidence, Index};

#[derive(Debug)]
pub(crate) struct RestoreHistory {
    index: Index,
    rewinds: Vec<String>,
    pub(super) listed: BTreeSet<Path>,
    pub(super) envelopes: BTreeMap<Path, (object_store::ObjectMeta, RejectionMarker, bool)>,
}
impl RestoreHistory {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            index: Index::new(config)?,
            rewinds: Vec::new(),
            listed: BTreeSet::new(),
            envelopes: BTreeMap::new(),
        })
    }
    fn check_bounds(&self, additional: usize) -> Result<()> {
        if self.rewinds.len() + self.envelopes.len() + self.listed.len() + additional
            > RESTORE_HISTORY_LIMIT
        {
            return Err(WriterError::RestoreHistorySummaryLimit);
        }
        Ok(())
    }
    pub(crate) async fn refresh(&mut self, config: &Config, head: &ServingHead) -> Result<()> {
        let (store, root) = namespace_store(config)?;
        // Only fully validated prefixes enter the committed index. Staging is
        // invisible to consumers, including after repeated invalid joins.
        self.index.begin()?;
        for id in &head.rejected_envelope_ids {
            self.index.put(Evidence::Rejected, id, &(), true)?;
        }
        let mut next = head.content.as_ref().map(|p| p.manifest.clone());
        let mut newer_sequence: Option<u64> = None;
        let mut rewinds = None;
        while let Some(path) = next {
            if let Some(sequence) = self.index.get::<u64>(Evidence::Manifest, &path)? {
                if newer_sequence.is_some_and(|newer| sequence >= newer) {
                    return Err(WriterError::PublicationInvalid);
                }
                break;
            }
            let m: ContentManifest = fyrr_reader::storage::read_contract(
                store.as_ref(),
                &Path::from(format!("{root}/{path}")),
            )
            .await?;
            if newer_sequence.is_some_and(|newer| m.sequence >= newer) {
                return Err(WriterError::PublicationInvalid);
            }
            newer_sequence = Some(m.sequence);
            if rewinds.is_none() {
                if self.envelopes.len() + self.listed.len() + m.cursor_rewinds.len()
                    > RESTORE_HISTORY_LIMIT
                {
                    return Err(WriterError::RestoreHistorySummaryLimit);
                }
                rewinds = Some(m.cursor_rewinds);
            }
            for id in m.envelope_ids {
                self.index.put(Evidence::Applied, &id, &(), true)?;
            }
            for id in m.rejected_envelope_ids {
                self.index.put(Evidence::Rejected, &id, &(), true)?;
            }
            self.index
                .put(Evidence::Manifest, &path, &m.sequence, true)?;
            next = m
                .previous_publication_id
                .map(|id| format!("publications/{id}.json"));
        }
        self.index.promote()?;
        if let Some(rewinds) = rewinds {
            self.rewinds = rewinds;
        }
        self.check_bounds(0)
    }
    pub(super) fn identity(&self, id: &str) -> Result<Option<String>> {
        self.index.get(Evidence::Identity, id)
    }
    pub(crate) async fn pending(
        &mut self,
        config: &Config,
        authorization: bool,
    ) -> Result<Vec<PreparedEnvelope>> {
        let (store, root) = namespace_store(config)?;
        let prefix = root.join("envelopes");
        let mut listing = store.list(Some(&prefix));
        let mut pending = Vec::new();
        self.listed.clear();
        while let Some(object) = listing.try_next().await? {
            let path = object.location.clone();
            self.listed.insert(path.clone());
            self.check_bounds(0)?;
            if let Some((old, summary, _)) = self.envelopes.get(&path) {
                if old != &object {
                    return Err(WriterError::RestoreHistoryIdentityChanged);
                }
                if self
                    .index
                    .contains(Evidence::Applied, &summary.envelope_id)?
                    || self
                        .index
                        .contains(Evidence::Rejected, &summary.envelope_id)?
                    || (authorization && !summary.kind.is_authorization())
                {
                    continue;
                }
            }
            let bytes = store
                .get_opts(
                    &path,
                    object_store::GetOptions {
                        if_match: object.e_tag.clone(),
                        ..Default::default()
                    },
                )
                .await?
                .bytes()
                .await?;
            let envelope: PreparedEnvelope =
                fyrr_core::parse_contract(&bytes).map_err(|_| WriterError::PublicationInvalid)?;
            if envelope.namespace != config.namespace.name {
                return Err(WriterError::EnvelopeNamespaceInvalid);
            }
            let hash = digest(&bytes);
            if self
                .identity(&envelope.envelope_id)?
                .is_some_and(|old| old != hash)
            {
                return Err(WriterError::RestoreHistoryIdentityChanged);
            }
            self.index
                .put(Evidence::Identity, &envelope.envelope_id, &hash, false)?;
            let summary = RejectionMarker {
                schema_version: envelope.schema_version,
                envelope_id: envelope.envelope_id.clone(),
                source_id: envelope.source_id.clone(),
                kind: envelope.kind,
                sequence: envelope.sequence,
                rejection_generation: envelope.rejection_generation,
                restore_sequence: envelope.restore_sequence,
                start_cursor: None,
                reason: "restore_rewind".into(),
            };
            self.envelopes.insert(
                path,
                (object, summary, crate::apply::complete_read(&envelope)),
            );
            self.check_bounds(0)?;
            if !self
                .index
                .contains(Evidence::Applied, &envelope.envelope_id)?
                && !self
                    .index
                    .contains(Evidence::Rejected, &envelope.envelope_id)?
                && (!authorization || envelope.kind.is_authorization())
            {
                pending.push(envelope);
            }
        }
        pending.sort_by(|a, b| (&a.source_id, a.sequence).cmp(&(&b.source_id, b.sequence)));
        Ok(pending)
    }
    pub(crate) fn summaries(&self) -> impl Iterator<Item = &RejectionMarker> {
        self.envelopes
            .iter()
            .filter(|(p, _)| self.listed.contains(p))
            .map(|(_, (_, s, _))| s)
    }
    async fn visit_markers(
        &self,
        config: &Config,
        mut visit: impl FnMut(&Self, RejectionMarker) -> Result<()> + Send,
    ) -> Result<()> {
        let (store, root) = namespace_store(config)?;
        let mut after = String::new();
        while let Some(id) = self.index.next_rejected(&after)? {
            after = id;
            let path = format!("rejections/{after}.json");
            visit(self, self.marker(store.as_ref(), &root, &path).await?)?;
        }
        for path in &self.rewinds {
            visit(self, self.marker(store.as_ref(), &root, path).await?)?;
        }
        Ok(())
    }
    async fn marker(
        &self,
        store: &dyn ObjectStore,
        root: &Path,
        path: &str,
    ) -> Result<RejectionMarker> {
        if let Some(marker) = self.index.get(Evidence::Marker, path)? {
            return Ok(marker);
        }
        let marker =
            fyrr_reader::storage::read_contract(store, &Path::from(format!("{root}/{path}")))
                .await?;
        self.index.put(Evidence::Marker, path, &marker, false)?;
        Ok(marker)
    }
    pub(super) async fn fences(
        &self,
        config: &Config,
        candidates: &[PreparedEnvelope],
    ) -> Result<RejectionFences> {
        let mut fences = RejectionFences::new(candidates)?;
        self.visit_markers(config, |_, marker| fences.observe(&marker))
            .await?;
        Ok(fences)
    }
    pub(super) async fn unconsumed_rejections(
        &self,
        config: &Config,
        applying: &[PreparedEnvelope],
    ) -> Result<Vec<RejectionMarker>> {
        let mut unresolved = BTreeMap::new();
        self.visit_markers(config, |history, marker| {
            if !history.consumed(&marker, applying)? {
                let key = (marker.source_id.clone(), marker.kind.as_str());
                // Consumers block authorization sources and prioritize a
                // replacement newer than ANY unresolved generation: its minimum
                // is sufficient. Each original fence is evaluated before folding.
                let old = unresolved.entry(key).or_insert_with(|| marker.clone());
                if marker.rejection_generation < old.rejection_generation {
                    *old = marker;
                }
                history.check_bounds(unresolved.len())?;
            }
            Ok(())
        })
        .await?;
        Ok(unresolved.into_values().collect())
    }
    fn consumed(&self, marker: &RejectionMarker, applying: &[PreparedEnvelope]) -> Result<bool> {
        if applying.iter().any(|e| {
            crate::apply::complete_read(e)
                && replacement_matches(
                    marker,
                    &e.source_id,
                    e.kind,
                    e.rejection_generation,
                    e.restore_sequence,
                )
        }) {
            return Ok(true);
        }
        for (path, (_, e, complete)) in &self.envelopes {
            if self.listed.contains(path)
                && *complete
                && replacement_matches(
                    marker,
                    &e.source_id,
                    e.kind,
                    e.rejection_generation,
                    e.restore_sequence,
                )
                && self.index.contains(Evidence::Applied, &e.envelope_id)?
                && !self.index.contains(Evidence::Rejected, &e.envelope_id)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[derive(Debug)]
pub(super) struct RejectionFences {
    by_kind: BTreeMap<(String, &'static str), Fence>,
    wanted: BTreeSet<(String, &'static str)>,
}
#[derive(Debug, Default)]
struct Fence {
    restore_sequence: u64,
    authorization_restore_sequence: u64,
    rejection_generation: Option<u32>,
}
impl RejectionFences {
    pub(super) fn new(candidates: &[PreparedEnvelope]) -> Result<Self> {
        let mut wanted = BTreeSet::new();
        for envelope in candidates {
            wanted.insert((envelope.source_id.clone(), envelope.kind.as_str()));
            if wanted.len() > RESTORE_HISTORY_LIMIT {
                return Err(WriterError::RestoreHistorySummaryLimit);
            }
        }
        Ok(Self {
            wanted,
            by_kind: BTreeMap::new(),
        })
    }
    pub(super) fn observe(&mut self, marker: &RejectionMarker) -> Result<()> {
        let key = (marker.source_id.clone(), marker.kind.as_str());
        if !self.wanted.contains(&key) {
            return Ok(());
        }
        let fence = self.by_kind.entry(key).or_default();
        fence.restore_sequence = fence.restore_sequence.max(marker.restore_sequence);
        if marker.reason != "restore_rewind" {
            fence.authorization_restore_sequence = fence
                .authorization_restore_sequence
                .max(marker.restore_sequence);
            fence.rejection_generation = Some(
                fence
                    .rejection_generation
                    .map_or(marker.rejection_generation, |n| {
                        n.max(marker.rejection_generation)
                    }),
            );
        }
        if self.by_kind.len() > RESTORE_HISTORY_LIMIT {
            return Err(WriterError::RestoreHistorySummaryLimit);
        }
        Ok(())
    }
    pub(super) fn refusal(&self, envelope: &PreparedEnvelope) -> Option<&'static str> {
        let fence = self
            .by_kind
            .get(&(envelope.source_id.clone(), envelope.kind.as_str()))?;
        let authorization = envelope.kind.is_authorization();
        if envelope.restore_sequence
            < if authorization {
                fence.authorization_restore_sequence
            } else {
                fence.restore_sequence
            }
        {
            Some("restore_refetch")
        } else if authorization
            && fence
                .rejection_generation
                .is_some_and(|n| envelope.rejection_generation <= n)
        {
            Some("authorization_rewind_required")
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fyrr_core::connector::SyncKind::{Members, Permissions};

    #[test]
    fn replacement_read_preserves_rejection_fences() {
        let rejected = RejectionMarker {
            schema_version: ENVELOPE_CONTRACT_VERSION,
            envelope_id: "fixture".into(),
            source_id: "source".into(),
            kind: Permissions,
            sequence: 99,
            rejection_generation: 3,
            restore_sequence: 7,
            start_cursor: None,
            reason: "fixture".into(),
        };
        for (source, kind, generation, restore, expected) in [
            ("other", Permissions, 4, 7, false),
            ("source", Members, 4, 7, false),
            ("source", Permissions, 2, 7, false),
            ("source", Permissions, 3, 7, false),
            ("source", Permissions, 4, 6, false),
            ("source", Permissions, 4, 7, true),
            ("source", Permissions, 4, 8, true),
            ("source", Permissions, u32::MAX, u64::MAX, true),
        ] {
            assert_eq!(
                replacement_matches(&rejected, source, kind, generation, restore),
                expected,
                "{source} {kind:?} generation={generation} restore={restore}"
            );
        }
        let exhausted = RejectionMarker {
            rejection_generation: u32::MAX,
            ..rejected
        };
        assert!(!replacement_matches(
            &exhausted,
            "source",
            Permissions,
            u32::MAX,
            u64::MAX,
        ));
    }

    fn envelope(kind: &str) -> PreparedEnvelope {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../../fixtures/contracts/prepared-envelope/{kind}.json"
        ));
        let mut envelope: PreparedEnvelope =
            fyrr_core::parse_contract(&std::fs::read(path).unwrap()).unwrap();
        envelope.source_id = "source".into();
        envelope
    }

    fn marker() -> RejectionMarker {
        RejectionMarker {
            schema_version: ENVELOPE_CONTRACT_VERSION,
            envelope_id: "env-00000000000000000000000000000001".into(),
            source_id: "source".into(),
            kind: Permissions,
            sequence: 1,
            rejection_generation: 3,
            restore_sequence: 7,
            start_cursor: None,
            reason: "fixture".into(),
        }
    }

    #[test]
    fn acceptance_fences_preserve_kind_restore_and_generation_boundaries() {
        for (kind, reason, generation, restore, expected) in [
            ("content", "restore_rewind", 0, 6, Some("restore_refetch")),
            ("content", "fixture", 0, 7, None),
            ("permissions", "restore_rewind", 3, 6, None),
            ("permissions", "fixture", 3, 6, Some("restore_refetch")),
            (
                "permissions",
                "fixture",
                3,
                7,
                Some("authorization_rewind_required"),
            ),
            ("permissions", "fixture", 4, 7, None),
            ("members", "fixture", 4, 6, Some("restore_refetch")),
            ("reconcile", "fixture", 0, 7, None),
        ] {
            let mut envelope = envelope(kind);
            envelope.rejection_generation = generation;
            envelope.restore_sequence = restore;
            let mut marker = marker();
            marker.kind = envelope.kind;
            marker.reason = reason.into();
            let mut fences = RejectionFences::new(std::slice::from_ref(&envelope)).unwrap();
            fences.observe(&marker).unwrap();
            assert_eq!(
                fences.refusal(&envelope),
                expected,
                "{kind}/{reason}/{generation}/{restore}"
            );
            marker.source_id = "other".into();
            let mut unrelated = RejectionFences::new(std::slice::from_ref(&envelope)).unwrap();
            unrelated.observe(&marker).unwrap();
            assert_eq!(unrelated.refusal(&envelope), None);
        }
    }

    #[test]
    fn acceptance_fences_ignore_lifetime_source_churn() {
        let envelope = envelope("permissions");
        let mut fences = RejectionFences::new(std::slice::from_ref(&envelope)).unwrap();
        for id in 0..70_000 {
            let mut marker = marker();
            marker.source_id = format!("old-source-{id}");
            fences.observe(&marker).unwrap();
        }
        assert!(fences.by_kind.is_empty());
        assert_eq!(fences.refusal(&envelope), None);
    }

    #[tokio::test]
    async fn unresolved_folding_preserves_each_replacement_fence() {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target")
            .canonicalize()
            .unwrap()
            .join(format!("history-fold-{}", std::process::id()));
        let mut config = fyrr_core::config::parse(&format!(
            "[namespace]\nname='history-fold'\n[storage]\nuri='file://{}'\n[embedding]\nendpoint='http://127.0.0.1:9/v1'\n", directory.display(),
        )).unwrap().config;
        config.reader.mirror_path = Some(
            directory
                .with_file_name("history-fold-mirror")
                .display()
                .to_string(),
        );
        let history = RestoreHistory::new(&config).unwrap();
        for generation in [1_u32, 9] {
            let mut marker = marker();
            marker.envelope_id = format!("env-{generation:032x}");
            marker.rejection_generation = generation;
            history
                .index
                .put(Evidence::Rejected, &marker.envelope_id, &(), false)
                .unwrap();
            history
                .index
                .put(
                    Evidence::Marker,
                    &format!("rejections/{}.json", marker.envelope_id),
                    &marker,
                    false,
                )
                .unwrap();
        }
        let initial = history.unconsumed_rejections(&config, &[]).await.unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].rejection_generation, 1);
        let mut replacement = envelope("permissions");
        replacement.namespace = config.namespace.name.clone();
        for (generation, restore, expected) in [(5, 7, Some(9)), (10, 6, Some(1)), (10, 7, None)] {
            replacement.rejection_generation = generation;
            replacement.restore_sequence = restore;
            let left = history
                .unconsumed_rejections(&config, std::slice::from_ref(&replacement))
                .await
                .unwrap();
            assert_eq!(left.first().map(|m| m.rejection_generation), expected);
        }
        if let EnvelopePayload::Permissions(payload) = &mut replacement.payload {
            payload.complete_read = false;
        }
        let left = history
            .unconsumed_rejections(&config, &[replacement])
            .await
            .unwrap();
        assert_eq!(
            left[0].rejection_generation, 1,
            "an incomplete replacement consumes neither marker"
        );
    }
}

#[cfg(test)]
mod sequence_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "real S3 cached history; scripts/ci"]
    async fn cached_history_checks_sequence_at_joins_on_storage() {
        let config = fyrr_core::config::load(std::path::Path::new(
            &std::env::var_os("FYRR_TEST_HISTORY_CONFIG").unwrap(),
        ))
        .unwrap()
        .config;
        let (store, root) = namespace_store(&config).unwrap();
        let head: ServingHead =
            fyrr_reader::storage::read_contract(store.as_ref(), &root.clone().join("head.json"))
                .await
                .unwrap();
        let template: ContentManifest = fyrr_reader::storage::read_contract(
            store.as_ref(),
            &Path::from(format!(
                "{root}/{}",
                head.content.as_ref().unwrap().manifest
            )),
        )
        .await
        .unwrap();
        assert_eq!(template.sequence, 1);
        let mut cached = template.clone();
        cached.publication_id = "pub-00000000000000000000000000881000".into();
        cached.sequence = 2;
        cached.previous_publication_id = Some(template.publication_id.clone());
        store
            .put(
                &Path::from(format!(
                    "{root}/publications/{}.json",
                    cached.publication_id
                )),
                write_contract(&cached).into(),
            )
            .await
            .unwrap();
        let mut cached_head = head.clone();
        let pointer = cached_head.content.as_mut().unwrap();
        pointer.publication_id = cached.publication_id.clone();
        pointer.manifest = format!("publications/{}.json", cached.publication_id);
        pointer.sequence = cached.sequence;
        for sequence in [3, 2, 1] {
            let mut history = RestoreHistory::new(&config).unwrap();
            history.refresh(&config, &cached_head).await.unwrap();
            history.refresh(&config, &cached_head).await.unwrap();
            let mut parent = template.clone();
            parent.publication_id = format!("pub-{:032x}", 881100 + sequence);
            parent.sequence = sequence;
            parent.previous_publication_id = Some(cached.publication_id.clone());
            store
                .put(
                    &Path::from(format!(
                        "{root}/publications/{}.json",
                        parent.publication_id
                    )),
                    write_contract(&parent).into(),
                )
                .await
                .unwrap();
            let mut prefix_head = head.clone();
            let pointer = prefix_head.content.as_mut().unwrap();
            pointer.publication_id = parent.publication_id.clone();
            pointer.manifest = format!("publications/{}.json", parent.publication_id);
            pointer.sequence = sequence;
            if sequence > cached.sequence {
                history.refresh(&config, &prefix_head).await.unwrap();
            } else {
                for _ in 0..2 {
                    assert_eq!(
                        history.refresh(&config, &prefix_head).await,
                        Err(WriterError::PublicationInvalid),
                        "a malformed prefix must not join previously cached history",
                    );
                }
            }
        }
    }
}
