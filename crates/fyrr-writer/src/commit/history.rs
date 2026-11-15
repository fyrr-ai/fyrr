use super::*;

// Per-command evidence only: never retain envelope bodies or trust cursor caches.
// Bound aggregate retained history, identity and envelope summary entries.
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

#[derive(Debug, Default)]
pub(crate) struct RestoreHistory {
    pub(super) manifests: BTreeMap<String, u64>,
    pub(super) applied: BTreeSet<String>,
    pub(super) rejected: BTreeSet<String>,
    pub(super) markers: BTreeMap<String, RejectionMarker>,
    pub(super) rewinds: Vec<String>,
    pub(super) identities: BTreeMap<String, String>,
    pub(super) listed: BTreeSet<Path>,
    pub(super) envelopes: BTreeMap<Path, (object_store::ObjectMeta, RejectionMarker, bool)>,
}
impl RestoreHistory {
    fn entry_count(&self) -> usize {
        self.manifests.len()
            + self.applied.len()
            + self.rejected.len()
            + self.markers.len()
            + self.rewinds.len()
            + self.identities.len()
            + self.envelopes.len()
            + self.listed.len()
    }
    fn check_bounds(&self, additional: usize) -> Result<()> {
        if self.entry_count() + additional > RESTORE_HISTORY_LIMIT {
            return Err(WriterError::RestoreHistorySummaryLimit);
        }
        Ok(())
    }
    pub(crate) async fn refresh(&mut self, config: &Config, head: &ServingHead) -> Result<()> {
        let (store, root) = namespace_store(config)?;
        // Commit new summaries only after their entire prefix joins valid history.
        // A rejected prefix must not become a cache hit on a later refresh.
        let mut incoming = Self::default();
        incoming.rejected.extend(
            head.rejected_envelope_ids
                .iter()
                .filter(|id| !self.rejected.contains(*id))
                .cloned(),
        );
        self.check_bounds(incoming.entry_count())?;
        let mut next = head.content.as_ref().map(|p| p.manifest.clone());
        let mut newer_sequence: Option<u64> = None;
        let mut first = true;
        while let Some(path) = next {
            if let Some(&sequence) = self.manifests.get(&path) {
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
            // Strict descent also rejects cycles without a separate visited set.
            if newer_sequence.is_some_and(|newer| m.sequence >= newer) {
                return Err(WriterError::PublicationInvalid);
            }
            newer_sequence = Some(m.sequence);
            if first {
                incoming.rewinds = m.cursor_rewinds;
                first = false;
            }
            incoming.applied.extend(
                m.envelope_ids
                    .into_iter()
                    .filter(|id| !self.applied.contains(id)),
            );
            incoming.rejected.extend(
                m.rejected_envelope_ids
                    .into_iter()
                    .filter(|id| !self.rejected.contains(id)),
            );
            incoming.manifests.insert(path, m.sequence);
            self.check_bounds(incoming.entry_count())?;
            next = m
                .previous_publication_id
                .map(|id| format!("publications/{id}.json"));
        }
        self.manifests.append(&mut incoming.manifests);
        self.applied.append(&mut incoming.applied);
        self.rejected.append(&mut incoming.rejected);
        if !first {
            self.rewinds = incoming.rewinds;
        }
        self.check_bounds(0)
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
                if self.applied.contains(&summary.envelope_id)
                    || self.rejected.contains(&summary.envelope_id)
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
                .identities
                .get(&envelope.envelope_id)
                .is_some_and(|old| old != &hash)
            {
                return Err(WriterError::RestoreHistoryIdentityChanged);
            }
            self.identities.insert(envelope.envelope_id.clone(), hash);
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
            if !self.applied.contains(&envelope.envelope_id)
                && !self.rejected.contains(&envelope.envelope_id)
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
    pub(super) async fn rejection_markers(
        &mut self,
        config: &Config,
    ) -> Result<Vec<RejectionMarker>> {
        let (store, root) = namespace_store(config)?;
        let paths: Vec<_> = self
            .rejected
            .iter()
            .map(|id| format!("rejections/{id}.json"))
            .chain(self.rewinds.iter().cloned())
            .collect();
        for path in &paths {
            if !self.markers.contains_key(path) {
                let marker = fyrr_reader::storage::read_contract(
                    store.as_ref(),
                    &Path::from(format!("{root}/{path}")),
                )
                .await?;
                self.markers.insert(path.clone(), marker);
                self.check_bounds(0)?;
            }
        }
        Ok(paths.iter().map(|p| self.markers[p].clone()).collect())
    }
    pub(super) fn unconsumed(
        &self,
        markers: &[RejectionMarker],
        applying: &[PreparedEnvelope],
    ) -> Vec<RejectionMarker> {
        markers
            .iter()
            .filter(|m| {
                !applying.iter().any(|e| {
                    crate::apply::complete_read(e)
                        && replacement_matches(
                            m,
                            &e.source_id,
                            e.kind,
                            e.rejection_generation,
                            e.restore_sequence,
                        )
                }) && !self.envelopes.iter().any(|(path, (_, e, complete))| {
                    self.listed.contains(path)
                        && *complete
                        && self.applied.contains(&e.envelope_id)
                        && !self.rejected.contains(&e.envelope_id)
                        && replacement_matches(
                            m,
                            &e.source_id,
                            e.kind,
                            e.rejection_generation,
                            e.restore_sequence,
                        )
                })
            })
            .cloned()
            .collect()
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
}

#[cfg(test)]
mod sequence_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "real S3 cached history; scripts/ci"]
    async fn cached_history_checks_sequence_at_joins_on_real_s3() {
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
            let mut history = RestoreHistory::default();
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
