use super::*;

impl PreparedReader {
    pub(crate) async fn request_prologue(
        &mut self,
    ) -> Result<(Instant, crate::mirror::IoSnapshot, Telemetry, MirrorRequest)> {
        self.filter_selection = None;
        let pin = self.mirror_pin.request();
        let start = Instant::now();
        let before = self.mirror.counters();
        let stage = Instant::now();
        let mut telemetry = Telemetry::default();
        self.refresh_authorization().await?;
        let pin = pin?;
        self.stage(&mut telemetry, "admission", stage, before);
        Ok((start, before, telemetry, pin))
    }

    #[allow(clippy::assigning_clones)] // Release old subject buffers on a cache hit.
    pub(crate) async fn rebuild_admission(&mut self) -> Result<()> {
        // Cancellation must leave the previous mask unadmitted until a complete rebuild.
        self.scope.refuse();
        self.filter_selection = None;
        self.admission_build_us = 0;
        let build_started = Instant::now();
        let key = AuthorizationCacheKey {
            namespace: self.config.namespace.name.clone(),
            subject: self.scope.http_key().cloned().unwrap_or(self.principal()?),
            identity: self.head.identity.digest.clone(),
            identity_generation: self.head.identity.generation,
            policy: self.head.policy.digest.clone(),
            policy_generation: self.head.policy.generation,
            content: self.manifest.lance_version,
            hidden: self.hidden_sources.iter().cloned().collect(),
        };
        if let Some(entry) = self
            .admission_cache
            .lock()
            .map_err(|_| AuthorizationUnavailable)?
            .get(&key)
            && (!self.preparation_reservation.warm()
                || entry.maps.as_ref().is_some_and(|maps| {
                    self.vector_roots.get().is_some_and(|roots| {
                        roots.len() == maps.len()
                            && roots
                                .iter()
                                .zip(maps)
                                .all(|(root, map)| Arc::ptr_eq(root, &map.index))
                    })
                }))
        {
            self.eligible = entry.eligible.clone();
            self.maps = if self.preparation_reservation.warm() {
                entry.maps.clone()
            } else {
                None
            };
            self.unindexed = entry.unindexed.clone();
            self.reported_hidden_sources = entry.reported_hidden_sources.clone();
            self.fallback_probes = entry.fallback_probes;
            self.scope.admitted(self.head.identity.clone());
            return Ok(());
        }
        {
            let indexes = &self.indexes;
            let predicate = Self::predicate(
                &self.config,
                &self.identity,
                &self.policy,
                self.scope.principal(),
            )?;
            let mut eligible = RowAddrTreeMap::new();
            let mut scan = self.dataset.scan();
            scan.project(&["source_id"])
                .map_err(|_| PublicationInvalid)?;
            scan.filter(&predicate)
                .map_err(|_| AuthorizationUnavailable)?
                .with_row_id();
            #[cfg(test)]
            self.test_hooks.admission_scan.wait_once().await;
            let mut stream = scan
                .try_into_stream()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut readable_hidden = BTreeSet::new();
            while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
                let sources = strings(&batch, "source_id")?;
                for (row, id) in ids(&batch)?.values().iter().enumerate() {
                    let source = sources.value(row);
                    if self.hidden_sources.contains(source) {
                        // A retired source is denied for good and never reported.
                        if !self.policy.retired_sources.contains_key(source) {
                            readable_hidden.insert(source.to_owned());
                        }
                    } else {
                        eligible.insert(*id);
                    }
                }
            }
            self.reported_hidden_sources = readable_hidden.into_iter().take(64).collect();
            self.eligible = RowAddrMask::from_allowed(eligible);
            let coverage: BTreeSet<_> = indexes
                .iter()
                .filter(|i| i.name == Layout::VECTOR_INDEX)
                .flat_map(|i| i.fragment_bitmap.iter().flat_map(|b| b.iter()))
                .collect();
            self.unindexed = self
                .eligible
                .iter_addrs()
                .ok_or(AuthorizationUnavailable)?
                .map(u64::from)
                .filter(|id| !coverage.contains(&((id >> 32) as u32)))
                .collect();
        }
        self.maps = None;
        if let Ok(maps) = self.partition_admission().await
            && !maps.is_empty()
        {
            self.maps = Some(maps);
        }
        if let Some(maps) = &self.maps {
            use lance::cache::DeepSizeOf;
            let roots = maps.iter().map(|p| p.index.deep_size_of() as u64).sum();
            if !self.preparation_reservation.settle(roots)? {
                self.maps = None;
                self.vector_roots = Arc::default();
                self.centroid_routers = Arc::default();
                self.generation_state = GenerationState::Cold;
                self.admission_cache
                    .lock()
                    .map_err(|_| AuthorizationUnavailable)?
                    .retain_content(u64::MAX);
            }
        }
        self.admission_build_us = micros(build_started.elapsed());
        self.scope.admitted(self.head.identity.clone());
        let cap = self
            .config
            .reader
            .http
            .as_ref()
            .map_or(16, |h| h.authorization_cache_subjects);
        // Measure borrowed worker buffers. The generation pays for worker copies;
        // this separate allowance pays only for the retained cache copy.
        let incoming = key.memory_bound()
            + admission_memory_bound(
                &self.eligible,
                self.maps.as_deref(),
                &self.unindexed,
                &self.reported_hidden_sources,
            );
        let mut cache = self
            .admission_cache
            .lock()
            .map_err(|_| AuthorizationUnavailable)?;
        if let Some(claim) = cache.reserve(&key, incoming, cap) {
            let entry = CachedAdmission {
                _cache_claim: claim,
                eligible: self.eligible.clone(),
                maps: self.maps.clone(),
                unindexed: self.unindexed.clone(),
                reported_hidden_sources: self.reported_hidden_sources.clone(),
                fallback_probes: self.fallback_probes,
                _reservation: self.preparation_reservation.clone(),
                _native_session: self.native_session.clone(),
            };
            cache.insert(key, entry, cap);
        }
        Ok(())
    }

    async fn partition_admission(&mut self) -> Result<Vec<PartitionMap>> {
        let indexes = &self.indexes;
        let mut maps = Vec::new();

        if !self.preparation_reservation.warm() {
            return Err(PreparationBudgetExhausted);
        }
        let roots = self.vector_roots.clone();
        let roots = roots
            .get_or_try_init(|| async {
                let mut roots = Vec::new();
                for metadata in indexes.iter().filter(|i| i.name == Layout::VECTOR_INDEX) {
                    roots.push(
                        self.dataset
                            .open_vector_index("vector", &metadata.uuid, &NoOpMetricsCollector)
                            .await
                            .map_err(|_| PartitionMapUnavailable)?,
                    );
                }
                Ok::<_, ReaderError>(roots)
            })
            .await?;
        for (metadata, index) in indexes
            .iter()
            .filter(|i| i.name == Layout::VECTOR_INDEX)
            .zip(roots)
        {
            let index = index.clone();
            self.fallback_probes = self.fallback_probes.max(index.total_partitions());
            let mut allowed = Vec::new();
            for partition in 0..index.total_partitions() {
                let mut stream = index
                    .partition_reader(partition, false, &NoOpMetricsCollector)
                    .await
                    .map_err(|_| PartitionMapUnavailable)?;
                let mut permitted = false;
                while let Some(batch) = stream
                    .try_next()
                    .await
                    .map_err(|_| PartitionMapUnavailable)?
                {
                    let batch_ids = ids(&batch)?;
                    for id in batch_ids.values() {
                        // Index membership is only a routing
                        // hint. Row admission remains native.
                        if metadata
                            .fragment_bitmap
                            .as_ref()
                            .is_some_and(|b| b.contains((id >> 32) as u32))
                            && self.eligible.selected(*id)
                        {
                            permitted = true;
                        }
                    }
                }
                allowed.push(permitted);
            }
            maps.push(PartitionMap { index, allowed });
        }
        Ok::<_, ReaderError>(maps)
    }

    pub(crate) fn principal(&self) -> Result<String> {
        if let Some(principal) = self.scope.principal() {
            Ok(principal.to_owned())
        } else {
            hash_principal(
                &self.config,
                "key",
                self.config
                    .identity
                    .stdio_user
                    .as_deref()
                    .ok_or(AuthorizationUnavailable)?,
            )
            .map_err(ReaderError::from)
        }
    }
    pub(crate) async fn bind_subject(
        &mut self,
        subject: &crate::auth::AuthenticatedSubject,
    ) -> Result<()> {
        self.scope.begin(subject);
        self.refresh_authorization().await?;
        if subject.identity.as_ref() != Some(&self.head.identity) {
            return Err(AuthorizationUnavailable);
        }
        if subject.expires <= Utc::now().timestamp() {
            return Err(AuthorizationUnavailable);
        }
        let principal = self
            .identity
            .principal_for_subject(&subject.key)
            .ok_or(AuthorizationUnavailable)?;
        if principal.tokens.len() > self.config.identity.max_tokens_per_user as usize {
            return Err(AuthorizationUnavailable);
        }
        self.scope.resolve(principal.key.clone());
        self.rebuild_admission().await
    }
    pub(crate) fn expire_pin(&self) {
        self.mirror_pin.expire(
            (Utc::now() - self.manifest.committed_at)
                .to_std()
                .unwrap_or_default(),
            self.config.writer.purge_window.as_duration(),
        );
    }

    /// Whether the live head names the pinned publication or a later one of
    /// the same namespace incarnation. A namespace deleted and created again
    /// at the same URI has another incarnation and restarts its sequence.
    pub(crate) fn same_lineage(
        head: &ServingHead,
        publication_id: &str,
        sequence: u64,
        incarnation: &str,
    ) -> bool {
        head.incarnation == incarnation
            && head.content.as_ref().is_some_and(|live| {
                live.sequence > sequence || live.publication_id == publication_id
            })
    }

    pub(crate) fn authorization(&self) -> Result<()> {
        let now = Utc::now();
        if !Self::same_lineage(
            &self.head,
            &self.manifest.publication_id,
            self.manifest.sequence,
            &self.manifest.incarnation,
        ) {
            // Never serve a namespace that no longer exists at this URI.
            self.mirror_pin.retire();
            return Err(AuthorizationUnavailable);
        }
        if self
            .head
            .content
            .as_ref()
            .is_some_and(|p| p.sequence > self.manifest.sequence)
        {
            self.mirror_pin.retire();
        }
        self.expire_pin();
        let expired = self.mirror_pin.expired();
        if self.policy.namespace != self.config.namespace.name
            || self.identity.namespace != self.config.namespace.name
            || self.policy.generation != self.head.policy.generation
            || self.identity.generation != self.head.identity.generation
            || self.identity.built_at > now
            || !directory_fresh(&self.config, self.head.freshness.directory, now)
            || self
                .policy
                .row_acl_sequence
                .is_some_and(|sequence| self.manifest.sequence < sequence)
            || self.policy.retrieval_policy_digest != self.manifest.retrieval_policy_digest
        {
            return Err(AuthorizationUnavailable);
        }
        if expired {
            return Err(StorageUnavailable);
        }
        if self.config.reader.http.is_none() || self.scope.principal().is_some() {
            Self::predicate(
                &self.config,
                &self.identity,
                &self.policy,
                self.scope.principal(),
            )?;
        }
        Ok(())
    }
    /// Derived admission cache only; the published head owns all source freshness instants.
    /// The sources no request may read: those whose permission freshness
    /// expired, until a read succeeds, and sources retired permanently.
    /// One list feeds the admission predicate.
    pub(super) fn hidden(
        config: &Config,
        head: &ServingHead,
        policy: &PolicyArtifact,
    ) -> Result<BTreeSet<String>> {
        let mut hidden = Self::expired_sources(config, head)?;
        hidden.extend(policy.retired_sources.keys().cloned());
        Ok(hidden)
    }

    pub(crate) fn expired_sources(config: &Config, head: &ServingHead) -> Result<BTreeSet<String>> {
        let now = Utc::now();
        let deadline = chrono::Duration::from_std(config.identity.validity_deadline.as_duration())
            .map_err(|_| AuthorizationUnavailable)?;
        Ok(config
            .sources
            .iter()
            .filter(|s| s.permissions == fyrr_core::config::PermissionsMode::Sync)
            .filter(|s| {
                head.freshness
                    .sources
                    .iter()
                    .find(|f| f.source_id == s.id)
                    .is_none_or(|f| {
                        f.permissions_read_at > now
                            || f.permissions_read_at + deadline <= now
                            || (s.intervals.members.is_some()
                                && f.members_read_at
                                    .is_none_or(|at| at > now || at + deadline <= now))
                    })
            })
            .map(|s| s.id.clone())
            .collect())
    }

    pub(crate) fn reported_hidden_sources(&self) -> Vec<String> {
        self.reported_hidden_sources.clone()
    }

    pub(crate) fn admission_predicate(&self) -> Result<String> {
        let predicate = Self::predicate(
            &self.config,
            &self.identity,
            &self.policy,
            self.scope.principal(),
        )?;
        if self.hidden_sources.is_empty() {
            return Ok(predicate);
        }
        Ok(format!(
            "({predicate}) AND source_id NOT IN ({})",
            self.hidden_sources
                .iter()
                .map(|s| sql(s))
                .collect::<Vec<_>>()
                .join(",")
        ))
    }

    pub(super) fn predicate(
        config: &Config,
        identity: &IdentityArtifact,
        policy: &PolicyArtifact,
        principal_key: Option<&str>,
    ) -> Result<String> {
        let key = if let Some(key) = principal_key {
            key.to_owned()
        } else {
            hash_principal(
                config,
                "key",
                config
                    .identity
                    .stdio_user
                    .as_deref()
                    .ok_or(AuthorizationUnavailable)?,
            )?
        };
        let Some(principal) = identity.principals.iter().find(|p| p.key == key) else {
            return Err(AuthorizationUnavailable);
        };
        if principal.tokens.len() > config.identity.max_tokens_per_user as usize {
            return Err(AuthorizationUnavailable);
        }
        let tokens = principal
            .tokens
            .iter()
            .map(|id| {
                identity
                    .tokens
                    .get(*id as usize)
                    .ok_or(AuthorizationUnavailable)
                    .map(|t| sql(t))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if tokens.is_empty() {
            Err(AuthorizationUnavailable)
        } else {
            let row_acl = format!("array_has_any(readers, [{}])", tokens.join(","));
            let fixed = &policy.fixed_grants;
            if fixed.is_empty() {
                return Ok(row_acl);
            }
            let sources = fixed.keys().map(|s| sql(s)).collect::<Vec<_>>().join(",");
            let allowed = fixed
                .iter()
                .filter(|(_, readers)| readers.iter().any(|t| tokens.contains(&sql(t))))
                .map(|(s, _)| sql(s))
                .collect::<Vec<_>>();
            let source_grants = if allowed.is_empty() {
                "false".into()
            } else {
                format!("source_id IN ({})", allowed.join(","))
            };
            Ok(format!(
                "((({row_acl}) AND source_id NOT IN ({sources})) OR ({source_grants}))"
            ))
        }
    }

    pub(crate) async fn interrupted(&mut self) -> Result<bool> {
        self.filter_selection = None;
        let rebuild = self.execution_slot.as_ref().is_none_or(|slot| slot.held());
        self.refresh_authorization_inner(rebuild).await
    }

    pub(crate) async fn refresh_authorization(&mut self) -> Result<bool> {
        self.refresh_authorization_inner(true).await
    }
    pub(super) async fn refresh_authorization_inner(&mut self, rebuild: bool) -> Result<bool> {
        let root = &self.control_root;
        let store = self.control_store.clone();
        let current: ServingHead = read_contract(store.as_ref(), &root.clone().join("head.json"))
            .await
            .map_err(|code| {
                if code == crate::storage::StorageError::StorageUnavailable {
                    StorageUnavailable
                } else {
                    AuthorizationUnavailable
                }
            })?;
        if current.namespace != self.config.namespace.name {
            return Err(AuthorizationUnavailable);
        }
        let policy_changed = current.policy != self.head.policy;
        let hidden_sources = Self::hidden(&self.config, &current, &self.policy)?;
        let changed = hidden_sources != self.hidden_sources
            || self.scope.identity_for_refresh() != Some(&current.identity)
            || policy_changed;
        if changed && !rebuild {
            // An already-materialized answer can only use its original grant.
            // Refuse it without performing native admission work off the slot.
            return Err(AuthorizationUnavailable);
        }
        if policy_changed {
            self.policy = artifact(store.as_ref(), root, &current.policy)
                .await
                .map_err(|_| AuthorizationUnavailable)?;
        }
        let hidden_sources = Self::hidden(&self.config, &current, &self.policy)?;
        let changed = hidden_sources != self.hidden_sources
            || self.scope.identity_for_refresh() != Some(&current.identity)
            || policy_changed;
        let identity_changed = current.identity != self.head.identity;
        let refuse_binding = identity_changed
            && self.scope.http_key().is_some()
            && self.scope.identity_for_refresh() != Some(&current.identity);
        if identity_changed {
            self.scope.invalidate_principal();
            if let Some(subject) = self.scope.http_key() {
                // A principal belongs to the artifact that resolved it. A fresh
                // request resolves its binding again in bind_subject; an older
                // admission must never rebuild under the newer generation.
                let mut cache = self
                    .admission_cache
                    .lock()
                    .map_err(|_| AuthorizationUnavailable)?;
                cache.entries.retain(|(key, _)| &key.subject != subject);
                cache.entries.shrink_to_fit();
            }
            let identity: IdentityArtifact = artifact(store.as_ref(), root, &current.identity)
                .await
                .map_err(|_| AuthorizationUnavailable)?;
            self.identity = identity;
        }
        // Changed inputs cannot certify the old mask if validation fails.
        if changed {
            self.scope.refuse();
        }
        self.hidden_sources = hidden_sources;
        self.head.policy = current.policy;
        self.head.identity = current.identity;
        self.head.freshness = current.freshness;
        self.head.content = current.content;
        self.head.incarnation = current.incarnation;
        if refuse_binding {
            self.scope.refuse();
            return Err(AuthorizationUnavailable);
        }
        self.authorization()?;
        if changed && (self.config.reader.http.is_none() || self.scope.principal().is_some()) {
            self.rebuild_admission()
                .await
                .map_err(|_| AuthorizationUnavailable)?;
        }
        Ok(changed)
    }
}
