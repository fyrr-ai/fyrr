use super::*;
use futures::StreamExt;

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
        };
        if let Some(entry) = self
            .admission_cache
            .lock()
            .map_err(|_| AuthorizationUnavailable)?
            .get(&key)
            && (!self.preparation_reservation.warm() || self.maps_match(&entry.admission))
        {
            self.maps_enabled = self.preparation_reservation.warm();
            self.fallback_probes = entry.admission.fallback_probes;
            self.admission = entry.admission.clone();
            self.scope.admitted(self.head.identity.clone());
            return Ok(());
        }
        let mut eligible = {
            let predicate = Self::predicate(
                &self.config,
                &self.identity,
                &self.policy,
                self.scope.principal(),
            )?;
            let mut eligible = RowAddrTreeMap::new();
            let mut scan = self.dataset.scan();
            // The source column only tells retired sources apart; without
            // any, the rows the predicate selects are the eligible ones, and
            // an index-served predicate needs no data file at all.
            let by_source = !self.policy.retired_sources.is_empty();
            let columns: &[&str] = if by_source { &["source_id"] } else { &[] };
            scan.project(columns).map_err(|_| PublicationInvalid)?;
            scan.filter(&predicate)
                .map_err(|_| AuthorizationUnavailable)?
                .with_row_id();
            #[cfg(test)]
            self.test_hooks.admission_scan.wait_once().await;
            let mut stream = scan
                .try_into_stream()
                .await
                .map_err(|_| StorageUnavailable)?;
            while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
                if !by_source {
                    for id in ids(&batch)?.values() {
                        eligible.insert(*id);
                    }
                    continue;
                }
                let sources = strings(&batch, "source_id")?;
                for (row, id) in ids(&batch)?.values().iter().enumerate() {
                    let source = sources.value(row);
                    // A retired source is denied for good.
                    if !self.policy.retired_sources.contains_key(source) {
                        eligible.insert(*id);
                    }
                }
            }
            eligible
        };
        // Converts containers to runs where that is smaller; the rows are unchanged.
        eligible.optimize();
        let unindexed = Self::unindexed_rows(&eligible, |fragment| {
            self.indexes
                .iter()
                .filter(|i| i.name == Layout::VECTOR_INDEX)
                .any(|i| {
                    i.fragment_bitmap
                        .as_ref()
                        .is_some_and(|b| b.contains(fragment))
                })
        })?;
        let eligible = RowAddrMask::from_allowed(eligible);
        let mut maps = None;
        if let Ok(found) = self.partition_admission(&eligible).await
            && !found.is_empty()
        {
            maps = Some(found);
        }
        if maps.is_some() {
            use lance::cache::DeepSizeOf;
            let roots = self.vector_roots.get().map_or(0, |roots| {
                roots.iter().map(|r| r.deep_size_of() as u64).sum()
            });
            if !self.preparation_reservation.settle(roots)? {
                maps = None;
                self.vector_roots = Arc::default();
                self.centroid_routers = Arc::default();
                self.generation_state = GenerationState::Cold;
                self.admission_cache
                    .lock()
                    .map_err(|_| AuthorizationUnavailable)?
                    .retain_content(u64::MAX);
            }
        }
        self.maps_enabled = maps.is_some();
        let admission = Arc::new(Admission {
            eligible,
            unindexed,
            maps,
            fallback_probes: self.fallback_probes,
        });
        self.admission = admission.clone();
        self.admission_build_us = micros(build_started.elapsed());
        self.scope.admitted(self.head.identity.clone());
        let cap = self
            .config
            .reader
            .http
            .as_ref()
            .map_or(16, |h| h.authorization_cache_subjects);
        // The cache pays for the one shared admission. Workers that bind it
        // hold the same allocation, so no per-worker copy exists to charge.
        let incoming = key.memory_bound() + admission.memory_bound();
        let mut cache = self
            .admission_cache
            .lock()
            .map_err(|_| AuthorizationUnavailable)?;
        if let Some(claim) = cache.reserve(&key, incoming, cap) {
            cache.insert(
                key,
                CachedAdmission {
                    _cache_claim: claim,
                    admission,
                },
                cap,
            );
        }
        Ok(())
    }

    /// Eligible rows in fragments that no vector segment covers. Equal to
    /// filtering every eligible row by fragment coverage, without visiting rows.
    pub(super) fn unindexed_rows(
        eligible: &RowAddrTreeMap,
        covered: impl Fn(u32) -> bool,
    ) -> Result<RowAddrTreeMap> {
        let mut tail = RowAddrTreeMap::new();
        for (fragment, _) in eligible.iter() {
            // The scan inserts rows only, never whole fragments.
            let rows = eligible
                .get_fragment_bitmap(*fragment)
                .ok_or(AuthorizationUnavailable)?;
            if !covered(*fragment) {
                tail.insert_bitmap(*fragment, rows.clone());
            }
        }
        Ok(tail)
    }

    /// Whether a cached admission's partition maps describe this reader's
    /// vector roots: same segments in the same order with the same partitions.
    fn maps_match(&self, admission: &Admission) -> bool {
        let segments = || {
            self.indexes
                .iter()
                .filter(|i| i.name == Layout::VECTOR_INDEX)
        };
        admission.maps.as_ref().is_some_and(|maps| {
            self.vector_roots.get().is_some_and(|roots| {
                roots.len() == maps.len()
                    && segments().count() == maps.len()
                    && segments().zip(roots).zip(maps).all(|((meta, root), map)| {
                        map.segment == meta.uuid.to_string()
                            && map.admitted.len() == root.total_partitions()
                    })
            })
        })
    }

    async fn partition_admission(&mut self, eligible: &RowAddrMask) -> Result<Vec<PartitionMap>> {
        let indexes = &self.indexes;
        let mut maps = Vec::new();

        if !self.preparation_reservation.warm() {
            return Err(PreparationBudgetExhausted);
        }
        // The walk below reads every partition of every vector segment. Paged,
        // a merged index costs several small ranged requests per partition
        // (tens of thousands at 3,000,000 rows); whole, one request a file.
        // A file the mirror has no room for, or that does not match the
        // manifest, is not kept and the walk reads pages as before.
        let _ = self
            .mirror
            .prewarm_if_room(
                self.control_backend.clone(),
                &PreparationReservation::vector_index_files(&self.manifest)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .await;
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
            // A partition is a few local reads, and an index of the size its rows ask for has
            // thousands of them in every segment: read them INDEX_OPEN_CONCURRENCY at a time.
            let index = &index;
            let counts: Vec<(u32, u64, bool)> = futures::stream::iter(0..index.total_partitions())
                .map(|partition| async move {
                    let mut stream = index
                        .partition_reader(partition, false, &NoOpMetricsCollector)
                        .await
                        .map_err(|_| PartitionMapUnavailable)?;
                    let (mut rows, mut indexed, mut foreign) = (0u32, 0u64, false);
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
                            {
                                indexed += 1;
                                rows += u32::from(eligible.selected(*id));
                            } else {
                                foreign |= eligible.selected(*id);
                            }
                        }
                    }
                    Ok::<_, ReaderError>((rows, indexed, foreign))
                })
                .buffered(INDEX_OPEN_CONCURRENCY)
                .try_collect()
                .await?;
            let admitted = counts.iter().map(|count| count.0).collect();
            let indexed_rows = counts.iter().map(|count| count.1).sum();
            maps.push(PartitionMap {
                segment: metadata.uuid.to_string(),
                admitted,
                indexed_rows,
                foreign: counts.iter().any(|count| count.2),
            });
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
        if subject.expires <= fyrr_core::clock::now().timestamp() {
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
            (fyrr_core::clock::now() - self.manifest.committed_at)
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
        if !Self::same_lineage(
            &self.head,
            &self.manifest.publication_id,
            self.manifest.sequence,
            &self.manifest.incarnation,
        ) {
            // A head of this incarnation with a lower sequence is an older
            // read of the head, not another namespace: sequences only grow
            // within an incarnation. Refuse the request and keep the pin; if
            // the head really went back, the next version check sees it
            // replaced and retires the pin.
            let older_read = self.head.incarnation == self.manifest.incarnation
                && self
                    .head
                    .content
                    .as_ref()
                    .is_some_and(|live| live.sequence < self.manifest.sequence);
            if !older_read {
                // Never serve a namespace that no longer exists at this URI.
                self.mirror_pin.retire();
            }
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
            || !crate::storage::directory_published(&self.config, &self.identity)
            || self
                .policy
                .row_acl_sequence
                .is_some_and(|sequence| self.manifest.sequence < sequence)
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
    pub(crate) fn admission_predicate(&self) -> Result<String> {
        let predicate = Self::predicate(
            &self.config,
            &self.identity,
            &self.policy,
            self.scope.principal(),
        )?;
        if self.policy.retired_sources.is_empty() {
            return Ok(predicate);
        }
        Ok(format!(
            "({predicate}) AND source_id NOT IN ({})",
            self.policy
                .retired_sources
                .keys()
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
        // A namespace written open lets every admitted subject read every row of
        // the API's source; the principal and its checks above stay.
        if policy
            .settings
            .write_api
            .as_ref()
            .is_some_and(|api| api.open)
        {
            return Ok(format!(
                "source_id = {}",
                sql(fyrr_core::connector::API_SOURCE_ID)
            ));
        }
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
        let current: ServingHead = self.head_cache.read(&store, root).await.map_err(|code| {
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
        let changed =
            self.scope.identity_for_refresh() != Some(&current.identity) || policy_changed;
        if changed && !rebuild {
            // An already-materialized answer can only use its original grant.
            // Refuse it without performing native admission work off the slot.
            return Err(AuthorizationUnavailable);
        }
        if policy_changed {
            let mut cached = self.policy_cache.lock().await;
            if cached
                .as_ref()
                .is_none_or(|(pointer, _)| pointer != &current.policy)
            {
                let policy = artifact(store.as_ref(), root, &current.policy)
                    .await
                    .map_err(|_| AuthorizationUnavailable)?;
                *cached = Some((current.policy.clone(), Arc::new(policy)));
            }
            self.policy = cached.as_ref().ok_or(AuthorizationUnavailable)?.1.clone();
        }
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
            let mut cached = self.identity_cache.lock().await;
            if cached
                .as_ref()
                .is_none_or(|(pointer, _)| pointer != &current.identity)
            {
                let identity = artifact(store.as_ref(), root, &current.identity)
                    .await
                    .map_err(|_| AuthorizationUnavailable)?;
                *cached = Some((current.identity.clone(), Arc::new(identity)));
            }
            self.identity = cached.as_ref().ok_or(AuthorizationUnavailable)?.1.clone();
        }
        // Changed inputs cannot certify the old mask if validation fails.
        if changed {
            self.scope.refuse();
        }
        self.head.policy = current.policy;
        self.head.identity = current.identity;
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
