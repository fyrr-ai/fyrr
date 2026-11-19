use super::*;

impl PreparedReader {
    pub async fn open(config: &Config) -> Result<Self> {
        let reader = Self::open_shared(config, None, PreparationReservation::pool(config)?).await?;
        reader.report_cache_degradation(false);
        Ok(reader)
    }
    // Numeric resource telemetry at actual activation only.
    pub(crate) fn report_cache_degradation(&self, previously_degraded: bool) {
        let needed = PreparationReservation::cache_bytes(&self.manifest);
        if (self.native_session.capacity.0 < needed.0 || self.native_session.capacity.1 < needed.1)
            && !previously_degraded
        {
            emit(format_args!(
                "telemetry native_cache=degraded reason=cache_budget needed_bytes={} \
                    available_bytes={}",
                needed.0.saturating_add(needed.1),
                self.native_session.bytes()
            ));
        }
    }
    pub(crate) async fn open_shared(
        config: &Config,
        shared: Option<SharedReaderResources>,
        preparation_budget: MemoryPool,
    ) -> Result<Self> {
        let (store, root) = namespace_store(config)?;
        let options = config
            .storage
            .object_store_options(&ProcessEnvironment)
            .map_err(|_| StorageConfigurationInvalid)?;
        let mirror_path = config
            .mirror_directory(&ProcessEnvironment)
            .ok_or(StorageConfigurationInvalid)?;
        MirrorStore::private_directory(&mirror_path).map_err(|_| StorageUnavailable)?;
        let (
            shared_session,
            mirror,
            admission_cache,
            query_embeddings,
            policy_cache,
            identity_cache,
        ) = if let Some((session, mirror, cache, queries, policies, identities)) = shared {
            (
                session.upgrade(),
                mirror,
                cache,
                queries,
                policies,
                identities,
            )
        } else {
            (
                None,
                Arc::new(MirrorStore::new(
                    mirror_path.join(digest(
                        format!(
                            "{}:{}",
                            config.storage.endpoint.as_deref().unwrap_or(""),
                            dataset_uri(config)
                        )
                        .as_bytes(),
                    )),
                    config.reader.mirror_max_bytes,
                )?),
                Arc::new(std::sync::Mutex::new(AuthorizationCache::default())),
                embedding::QueryEmbeddings::new(
                    &config.embedding,
                    preparation_budget.permits.clone(),
                )?,
                Arc::default(),
                Arc::default(),
            )
        };
        mirror.bind_dataset(&dataset_uri(config))?;
        let control_backend = store;
        let store = mirror.wrap("control", control_backend.clone());
        let head: ServingHead =
            read_contract(store.as_ref(), &root.clone().join("head.json")).await?;
        if head.namespace != config.namespace.name {
            return Err(AuthorizationUnavailable);
        }
        let pointer = head.content.as_ref().ok_or(PublicationUnavailable)?;
        let (manifest, policy, identity) = Self::read_publication(
            store.as_ref(),
            &root,
            &head,
            pointer,
            &config.namespace.name,
            &embedding::identity(&config.embedding),
            (&policy_cache, &identity_cache),
        )
        .await?;
        let mirror = Arc::new(mirror.for_publication(&manifest));
        if config.reader.http.is_none() {
            Self::predicate(config, &identity, &policy, None)?;
        }
        let (dataset, mirror_pin, generation_state, native_session, preparation_reservation) =
            Self::open_generation(
                config,
                &manifest,
                &mirror,
                shared_session,
                &preparation_budget,
                options,
            )
            .await?;
        let indexes = verify_indices(&dataset, &manifest, config).await?;
        let mut reader = Self {
            #[cfg(test)]
            test_hooks: Arc::default(),
            #[cfg(test)]
            stage_trace: None,
            config: config.clone(),
            dataset,
            manifest,
            hidden_sources: Self::hidden(config, &head, &policy)?,
            reported_hidden_sources: Vec::new(),
            head,
            policy,
            identity,
            policy_cache,
            identity_cache,
            scope: scope::RequestScope::new(config.reader.http.is_some()),
            admission_cache,
            admission_build_us: 0,
            eligible: RowAddrMask::allow_nothing(),
            indexes,
            unindexed: BTreeSet::new(),
            fallback_probes: config.reader.nprobes.saturating_mul(2),
            maps: None,
            vector_roots: Arc::default(),
            centroid_routers: Arc::default(),
            mirror,
            mirror_pin,
            generation_state,
            generation_id: Arc::new(()),
            native_session,
            preparation_budget,
            preparation_reservation,
            query_embeddings,
            reranker: config
                .namespace
                .rerank
                .as_ref()
                .map(|_| crate::rerank::Reranker::new().map(Arc::new))
                .transpose()?,
            control_store: store,
            control_backend,
            control_root: root,
            filter_selection: None,
            response_claim: None,
            execution_slot: None,
        };
        reader.authorization()?;
        if reader.config.reader.http.is_none() {
            reader.rebuild_admission().await?;
        }
        reader.prepare_routing().await?;
        reader
            .mirror
            .evict_unpinned()
            .await
            .map_err(|_| StorageUnavailable)?;
        Ok(reader)
    }

    /// Read the pinned manifest, policy and identity. All three locations are
    /// known from the head, so the reads run together instead of one after
    /// another. Their results are then judged in the order the sequential code
    /// judged them (manifest read, manifest checks, policy, identity), so every
    /// failure reports the error it reported before.
    pub(super) async fn read_publication(
        store: &dyn object_store::ObjectStore,
        root: &object_store::path::Path,
        head: &ServingHead,
        pointer: &fyrr_core::publication::ContentPointer,
        namespace: &str,
        embedding: &fyrr_core::envelope::EmbeddingIdentity,
        (policy_cache, identity_cache): (
            &ArtifactCache<PolicyArtifact>,
            &ArtifactCache<IdentityArtifact>,
        ),
    ) -> Result<(ContentManifest, Arc<PolicyArtifact>, Arc<IdentityArtifact>)> {
        let manifest_path = object_store::path::Path::from(format!("{root}/{}", pointer.manifest));
        let (manifest, policy, identity) = tokio::join!(
            read_contract::<ContentManifest>(store, &manifest_path),
            async {
                let mut cached = policy_cache.lock().await;
                if cached
                    .as_ref()
                    .is_none_or(|(pointer, _)| pointer != &head.policy)
                {
                    let policy = artifact::<PolicyArtifact>(store, root, &head.policy)
                        .await
                        .map_err(|_| AuthorizationUnavailable)?;
                    *cached = Some((head.policy.clone(), Arc::new(policy)));
                }
                Ok::<_, ReaderError>(cached.as_ref().ok_or(AuthorizationUnavailable)?.1.clone())
            },
            async {
                let mut cached = identity_cache.lock().await;
                if cached
                    .as_ref()
                    .is_none_or(|(pointer, _)| pointer != &head.identity)
                {
                    let identity = artifact::<IdentityArtifact>(store, root, &head.identity)
                        .await
                        .map_err(|_| AuthorizationUnavailable)?;
                    *cached = Some((head.identity.clone(), Arc::new(identity)));
                }
                Ok::<_, ReaderError>(cached.as_ref().ok_or(AuthorizationUnavailable)?.1.clone())
            },
        );
        let manifest = manifest?;
        if manifest.namespace != namespace
            || manifest.publication_id != pointer.publication_id
            || manifest.lance_version != pointer.lance_version
        {
            return Err(PublicationInvalid);
        }
        if &manifest.embedding != embedding {
            return Err(EmbeddingProfileMismatch);
        }
        Ok((manifest, policy?, identity?))
    }

    async fn open_generation(
        config: &Config,
        manifest: &ContentManifest,
        mirror: &Arc<MirrorStore>,
        shared_session: Option<Arc<NativeSession>>,
        preparation_budget: &MemoryPool,
        options: fyrr_core::config::StorageOptions,
    ) -> Result<(
        Arc<Dataset>,
        Arc<MirrorLease>,
        GenerationState,
        Arc<NativeSession>,
        Arc<PreparationReservation>,
    )> {
        let mirror_pin = mirror
            .pin(&manifest.files)
            .await
            .map_err(|_| StorageUnavailable)?;
        let needed = PreparationReservation::cache_bytes(manifest);
        // Read the pinned manifest's row count without constructing index roots
        // or charging a native cache before serving-generation admission.
        let dataset = open_version(
            &dataset_uri(config),
            options.clone(),
            manifest.lance_version,
            Some((
                Arc::new(lance::session::Session::new(0, 0, Arc::default())),
                mirror.clone(),
            )),
        )
        .await
        .map_err(|_| StorageUnavailable)?;
        let rows = dataset
            .manifest()
            .fragments
            .iter()
            .try_fold(0u64, |sum, fragment| {
                fragment
                    .physical_rows
                    .and_then(|n| sum.checked_add(n as u64))
                    .ok_or(PublicationInvalid)
            })?;
        let warm = PreparationReservation::warm_estimate(config, manifest, rows).0;
        let required = PreparationReservation::minimum_bytes(0, warm, needed);
        // The fixed pool already excludes headroom, shared tables and startup
        // RSS (including page rounding). Fund both warm claims and the native
        // cache minimum before constructing any serving generation.
        if required > preparation_budget.cap {
            let minimum = config
                .reader
                .memory_max_bytes
                .as_u64()
                .saturating_sub(preparation_budget.cap)
                .saturating_add(required);
            emit(format_args!(
                "telemetry generation_preparation=admission minimum_bytes={minimum} \
                    budget_bytes={}",
                config.reader.memory_max_bytes.as_u64()
            ));
            return Err(PreparationBudgetImpossible);
        }
        let preparation_reservation = PreparationReservation::acquire(
            config,
            manifest,
            rows,
            preparation_budget.permits.clone(),
        )?;
        // A paid old cache must not force a generation cold when a handover
        // could release it and admit the roots first. The dispatcher owns that drain.
        if !preparation_reservation.warm()
            && shared_session.is_some()
            && preparation_reservation.warm_bytes() <= preparation_budget.cap
        {
            return Err(PreparationBudgetExhausted);
        }
        let native_session = NativeSession::acquire(
            preparation_budget.clone(),
            needed,
            preparation_reservation.warm_bytes(),
            shared_session,
        )?;
        let dataset = Arc::new(
            open_version(
                &dataset_uri(config),
                options,
                manifest.lance_version,
                Some((native_session.session.clone(), mirror.clone())),
            )
            .await
            .map_err(|_| StorageUnavailable)?,
        );
        let generation_state = if preparation_reservation.warm() {
            GenerationState::Serving
        } else {
            GenerationState::Cold
        };
        Ok((
            dataset,
            mirror_pin,
            generation_state,
            native_session,
            preparation_reservation,
        ))
    }
    pub(crate) fn worker(
        &self,
    ) -> impl std::future::Future<Output = Result<Self>> + Send + 'static {
        let config = self.config.clone();
        let manifest = self.manifest.clone();
        let head = self.head.clone();
        let hidden_sources = self.hidden_sources.clone();
        let policy = self.policy.clone();
        let policy_cache = self.policy_cache.clone();
        let identity_cache = self.identity_cache.clone();
        let identity = self.identity.clone();
        let scope = self.scope.without_admission();
        let admission_cache = self.admission_cache.clone();
        let indexes = self.indexes.clone();
        let vector_roots = self.vector_roots.clone();
        let centroid_routers = self.centroid_routers.clone();
        let mirror_pin = self.mirror_pin.clone();
        let generation_id = self.generation_id.clone();
        let native_session = self.native_session.clone();
        let preparation_budget = self.preparation_budget.clone();
        let preparation_reservation = self.preparation_reservation.clone();
        let control_backend = self.control_backend.clone();
        let control_root = self.control_root.clone();
        let session = self.dataset.session();
        let mirror = Arc::new(self.mirror.worker());
        let generation_state = self.generation_state;
        let fallback_probes = self.fallback_probes;
        let reranker = self.reranker.clone();
        let query_embeddings = self.query_embeddings.clone();
        #[cfg(test)]
        let test_hooks = self.test_hooks.clone();
        async move {
            let options = config
                .storage
                .object_store_options(&ProcessEnvironment)
                .map_err(|_| StorageUnavailable)?;
            let dataset = Arc::new(
                open_version(
                    &dataset_uri(&config),
                    options,
                    manifest.lance_version,
                    Some((session.clone(), mirror.clone())),
                )
                .await
                .map_err(|_| StorageUnavailable)?,
            );
            let control_store = mirror.wrap("control", control_backend.clone());
            let mut worker = Self {
                #[cfg(test)]
                test_hooks,
                #[cfg(test)]
                stage_trace: None,
                config: config.clone(),
                dataset,
                manifest: manifest.clone(),
                head: head.clone(),
                hidden_sources: hidden_sources.clone(),
                reported_hidden_sources: Vec::new(),
                policy_cache: policy_cache.clone(),
                identity_cache: identity_cache.clone(),
                policy: policy.clone(),
                identity: identity.clone(),
                scope: scope.clone(),
                admission_cache: admission_cache.clone(),
                admission_build_us: 0,
                eligible: RowAddrMask::allow_nothing(),
                indexes: indexes.clone(),
                unindexed: BTreeSet::new(),
                fallback_probes,
                maps: None,
                vector_roots: vector_roots.clone(),
                centroid_routers: centroid_routers.clone(),
                mirror,
                mirror_pin: mirror_pin.clone(),
                generation_state,
                generation_id: generation_id.clone(),
                native_session,
                preparation_budget: preparation_budget.clone(),
                preparation_reservation: preparation_reservation.clone(),
                query_embeddings,
                reranker,
                control_store,
                control_backend: control_backend.clone(),
                control_root: control_root.clone(),
                filter_selection: None,
                response_claim: None,
                execution_slot: None,
            };
            if worker.config.reader.http.is_none() || worker.scope.principal().is_some() {
                worker.rebuild_admission().await?;
            }
            Ok(worker)
        }
    }
    /// Verify all immutable mirror inputs before advertising warm readiness.
    pub async fn prewarm(&mut self) -> Result<()> {
        self.refresh_authorization().await?;
        let result = self.prewarm_files().await;
        self.refresh_authorization().await?;
        result
    }
    /// Background physical-cache preparation has no caller or result rows.
    /// Activation and every subsequent request retain their live head checks.
    pub(crate) async fn prewarm_files(&mut self) -> Result<()> {
        self.authorization()?;
        if !self.preparation_reservation.warm() {
            return Ok(());
        }
        tokio::select! {
            biased;
            () = self.mirror_pin.expiry.cancelled() => return Err(StorageUnavailable),
            warmed = self.mirror.prewarm(self.control_backend.clone(),
                 &self.manifest.files) => warmed?,

        }
        self.authorization()?;
        let needed = PreparationReservation::cache_bytes(&self.manifest);
        if self.native_session.capacity.0 >= needed.0 && self.native_session.capacity.1 >= needed.1
        {
            self.generation_state = GenerationState::WarmReady;
        }
        Ok(())
    }
    pub(crate) fn release_unreserved_roots(&mut self) -> bool {
        if self.preparation_reservation.warm() {
            return false;
        }
        let changed = self.generation_state != GenerationState::Cold;
        self.generation_state = GenerationState::Cold;
        self.maps = None;
        if self.vector_roots.get().is_some() {
            self.vector_roots = Arc::default();
            self.centroid_routers = Arc::default();
        }
        changed
    }
    pub fn readiness(&self) -> serde_json::Value {
        self.readiness_state().into_wire()
    }
    pub(crate) fn readiness_state(&self) -> crate::readiness::Readiness {
        let io = self.mirror.totals();
        let state = if self.preparation_reservation.warm() {
            self.generation_state
        } else {
            GenerationState::Cold
        };
        let needed = PreparationReservation::cache_bytes(&self.manifest);
        let cache_degraded =
            needed.0 > self.native_session.capacity.0 || needed.1 > self.native_session.capacity.1;
        crate::readiness::Readiness {
            ready: true,
            state,
            publication_id: self.manifest.publication_id.clone(),
            lance_version: self.manifest.lance_version,
            io,
            preparation_reserved_bytes: self.preparation_reservation.bytes(),
            minimum_memory_bytes: PreparationReservation::minimum_bytes(
                self.config
                    .reader
                    .memory_max_bytes
                    .as_u64()
                    .saturating_sub(self.preparation_budget.cap),
                self.preparation_reservation.warm_bytes(),
                needed,
            ),
            native_cache_needed_bytes: needed.0.saturating_add(needed.1),
            native_cache_available_bytes: self
                .native_session
                .capacity
                .0
                .saturating_add(self.native_session.capacity.1),
            cache_degraded,
            rss_bytes: PreparationReservation::rss().ok(),
        }
    }
}
