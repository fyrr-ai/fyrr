use super::*;

impl Display for MirrorStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("fyrr immutable mirror")
    }
}

impl WrappingObjectStore for MirrorStore {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(self.wrapped(store_prefix, original))
    }
    fn wrap_paginated(
        &self,
        _: &str,
        _: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        None
    }
}

#[async_trait::async_trait]
impl ObjectStore for MirrorStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.get_immutable(location, options, true).await
    }
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(Self::readonly())
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Self::readonly())
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        Box::pin(futures::stream::once(async { Err(Self::readonly()) }))
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let counters = self.counters.read().expect("counter scope lock").clone();
        self.record(&counters, Io::Request, 1);
        self.target().list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let counters = self.counters.read().expect("counter scope lock").clone();
        self.record(&counters, Io::Request, 1);
        self.target().list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(Self::readonly())
    }
    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> Result<()> {
        Err(Self::readonly())
    }
}

impl MirrorStore {
    /// This mirror reading through `original` under the Lance `store_prefix`.
    pub(super) fn wrapped(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Self {
        let mut store = self.clone();
        store.target = Some(original);
        store.prefix = store_prefix.into();
        store
    }
    /// Fills the complete file of `location`: a verified copy answers at once,
    /// otherwise the whole object is downloaded. An object that cannot be kept
    /// is answered by a head rather than a download nobody reads.
    pub(super) async fn fill(&self, location: &Path) -> Result<GetResult> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };
        self.get_immutable(location, options, false).await
    }
    /// A read through the mirror. With `paged`, a head of a pinned `data/`
    /// file is answered from the manifest and a ranged read of one is served
    /// in pages, or downloads a small file whole when it fits; without it, a
    /// miss downloads the complete file.
    async fn get_immutable(
        &self,
        location: &Path,
        options: GetOptions,
        paged: bool,
    ) -> Result<GetResult> {
        // Test-only gate on a real native data-file read, including mirror hits.
        #[cfg(test)]
        if !options.head && location.as_ref().contains("/data/") {
            self.sample_read.wait_once().await;
        }
        let counters = self.counters.read().expect("counter scope lock").clone();
        if !self.immutable(location)
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.version.is_some()
        {
            return self.pass_through(location, options, &counters).await;
        }
        if self.absent_version_manifest(location.as_ref()) {
            return Err(Error::NotFound {
                path: location.to_string(),
                source: "not a version manifest of the publication".into(),
            });
        }
        let Some(name) = self.blob_name(&self.prefix, location.as_ref()) else {
            return self.pass_through(location, options, &counters).await;
        };
        let file = self.directory.join(name);
        if crate::sync::lock(&self.oversized).contains(&file) {
            return self.pass_through(location, options, &counters).await;
        }
        let size = self
            .manifest_size(&file)
            .filter(|_| paged && self.ranged_class(location));
        if let Some(size) = size
            && options.head
        {
            return Ok(Self::head_result(location, &file, size));
        }
        if let Some((opened, metadata)) = self
            .open_verified_blob(&file)
            .await
            .map_err(|_| Self::failure())?
        {
            let result = Self::blob_result(location, file, options, opened, metadata)?;
            self.record(&counters, Io::Hit, 1);
            return Ok(result);
        }
        // A complete file that failed verification is downloaded again whole.
        let unverified = file.symlink_metadata().is_ok();
        // A small data file is downloaded whole when the mirror has room for
        // it; otherwise, as a larger one, in pages or past the mirror.
        if let (Some(size), Some(range), false) = (size, &options.range, unverified)
            && (size > self.whole_bytes || !self.whole_fits(&file, size).await?)
        {
            if !self.pages_fit(size) {
                if size > self.max_bytes {
                    Self::remember_oversized(&self.oversized, file);
                }
                return self.pass_through(location, options, &counters).await;
            }
            let range = range.as_range(size).map_err(|source| Error::Generic {
                store: "fyrr immutable mirror",
                source: Box::new(source),
            })?;
            return self
                .read_pages(location, range, size, &file, &counters)
                .await;
        }
        let object = self
            .download_lock(&file)
            .await
            .map_err(|_| Self::failure())?;
        let result = self
            .populate(location, options, &counters, file, size)
            .await;
        object.finish().await.map_err(|_| Self::failure())?;
        result
    }
    // The caller retains a per-object flock through every population outcome.
    async fn populate(
        &self,
        location: &Path,
        options: GetOptions,
        counters: &IoCounters,
        file: PathBuf,
        expected: Option<u64>,
    ) -> Result<GetResult> {
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await.map_err(|_| Self::failure())?;
        if let Some((opened, metadata)) = self
            .open_verified_blob(&file)
            .await
            .map_err(|_| Self::failure())?
        {
            let result = Self::blob_result(location, file, options, opened, metadata)?;
            self.record(counters, Io::Hit, 1);
            return Ok(result);
        }
        drop(_host);
        drop(_lock);
        self.record(counters, Io::Request, 1);
        let download = self.target().get(location).await?;
        let metadata = download.meta.clone();
        // A pinned data file has the size its manifest states, as in pages.
        if expected.is_some_and(|size| size != metadata.size) {
            return Err(Self::failure());
        }
        if metadata.size > self.max_bytes {
            Self::remember_oversized(&self.oversized, file);
            return self.pass_through(location, options, counters).await;
        }
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await.map_err(|_| Self::failure())?;
        let temporary = file.with_extension("partial");
        Self::remove_partial(&temporary).map_err(|_| Self::failure())?;
        // The complete file supersedes any pages of the same object; their
        // room is released before the complete file reserves its own.
        self.remove_pages(&file).map_err(|_| Self::failure())?;
        if !self.make_room(metadata.size)? {
            drop(_host);
            drop(_lock);
            self.record(counters, Io::Request, 1);
            let result = self.target().get_opts(location, options).await?;
            self.record(counters, Io::Bytes, result.range.end - result.range.start);
            return Ok(result);
        }
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|_| Self::failure())?;
        // Sparse length reserves capacity across all processes before downloading.
        output.set_len(metadata.size).map_err(|_| Self::failure())?;
        drop(_host);
        drop(_lock);
        let mut stream = download.into_stream();
        let mut hasher = Sha256::new();
        let mut written = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            written = written
                .checked_add(chunk.len() as u64)
                .ok_or_else(Self::failure)?;
            if written > metadata.size {
                return Err(Self::failure());
            }
            output.write_all(&chunk).map_err(|_| Self::failure())?;
            hasher.update(&chunk);
            self.record(counters, Io::Bytes, chunk.len() as u64);
        }
        if written != metadata.size {
            return Err(Self::failure());
        }
        #[cfg(test)]
        tests::pause("downloaded");
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await.map_err(|_| Self::failure())?;
        let checksum = file.with_extension("checksum");
        // Readers never trust a checksum until it matches the complete immutable blob.
        let mut proof = Self::private_file(&checksum, true).map_err(|_| Self::failure())?;
        proof.set_len(0).map_err(|_| Self::failure())?;
        proof.rewind().map_err(|_| Self::failure())?;
        write!(proof, "{}", hex::encode(hasher.finalize())).map_err(|_| Self::failure())?;
        // Both files are recoverable. Restart verifies their size and checksum.
        #[cfg(test)]
        tests::pause("proof");
        std::fs::rename(temporary, &file).map_err(|_| Self::failure())?;
        // Pages a concurrent read kept meanwhile are superseded too.
        self.remove_pages(&file).map_err(|_| Self::failure())?;
        #[cfg(test)]
        tests::pause("published");
        crate::sync::lock(&self.verified).insert(
            file.clone(),
            CachedFile::of(&output.metadata().map_err(|_| Self::failure())?),
        );
        let (opened, metadata) = self
            .open_verified_blob(&file)
            .await
            .map_err(|_| Self::failure())?
            .ok_or_else(Self::failure)?;
        Self::blob_result(location, file, options, opened, metadata)
    }

    fn remember_oversized(
        oversized: &std::sync::Mutex<std::collections::BTreeSet<PathBuf>>,
        file: PathBuf,
    ) {
        let mut oversized = crate::sync::lock(oversized);
        // Bound derived metadata independently of the file and memory caches.
        if oversized.len() >= 4096 {
            oversized.clear();
        }
        oversized.insert(file);
    }
    /// Complete files and page files, counted against the byte budget at their
    /// apparent length.
    fn counted(name: &str) -> bool {
        [".blob", ".sparse", ".map"]
            .iter()
            .any(|suffix| Self::cache_name(name, suffix))
    }

    // The caller holds global locks; active partial lengths reserve incoming bytes.
    //
    // Every change to the counted bytes (a partial's reservation, its rename, an
    // eviction, a sweep) happens under those same locks, in this or another
    // process, so a listing taken here is exact. It is read without a stat: a
    // `.blob` has the size of the object its name addresses, and a `.partial` is
    // reserved at its final length, so a file already seen under the same name
    // and inode keeps its size. Only the evicting path below scans the directory
    // and the leases.
    pub(super) fn make_room(&self, incoming: u64) -> Result<bool> {
        if self.used_bytes()?.saturating_add(incoming) <= self.max_bytes {
            return Ok(true);
        }
        self.make_room_by_eviction(incoming)
    }

    /// Whether a small data file of `size` fits next to what the mirror
    /// holds, without evicting: a mirror at its budget, even with unpinned
    /// files of an older generation, reads it in pages, which may evict, and
    /// warming copies it later. One already read in pages stays in pages until
    /// warming replaces them, without a scan of the mirror. Population checks
    /// the room again.
    async fn whole_fits(&self, file: &std::path::Path, size: u64) -> Result<bool> {
        if size > self.max_bytes || file.with_extension("sparse").symlink_metadata().is_ok() {
            return Ok(false);
        }
        // `used_bytes` records sizes and removes dead partials: global locks.
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await.map_err(|_| Self::failure())?;
        Ok(self.used_bytes()?.saturating_add(size) <= self.max_bytes)
    }
    // Apparent bytes of every `.blob` and `.partial` seen under its name. A name
    // fixes its size (mirrored objects are immutable), so an entry is statted
    // once; a dead partial stays counted until the eviction path or a sweep
    // removes it, which only makes this count conservative.
    fn used_bytes(&self) -> Result<u64> {
        #[cfg(test)]
        self.room_probes.listings.fetch_add(1, Ordering::SeqCst);
        let mut known = crate::sync::lock(&self.sizes);
        let mut seen = BTreeMap::new();
        for entry in std::fs::read_dir(&self.directory).map_err(|_| Self::failure())? {
            let entry = entry.map_err(|_| Self::failure())?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let partial = Self::cache_name(&name, ".partial");
            if !partial && !Self::counted(&name) {
                continue;
            }
            let inode = std::os::unix::fs::DirEntryExt::ino(&entry);
            if let Some(&(known_inode, size)) = known.get(&name)
                && known_inode == inode
            {
                seen.insert(name, (inode, size));
                continue;
            }
            #[cfg(test)]
            self.room_probes.stats.fetch_add(1, Ordering::SeqCst);
            let path = entry.path();
            if partial {
                let lock = Self::private_file(&path.with_extension("download"), true)
                    .map_err(|_| Self::failure())?;
                match lock.try_lock() {
                    Ok(()) => {
                        Self::remove_partial(&path).map_err(|_| Self::failure())?;
                        Self::remove_partial(&path.with_extension("download"))
                            .map_err(|_| Self::failure())?;
                        continue;
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(_) => return Err(Self::failure()),
                }
            }
            let meta = path.symlink_metadata().map_err(|_| Self::failure())?;
            if !Self::owned_private_file(&meta) {
                return Err(Self::failure());
            }
            seen.insert(name, (inode, meta.len()));
        }
        let used = seen.values().map(|&(_, size)| size).sum();
        *known = seen;
        Ok(used)
    }

    // The exact accounting: idle partials are removed, unpinned blobs are
    // evicted oldest first until the incoming bytes fit.
    fn make_room_by_eviction(&self, incoming: u64) -> Result<bool> {
        #[cfg(test)]
        self.room_probes.scans.fetch_add(1, Ordering::SeqCst);
        let pinned_files = self.live_files().map_err(|_| Self::failure())?;
        let mut entries = Vec::new();
        let mut used = 0;
        for entry in std::fs::read_dir(&self.directory).map_err(|_| Self::failure())? {
            let entry = entry.map_err(|_| Self::failure())?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if Self::cache_name(&name, ".partial") {
                let path = entry.path();
                let lock = Self::private_file(&path.with_extension("download"), true)
                    .map_err(|_| Self::failure())?;
                match lock.try_lock() {
                    Ok(()) => {
                        Self::remove_partial(&path).map_err(|_| Self::failure())?;
                        Self::remove_partial(&path.with_extension("download"))
                            .map_err(|_| Self::failure())?;
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {
                        let metadata = path.symlink_metadata().map_err(|_| Self::failure())?;
                        if !Self::owned_private_file(&metadata) {
                            return Err(Self::failure());
                        }
                        used += metadata.len();
                    }
                    Err(_) => return Err(Self::failure()),
                }
                continue;
            }
            if !Self::counted(&name) {
                continue;
            }
            let meta = entry
                .path()
                .symlink_metadata()
                .map_err(|_| Self::failure())?;
            if !Self::owned_private_file(&meta) {
                return Err(Self::failure());
            }
            used += meta.len();
            // A map is evicted with its sparse file, or alone if it has none.
            if Self::cache_name(&name, ".map") && entry.path().with_extension("sparse").exists() {
                continue;
            }
            entries.push((
                meta.modified().map_err(|_| Self::failure())?,
                entry.path(),
                meta.len(),
            ));
        }
        entries.sort_by_key(|entry| entry.0);
        for (_, path, size) in entries {
            if used + incoming <= self.max_bytes {
                break;
            }
            let blob = path.with_extension("blob");
            let pinned = blob
                .file_name()
                .is_some_and(|name| pinned_files.contains(&name.to_string_lossy().to_string()));
            if pinned {
                continue;
            }
            if path.extension().is_some_and(|e| e != "blob") {
                let held: u64 = ["sparse", "map"]
                    .iter()
                    .filter_map(|e| blob.with_extension(e).symlink_metadata().ok())
                    .map(|meta| meta.len())
                    .sum();
                self.remove_pages(&blob).map_err(|_| Self::failure())?;
                used = used.saturating_sub(held);
                continue;
            }
            Self::remove_partial(&path.with_extension("checksum")).map_err(|_| Self::failure())?;
            crate::sync::lock(&self.verified).remove(&path);
            Verifications::forget(&path);
            std::fs::remove_file(path).map_err(|_| Self::failure())?;
            used -= size;
        }

        Ok(used + incoming <= self.max_bytes)
    }
}
