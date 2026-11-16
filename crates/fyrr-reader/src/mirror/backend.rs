use super::*;

impl Display for MirrorStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("fyrr immutable mirror")
    }
}

impl WrappingObjectStore for MirrorStore {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let mut store = self.clone();
        store.target = Some(original);
        store.prefix = store_prefix.into();
        Arc::new(store)
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
        let Some(name) = self.blob_name(&self.prefix, location.as_ref()) else {
            return self.pass_through(location, options, &counters).await;
        };
        let file = self.directory.join(name);
        if let Some((opened, metadata)) = self
            .open_verified_blob(&file)
            .await
            .map_err(|_| Self::failure())?
        {
            let result = Self::blob_result(location, file, options, opened, metadata)?;
            self.record(&counters, Io::Hit, 1);
            return Ok(result);
        }
        // Hits and local reads never hold the miss/eviction lock.
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await.map_err(|_| Self::failure())?;
        if let Some((opened, metadata)) = self
            .open_verified_blob(&file)
            .await
            .map_err(|_| Self::failure())?
        {
            let result = Self::blob_result(location, file, options, opened, metadata)?;
            self.record(&counters, Io::Hit, 1);
            return Ok(result);
        }
        self.record(&counters, Io::Request, 1);
        let metadata = self.target().head(location).await?;
        if metadata.size > self.max_bytes {
            return self.pass_through(location, options, &counters).await;
        }
        if !self.make_room(metadata.size)? {
            self.record(&counters, Io::Request, 1);
            let result = self.target().get_opts(location, options).await?;
            self.record(&counters, Io::Bytes, result.range.end - result.range.start);
            return Ok(result);
        }
        let temporary = file.with_extension("partial");
        Self::remove_partial(&temporary).map_err(|_| Self::failure())?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|_| Self::failure())?;
        self.record(&counters, Io::Request, 1);
        let mut stream = self.target().get(location).await?.into_stream();
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
            self.record(&counters, Io::Bytes, chunk.len() as u64);
        }
        if written != metadata.size {
            return Err(Self::failure());
        }
        #[cfg(test)]
        tests::pause("downloaded");
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
    // The caller retains the process and host miss locks through the download.
    fn make_room(&self, incoming: u64) -> Result<bool> {
        let pinned_files = self.live_files().map_err(|_| Self::failure())?;
        let mut entries = Vec::new();
        let mut used = 0;
        for entry in std::fs::read_dir(&self.directory).map_err(|_| Self::failure())? {
            let entry = entry.map_err(|_| Self::failure())?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !Self::cache_name(&name, ".blob") {
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
            let pinned = path
                .file_name()
                .is_some_and(|name| pinned_files.contains(&name.to_string_lossy().to_string()));
            if pinned {
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
