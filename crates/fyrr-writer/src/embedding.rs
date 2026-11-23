//! Host-local, source-lease-owned document cache for live provider vectors.
use crate::{Result, WriterError};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use fyrr_core::config::{Config, DenseElement, EmbeddingConfig};
use fyrr_reader::embedding::{EmbeddingClient, ProviderStats, Role, cache_key, digest, validate};

const MAX_BYTES: u64 = 256 * 1024 * 1024;
const CACHE_MAGIC: &[u8; 8] = b"FYRRWC01";
const HEADER_BYTES: usize = 8 + 4 + 71;

fn checksum(key: &str, dimensions: u32, bytes: &[u8]) -> String {
    digest(&[key.as_bytes(), &dimensions.to_le_bytes(), bytes].concat())
}

pub struct WriterEmbedding {
    config: EmbeddingConfig,
    element: DenseElement,
    client: Option<EmbeddingClient>,
    cap: Option<u64>,
    local: LocalCache,
    pub hits: u64,
    pub misses: u64,
    pub stats: ProviderStats,
}

impl std::fmt::Debug for WriterEmbedding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WriterEmbedding")
            .field("hits", &self.hits)
            .field("misses", &self.misses)
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl WriterEmbedding {
    pub async fn open(config: &Config, source: &str) -> Result<Self> {
        let embedding = config.embedding.clone();
        let cap = std::env::var("FYRR_WRITER_EMBEDDING_CAP")
            .ok()
            .map(|limit| {
                limit
                    .parse::<u64>()
                    .map_err(|_| WriterError::EmbeddingSpendConfigurationInvalid)
            })
            .transpose()?;
        let stats = ProviderStats::default();
        let local = {
            let base = embedding
                .local_cache
                .as_ref()
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("XDG_CACHE_HOME")
                        .map(PathBuf::from)
                        .or_else(|| {
                            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache"))
                        })
                        .map(|root| root.join("fyrr/writer-embeddings"))
                })
                .ok_or(WriterError::EmbeddingLocalCacheConfigurationInvalid)?;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&base)
                .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            let base =
                fs::canonicalize(base).map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            // Separate namespaces and sources never share mutable cache ownership.
            let scope = digest(
                serde_json::json!([
                    config.storage.uri,
                    config.storage.endpoint,
                    config.namespace.name,
                    source
                ])
                .to_string()
                .as_bytes(),
            );
            let local = base.join(&scope[7..]);
            match fs::DirBuilder::new().mode(0o700).create(&local) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => return Err(WriterError::EmbeddingLocalCacheUnavailable),
            }
            let metadata = fs::symlink_metadata(&local)
                .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(WriterError::EmbeddingLocalCacheConfigurationInvalid);
            }
            LocalCache::open(local)?
        };
        Ok(Self {
            config: embedding,
            element: config.namespace.dense_element,
            client: None,
            cap,
            local,
            hits: 0,
            misses: 0,
            stats,
        })
    }

    pub async fn embed(&mut self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut vectors = BTreeMap::new();
        let mut missing = BTreeMap::new();
        let keys = texts
            .iter()
            .map(|text| cache_key(&self.config, text, Role::Document))
            .collect::<Vec<_>>();
        for (text, key) in texts.iter().zip(&keys) {
            if vectors.contains_key(key) || missing.contains_key(key) {
                continue;
            }
            let vector = read_vector(
                &self.local.root.join(&key[7..]),
                self.config.dimensions,
                self.element,
            )?;
            if let Some(vector) = vector {
                self.hits += 1;
                vectors.insert(key.clone(), vector);
            } else {
                self.misses += 1;
                missing.insert(key.clone(), *text);
            }
        }
        let missing = missing.into_iter().collect::<Vec<_>>();
        for batch in missing.chunks(self.config.batch_size as usize) {
            let client = initialize_client(&self.config, self.cap, &mut self.client)?;
            let input = batch.iter().map(|(_, text)| *text).collect::<Vec<_>>();
            let embedded = client
                .embed(&input, Role::Document, &mut self.stats)
                .await
                .map_err(WriterError::from)?;
            for ((key, _), vector) in batch.iter().zip(embedded) {
                // Before it is kept: a vector the namespace cannot store would
                // fail every retry from the cache, after the provider is fixed.
                validate(&vector, self.config.dimensions, self.element)
                    .map_err(|_| WriterError::EmbeddingBatchInvalid)?;
                self.local.write(&key[7..], &vector)?;
                vectors.insert(key.clone(), vector);
            }
        }
        keys.iter()
            .map(|key| {
                vectors
                    .get(key)
                    .cloned()
                    .ok_or(WriterError::EmbeddingBatchInvalid)
            })
            .collect()
    }

    /// Verify the provider even when all documents are unchanged or cached.
    /// Without one, every document brings its vector and there is nothing to verify.
    pub async fn start(&mut self) -> Result<()> {
        if self.config.endpoint.is_empty() {
            return Ok(());
        }
        initialize_client(&self.config, self.cap, &mut self.client)?
            .verify_model(&mut self.stats)
            .await
            .map_err(WriterError::from)
    }
}

fn initialize_client<'a>(
    config: &EmbeddingConfig,
    cap: Option<u64>,
    slot: &'a mut Option<EmbeddingClient>,
) -> Result<&'a EmbeddingClient> {
    match slot {
        Some(client) => Ok(client),
        slot @ None => {
            if config.endpoint.is_empty() {
                return Err(WriterError::EmbeddingConfigurationInvalid);
            }
            let mut config = config.clone();
            let spend = config
                .spend
                .as_mut()
                .ok_or(WriterError::EmbeddingSpendLimitRequired)?;
            if let Some(cap) = cap {
                spend.limit_microusd = spend.limit_microusd.min(cap);
            }
            if spend.limit_microusd < spend.max_call_microusd {
                return Err(WriterError::EmbeddingSpendLimitExceeded);
            }
            Ok(slot.insert(EmbeddingClient::new(&config, None).map_err(WriterError::from)?))
        }
    }
}

fn read_vector(path: &Path, dimensions: u32, element: DenseElement) -> Result<Option<Vec<f32>>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(WriterError::EmbeddingLocalCacheUnavailable),
    };
    let expected = HEADER_BYTES as u64 + u64::from(dimensions) * 4;
    if file
        .metadata()
        .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?
        .len()
        != expected
    {
        return Ok(None);
    }
    let mut bytes = vec![0; crate::narrow_usize(expected)];
    if let Err(error) = file.read_exact(&mut bytes) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(WriterError::EmbeddingLocalCacheUnavailable);
    }
    let key = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(WriterError::EmbeddingLocalCacheInvalid)?;
    let payload = &bytes[HEADER_BYTES..];
    if &bytes[..8] != CACHE_MAGIC
        || bytes[8..12] != dimensions.to_le_bytes()
        || bytes[12..HEADER_BYTES] != *checksum(key, dimensions, payload).as_bytes()
    {
        return Ok(None);
    }
    let vector = payload
        .chunks_exact(4)
        .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect::<Vec<_>>();
    if validate(&vector, dimensions, element).is_err() {
        return Ok(None);
    }
    Ok(Some(vector))
}

// Charge one filesystem block of overhead as well as the payload: tiny profiles
// cannot create an unbounded number of files or directory inventory entries.
struct LocalCache {
    root: PathBuf,
    files: VecDeque<(PathBuf, u64)>,
    bytes: u64,
}
impl LocalCache {
    fn open(root: PathBuf) -> Result<Self> {
        let mut files = Vec::new();
        for entry in fs::read_dir(&root).map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)? {
            let entry = entry.map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(WriterError::EmbeddingLocalCacheInvalid);
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') {
                fs::remove_file(entry.path())
                    .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
                continue;
            }
            if !fyrr_core::limits::ascii_hex(&name, 64) {
                return Err(WriterError::EmbeddingLocalCacheInvalid);
            }
            files.push((
                metadata
                    .modified()
                    .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?,
                entry.path(),
                metadata
                    .len()
                    .div_ceil(4096)
                    .saturating_mul(4096)
                    .saturating_add(4096),
            ));
        }
        files.sort_unstable();
        let bytes = files.iter().map(|(_, _, size)| size).sum();
        let mut cache = Self {
            root,
            bytes,
            files: files
                .into_iter()
                .map(|(_, path, size)| (path, size))
                .collect(),
        };
        cache.trim(0)?;
        Ok(cache)
    }
    fn trim(&mut self, incoming: u64) -> Result<()> {
        while self.bytes.saturating_add(incoming) > MAX_BYTES {
            let (path, size) = self
                .files
                .pop_front()
                .ok_or(WriterError::EmbeddingLocalCacheInvalid)?;
            fs::remove_file(path).map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            self.bytes -= size;
        }
        Ok(())
    }
    fn write(&mut self, key: &str, vector: &[f32]) -> Result<()> {
        let bytes = vector
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let dimensions = crate::narrow_u32(vector.len() as u64);
        let hash = checksum(key, dimensions, &bytes);
        let size = (HEADER_BYTES as u64 + bytes.len() as u64).div_ceil(4096) * 4096 + 4096;
        self.trim(size)?;
        let temporary = self.root.join(format!(".{}-{}", std::process::id(), key));
        let path = self.root.join(key);
        let result: Result<()> = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)
                .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            file.write_all(CACHE_MAGIC)
                .and_then(|()| file.write_all(&dimensions.to_le_bytes()))
                .and_then(|()| file.write_all(hash.as_bytes()))
                .and_then(|()| file.write_all(&bytes))
                .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            #[cfg(test)]
            tests::pause("written");
            // Reconstructible cache: restart validates the complete entry before use.
            fs::rename(&temporary, &path)
                .map_err(|_| WriterError::EmbeddingLocalCacheUnavailable)?;
            #[cfg(test)]
            tests::pause("published");
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result?;
        self.files.retain(|(existing, previous_size)| {
            if existing == &path {
                self.bytes -= previous_size;
                false
            } else {
                true
            }
        });
        self.bytes += size;
        self.files.push_back((path, size));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    pub(super) fn pause(stage: &str) {
        if std::env::var("FYRR_TEST_VECTOR_STAGE").as_deref() == Ok(stage) {
            fs::write(std::env::var_os("FYRR_TEST_VECTOR_MARKER").unwrap(), b"").unwrap();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }

    #[test]
    #[ignore = "child of embedding cache crash test"]
    fn local_cache_write_child_process() {
        let root = PathBuf::from(std::env::var_os("FYRR_TEST_VECTOR_ROOT").unwrap());
        LocalCache::open(root)
            .unwrap()
            .write(&"a".repeat(64), &[0.25; 512])
            .unwrap();
    }

    #[test]
    fn local_cache_crash_boundaries_recover_as_misses() {
        for stage in ["written", "published"] {
            let scratch = Scratch::new();
            let marker = scratch.0.with_extension("paused");
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "--ignored",
                    "embedding::tests::local_cache_write_child_process",
                ])
                .env("FYRR_TEST_VECTOR_ROOT", &scratch.0)
                .env("FYRR_TEST_VECTOR_STAGE", stage)
                .env("FYRR_TEST_VECTOR_MARKER", &marker)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let start = std::time::Instant::now();
            while !marker.exists() {
                assert!(
                    start.elapsed() < std::time::Duration::from_secs(30),
                    "{stage} boundary"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            child.kill().unwrap();
            child.wait().unwrap();
            fs::remove_file(&marker).unwrap();
            let key = "a".repeat(64);
            if stage == "published" {
                fs::write(scratch.0.join(&key), b"torn").unwrap();
            }
            let mut cache = LocalCache::open(scratch.0.clone()).unwrap();
            assert_eq!(
                read_vector(&scratch.0.join(&key), 512, DenseElement::F16).unwrap(),
                None
            );
            cache.write(&key, &[0.5; 512]).unwrap();
            drop(cache);
            let cache = LocalCache::open(scratch.0.clone()).unwrap();
            assert_eq!(cache.files.len(), 1);
            assert_eq!(
                read_vector(&scratch.0.join(&key), 512, DenseElement::F16).unwrap(),
                Some(vec![0.5; 512])
            );
            // Same-sized corruption must also be a miss, even with a valid header.
            let path = scratch.0.join(&key);
            let mut bytes = fs::read(&path).unwrap();
            *bytes.last_mut().unwrap() ^= 1;
            fs::write(&path, bytes).unwrap();
            assert_eq!(read_vector(&path, 512, DenseElement::F16).unwrap(), None);
        }
    }

    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/tmp")
                .join(format!(
                    "writer-cache-{}-{:?}",
                    std::process::id(),
                    std::thread::current().id()
                ));
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&root)
                .unwrap();
            Self(root)
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn local_cache_restart_dimension_validation_and_private_files() {
        let scratch = Scratch::new();
        let key = "a".repeat(64);
        let mut cache = LocalCache::open(scratch.0.clone()).unwrap();
        cache.write(&key, &[0.125; 512]).unwrap();
        assert_eq!(
            fs::metadata(scratch.0.join(&key))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        drop(cache);
        let cache = LocalCache::open(scratch.0.clone()).unwrap();
        assert_eq!(cache.files.len(), 1);
        assert_eq!(
            read_vector(&scratch.0.join(&key), 512, DenseElement::F16).unwrap(),
            Some(vec![0.125; 512])
        );
        assert_eq!(
            read_vector(&scratch.0.join(&key), 1024, DenseElement::F16),
            Ok(None)
        );
        fs::write(scratch.0.join(&key), [0u8; 2048]).unwrap();
        assert_eq!(
            read_vector(&scratch.0.join(&key), 512, DenseElement::F16),
            Ok(None)
        );
        drop(cache);
        let mut cache = LocalCache::open(scratch.0.clone()).unwrap();
        cache.write(&key, &[0.25; 512]).unwrap();
        assert_eq!(cache.files.len(), 1);
        assert_eq!(
            read_vector(&scratch.0.join(&key), 512, DenseElement::F16).unwrap(),
            Some(vec![0.25; 512])
        );
        fs::remove_file(scratch.0.join(&key)).unwrap();
        assert_eq!(
            read_vector(&scratch.0.join(&key), 512, DenseElement::F16).unwrap(),
            None
        );
    }

    #[test]
    fn local_cache_evicts_at_fixed_bound_and_refuses_symlinks() {
        let scratch = Scratch::new();
        let old = scratch.0.join("a".repeat(64));
        fs::File::create(&old)
            .unwrap()
            .set_len(MAX_BYTES - 4096)
            .unwrap();
        let mut cache = LocalCache::open(scratch.0.clone()).unwrap();
        cache.write(&"b".repeat(64), &[1.; 512]).unwrap();
        assert!(!old.exists());
        assert!(cache.bytes <= MAX_BYTES);
        assert_eq!(cache.files.len(), 1);
        let link = scratch.0.join("c".repeat(64));
        symlink(scratch.0.join("b".repeat(64)), &link).unwrap();
        assert_eq!(
            read_vector(&link, 512, DenseElement::F16),
            Err(WriterError::EmbeddingLocalCacheUnavailable)
        );
        assert!(matches!(
            LocalCache::open(scratch.0.clone()),
            Err(WriterError::EmbeddingLocalCacheInvalid)
        ));
    }
    #[tokio::test]
    #[ignore = "diagnostic: local embedding cache on one runtime thread"]
    async fn local_cache_runtime_io_measurement() {
        let scratch = Scratch::new();
        let mut cache = LocalCache::open(scratch.0.clone()).unwrap();
        for n in 0..1024 {
            cache.write(&format!("{n:064x}"), &[0.25; 512]).unwrap();
        }
        drop(cache);
        let cache = crate::io_probe::measure("embedding-cache-open-1024", async {
            LocalCache::open(scratch.0.clone()).unwrap()
        })
        .await;
        assert_eq!(cache.files.len(), 1024);
        crate::io_probe::measure("embedding-cache-hit-1024x512", async {
            for n in 0..1024 {
                assert!(
                    read_vector(&scratch.0.join(format!("{n:064x}")), 512)
                        .unwrap()
                        .is_some()
                );
            }
        })
        .await;
    }
}
