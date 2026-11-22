//! Page-granular read-through of `data/` and `_indices/` files.
//!
//! A Lance data file holds every column of its rows, and most reads want one
//! column or a few rows: the admission scan reads `source_id` of every file,
//! rescoring and hydration read scattered rows. An index file (a vector
//! partition, a full-text posting list, a scalar index page) is read the same
//! way, a few ranges of a large object. Copying the whole object on the first
//! such read downloads what the query never touches. A ranged read of a
//! `data/` or `_indices/` file larger than [`WHOLE_FILE_BYTES`] whose size the pinned
//! manifest states therefore fetches only the 4 KiB pages it lacks and keeps
//! them in two owned-private files named by the blob stem (a smaller file is
//! copied whole when the mirror has room for it):
//!
//! - `<stem>.sparse`: a sparse file of the object's length holding the pages
//!   fetched so far;
//! - `<stem>.map`: a header (magic, object size, a random generation, ETag of
//!   the first response) and one 16-byte entry per page, the truncated
//!   SHA-256 of the page as it was received, or zeros while it is absent.
//!
//! A page is written before its entry. A page whose entry is absent, torn or
//! does not match its bytes is fetched again, so no write needs a flush for
//! correctness. Each page is verified against its entry once per process and
//! per generation of the map before it is served: a stem removed and created
//! again, even on reused inodes, has a new generation and is verified anew.
//! A response whose size differs from the manifest, or whose ETag differs
//! from the one the map recorded, fails that read and drops the stem's pages.
//! A later read keeps the object again only if its size still matches.
//! Both files are created under the global locks, inside the byte budget, and
//! are evicted and swept with their stem. A complete `.blob` of the same
//! object, written by `prewarm` or a whole read, supersedes them.
use super::*;
use futures::TryStreamExt;
use std::os::unix::fs::FileExt;

const PAGE: u64 = 4096;
const ENTRY_BYTES: usize = 16;
const ENTRY: u64 = ENTRY_BYTES as u64;
const HEADER_BYTES: usize = 128;
const HEADER: u64 = HEADER_BYTES as u64;
const MAGIC: &[u8; 8] = b"FYRRMAP1";
/// Header layout: magic, size, generation, ETag length, ETag.
const GENERATION: std::ops::Range<usize> = 16..32;
const TAG: usize = 34;
/// Missing pages closer than this are fetched by one request, as Lance merges
/// its own reads (`block_size`, 64 KiB).
const GAP_PAGES: u64 = 16;
/// The largest single request, Lance's `max_iop_size` (16 MiB).
const FETCH_PAGES: u64 = (16 << 20) / PAGE;
/// Requests of one read in flight at once.
const FETCH_CONCURRENCY: usize = 16;
/// A data or index file up to this size is downloaded whole on its first read rather
/// than in pages. Opening a reader touches the footer, the column metadata
/// and the first page of every data file, two or three dependent requests,
/// and warming then downloads the file anyway: one request instead of up to
/// four, at the cost of transferring the file before the first answer.
// shortcut: chosen from a measurement with added latency and no bandwidth
// limit; confirm it on an object store over a bandwidth-bound link.
pub(super) const WHOLE_FILE_BYTES: u64 = 2 << 20;

/// Pages of one stem verified in this process, valid for one map generation.
#[derive(Debug, Default)]
pub(super) struct VerifiedPages {
    generation: [u8; 16],
    bits: Vec<u64>,
}
impl VerifiedPages {
    fn get(&self, page: u64) -> bool {
        self.bits
            .get(usize::try_from(page / 64).unwrap_or(usize::MAX))
            .is_some_and(|word| word & (1 << (page % 64)) != 0)
    }
    fn set(&mut self, page: u64) {
        let Ok(word) = usize::try_from(page / 64) else {
            return;
        };
        if self.bits.len() <= word {
            self.bits.resize(word + 1, 0);
        }
        self.bits[word] |= 1 << (page % 64);
    }
}

/// The open sparse copy and page map of one stem.
struct Pages {
    sparse: std::fs::File,
    map: std::fs::File,
    generation: [u8; 16],
    tag: Vec<u8>,
}

/// One fetched run of pages: `[first, end)` and the bytes of those pages.
struct Fetched {
    first: u64,
    end: u64,
    /// The object size the response stated.
    size: u64,
    bytes: Vec<u8>,
}

impl MirrorStore {
    /// A `data/` or `_indices/` file of the bound dataset.
    pub(super) fn ranged_class(&self, location: &Path) -> bool {
        let key = crate::sync::lock(&self.dataset_cache_key);
        key.as_ref().is_some_and(|(_, root)| {
            location
                .as_ref()
                .strip_prefix(root.as_str())
                .and_then(|relative| relative.strip_prefix('/'))
                .is_some_and(|relative| {
                    relative.starts_with("data/") || relative.starts_with("_indices/")
                })
        })
    }
    /// The size a pinned manifest states for the object behind `blob`.
    pub(super) fn manifest_size(&self, blob: &std::path::Path) -> Option<u64> {
        let name = blob.file_name()?.to_str()?;
        crate::sync::lock(&self.manifest_sizes).get(name).copied()
    }
    /// A head of a paged file answered from the manifest: no request, no download.
    pub(super) fn head_result(location: &Path, blob: &std::path::Path, size: u64) -> GetResult {
        GetResult {
            payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
            meta: Self::object_meta(location, blob, size),
            range: 0..0,
            attributes: Default::default(),
            extensions: Default::default(),
        }
    }
    /// Metadata of a paged object. The tag is derived from the blob name, which
    /// addresses this object: a head and a paged read of it agree, and no other
    /// object shares it. A read served by the complete file carries that file's
    /// local tag instead; Lance keys cached state by ETag only for version
    /// manifests, which are never paged.
    fn object_meta(location: &Path, blob: &std::path::Path, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: location.clone(),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size,
            e_tag: blob
                .file_stem()
                .map(|stem| format!("\"fyrr-{}\"", stem.to_string_lossy())),
            version: None,
        }
    }
    /// An offset or length within one response or page.
    fn local(value: u64) -> Result<usize> {
        usize::try_from(value).map_err(|_| Self::failure())
    }
    fn page_len(size: u64, page: u64) -> u64 {
        PAGE.min(size - page * PAGE)
    }
    fn page_digest(bytes: &[u8]) -> [u8; ENTRY_BYTES] {
        let digest = Sha256::digest(bytes);
        let mut entry = [0; ENTRY_BYTES];
        entry.copy_from_slice(&digest[..ENTRY_BYTES]);
        entry
    }
    /// Whether the page files of an object of `size` bytes fit the budget.
    pub(super) fn pages_fit(&self, size: u64) -> bool {
        size.saturating_add(Self::map_len(size)) <= self.max_bytes
    }
    fn map_len(size: u64) -> u64 {
        HEADER + size.div_ceil(PAGE) * ENTRY
    }
    /// An owned private file, or `None` when it is absent or was unlinked
    /// (another task superseded or dropped the stem) since it was opened.
    fn open_existing(path: &std::path::Path) -> std::io::Result<Option<std::fs::File>> {
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let meta = file.metadata()?;
        if meta.nlink() == 0 {
            return Ok(None);
        }
        if !Self::owned_private_file(&meta) {
            return Err(std::io::Error::other("storage_unavailable"));
        }
        Ok(Some(file))
    }
    /// The stem's files when both exist, are of the expected lengths and the
    /// map header names this object. A map without a valid header is still
    /// being created, or its creator died; either way it serves no pages.
    fn open_pages(blob: &std::path::Path, size: u64) -> std::io::Result<Option<Pages>> {
        let Some(map) = Self::open_existing(&blob.with_extension("map"))? else {
            return Ok(None);
        };
        let Some(sparse) = Self::open_existing(&blob.with_extension("sparse"))? else {
            return Ok(None);
        };
        // A map unlinked while the sparse file was opened may belong to an
        // older generation than the sparse file: neither is used then.
        let map_meta = map.metadata()?;
        if map_meta.nlink() == 0
            || sparse.metadata()?.len() != size
            || map_meta.len() != Self::map_len(size)
        {
            return Ok(None);
        }
        let mut header = [0; HEADER_BYTES];
        map.read_exact_at(&mut header, 0)?;
        let Some((generation, tag)) = Self::parse_header(&header, size) else {
            return Ok(None);
        };
        Ok(Some(Pages {
            sparse,
            map,
            generation,
            tag,
        }))
    }
    fn parse_header(header: &[u8; HEADER_BYTES], size: u64) -> Option<([u8; 16], Vec<u8>)> {
        if &header[..8] != MAGIC || header[8..16] != size.to_le_bytes() {
            return None;
        }
        let generation: [u8; 16] = header[GENERATION].try_into().ok()?;
        if generation == [0; 16] {
            return None;
        }
        let len = usize::from(u16::from_le_bytes([header[32], header[33]]));
        Some((generation, header.get(TAG..TAG + len)?.to_vec()))
    }
    fn header(size: u64, generation: [u8; 16], tag: &[u8]) -> Option<[u8; HEADER_BYTES]> {
        let mut header = [0; HEADER_BYTES];
        header[..8].copy_from_slice(MAGIC);
        header[8..16].copy_from_slice(&size.to_le_bytes());
        header[GENERATION].copy_from_slice(&generation);
        header[32..34].copy_from_slice(&u16::try_from(tag.len()).ok()?.to_le_bytes());
        header.get_mut(TAG..TAG + tag.len())?.copy_from_slice(tag);
        Some(header)
    }
    /// The bytes of `page` when it is present and matches its entry. A page
    /// verified once in this generation of the map is not hashed again.
    fn read_page(
        &self,
        blob: &std::path::Path,
        pages: &Pages,
        size: u64,
        page: u64,
    ) -> Option<Vec<u8>> {
        let verified = crate::sync::lock(&self.verified_pages)
            .get(blob)
            .is_some_and(|v| v.generation == pages.generation && v.get(page));
        let mut bytes = vec![0; usize::try_from(Self::page_len(size, page)).ok()?];
        pages.sparse.read_exact_at(&mut bytes, page * PAGE).ok()?;
        if verified {
            return Some(bytes);
        }
        let mut entry = [0; ENTRY_BYTES];
        pages
            .map
            .read_exact_at(&mut entry, HEADER + page * ENTRY)
            .ok()?;
        if entry == [0; ENTRY_BYTES] || Self::page_digest(&bytes) != entry {
            return None;
        }
        self.mark_verified(blob, pages.generation, page);
        Some(bytes)
    }
    fn mark_verified(&self, blob: &std::path::Path, generation: [u8; 16], page: u64) {
        let mut verified = crate::sync::lock(&self.verified_pages);
        let entry = verified.entry(blob.to_owned()).or_default();
        if entry.generation != generation {
            *entry = VerifiedPages {
                generation,
                bits: Vec::new(),
            };
        }
        entry.set(page);
    }
    /// Missing pages grouped into requests: gaps up to `GAP_PAGES` are fetched
    /// with their neighbours, and no request exceeds `FETCH_PAGES`.
    fn plan(missing: &[u64]) -> Vec<(u64, u64)> {
        let mut runs: Vec<(u64, u64)> = Vec::new();
        for &page in missing {
            match runs.last_mut() {
                Some((first, end))
                    if page - *end <= GAP_PAGES && page + 1 - *first <= FETCH_PAGES =>
                {
                    *end = page + 1;
                }
                _ => runs.push((page, page + 1)),
            }
        }
        runs
    }
    async fn fetch_run(
        &self,
        location: &Path,
        size: u64,
        (first, end): (u64, u64),
        counters: &IoCounters,
    ) -> Result<(Fetched, Vec<u8>)> {
        let range = first * PAGE..(end * PAGE).min(size);
        self.record(counters, Io::Request, 1);
        let response = self
            .target()
            .get_opts(
                location,
                GetOptions {
                    range: Some(range.clone().into()),
                    ..Default::default()
                },
            )
            .await?;
        if response.meta.size != size {
            let fetched = Fetched {
                first,
                end,
                size: response.meta.size,
                bytes: Vec::new(),
            };
            return Ok((fetched, Vec::new()));
        }
        if response.range != range {
            return Err(Self::failure());
        }
        let tag = response.meta.e_tag.clone().unwrap_or_default().into_bytes();
        let bytes = response.bytes().await?.to_vec();
        self.record(counters, Io::Bytes, bytes.len() as u64);
        if bytes.len() as u64 != range.end - range.start {
            return Err(Self::failure());
        }
        Ok((
            Fetched {
                first,
                end,
                size,
                bytes,
            },
            tag,
        ))
    }
    /// Answers `range` of a `data/` or `_indices/` object of `size` bytes from its pages,
    /// fetching the missing ones. File I/O and hashing run on the blocking pool.
    pub(super) async fn read_pages(
        &self,
        location: &Path,
        range: std::ops::Range<u64>,
        size: u64,
        blob: &std::path::Path,
        counters: &IoCounters,
    ) -> Result<GetResult> {
        let (first, end) = (range.start / PAGE, range.end.div_ceil(PAGE));
        let (mut pages, mut held) = {
            let mirror = self.clone();
            let blob = blob.to_owned();
            tokio::task::spawn_blocking(move || -> Result<_> {
                let Some(pages) = Self::open_pages(&blob, size).map_err(|_| Self::failure())?
                else {
                    return Ok((None, vec![None; Self::local(end - first)?]));
                };
                let held = (first..end)
                    .map(|page| mirror.read_page(&blob, &pages, size, page))
                    .collect::<Vec<_>>();
                Ok((Some(Arc::new(pages)), held))
            })
            .await
            .map_err(|_| Self::failure())??
        };
        let missing: Vec<u64> = (first..end)
            .zip(&held)
            .filter(|(_, bytes)| bytes.is_none())
            .map(|(page, _)| page)
            .collect();
        if missing.is_empty() {
            self.record(counters, Io::Hit, 1);
        } else {
            let responses: Vec<_> = futures::stream::iter(
                Self::plan(&missing)
                    .into_iter()
                    .map(|run| self.fetch_run(location, size, run, counters)),
            )
            .buffered(FETCH_CONCURRENCY)
            .try_collect()
            .await?;
            if responses.iter().any(|(f, _)| f.size != size) {
                // The object changed under its immutable name.
                self.drop_pages(blob).await;
                return Err(Self::failure());
            }
            let tag = responses[0].1.clone();
            if responses.iter().any(|(_, t)| *t != tag)
                || pages.as_ref().is_some_and(|p| p.tag != tag)
            {
                // The object changed under its immutable name.
                self.drop_pages(blob).await;
                return Err(Self::failure());
            }
            let fetched: Vec<Fetched> = responses.into_iter().map(|(f, _)| f).collect();
            for f in &fetched {
                for page in f.first..f.end {
                    let start = Self::local((page - f.first) * PAGE)?;
                    let len = Self::local(Self::page_len(size, page))?;
                    held[Self::local(page - first)?] = Some(f.bytes[start..start + len].to_vec());
                }
            }
            if pages.is_none() {
                pages = self.create_pages(blob, size, &tag).await?.map(Arc::new);
            }
            if let Some(pages) = pages.clone() {
                let mirror = self.clone();
                let blob = blob.to_owned();
                tokio::task::spawn_blocking(move || {
                    mirror.store_pages(&blob, &pages, size, &fetched)
                })
                .await
                .map_err(|_| Self::failure())??;
            }
        }
        let mut out = Vec::with_capacity(Self::local(range.end - range.start)?);
        for (page, bytes) in (first..end).zip(held) {
            let bytes = bytes.ok_or_else(Self::failure)?;
            let at = page * PAGE;
            let from = Self::local(range.start.saturating_sub(at))?;
            let to = Self::local(range.end.min(at + bytes.len() as u64) - at)?;
            out.extend_from_slice(&bytes[from..to]);
        }
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(out.into()) }).boxed(),
            ),
            meta: Self::object_meta(location, blob, size),
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }
    /// Writes fetched pages, each before its entry.
    fn store_pages(
        &self,
        blob: &std::path::Path,
        pages: &Pages,
        size: u64,
        fetched: &[Fetched],
    ) -> Result<()> {
        for f in fetched {
            for page in f.first..f.end {
                let start = Self::local((page - f.first) * PAGE)?;
                let bytes = &f.bytes[start..start + Self::local(Self::page_len(size, page))?];
                pages
                    .sparse
                    .write_all_at(bytes, page * PAGE)
                    .map_err(|_| Self::failure())?;
                pages
                    .map
                    .write_all_at(&Self::page_digest(bytes), HEADER + page * ENTRY)
                    .map_err(|_| Self::failure())?;
                self.mark_verified(blob, pages.generation, page);
            }
        }
        Ok(())
    }
    /// Creates the stem's files inside the byte budget, or returns `None` when
    /// they do not fit or the complete file exists (the read is then served
    /// without keeping its pages). Creation holds the global locks, so a map
    /// without a valid header seen here was left by a dead creator and is
    /// replaced.
    async fn create_pages(
        &self,
        blob: &std::path::Path,
        size: u64,
        tag: &[u8],
    ) -> Result<Option<Pages>> {
        let generation = crate::storage::random_tag()
            .map_err(|_| Self::failure())?
            .to_be_bytes();
        let Some(header) = Self::header(size, generation, tag) else {
            return Ok(None);
        };
        let _lock = self.lock.lock().await;
        let _host = self.host_lock().await.map_err(|_| Self::failure())?;
        // The complete file, published or being downloaded, supersedes pages.
        // A partial left by a failed download is removed rather than obeyed.
        let partial = blob.with_extension("partial");
        if partial.exists() {
            Self::remove_idle_partial(&partial).map_err(|_| Self::failure())?;
        }
        if blob.symlink_metadata().is_ok() || partial.exists() {
            return Ok(None);
        }
        if let Some(pages) = Self::open_pages(blob, size).map_err(|_| Self::failure())? {
            if pages.tag != tag {
                // The object changed under its immutable name.
                self.remove_pages(blob).map_err(|_| Self::failure())?;
                return Err(Self::failure());
            }
            return Ok(Some(pages));
        }
        self.remove_pages(blob).map_err(|_| Self::failure())?;
        if !self.make_room(size + Self::map_len(size))? {
            return Ok(None);
        }
        let create = |path: &std::path::Path, len: u64| -> std::io::Result<std::fs::File> {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
                .open(path)?;
            file.set_len(len)?;
            Ok(file)
        };
        let sparse = create(&blob.with_extension("sparse"), size).map_err(|_| Self::failure())?;
        let map = create(&blob.with_extension("map"), Self::map_len(size))
            .map_err(|_| Self::failure())?;
        map.write_all_at(&header, 0).map_err(|_| Self::failure())?;
        Ok(Some(Pages {
            sparse,
            map,
            generation,
            tag: tag.to_vec(),
        }))
    }
    /// Removes a stem's page files, under the global locks.
    async fn drop_pages(&self, blob: &std::path::Path) {
        let _lock = self.lock.lock().await;
        if let Ok(_host) = self.host_lock().await {
            let _ = self.remove_pages(blob);
        }
    }
    /// Removes a stem's page files; the caller holds the global locks. The
    /// budget forgets their sizes, since a new file may reuse their inodes.
    pub(super) fn remove_pages(&self, blob: &std::path::Path) -> std::io::Result<()> {
        for extension in ["sparse", "map"] {
            let path = blob.with_extension(extension);
            Self::remove_partial(&path)?;
            if let Some(name) = path.file_name() {
                crate::sync::lock(&self.sizes).remove(name.to_string_lossy().as_ref());
            }
        }
        crate::sync::lock(&self.verified_pages).remove(blob);
        Ok(())
    }
}
