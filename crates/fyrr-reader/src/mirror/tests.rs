use super::*;

#[tokio::test]
async fn mutable_control_reads_through_native_named_storage_prefix() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-control-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    ));
    let mirror = MirrorStore::new(directory.clone(), 1024).unwrap();
    mirror
        .bind_dataset("s3://bucket/export/dataset.lance/data/ns/dataset.lance")
        .unwrap();
    let original = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("export/dataset.lance/data/ns/head.json");
    original.put(&path, "old".into()).await.unwrap();
    let store = mirror.wrap("control", original.clone());
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "old"
    );
    original.put(&path, "new".into()).await.unwrap();
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "new"
    );
    assert_eq!(
        mirror.totals(),
        IoSnapshot {
            requests: 2,
            bytes: 6,
            hits: 0
        }
    );
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        1,
        "only the host coordination lock exists"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn interrupted_partial_recovers_without_following_symlinks() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-partial-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    ));
    let source = directory.join("source");
    std::fs::create_dir_all(source.join("namespace/dataset.lance/data")).unwrap();
    let original = source.join("namespace/dataset.lance/data/file.lance");
    std::fs::write(&original, b"immutable file").unwrap();
    let cache = directory.join("mirror");
    let mirror = MirrorStore::new(cache.clone(), 1024).unwrap();
    let target = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(source).unwrap());
    let store = mirror.wrap("namespace", target.clone());
    let path = Path::from("namespace/dataset.lance/data/file.lance");
    let blob = cache.join(format!(
        "{}.blob",
        &digest(format!("namespace:{path}").as_bytes())[7..]
    ));
    let partial = blob.with_extension("partial");
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&partial)
        .unwrap()
        .write_all(b"incomplete")
        .unwrap();
    // Retry in the same process: the miss lock removes this incomplete write.
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "immutable file"
    );
    assert_eq!(
        mirror.totals().requests,
        1,
        "cold population needs only GET"
    );
    assert!(!partial.exists());
    std::fs::rename(&blob, &partial).unwrap();
    std::fs::write(&partial, b"interrupted rename").unwrap();
    let restarted = MirrorStore::new(cache.clone(), 1024).unwrap();
    assert!(!partial.exists(), "startup clears incomplete writes");
    let store = restarted.wrap("namespace", target);
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "immutable file"
    );
    assert_eq!(
        std::fs::metadata(&blob).unwrap().permissions().mode() & 0o777,
        0o600
    );
    std::os::unix::fs::symlink(&original, &partial).unwrap();
    assert!(MirrorStore::new(cache.clone(), 1024).is_err());
    std::fs::remove_file(&partial).unwrap();
    std::os::unix::fs::symlink(&original, cache.join("unexpected")).unwrap();
    assert!(MirrorStore::new(cache, 1024).is_err());
    assert_eq!(std::fs::read(&original).unwrap(), b"immutable file");
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn version_manifest_entries_are_bound_to_their_publication() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-versions-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
    ));
    let mut mirror = MirrorStore::new(directory.clone(), 1024).unwrap();
    mirror
        .bind_dataset("s3://bucket/root/namespaces/ns/dataset.lance")
        .unwrap();
    let original = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("root/namespaces/ns/dataset.lance/_versions/1.manifest");
    let read = |mirror: &MirrorStore| {
        let store = mirror.wrap("s3$bucket", original.clone());
        let path = path.clone();
        async move { store.get(&path).await.unwrap().bytes().await.unwrap() }
    };
    original.put(&path, "first".into()).await.unwrap();
    assert_eq!(read(&mirror).await, "first");
    assert_eq!(
        std::fs::read_dir(&directory).unwrap().count(),
        1,
        "an unbound version manifest is never cached"
    );
    mirror.publication = Some("first-publication".into());
    assert_eq!(read(&mirror).await, "first");
    // A namespace recreated at the same URI rewrites the same name.
    original.put(&path, "second".into()).await.unwrap();
    assert_eq!(read(&mirror).await, "first", "the bound entry is a hit");
    mirror.publication = Some("second-publication".into());
    assert_eq!(read(&mirror).await, "second");
    std::fs::remove_dir_all(directory).unwrap();
}

/// The same process-kill boundary mechanism used by the local storage tests.
pub(super) fn pause(stage: &str) {
    if std::env::var("FYRR_TEST_MIRROR_STAGE").as_deref() == Ok(stage) {
        std::fs::write(std::env::var_os("FYRR_TEST_MIRROR_MARKER").unwrap(), b"").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}

#[tokio::test]
#[ignore = "child of mirror population crash test"]
#[allow(clippy::print_stderr)]
async fn mirror_population_child_process() {
    let root = PathBuf::from(std::env::var_os("FYRR_TEST_MIRROR_ROOT").unwrap());
    let mirror = MirrorStore::new(root.join("mirror"), 1024).unwrap();
    let target = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(root.join("source")).unwrap(),
    );
    mirror
        .wrap("namespace", target)
        .get(&Path::from("namespace/dataset.lance/data/file.lance"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    if let Ok(hits) = std::env::var("FYRR_TEST_MIRROR_HITS") {
        let hits: usize = hits.parse().unwrap();
        let mirror = MirrorStore::new(root.join("mirror"), 1024).unwrap();
        let target = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(root.join("source")).unwrap(),
        );
        let store = mirror.wrap("namespace", target);
        let path = Path::from("namespace/dataset.lance/data/file.lance");
        store.get(&path).await.unwrap().bytes().await.unwrap();
        eprintln!("phase=warm-mirror-hits");
        let start = std::time::Instant::now();
        for _ in 0..hits {
            assert_eq!(
                store.get(&path).await.unwrap().bytes().await.unwrap(),
                "immutable file"
            );
        }
        eprintln!(
            "phase=done hits={hits} elapsed_ns={}",
            start.elapsed().as_nanos()
        );
        assert_eq!(mirror.totals().hits, hits as u64 + 1);
    }
}

#[tokio::test]
async fn mirror_population_crash_boundaries_recover_verified_bytes() {
    for stage in ["downloaded", "proof", "published"] {
        let root =
            std::env::temp_dir().join(format!("fyrr-mirror-crash-{}-{stage}", std::process::id()));
        std::fs::create_dir_all(root.join("source/namespace/dataset.lance/data")).unwrap();
        std::fs::write(
            root.join("source/namespace/dataset.lance/data/file.lance"),
            b"immutable file",
        )
        .unwrap();
        let marker = root.join("paused");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "--ignored",
                "mirror::tests::mirror_population_child_process",
            ])
            .env("FYRR_TEST_MIRROR_ROOT", &root)
            .env("FYRR_TEST_MIRROR_STAGE", stage)
            .env("FYRR_TEST_MIRROR_MARKER", &marker)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let start = std::time::Instant::now();
        while !marker.exists() {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(30),
                "{stage} boundary"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        if stage == "downloaded" {
            let cache = root.join("mirror");
            let mirror = MirrorStore::new(cache.clone(), 1024).unwrap();
            std::fs::write(
                root.join("source/namespace/dataset.lance/data/other.lance"),
                b"other",
            )
            .unwrap();
            let target = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(root.join("source")).unwrap(),
            );
            let store = mirror.wrap("namespace", target);
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                store.get(&Path::from("namespace/dataset.lance/data/other.lance")),
            )
            .await;
            // Kill before asserting so a failure cannot leave a paused child.
            if result.is_err() {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("unrelated object blocked behind a paused download");
            }
            assert_eq!(result.unwrap().unwrap().bytes().await.unwrap(), "other");
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
            mirror.evict_unpinned().await.unwrap();
            assert!(
                std::fs::read_dir(&cache).unwrap().any(|entry| entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "partial")),
                "startup and sweep preserve the active download"
            );
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let cache = root.join("mirror");
        // A power loss may retain either file, truncate it, or lose its pair.
        for entry in std::fs::read_dir(&cache).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_some_and(|extension| extension == "blob")
            {
                std::fs::write(path, b"corrupt").unwrap();
            }
        }
        let mirror = MirrorStore::new(cache, 1024).unwrap();
        let target = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(root.join("source")).unwrap(),
        );
        let store = mirror.wrap("namespace", target);
        assert_eq!(
            store
                .get(&Path::from("namespace/dataset.lance/data/file.lance"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "immutable file"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn verified_descriptor_never_serves_a_swapped_or_rewritten_blob() {
    let root = std::env::temp_dir().join(format!(
        "fyrr-mirror-descriptor-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    ));
    let source = root.join("source");
    std::fs::create_dir_all(source.join("namespace/dataset.lance/data")).unwrap();
    std::fs::write(
        source.join("namespace/dataset.lance/data/file.lance"),
        b"immutable file",
    )
    .unwrap();
    let cache = root.join("mirror");
    let mirror = MirrorStore::new(cache.clone(), 1024).unwrap();
    let target = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(source).unwrap());
    let store = mirror.wrap("namespace", target);
    let path = Path::from("namespace/dataset.lance/data/file.lance");
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "immutable file"
    );
    let blob = cache.join(format!(
        "{}.blob",
        &digest(format!("namespace:{path}").as_bytes())[7..]
    ));
    let gate = mirror.verified_read.clone();
    gate.enabled.store(true, Ordering::SeqCst);
    let reading = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move { store.get(&path).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified())
        .await
        .unwrap();
    let replacement = blob.with_extension("replacement");
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&replacement)
        .unwrap()
        .write_all(b"corrupted file")
        .unwrap();
    std::fs::rename(replacement, &blob).unwrap();
    gate.release.add_permits(1);
    assert_eq!(
        reading.await.unwrap().unwrap().bytes().await.unwrap(),
        "immutable file",
        "serve the verified inode, not the replacement path"
    );
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "immutable file",
        "the replacement must be fetched again"
    );
    // Rewrite the same inode after checksum memoization; its new fingerprint
    // must force verification and refetch rather than trusting the old memo.
    std::fs::write(&blob, b"corrupted file").unwrap();
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "immutable file"
    );
    let external = root.join("external");
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&external)
        .unwrap()
        .write_all(b"corrupted file")
        .unwrap();
    std::fs::remove_file(&blob).unwrap();
    std::os::unix::fs::symlink(&external, &blob).unwrap();
    assert!(
        store.get(&path).await.is_err(),
        "a symlink must never be followed"
    );
    assert_eq!(std::fs::read(external).unwrap(), b"corrupted file");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn prewarm_rejects_manifest_size_mismatch() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-size-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    ));
    let mirror = MirrorStore::new(directory.clone(), 1024).unwrap();
    mirror.bind_dataset("s3://bucket/dataset.lance").unwrap();
    let store = Arc::new(object_store::memory::InMemory::new());
    store
        .put(&Path::from("dataset.lance/data/file.lance"), "data".into())
        .await
        .unwrap();
    let mut files = vec![fyrr_core::envelope::FileRef {
        path: "data/file.lance".into(),
        size_bytes: 5,
    }];
    assert_eq!(
        mirror.prewarm(store.clone(), &files).await,
        Err(StorageUnavailable)
    );
    files[0].size_bytes = 4;
    assert!(mirror.prewarm(store, &files).await.is_ok());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn lease_inventory_buffering_bounds_underlying_reads() {
    struct Counted<'a> {
        bytes: &'a [u8],
        reads: &'a std::cell::Cell<usize>,
    }
    impl std::io::Read for Counted<'_> {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            self.reads.set(self.reads.get() + 1);
            std::io::Read::read(&mut self.bytes, output)
        }
    }
    let paths = vec![format!("{}.blob", "a".repeat(64)); 1000];
    let bytes = serde_json::to_vec(&paths).unwrap();
    let reads = std::cell::Cell::new(0);
    let raw: Vec<String> = serde_json::from_reader(Counted {
        bytes: &bytes,
        reads: &reads,
    })
    .unwrap();
    let before = reads.replace(0);
    let buffered: Vec<String> = serde_json::from_reader(std::io::BufReader::new(Counted {
        bytes: &bytes,
        reads: &reads,
    }))
    .unwrap();
    assert_eq!(raw, buffered);
    assert_eq!(buffered, paths);
    assert!(before >= bytes.len());
    assert!(reads.get() <= bytes.len().div_ceil(8192) + 1);
}

#[tokio::test]
async fn oversized_objects_use_direct_ranges_after_first_read() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-oversized-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    ));
    let mirror = MirrorStore::new(directory.clone(), 2).unwrap();
    let source = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("namespace/dataset.lance/data/large.lance");
    source.put(&path, "0123456789".into()).await.unwrap();
    let store = mirror.wrap("namespace", source);
    assert_eq!(store.get_range(&path, 2..4).await.unwrap(), "23");
    assert_eq!(
        std::fs::read_dir(&directory)
            .unwrap()
            .filter(|entry| entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "download"))
            .count(),
        0,
        "completed downloads leave no lock files"
    );
    let before = mirror.totals();
    assert_eq!(before.requests, 2);
    assert_eq!(store.get_range(&path, 6..8).await.unwrap(), "67");
    let after = mirror.totals();
    assert_eq!(after.requests - before.requests, 1);
    assert_eq!(after.bytes - before.bytes, 2);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn retired_request_drops_schedule_one_sweep() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-retired-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_micros()
    ));
    let mirror = Arc::new(MirrorStore::new(directory.clone(), 1024).unwrap());
    mirror
        .bind_dataset("s3://bucket/namespace/dataset.lance")
        .unwrap();
    let source = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("namespace/dataset.lance/data/file.lance");
    source.put(&path, "data".into()).await.unwrap();
    let store = mirror.wrap("s3$bucket", source);
    store.get(&path).await.unwrap().bytes().await.unwrap();
    let blob = directory.join(mirror.blob_name("s3$bucket", path.as_ref()).unwrap());
    let lease = mirror
        .pin(&[fyrr_core::envelope::FileRef {
            path: "data/file.lance".into(),
            size_bytes: 4,
        }])
        .await
        .unwrap();
    let first = lease.request().unwrap();
    let last = lease.request().unwrap();
    lease.retire();
    drop(first);
    assert!(
        !lease.sweep_requested.load(Ordering::SeqCst),
        "a drop while the lease flock is held cannot consume final cleanup"
    );
    lease.release(true);
    mirror.evict_unpinned().await.unwrap();
    assert!(blob.exists(), "the remaining request still pins the blob");
    drop(last);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while blob.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(lease.sweep_requested.load(Ordering::SeqCst));
    drop(lease);
    std::fs::remove_dir_all(directory).unwrap();
}

fn blob_bytes(directory: &std::path::Path) -> u64 {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            MirrorStore::cache_name(&name, ".blob") || MirrorStore::cache_name(&name, ".partial")
        })
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

fn temporary_directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "fyrr-mirror-{name}-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
    ))
}

/// `count` objects of `size` bytes under `dataset.lance/data/`, named in manifest order.
async fn numbered_files(
    source: &object_store::memory::InMemory,
    count: usize,
    size: usize,
) -> Vec<fyrr_core::envelope::FileRef> {
    let mut files = Vec::new();
    for index in 0..count {
        let path = format!("data/f{index:03}.lance");
        let bytes = vec![u8::try_from(index % 251).unwrap(); size];
        source
            .put(&Path::from(format!("dataset.lance/{path}")), bytes.into())
            .await
            .unwrap();
        files.push(fyrr_core::envelope::FileRef {
            path,
            size_bytes: size as u64,
        });
    }
    files
}

fn bound_mirror(directory: &std::path::Path, max_bytes: u64) -> Arc<MirrorStore> {
    let mirror = Arc::new(MirrorStore::new(directory.to_owned(), max_bytes).unwrap());
    mirror.bind_dataset("s3://bucket/dataset.lance").unwrap();
    mirror
}

fn probes(mirror: &MirrorStore) -> (u64, u64, u64) {
    let probes = &mirror.room_probes;
    (
        probes.listings.load(Ordering::SeqCst),
        probes.stats.load(Ordering::SeqCst),
        probes.scans.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn make_room_scans_only_when_a_fill_does_not_fit() {
    let directory = temporary_directory("room-fit");
    let mirror = bound_mirror(&directory, 1000);
    let source = Arc::new(object_store::memory::InMemory::new());
    let files = numbered_files(&source, 8, 10).await;
    mirror.prewarm(source.clone(), &files).await.unwrap();
    let (listings, stats, scans) = probes(&mirror);
    assert_eq!(listings, 8, "one names-only listing per miss");
    assert_eq!(scans, 0, "nothing needed eviction, so nothing was scanned");
    assert!(
        stats <= 16,
        "each file is stat-ed when first seen, as a partial and as a blob: {stats}"
    );
    // Every entry is a hit now: no miss, no make_room, no directory work.
    mirror.prewarm(source.clone(), &files).await.unwrap();
    assert_eq!(probes(&mirror), (listings, stats, 0));
    assert_eq!(mirror.totals().hits, 8);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn make_room_accounts_files_left_by_an_earlier_process() {
    let directory = temporary_directory("room-earlier");
    let source = Arc::new(object_store::memory::InMemory::new());
    let files = numbered_files(&source, 5, 100).await;
    let earlier = bound_mirror(&directory, 1000);
    earlier.prewarm(source.clone(), &files[..3]).await.unwrap();
    assert_eq!(blob_bytes(&directory), 300);
    drop(earlier);
    // A new process with a smaller budget starts on the same directory.
    let mirror = bound_mirror(&directory, 350);
    let store = mirror.wrap("s3$bucket", source.clone());
    store
        .get(&Path::from("dataset.lance/data/f003.lance"))
        .await
        .unwrap();
    // 300 + 100 > 350: the files of the earlier process were counted, so one
    // of them was evicted to make room.
    assert_eq!(probes(&mirror).2, 1);
    assert_eq!(blob_bytes(&directory), 300);
    // A budget the earlier files leave room in needs no scan.
    let roomy = bound_mirror(&directory, 1000);
    roomy
        .wrap("s3$bucket", source.clone())
        .get(&Path::from("dataset.lance/data/f004.lance"))
        .await
        .unwrap();
    assert_eq!(probes(&roomy).2, 0);
    assert_eq!(blob_bytes(&directory), 400);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn make_room_sees_files_another_process_added_since_its_last_call() {
    let directory = temporary_directory("room-foreign");
    let source = Arc::new(object_store::memory::InMemory::new());
    let files = numbered_files(&source, 6, 100).await;
    let ours = bound_mirror(&directory, 300);
    let other = bound_mirror(&directory, 300);
    ours.prewarm(source.clone(), &files[..1]).await.unwrap();
    // The other process fills two more files. Our last view held one.
    other.prewarm(source.clone(), &files[1..3]).await.unwrap();
    let store = ours.wrap("s3$bucket", source.clone());
    store
        .get(&Path::from("dataset.lance/data/f003.lance"))
        .await
        .unwrap();
    assert!(blob_bytes(&directory) <= 300, "{}", blob_bytes(&directory));
    assert_eq!(
        probes(&ours).2,
        1,
        "the stale view did not fit and was scanned"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "test-store")]
mod latency {
    use super::*;
    use crate::storage::test_store::latency::LatencyStore;
    use std::time::Duration;

    const LATENCY: Duration = Duration::from_secs(1);

    struct Fixture {
        directory: PathBuf,
        mirror: Arc<MirrorStore>,
        source: Arc<object_store::memory::InMemory>,
        files: Vec<fyrr_core::envelope::FileRef>,
    }
    async fn fixture(name: &str, count: usize, size: usize, max_bytes: u64) -> Fixture {
        let directory = temporary_directory(name);
        let mirror = bound_mirror(&directory, max_bytes);
        let source = Arc::new(object_store::memory::InMemory::new());
        let files = numbered_files(&source, count, size).await;
        Fixture {
            directory,
            mirror,
            source,
            files,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn prewarm_fills_files_within_the_concurrency_bound() {
        let Fixture {
            directory,
            mirror,
            source,
            files,
        } = fixture("prewarm-rounds", 40, 10, 1024).await;
        let _lease = mirror.pin(&files).await.unwrap();
        let store = LatencyStore::new(source, LATENCY);
        let start = tokio::time::Instant::now();
        mirror
            .prewarm(Arc::new(store.clone()), &files)
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(store.requests(), 40, "one object GET per file");
        assert_eq!(store.bytes(), 400);
        assert!(
            store
                .reads()
                .iter()
                .all(|read| !read.head && read.range.is_none())
        );
        assert_eq!(store.peak_in_flight(), MirrorStore::PREWARM_CONCURRENCY);
        assert_eq!(
            store.rounds(elapsed),
            40_u128.div_ceil(MirrorStore::PREWARM_CONCURRENCY as u128),
            "a sequential pass takes 40 rounds: {elapsed:?}"
        );
        let totals = mirror.totals();
        assert_eq!((totals.requests, totals.bytes), (40, 400));
        for file in &files {
            let path = Path::from(format!("dataset.lance/{}", file.path));
            let blob = directory.join(mirror.blob_name("s3$bucket", path.as_ref()).unwrap());
            assert!(mirror.verified_file(&blob).await.unwrap());
            assert!(blob.with_extension("checksum").exists());
        }
        assert_eq!(blob_bytes(&directory), 400);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn prewarm_failure_stops_new_fills_and_keeps_the_error() {
        let Fixture {
            directory,
            mirror,
            source,
            mut files,
        } = fixture("prewarm-missing", 60, 10, 4096).await;
        source
            .delete(&Path::from("dataset.lance/data/f007.lance"))
            .await
            .unwrap();
        let _lease = mirror.pin(&files).await.unwrap();
        let store = LatencyStore::new(source.clone(), LATENCY);
        assert_eq!(
            mirror.prewarm(Arc::new(store.clone()), &files).await,
            Err(StorageUnavailable)
        );
        assert!(
            store.requests() < 2 * MirrorStore::PREWARM_CONCURRENCY,
            "no new fill starts once one failed: {}",
            store.requests()
        );
        // A size mismatch is the same failure as a missing object.
        source
            .put(&Path::from("dataset.lance/data/f007.lance"), "x".into())
            .await
            .unwrap();
        files[7].size_bytes = 10;
        assert_eq!(
            mirror.prewarm(source.clone(), &files).await,
            Err(StorageUnavailable)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn prewarm_reports_a_storage_failure_before_a_capacity_failure() {
        // The first file is larger than the whole budget and the second is
        // missing. A capacity failure can be caused by another failure in the
        // batch, so the storage failure is the one reported.
        let Fixture {
            directory,
            mirror,
            source,
            mut files,
        } = fixture("prewarm-order", 6, 10, 25).await;
        source
            .put(
                &Path::from("dataset.lance/data/f000.lance"),
                vec![0u8; 30].into(),
            )
            .await
            .unwrap();
        files[0].size_bytes = 30;
        source
            .delete(&Path::from("dataset.lance/data/f001.lance"))
            .await
            .unwrap();
        let _lease = mirror.pin(&files).await.unwrap();
        let store = LatencyStore::new(source.clone(), LATENCY);
        assert_eq!(
            mirror.prewarm(Arc::new(store), &files).await,
            Err(StorageUnavailable)
        );
        std::fs::remove_dir_all(directory).unwrap();
        // Now the missing file comes first: file 0 fits, 1 is missing, and
        // later files beyond the budget are out of capacity.
        let Fixture {
            directory,
            mirror,
            source,
            files,
        } = fixture("prewarm-order-second", 6, 10, 25).await;
        source
            .delete(&Path::from("dataset.lance/data/f001.lance"))
            .await
            .unwrap();
        let _lease = mirror.pin(&files).await.unwrap();
        let store = LatencyStore::new(source, LATENCY);
        assert_eq!(
            mirror.prewarm(Arc::new(store), &files).await,
            Err(StorageUnavailable)
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_fills_never_exceed_the_byte_budget() {
        let budget = 205;
        let Fixture {
            directory,
            mirror,
            source,
            files,
        } = fixture("prewarm-budget", 40, 10, budget).await;
        let _lease = mirror.pin(&files).await.unwrap();
        let worst = Arc::new(AtomicU64::new(0));
        let store = LatencyStore::new(source, LATENCY).observed({
            let (directory, worst) = (directory.clone(), worst.clone());
            move |_| {
                worst.fetch_max(blob_bytes(&directory), Ordering::SeqCst);
            }
        });
        // 40 pinned files of 10 bytes cannot all fit 205 bytes, so the fill fails
        // the way a sequential one fails: out of capacity.
        assert_eq!(
            mirror.prewarm(Arc::new(store.clone()), &files).await,
            Err(MirrorCapacityExhausted)
        );
        assert!(store.peak_in_flight() <= MirrorStore::PREWARM_CONCURRENCY);
        assert!(worst.load(Ordering::SeqCst) <= budget);
        let used = blob_bytes(&directory);
        assert!(used <= budget && used >= 190, "{used}");
        std::fs::remove_dir_all(directory).unwrap();
    }
}
