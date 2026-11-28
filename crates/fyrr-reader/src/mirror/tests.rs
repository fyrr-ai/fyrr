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

/// Lance heads a version under its other manifest name each time it opens
/// the dataset. A publication lists its version manifests, so the mirror
/// answers that miss itself.
#[tokio::test]
async fn a_version_manifest_the_publication_lacks_needs_no_storage_request() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-absent-version-{}-{}",
        std::process::id(),
        fyrr_core::clock::now().timestamp_nanos_opt().unwrap()
    ));
    let mut mirror = MirrorStore::new(directory.clone(), 1024).unwrap();
    mirror
        .bind_dataset("s3://bucket/root/namespaces/ns/dataset.lance")
        .unwrap();
    let root = "root/namespaces/ns/dataset.lance";
    let listed = Path::from(format!("{root}/_versions/13.manifest"));
    let other = Path::from(format!("{root}/_versions/18446744073709551602.manifest"));
    let original = Arc::new(object_store::memory::InMemory::new());
    original.put(&listed, "manifest".into()).await.unwrap();
    let source =
        crate::storage::test_store::latency::LatencyStore::new(original, std::time::Duration::ZERO);
    let absent = |error: object_store::Error| matches!(error, object_store::Error::NotFound { .. });

    // Without a publication the mirror cannot know, and asks storage.
    let store = mirror.wrap("s3$bucket", Arc::new(source.clone()));
    assert!(absent(store.head(&other).await.unwrap_err()));
    assert_eq!(source.requests(), 1);

    mirror.publication = Some("publication".into());
    let file = |path: &str| fyrr_core::envelope::FileRef {
        path: path.into(),
        size_bytes: 8,
    };
    // A publication that lists no version manifest decides nothing.
    mirror.version_manifests = Some(MirrorStore::version_manifests(&[file("data/a.lance")]));
    let store = mirror.wrap("s3$bucket", Arc::new(source.clone()));
    assert!(absent(store.head(&other).await.unwrap_err()));
    assert_eq!(source.requests(), 2);

    mirror.version_manifests = Some(MirrorStore::version_manifests(&[
        file("data/a.lance"),
        file("_versions/13.manifest"),
    ]));
    let store = mirror.wrap("s3$bucket", Arc::new(source.clone()));
    assert!(absent(store.head(&other).await.unwrap_err()));
    assert!(absent(store.get(&other).await.unwrap_err()));
    assert_eq!(source.requests(), 2, "the publication settles it");
    assert_eq!(
        store.get(&listed).await.unwrap().bytes().await.unwrap(),
        "manifest"
    );
    assert_eq!(source.requests(), 3);
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
            // The paused child never finishes its download, so any finite
            // timeout tells waiting behind it apart from not waiting.
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
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
    assert_eq!(
        std::fs::read_dir(&directory)
            .unwrap()
            .filter(|entry| {
                let path = entry.as_ref().unwrap().path();
                ["blob", "partial", "checksum"]
                    .iter()
                    .any(|extension| path.extension().is_some_and(|e| e == *extension))
            })
            .count(),
        0,
        "an object of another size is kept in no form"
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

/// A mirror bound to `s3://bucket/dataset.lance` that reads every pinned
/// data file in pages, however small.
fn bound_mirror(directory: &std::path::Path, max_bytes: u64) -> Arc<MirrorStore> {
    let mut mirror = MirrorStore::new(directory.to_owned(), max_bytes).unwrap();
    mirror.whole_bytes = 0;
    mirror.bind_dataset("s3://bucket/dataset.lance").unwrap();
    Arc::new(mirror)
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
    async fn warming_downloads_nothing_while_a_request_runs() {
        let Fixture {
            directory,
            mirror,
            source,
            files,
        } = fixture("prewarm-yields", 20, 10, 1024).await;
        let lease = mirror.pin(&files).await.unwrap();
        let store = LatencyStore::new(source, LATENCY);
        let request = lease.request().unwrap();
        // The request's own fill does not stop for it.
        mirror
            .prewarm_if_room(Arc::new(store.clone()), &files[..1])
            .await
            .unwrap();
        assert_eq!(store.requests(), 1);
        // Warming takes what the mirror holds, and stops before a download.
        assert_eq!(
            mirror.prewarm(Arc::new(store.clone()), &files).await,
            Err(Busy)
        );
        assert_eq!(store.requests(), 1, "no download while a request runs");
        drop(request);
        mirror
            .prewarm(Arc::new(store.clone()), &files)
            .await
            .unwrap();
        assert_eq!(store.requests(), 20, "the rest once no request runs");
        // With every file here, a request stops nothing.
        let _request = lease.request().unwrap();
        mirror
            .prewarm(Arc::new(store.clone()), &files)
            .await
            .unwrap();
        assert_eq!(store.requests(), 20);
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

/// A mirror bound to `dataset.lance` with one pinned `data/` object of `size`
/// bytes, and the store a reader of that mirror sees.
struct PagedFixture {
    directory: PathBuf,
    mirror: Arc<MirrorStore>,
    source: Arc<object_store::memory::InMemory>,
    store: Arc<dyn ObjectStore>,
    files: Vec<fyrr_core::envelope::FileRef>,
    lease: Option<Arc<MirrorLease>>,
    path: Path,
    blob: PathBuf,
}
const PAGED_SIZE: usize = 100_000;
impl PagedFixture {
    async fn new(name: &str, max_bytes: u64) -> Self {
        Self::at(name, max_bytes, "data/f.lance").await
    }
    /// The object at `relative` to the dataset root.
    async fn at(name: &str, max_bytes: u64, relative: &str) -> Self {
        let directory = temporary_directory(name);
        let mirror = bound_mirror(&directory, max_bytes);
        let source = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from(format!("dataset.lance/{relative}"));
        source.put(&path, Self::bytes(0).into()).await.unwrap();
        let files = vec![fyrr_core::envelope::FileRef {
            path: relative.into(),
            size_bytes: PAGED_SIZE as u64,
        }];
        let lease = Some(mirror.pin(&files).await.unwrap());
        let store = mirror.wrap("s3$bucket", source.clone());
        let blob = directory.join(mirror.blob_name("s3$bucket", path.as_ref()).unwrap());
        Self {
            directory,
            mirror,
            source,
            store,
            files,
            lease,
            path,
            blob,
        }
    }
    /// The object's content; `seed` changes every byte.
    fn bytes(seed: usize) -> Vec<u8> {
        (0..PAGED_SIZE)
            .map(|i| u8::try_from((i * 7 + seed) % 251).unwrap())
            .collect()
    }
    async fn read(&self, range: std::ops::Range<u64>) -> object_store::Result<Vec<u8>> {
        Ok(self.store.get_range(&self.path, range).await?.to_vec())
    }
    fn exists(&self, extension: &str) -> bool {
        self.blob.with_extension(extension).exists()
    }
}
impl Drop for PagedFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// A small data file in a mirror of `max_bytes`, pinned at `PAGED_SIZE`,
/// whose stored object holds `content`.
async fn small_file(
    name: &str,
    max_bytes: u64,
    content: Vec<u8>,
) -> (
    PathBuf,
    Arc<MirrorStore>,
    Arc<dyn ObjectStore>,
    Arc<MirrorLease>,
    Path,
) {
    let directory = temporary_directory(name);
    let mirror = Arc::new({
        let mirror = MirrorStore::new(directory.clone(), max_bytes).unwrap();
        mirror.bind_dataset("s3://bucket/dataset.lance").unwrap();
        mirror
    });
    let source = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("dataset.lance/data/f.lance");
    source.put(&path, content.into()).await.unwrap();
    let lease = mirror
        .pin(&[fyrr_core::envelope::FileRef {
            path: "data/f.lance".into(),
            size_bytes: PAGED_SIZE as u64,
        }])
        .await
        .unwrap();
    let store = mirror.wrap("s3$bucket", source);
    (directory, mirror, store, lease, path)
}

/// A small data file larger than the mirror is read past it, one request per
/// read, and one whose stored size differs from the manifest fails.
#[tokio::test]
async fn a_small_data_file_that_cannot_be_kept_whole_is_not_downloaded_whole() {
    let expected = PagedFixture::bytes(0);
    let (directory, mirror, store, _lease, path) =
        small_file("small-no-room", 50_000, expected.clone()).await;
    for range in [90_000..100_000_usize, 0..4096] {
        let bytes = range.start as u64..range.end as u64;
        assert_eq!(
            store.get_range(&path, bytes).await.unwrap(),
            expected[range]
        );
    }
    assert_eq!(mirror.totals().requests, 2, "one ranged request per read");
    assert!(
        mirror.totals().bytes < PAGED_SIZE as u64,
        "never the whole file"
    );
    std::fs::remove_dir_all(directory).unwrap();

    // A file that fits alone but not next to a pinned one: no whole GET.
    let (directory, mirror, store, _lease, path) =
        small_file("small-full", 150_000, expected.clone()).await;
    let other = Path::from("dataset.lance/data/g.lance");
    let source = Arc::new(object_store::memory::InMemory::new());
    source.put(&path, expected.clone().into()).await.unwrap();
    source.put(&other, expected.clone().into()).await.unwrap();
    let _other_lease = mirror
        .pin(&[fyrr_core::envelope::FileRef {
            path: "data/g.lance".into(),
            size_bytes: PAGED_SIZE as u64,
        }])
        .await
        .unwrap();
    let store_both = mirror.wrap("s3$bucket", source);
    drop(store);
    store_both.get_range(&other, 0..4096).await.unwrap();
    let before = mirror.totals();
    assert_eq!((before.requests, before.bytes), (1, PAGED_SIZE as u64));
    for range in [90_000..100_000_usize, 0..4096] {
        let bytes = range.start as u64..range.end as u64;
        assert_eq!(
            store_both.get_range(&path, bytes).await.unwrap(),
            expected[range]
        );
    }
    let after = mirror.totals();
    assert_eq!(
        after.requests - before.requests,
        2,
        "one ranged request per read"
    );
    assert!(
        after.bytes - before.bytes < PAGED_SIZE as u64,
        "never the whole file"
    );
    std::fs::remove_dir_all(directory).unwrap();

    let (directory, _mirror, store, _lease, path) =
        small_file("small-changed", 1 << 20, vec![0; 10]).await;
    assert!(store.get_range(&path, 0..5).await.is_err());
    assert!(
        !std::fs::read_dir(&directory).unwrap().any(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|e| e == "blob")),
        "a file of another size is not kept"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

/// A data or index file no larger than `WHOLE_FILE_BYTES` is downloaded whole by its
/// first ranged read: one request instead of one per scattered range.
#[tokio::test]
async fn a_small_data_file_is_downloaded_whole_on_its_first_read() {
    for relative in ["data/f.lance", "_indices/u/index.idx"] {
        small_file_is_downloaded_whole(relative).await;
    }
}

async fn small_file_is_downloaded_whole(relative: &str) {
    let directory = temporary_directory("small-whole");
    let mirror = Arc::new({
        let mirror = MirrorStore::new(directory.clone(), 1 << 20).unwrap();
        assert!(mirror.whole_bytes >= PAGED_SIZE as u64);
        mirror.bind_dataset("s3://bucket/dataset.lance").unwrap();
        mirror
    });
    let source = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from(format!("dataset.lance/{relative}"));
    let expected = PagedFixture::bytes(0);
    source.put(&path, expected.clone().into()).await.unwrap();
    let _lease = mirror
        .pin(&[fyrr_core::envelope::FileRef {
            path: relative.into(),
            size_bytes: PAGED_SIZE as u64,
        }])
        .await
        .unwrap();
    let store = mirror.wrap("s3$bucket", source);
    assert_eq!(store.head(&path).await.unwrap().size, PAGED_SIZE as u64);
    assert_eq!(
        mirror.totals().requests,
        0,
        "a pinned head needs no request"
    );
    for range in [90_000..100_000_usize, 0..4096, 50_000..50_010] {
        let bytes = range.start as u64..range.end as u64;
        assert_eq!(
            store.get_range(&path, bytes).await.unwrap(),
            expected[range]
        );
    }
    assert_eq!(
        (mirror.totals().requests, mirror.totals().bytes),
        (1, PAGED_SIZE as u64),
        "one whole download serves every range"
    );
    let blob = directory.join(mirror.blob_name("s3$bucket", path.as_ref()).unwrap());
    assert!(blob.exists() && !blob.with_extension("sparse").exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn data_ranges_fetch_only_the_pages_they_lack() {
    let f = PagedFixture::new("pages", 1 << 20).await;
    let expected = PagedFixture::bytes(0);
    let head = f.store.head(&f.path).await.unwrap();
    assert_eq!(head.size, PAGED_SIZE as u64);
    assert_eq!(
        f.mirror.totals().requests,
        0,
        "a pinned head needs no request"
    );
    assert_eq!(
        f.read(10_000..10_100).await.unwrap(),
        expected[10_000..10_100]
    );
    assert_eq!(
        (f.mirror.totals().requests, f.mirror.totals().bytes),
        (1, 4096),
        "one page"
    );
    assert_eq!(
        f.read(10_000..10_100).await.unwrap(),
        expected[10_000..10_100]
    );
    assert_eq!(
        f.mirror.totals().requests,
        1,
        "a held page is not fetched again"
    );
    assert_eq!(
        f.read(10_000..20_000).await.unwrap(),
        expected[10_000..20_000]
    );
    assert_eq!(
        (f.mirror.totals().requests, f.mirror.totals().bytes),
        (2, 3 * 4096),
        "only the two missing pages"
    );
    assert_eq!(f.read(90_000..100_000).await.unwrap(), expected[90_000..]);
    assert_eq!(
        f.mirror.totals().bytes,
        3 * 4096 + (100_000 - 21 * 4096),
        "the last page is short"
    );
    // Missing pages a few pages apart are fetched by one request.
    assert_eq!(f.read(0..1).await.unwrap(), expected[..1]);
    let before = f.mirror.totals();
    let mut ranges = f
        .store
        .get_ranges(&f.path, &[30_000..30_010, 50_000..50_010])
        .await
        .unwrap()
        .into_iter();
    assert_eq!(ranges.next().unwrap(), expected[30_000..30_010]);
    assert_eq!(ranges.next().unwrap(), expected[50_000..50_010]);
    assert_eq!(f.mirror.totals().requests - before.requests, 1);
    assert!(f.exists("sparse") && f.exists("map") && !f.exists("blob"));
    assert_eq!(
        std::fs::metadata(f.blob.with_extension("map"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

/// An `_indices/` file above the threshold is read in verified pages like a
/// data file, and warming replaces its pages with the complete file.
#[tokio::test]
async fn index_ranges_fetch_only_the_pages_they_lack() {
    let f = PagedFixture::at("pages-index", 1 << 20, "_indices/u/index.idx").await;
    let expected = PagedFixture::bytes(0);
    assert_eq!(f.store.head(&f.path).await.unwrap().size, PAGED_SIZE as u64);
    assert_eq!(
        f.mirror.totals().requests,
        0,
        "a pinned head needs no request"
    );
    assert_eq!(
        f.read(10_000..10_100).await.unwrap(),
        expected[10_000..10_100]
    );
    assert_eq!(
        (f.mirror.totals().requests, f.mirror.totals().bytes),
        (1, 4096),
        "one page"
    );
    assert_eq!(
        f.read(10_000..20_000).await.unwrap(),
        expected[10_000..20_000]
    );
    assert_eq!(
        (f.mirror.totals().requests, f.mirror.totals().bytes),
        (2, 3 * 4096),
        "only the two missing pages"
    );
    assert!(f.exists("sparse") && f.exists("map") && !f.exists("blob"));
    f.mirror.prewarm(f.source.clone(), &f.files).await.unwrap();
    assert!(f.exists("blob") && !f.exists("sparse") && !f.exists("map"));
    let requests = f.mirror.totals().requests;
    assert_eq!(
        f.read(60_000..70_000).await.unwrap(),
        expected[60_000..70_000]
    );
    assert_eq!(f.mirror.totals().requests, requests);
}

/// An index file that fits the mirror but not together with its page map is
/// downloaded whole by one request, not read past the mirror range by range.
#[tokio::test]
async fn an_index_file_that_fits_only_without_its_map_is_downloaded_whole() {
    let size = PAGED_SIZE as u64;
    let f = PagedFixture::at("pages-no-map-room", size, "_indices/u/index.idx").await;
    assert!(!f.mirror.pages_fit(size));
    let expected = PagedFixture::bytes(0);
    for range in [90_000..100_000_usize, 0..4096] {
        let bytes = range.start as u64..range.end as u64;
        assert_eq!(f.read(bytes).await.unwrap(), expected[range]);
    }
    assert_eq!(
        (f.mirror.totals().requests, f.mirror.totals().bytes),
        (1, size),
        "one whole download serves every range"
    );
    assert!(f.exists("blob") && !f.exists("sparse"));
}

#[tokio::test]
async fn a_damaged_or_unrecorded_page_is_fetched_again() {
    let f = PagedFixture::new("pages-damaged", 1 << 20).await;
    let expected = PagedFixture::bytes(0);
    assert_eq!(f.read(0..20_000).await.unwrap(), expected[..20_000]);
    let requests = f.mirror.totals().requests;
    {
        let sparse = std::fs::OpenOptions::new()
            .write(true)
            .open(f.blob.with_extension("sparse"))
            .unwrap();
        std::os::unix::fs::FileExt::write_all_at(&sparse, &[0xff; 10], 4096).unwrap();
        let map = std::fs::OpenOptions::new()
            .write(true)
            .open(f.blob.with_extension("map"))
            .unwrap();
        // The entry of page 3 is lost, as after a crash between page and entry.
        std::os::unix::fs::FileExt::write_all_at(&map, &[0; 16], 128 + 3 * 16).unwrap();
    }
    // A restarted process verifies each page before serving it.
    let mut restarted = MirrorStore::new(f.directory.clone(), 1 << 20).unwrap();
    restarted.whole_bytes = 0;
    restarted.bind_dataset("s3://bucket/dataset.lance").unwrap();
    let restarted = Arc::new(restarted);
    let _lease = restarted.pin(&f.files).await.unwrap();
    let store = restarted.wrap("s3$bucket", f.source.clone());
    assert_eq!(
        store.get_range(&f.path, 0..20_000).await.unwrap().to_vec(),
        expected[..20_000]
    );
    assert_eq!(
        (restarted.totals().requests, restarted.totals().bytes),
        (1, 3 * 4096),
        "pages 1 to 3 are fetched again in one request"
    );
    assert_eq!(f.mirror.totals().requests, requests);
}

#[tokio::test]
async fn a_changed_object_fails_closed_and_drops_its_pages() {
    let f = PagedFixture::new("pages-changed", 1 << 20).await;
    f.read(0..100).await.unwrap();
    f.source
        .put(&f.path, PagedFixture::bytes(1).into())
        .await
        .unwrap();
    assert!(f.read(50_000..50_100).await.is_err());
    assert!(!f.exists("sparse") && !f.exists("map"));
    // A size that differs from the manifest fails too.
    f.source.put(&f.path, vec![0; 10].into()).await.unwrap();
    assert!(f.read(0..5).await.is_err());
}

#[tokio::test]
async fn prewarm_replaces_pages_with_the_complete_file() {
    let f = PagedFixture::new("pages-prewarm", 1 << 20).await;
    let expected = PagedFixture::bytes(0);
    f.read(0..100).await.unwrap();
    f.mirror.prewarm(f.source.clone(), &f.files).await.unwrap();
    assert!(f.exists("blob") && !f.exists("sparse") && !f.exists("map"));
    let requests = f.mirror.totals().requests;
    assert_eq!(
        f.read(60_000..70_000).await.unwrap(),
        expected[60_000..70_000]
    );
    assert_eq!(f.mirror.totals().requests, requests);
}

#[tokio::test]
async fn pages_obey_the_byte_budget_and_the_pins() {
    // The object does not fit: ranges are served and nothing is kept.
    let small = PagedFixture::new("pages-budget", 50_000).await;
    let expected = PagedFixture::bytes(0);
    assert_eq!(small.read(0..10).await.unwrap(), expected[..10]);
    assert_eq!(small.read(0..10).await.unwrap(), expected[..10]);
    assert_eq!(small.mirror.totals().requests, 2);
    assert!(!small.exists("sparse") && !small.exists("map"));

    let mut f = PagedFixture::new("pages-pins", 1 << 20).await;
    f.read(0..10).await.unwrap();
    f.mirror.evict_unpinned().await.unwrap();
    assert!(f.exists("sparse") && f.exists("map"), "pinned pages stay");
    drop(f.lease.take());
    f.mirror.evict_unpinned().await.unwrap();
    assert!(!f.exists("sparse") && !f.exists("map"), "unpinned pages go");
}

#[tokio::test]
async fn unpinned_pages_are_evicted_to_make_room() {
    let mut f = PagedFixture::new("pages-evict", 150_000).await;
    f.read(0..10).await.unwrap();
    drop(f.lease.take());
    // Another object needs the room the unpinned pages hold.
    let other = Path::from("dataset.lance/data/g.lance");
    f.source.put(&other, vec![1; 100_000].into()).await.unwrap();
    assert_eq!(
        f.store
            .get(&other)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .len(),
        100_000
    );
    assert!(!f.exists("sparse") && !f.exists("map"));
}

#[tokio::test]
async fn pages_of_a_new_map_generation_are_verified_again() {
    let f = PagedFixture::new("pages-generation", 1 << 20).await;
    let expected = PagedFixture::bytes(0);
    f.read(0..40_000).await.unwrap();
    // The stem as another process would leave it after removing and creating
    // it again on the same inodes: a new generation, no pages held.
    {
        let map = std::fs::OpenOptions::new()
            .write(true)
            .open(f.blob.with_extension("map"))
            .unwrap();
        std::os::unix::fs::FileExt::write_all_at(&map, &[0x5a; 16], 16).unwrap();
        std::os::unix::fs::FileExt::write_all_at(&map, &[0; 10 * 16], 128).unwrap();
        let sparse = std::fs::OpenOptions::new()
            .write(true)
            .open(f.blob.with_extension("sparse"))
            .unwrap();
        sparse.set_len(0).unwrap();
        sparse.set_len(PAGED_SIZE as u64).unwrap();
    }
    let requests = f.mirror.totals().requests;
    assert_eq!(f.read(0..40_000).await.unwrap(), expected[..40_000]);
    assert_eq!(
        f.mirror.totals().requests,
        requests + 1,
        "pages verified under the old generation are not trusted"
    );
}

#[tokio::test]
async fn a_size_change_drops_the_kept_pages() {
    let f = PagedFixture::new("pages-size", 1 << 20).await;
    f.read(0..100).await.unwrap();
    assert!(f.exists("sparse"));
    // A longer object: the range is still served, with another size.
    f.source
        .put(&f.path, vec![0; 2 * PAGED_SIZE].into())
        .await
        .unwrap();
    assert!(f.read(50_000..50_100).await.is_err());
    assert!(!f.exists("sparse") && !f.exists("map"));
}

#[tokio::test]
async fn a_damaged_complete_file_is_downloaded_again() {
    let f = PagedFixture::new("pages-damaged-blob", 1 << 20).await;
    f.mirror.prewarm(f.source.clone(), &f.files).await.unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&f.blob)
        .unwrap()
        .write_all(b"rot")
        .unwrap();
    // A new process verifies the complete file before trusting it.
    let restarted = Arc::new({
        let mut restarted = MirrorStore::new(f.directory.clone(), 1 << 20).unwrap();
        restarted.whole_bytes = 0;
        restarted.bind_dataset("s3://bucket/dataset.lance").unwrap();
        restarted
    });
    let _lease = restarted.pin(&f.files).await.unwrap();
    let store = restarted.wrap("s3$bucket", f.source.clone());
    let expected = PagedFixture::bytes(0);
    assert_eq!(
        store.get_range(&f.path, 0..10).await.unwrap().to_vec(),
        expected[..10]
    );
    assert_eq!(
        store
            .get_range(&f.path, 5_000..5_010)
            .await
            .unwrap()
            .to_vec(),
        expected[5_000..5_010]
    );
    assert_eq!(
        restarted.totals().requests,
        1,
        "one whole download heals it"
    );
    assert!(!f.exists("sparse"));
}

#[tokio::test]
async fn heads_and_ranges_of_paged_files_share_a_stable_tag() {
    let f = PagedFixture::new("pages-tag", 1 << 20).await;
    let head = f.store.head(&f.path).await.unwrap();
    let read = f
        .store
        .get_opts(
            &f.path,
            GetOptions {
                range: Some((0..10).into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(head.e_tag.is_some());
    assert_eq!(read.meta.e_tag, head.e_tag);
    // Suffix and offset ranges resolve against the manifest size.
    let expected = PagedFixture::bytes(0);
    let suffix = f
        .store
        .get_opts(
            &f.path,
            GetOptions {
                range: Some(GetRange::Suffix(10)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(suffix.range, 99_990..100_000);
    assert_eq!(suffix.bytes().await.unwrap(), expected[99_990..]);
    let offset = f
        .store
        .get_opts(
            &f.path,
            GetOptions {
                range: Some(GetRange::Offset(99_000)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(offset.bytes().await.unwrap(), expected[99_000..]);
}

#[tokio::test]
async fn heads_of_other_files_are_not_answered_from_the_manifest() {
    let directory = temporary_directory("pages-index-head");
    let mirror = bound_mirror(&directory, 1 << 20);
    let source = Arc::new(object_store::memory::InMemory::new());
    let path = Path::from("dataset.lance/_deletions/i/part.arrow");
    source.put(&path, vec![7; 100].into()).await.unwrap();
    let _lease = mirror
        .pin(&[fyrr_core::envelope::FileRef {
            path: "_deletions/i/part.arrow".into(),
            size_bytes: 100,
        }])
        .await
        .unwrap();
    let store = mirror.wrap("s3$bucket", source);
    assert_eq!(store.head(&path).await.unwrap().size, 100);
    assert_eq!(mirror.totals().requests, 1, "the head reads the object");
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn a_paged_file_larger_than_the_budget_evicts_nothing() {
    let f = PagedFixture::new("pages-oversized", 60_000).await;
    // An unpinned file the oversized reads must leave in place.
    let other = Path::from("dataset.lance/data/g.lance");
    f.source.put(&other, vec![1; 1_000].into()).await.unwrap();
    f.store.get(&other).await.unwrap();
    let kept = f
        .directory
        .join(f.mirror.blob_name("s3$bucket", other.as_ref()).unwrap());
    assert!(kept.exists());
    let expected = PagedFixture::bytes(0);
    for _ in 0..2 {
        assert_eq!(f.read(0..10).await.unwrap(), expected[..10]);
    }
    assert!(kept.exists());
    assert!(!f.exists("sparse"));
    assert_eq!(f.mirror.totals().requests, 3);
}

#[tokio::test]
async fn prewarm_after_pages_needs_no_room_for_both() {
    // Room for the complete file, not for the file and its pages.
    let f = PagedFixture::new("pages-room", 150_000).await;
    f.read(0..100_000).await.unwrap();
    assert!(f.exists("sparse"));
    f.mirror.prewarm(f.source.clone(), &f.files).await.unwrap();
    assert!(f.exists("blob") && !f.exists("sparse") && !f.exists("map"));
}

/// A whole copy for a walk that reads every partition costs one request, and
/// none where the mirror has no room for it: the walk then reads pages.
#[tokio::test]
async fn prewarm_if_room_copies_whole_only_what_fits_and_asks_nothing_otherwise() {
    let f = PagedFixture::at("room-fits", 1 << 20, "_indices/u/aux.idx").await;
    f.mirror
        .prewarm_if_room(f.source.clone(), &f.files)
        .await
        .unwrap();
    assert!(f.exists("blob"));
    assert_eq!(f.mirror.totals().requests, 1);

    let f = PagedFixture::at("room-short", 50_000, "_indices/u/aux.idx").await;
    for _ in 0..2 {
        assert_eq!(
            f.mirror.prewarm_if_room(f.source.clone(), &f.files).await,
            Err(MirrorCapacityExhausted)
        );
    }
    assert!(!f.exists("blob") && !f.exists("partial"));
    assert_eq!(f.mirror.totals().requests, 0, "no request, no head");

    // Room for the file, not beside what the mirror holds: nothing is evicted.
    let f = PagedFixture::at("room-held", 150_000, "_indices/u/aux.idx").await;
    let held = f.directory.join(format!("{}.blob", "a".repeat(64)));
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&held)
        .unwrap()
        .write_all(&[0; 80_000])
        .unwrap();
    assert_eq!(
        f.mirror.prewarm_if_room(f.source.clone(), &f.files).await,
        Err(MirrorCapacityExhausted)
    );
    assert!(!f.exists("blob") && held.exists(), "nothing is evicted");
    assert_eq!(f.mirror.totals().requests, 0);
}

/// Room is reserved under the mirror's locks before any request: two files
/// that do not fit together are not both fetched, whatever their order, and
/// the one that is left costs nothing.
#[tokio::test]
async fn prewarm_if_room_reserves_room_for_one_of_two_files_that_do_not_fit_together() {
    let directory = temporary_directory("room-race");
    let mirror = bound_mirror(&directory, 150_000);
    let source = Arc::new(object_store::memory::InMemory::new());
    let mut files = Vec::new();
    for name in ["a", "b"] {
        let relative = format!("_indices/u/{name}.idx");
        source
            .put(
                &Path::from(format!("dataset.lance/{relative}")),
                PagedFixture::bytes(0).into(),
            )
            .await
            .unwrap();
        files.push(fyrr_core::envelope::FileRef {
            path: relative,
            size_bytes: PAGED_SIZE as u64,
        });
    }
    let _lease = mirror.pin(&files).await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            mirror.prewarm_if_room(source.clone(), &files).await,
            Err(MirrorCapacityExhausted)
        );
    }
    let blobs = std::fs::read_dir(&directory)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|e| e == "blob")
        })
        .count();
    assert_eq!(blobs, 1, "one of the two files was reserved");
    assert_eq!(mirror.totals().requests, 1, "the other asked nothing");
    assert!(
        std::fs::read_dir(&directory).unwrap().all(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_none_or(|e| e != "partial")),
        "no reservation is left behind"
    );
    std::fs::remove_dir_all(directory).unwrap();
}

/// A complete copy whose length is not the manifest's is never served: it is
/// removed with its checksum, and the read goes on from the object.
#[tokio::test]
async fn a_complete_copy_of_another_length_than_the_manifest_is_removed_not_served() {
    let f = PagedFixture::at("blob-length", 1 << 20, "_indices/u/aux.idx").await;
    f.mirror.prewarm(f.source.clone(), &f.files).await.unwrap();
    assert!(f.exists("blob") && f.exists("checksum"));
    let name = f.blob.file_name().unwrap().to_string_lossy().into_owned();
    crate::sync::lock(&f.mirror.manifest_sizes).insert(name.clone(), PAGED_SIZE as u64 + 1);
    // The object has another length than the manifest now says.
    assert!(f.read(0..10).await.is_err());
    assert!(!f.exists("blob") && !f.exists("checksum"));
    // With the manifest right again, the read is served from the object.
    crate::sync::lock(&f.mirror.manifest_sizes).insert(name, PAGED_SIZE as u64);
    assert_eq!(f.read(0..10).await.unwrap(), PagedFixture::bytes(0)[..10]);
}

/// An object whose size is not the manifest's is never kept, so no later read
/// serves it.
#[tokio::test]
async fn prewarm_if_room_keeps_no_object_of_another_size() {
    let mut f = PagedFixture::at("room-size", 1 << 20, "_indices/u/aux.idx").await;
    f.files[0].size_bytes += 1;
    assert!(
        f.mirror
            .prewarm_if_room(f.source.clone(), &f.files)
            .await
            .is_err()
    );
    assert!(!f.exists("blob") && !f.exists("partial"));
}

#[tokio::test]
async fn a_partial_left_by_a_failed_download_does_not_stop_pages() {
    let f = PagedFixture::new("pages-stale-partial", 1 << 20).await;
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(f.blob.with_extension("partial"))
        .unwrap()
        .write_all(b"interrupted")
        .unwrap();
    f.read(0..10).await.unwrap();
    assert!(f.exists("sparse") && !f.exists("partial"));
}
