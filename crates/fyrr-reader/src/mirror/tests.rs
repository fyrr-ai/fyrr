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
