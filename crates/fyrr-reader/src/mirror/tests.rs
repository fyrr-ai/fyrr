use super::*;

#[tokio::test]
async fn mutable_control_reads_through_native_named_storage_prefix() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-control-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
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

#[tokio::test(flavor = "current_thread")]
async fn cancelled_checksums_are_bounded_shared_and_failed_results_are_memoized() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-hash-timer-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let mirror = MirrorStore::new(directory.clone(), 512 * 1024 * 1024).unwrap();
    let mut blobs = Vec::new();
    for name in ['a', 'b', 'c'] {
        let blob = directory.join(format!("{}.blob", name.to_string().repeat(64)));
        MirrorStore::private_file(&blob, true)
            .unwrap()
            .set_len(128 * 1024 * 1024)
            .unwrap();
        std::fs::write(blob.with_extension("checksum"), "0".repeat(64)).unwrap();
        std::fs::set_permissions(
            blob.with_extension("checksum"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        blobs.push(blob);
    }
    let pool = Verifications::shared();
    // Occupy all permits so every caller joins before any hash can finish.
    let permits = pool
        .permits
        .clone()
        .acquire_many_owned(u32::try_from(Verifications::LIMIT).unwrap())
        .await
        .unwrap();
    let mut callers = Vec::new();
    for i in 0..Verifications::LIMIT + 8 {
        let mirror = mirror.clone();
        let blob = blobs[i % blobs.len()].clone();
        callers.push(tokio::spawn(
            async move { mirror.verified_file(&blob).await },
        ));
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if blobs
                .iter()
                .all(|p| pool.files.lock().unwrap().contains_key(p))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for caller in callers {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
    }
    let observations = {
        let files = pool.files.lock().unwrap();
        blobs
            .iter()
            .map(|p| {
                let v = files.get(p).unwrap();
                (v.started.clone(), v.result.clone())
            })
            .collect::<Vec<_>>()
    };
    drop(permits);
    // Work survives the cancelled callers, but only the blocking tasks own
    // permits. A second caller still joins the same file's original hash.
    let second = mirror.clone();
    let same = blobs[0].clone();
    let joined = tokio::spawn(async move { second.verified_file(&same).await });
    for (_, mut result) in observations.iter().cloned() {
        while result.borrow().is_none() {
            assert!(pool.permits.available_permits() <= Verifications::LIMIT);
            result.changed().await.unwrap();
        }
        assert!(matches!(&*result.borrow(), Some(Ok(false))));
    }
    assert!(!joined.await.unwrap().unwrap());
    assert!(pool.peak.load(Ordering::SeqCst) > 0);
    assert!(
        pool.peak.load(Ordering::SeqCst) <= Verifications::LIMIT as u64,
        "cancelled callers must not release permits still owned by hash tasks"
    );
    for (starts, _) in &observations {
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }
    // A known bad, unchanged blob does not launch another hash.
    for blob in &blobs {
        assert!(!mirror.verified_file(blob).await.unwrap());
    }
    for (starts, _) in &observations {
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }
    // Repaired proof metadata invalidates the failed memo, even if the blob
    // inode and contents did not change.
    let blob = &blobs[0];
    let mut hash = Sha256::new();
    let zeroes = [0u8; 65536];
    for _ in 0..(128 * 1024 * 1024 / zeroes.len()) {
        hash.update(zeroes);
    }
    std::fs::write(
        blob.with_extension("checksum"),
        hex::encode(hash.finalize()),
    )
    .unwrap();
    assert!(mirror.verified_file(blob).await.unwrap());
    std::fs::remove_dir_all(directory).unwrap();
    for blob in blobs {
        Verifications::forget(&blob);
    }
}

#[test]
fn verified_memo_hit_never_waits_for_the_blocking_pool() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let directory = std::env::temp_dir().join(format!(
            "fyrr-hash-hit-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let mirror = MirrorStore::new(directory.clone(), 1024).unwrap();
        let blob = directory.join(format!("{}.blob", "a".repeat(64)));
        MirrorStore::private_file(&blob, true)
            .unwrap()
            .write_all(b"immutable")
            .unwrap();
        let proof = hex::encode(Sha256::digest(b"immutable"));
        MirrorStore::private_file(&blob.with_extension("checksum"), true)
            .unwrap()
            .write_all(proof.as_bytes())
            .unwrap();
        assert!(mirror.verified_file(&blob).await.unwrap());
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            held.recv().unwrap();
        });
        waiting.await.unwrap();
        let hit = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            mirror.verified_file(&blob),
        )
        .await;
        release.send(()).unwrap();
        blocker.await.unwrap();
        assert!(
            hit.unwrap().unwrap(),
            "memo hits must not queue behind the occupied blocking pool"
        );
        std::fs::remove_dir_all(directory).unwrap();
        Verifications::forget(&blob);
    });
}

#[tokio::test]
async fn interrupted_partial_recovers_without_following_symlinks() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-partial-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
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
async fn warm_file_read_does_not_wait_for_miss_lock() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-lock-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_micros()
    ));
    let source = directory.join("source");
    std::fs::create_dir_all(source.join("namespace/dataset.lance/data")).unwrap();
    std::fs::write(
        source.join("namespace/dataset.lance/data/file.lance"),
        b"immutable file",
    )
    .unwrap();
    let mirror = MirrorStore::new(directory.join("mirror"), 1024).unwrap();
    let store = mirror.wrap(
        "namespace",
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(source).unwrap()),
    );
    let path = Path::from("namespace/dataset.lance/data/file.lance");
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "immutable file"
    );
    let held = mirror.lock.lock().await;
    let bytes = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        store.get(&path).await.unwrap().bytes().await.unwrap()
    })
    .await
    .expect("warm hit bypasses global lock");
    assert_eq!(bytes, "immutable file");
    drop(held);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn version_manifest_entries_are_bound_to_their_publication() {
    let directory = std::env::temp_dir().join(format!(
        "fyrr-mirror-versions-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
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
