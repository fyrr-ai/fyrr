// Process-kill tests verify restart recovery; they do not simulate power loss.
use super::*;
use object_store::{ObjectStoreExt, UpdateVersion};
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::process::{Child, Command};
use std::time::Duration;

const CHILD: &str = "storage::local::tests::local_store_child_process";

/// Test-only fault point: with `FYRR_TEST_LOCAL_PAUSE_<STAGE>` set, a write
/// announces the stage by creating the named file and then never returns, so
/// its process can be killed exactly there.
pub(super) fn pause(stage: &str) {
    let variable = format!("FYRR_TEST_LOCAL_PAUSE_{}", stage.to_uppercase());
    if let Some(marker) = std::env::var_os(variable) {
        std::fs::write(marker, b"").expect("announce the paused stage");
        loop {
            std::thread::sleep(Duration::from_secs(60));
        }
    }
}

/// Test-only gate: with `FYRR_TEST_LOCAL_HOLD_<STAGE>` set, a write announces
/// the stage once by creating the named file and continues when the file
/// with the added extension `go` appears.
pub(super) fn hold(stage: &str) {
    let variable = format!("FYRR_TEST_LOCAL_HOLD_{}", stage.to_uppercase());
    let Some(marker) = std::env::var_os(variable).map(PathBuf::from) else {
        return;
    };
    if marker.exists() {
        return;
    }
    std::fs::write(&marker, b"").expect("announce the held stage");
    while !marker.with_extension("go").exists() {
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn reached(marker: &std::path::Path, process: &mut Child) {
    let started = std::time::Instant::now();
    while !marker.exists() {
        if started.elapsed() >= Duration::from_secs(30) {
            process.kill().unwrap();
            process.wait().unwrap();
            panic!("{} was not reached", marker.display());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn put_at(store: &LocalStore, relative: &str, bytes: &[u8], mode: PutMode) -> Result<PutResult> {
    store.put_blocking(&at(store, relative), bytes.to_vec().into(), mode)
}

/// The bytes and tag of one object, read from one descriptor.
fn read_at(store: &LocalStore, relative: &str) -> (Vec<u8>, String) {
    let read = store
        .get_blocking(at(store, relative), GetOptions::default())
        .unwrap();
    let tag = read.meta.e_tag.clone().unwrap();
    (
        futures::executor::block_on(read.bytes()).unwrap().to_vec(),
        tag,
    )
}

const STRESS_ROUNDS: usize = 20;

/// One contender of the stress test: per round a key of its own, a key every
/// contender creates, and one compare-and-swap increment of a shared counter.
fn stress(store: &LocalStore) {
    let id: usize = std::env::var("FYRR_TEST_LOCAL_ID")
        .unwrap()
        .parse()
        .unwrap();
    let mut log = Vec::new();
    for round in 0..STRESS_ROUNDS {
        let own = format!("namespaces/ns/own/{id}-{round}");
        put_at(store, &own, &payload(round, id), PutMode::Create).unwrap();
        let raced = format!("namespaces/ns/raced/{round}");
        if outcome(put_at(store, &raced, &payload(round, id), PutMode::Create)) == Outcome::Won {
            log.push(format!("raced {round}"));
        }
        loop {
            let (bytes, tag) = read_at(store, "namespaces/ns/counter");
            let next = String::from_utf8(bytes).unwrap().parse::<u64>().unwrap() + 1;
            let mode = PutMode::Update(version(Some(tag.clone())));
            match outcome(put_at(
                store,
                "namespaces/ns/counter",
                next.to_string().as_bytes(),
                mode,
            )) {
                Outcome::Won => {
                    log.push(format!("swapped {tag}"));
                    break;
                }
                Outcome::Precondition => {}
                Outcome::Exists => panic!("an update cannot report an existing object"),
            }
        }
    }
    std::fs::write(
        std::env::var_os("FYRR_TEST_LOCAL_RESULT").unwrap(),
        log.join("\n"),
    )
    .unwrap();
}

/// The staging files currently present in `root`.
fn staged(root: &std::path::Path) -> Vec<PathBuf> {
    match std::fs::read_dir(root.join(STAGES)) {
        Ok(entries) => entries.map(|entry| entry.unwrap().path()).collect(),
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("{error}"),
    }
}

#[test]
fn local_store_refuses_a_symlinked_root_except_through_macos_system_links() {
    let base = scratch("symlink");
    std::fs::create_dir_all(base.join("target")).unwrap();
    std::os::unix::fs::symlink(base.join("target"), base.join("link")).unwrap();
    for root in [base.join("link"), base.join("link/storage")] {
        let error = LocalStore::open(&root).unwrap_err();
        assert_eq!(error.to_string(), "symlink in storage root");
    }
    #[cfg(target_os = "macos")]
    {
        // /tmp is a system link into /private; a root below it opens there.
        assert!(std::fs::symlink_metadata("/tmp").unwrap().is_symlink());
        let name = base.file_name().unwrap();
        let aliased = std::path::Path::new("/tmp").join(name);
        let store = LocalStore::open(&aliased.join("storage")).unwrap();
        assert_eq!(store.root_path, aliased.join("storage"));
        let real = std::path::Path::new("/private/tmp").join(name);
        assert!(real.join("storage").is_dir());
        assert_eq!(
            system_path(&aliased),
            real,
            "only the system link is replaced"
        );
        // A link of the user's below the system link is still refused.
        std::os::unix::fs::symlink(real.join("storage"), real.join("link")).unwrap();
        let error = LocalStore::open(&aliased.join("link")).unwrap_err();
        assert_eq!(error.to_string(), "symlink in storage root");
        // The scratch directories of these tests live below /var.
        assert!(LocalStore::open(&base.join("storage")).is_ok());
        std::fs::remove_dir_all(real).unwrap();
    }
    std::fs::remove_dir_all(base).unwrap();
}

fn scratch(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "fyrr-local-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::from(fyrr_core::clock::now())
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn at(store: &LocalStore, relative: &str) -> Path {
    Path::from(format!("{}/{relative}", store.root()))
}

fn object(store: &LocalStore) -> Path {
    at(store, "namespaces/ns/head.json")
}

fn version(e_tag: Option<String>) -> UpdateVersion {
    UpdateVersion {
        e_tag,
        version: None,
    }
}

fn put(store: &LocalStore, bytes: &[u8], mode: PutMode) -> Result<PutResult> {
    store.put_blocking(&object(store), bytes.to_vec().into(), mode)
}

fn read(store: &LocalStore, root: &std::path::Path) -> Vec<u8> {
    std::fs::read(store.inner.path_to_filesystem(&object(store)).unwrap())
        .unwrap_or_else(|_| panic!("object under {}", root.display()))
}

fn current(store: &LocalStore) -> String {
    let path = store.inner.path_to_filesystem(&object(store)).unwrap();
    version_tag(read_version(&File::open(path).unwrap()).unwrap())
}

/// Same-size payloads exercise versions independently of file size.
fn payload(round: usize, contender: usize) -> Vec<u8> {
    format!("round-{round:04}-contender-{contender:04}").into_bytes()
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Won,
    Precondition,
    Exists,
}

fn outcome(result: Result<PutResult>) -> Outcome {
    match result {
        Ok(_) => Outcome::Won,
        Err(Error::Precondition { .. }) => Outcome::Precondition,
        Err(Error::AlreadyExists { .. }) => Outcome::Exists,
        Err(error) => panic!("unexpected storage error: {error}"),
    }
}

fn child(root: &std::path::Path, action: &str, extra: &[(&str, &std::ffi::OsStr)]) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            CHILD,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("FYRR_TEST_LOCAL_ROOT", root)
        .env("FYRR_TEST_LOCAL_ACTION", action)
        // The child's libtest summary must not count as the parent's.
        .stdout(std::process::Stdio::null());
    for (key, value) in extra {
        command.env(key, value);
    }
    command.spawn().unwrap()
}

/// One storage write in its own process, started by the race and crash tests.
#[test]
#[ignore = "child process of the local storage process tests"]
fn local_store_child_process() {
    let root = PathBuf::from(std::env::var_os("FYRR_TEST_LOCAL_ROOT").unwrap());
    let store = LocalStore::open(&root).unwrap();
    if std::env::var("FYRR_TEST_LOCAL_ACTION").as_deref() == Ok("native") {
        let dataset = at(&store, "namespaces/ns/dataset.lance");
        store
            .persist_native(
                &dataset,
                &[fyrr_core::envelope::FileRef {
                    path: "data/new.lance".into(),
                    size_bytes: 6,
                }],
            )
            .unwrap();
        pause("native");
        put(&store, b"new-reference", PutMode::Overwrite).unwrap();
        return;
    }
    if std::env::var("FYRR_TEST_LOCAL_ACTION").as_deref() == Ok("cleanup") {
        let store = store.cleanup_batch();
        store
            .delete_blocking(&at(&store, "namespaces/ns/retired"))
            .unwrap();
        futures::executor::block_on(store.finish_cleanup()).unwrap();
        return;
    }
    if let Some(go) = std::env::var_os("FYRR_TEST_LOCAL_GO") {
        while !std::path::Path::new(&go).exists() {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
    if std::env::var("FYRR_TEST_LOCAL_ACTION").as_deref() == Ok("stress") {
        return stress(&store);
    }
    if std::env::var("FYRR_TEST_LOCAL_ACTION").as_deref() == Ok("delete") {
        store.delete_blocking(&object(&store)).unwrap();
        std::fs::write(
            std::env::var_os("FYRR_TEST_LOCAL_RESULT").unwrap(),
            "deleted",
        )
        .unwrap();
        return;
    }
    let bytes = std::env::var("FYRR_TEST_LOCAL_PAYLOAD")
        .unwrap()
        .into_bytes();
    let mode = match std::env::var("FYRR_TEST_LOCAL_ACTION").unwrap().as_str() {
        "create" => PutMode::Create,
        "overwrite" => PutMode::Overwrite,
        "update" => PutMode::Update(version(std::env::var("FYRR_TEST_LOCAL_EXPECTED").ok())),
        other => panic!("unknown action {other}"),
    };
    let result = match outcome(put(&store, &bytes, mode)) {
        Outcome::Won => "won",
        Outcome::Precondition => "precondition",
        Outcome::Exists => "exists",
    };
    std::fs::write(std::env::var_os("FYRR_TEST_LOCAL_RESULT").unwrap(), result).unwrap();
}

fn race(root: &std::path::Path, action: &str, round: usize, expected: Option<&str>) -> usize {
    const PROCESSES: usize = 6;
    let go = root.join(format!("go-{round}"));
    let children: Vec<_> = (0..PROCESSES)
        .map(|contender| {
            let result = root.join(format!("result-{round}-{contender}"));
            let bytes = String::from_utf8(payload(round, contender)).unwrap();
            let mut extra = vec![
                ("FYRR_TEST_LOCAL_GO", go.as_os_str()),
                ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
                ("FYRR_TEST_LOCAL_PAYLOAD", bytes.as_ref()),
            ];
            if let Some(expected) = expected {
                extra.push(("FYRR_TEST_LOCAL_EXPECTED", expected.as_ref()));
            }
            (child(root, action, &extra), result)
        })
        .collect();
    // Every child is past process start-up and polling before the start.
    std::thread::sleep(Duration::from_millis(500));
    std::fs::write(&go, b"").unwrap();
    let mut winners = Vec::new();
    for (contender, (mut process, result)) in children.into_iter().enumerate() {
        assert!(process.wait().unwrap().success());
        match std::fs::read_to_string(result).unwrap().as_str() {
            "won" => winners.push(contender),
            "precondition" => assert_eq!(action, "update"),
            "exists" => assert_eq!(action, "create"),
            other => panic!("{other}"),
        }
    }
    assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");
    winners[0]
}

#[test]
fn local_store_updates_are_compare_and_swap_across_processes() {
    let root = scratch("processes");
    let store = LocalStore::open(&root).unwrap();
    let winner = race(&root, "create", 0, None);
    assert_eq!(read(&store, &root), payload(0, winner));
    for round in 1..=4 {
        let expected = current(&store);
        let winner = race(&root, "update", round, Some(&expected));
        assert_eq!(read(&store, &root), payload(round, winner));
        assert_ne!(current(&store), expected);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn local_store_crash_before_publication_keeps_the_old_object() {
    let root = scratch("crash");
    let store = LocalStore::open(&root).unwrap();
    put(&store, &payload(0, 0), PutMode::Create).unwrap();
    for (round, stage) in ["staged", "locked", "versioned"].into_iter().enumerate() {
        let expected = current(&store);
        let marker = root.join(format!("paused-{stage}"));
        let bytes = String::from_utf8(payload(round + 1, 9)).unwrap();
        let pause = format!("FYRR_TEST_LOCAL_PAUSE_{}", stage.to_uppercase());
        let result = root.join("crash-result");
        let mut process = child(
            &root,
            "update",
            &[
                (pause.as_str(), marker.as_os_str()),
                ("FYRR_TEST_LOCAL_PAYLOAD", bytes.as_ref()),
                ("FYRR_TEST_LOCAL_EXPECTED", expected.as_ref()),
                ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
            ],
        );
        let started = std::time::Instant::now();
        while !marker.exists() {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "{stage} not reached"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        // SIGKILL: no unwinding, no cleanup, the lock released by the kernel.
        process.kill().unwrap();
        process.wait().unwrap();
        assert!(!result.exists());
        assert_eq!(
            staged(&root).len(),
            1,
            "the killed writer left its staging file"
        );
        assert_eq!(read(&store, &root), payload(round, 0), "{stage}");
        assert_eq!(current(&store), expected, "{stage}");
        let listed: Vec<_> = futures::executor::block_on(
            store
                .list(Some(&at(&store, "namespaces")))
                .try_collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|o| o.location.clone())
                .collect::<Vec<_>>(),
            [object(&store)],
            "a staged file is never listed"
        );
        // The writer that saw the surviving version still wins, so the dead
        // process left no lock behind.
        let mode = PutMode::Update(version(Some(expected.clone())));
        put(
            &LocalStore::open(&root).unwrap(),
            &payload(round + 1, 0),
            mode,
        )
        .unwrap();
        assert_ne!(current(&store), expected);
        assert!(
            staged(&root).is_empty(),
            "the next store handle reaps the abandoned stage"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// A writer killed after it gave the object its name, or removed it, and
/// before it flushed the directory: the change is visible and complete,
/// nothing was acknowledged, and no lock or staging file is left behind.
#[test]
fn local_store_crash_after_publication_leaves_a_complete_object() {
    let root = scratch("crash-published");
    let store = LocalStore::open(&root).unwrap();
    let killed = |action: &str, stage: &str, expected: Option<&str>| {
        let marker = root.join(format!("paused-{action}"));
        let result = root.join(format!("result-{action}"));
        let pause = format!("FYRR_TEST_LOCAL_PAUSE_{stage}");
        let mut extra = vec![
            (pause.as_str(), marker.as_os_str()),
            ("FYRR_TEST_LOCAL_PAYLOAD", action.as_ref()),
            ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
        ];
        if let Some(expected) = expected {
            extra.push(("FYRR_TEST_LOCAL_EXPECTED", expected.as_ref()));
        }
        let mut process = child(&root, action, &extra);
        reached(&marker, &mut process);
        process.kill().unwrap();
        process.wait().unwrap();
        assert!(!result.exists(), "{action} was not acknowledged");
        assert!(staged(&root).is_empty(), "{action} left a staging file");
    };

    killed("create", "PUBLISHED", None);
    assert_eq!(read(&store, &root), b"create");
    let created = current(&store);

    killed("update", "PUBLISHED", Some(&created));
    assert_eq!(read(&store, &root), b"update");
    let updated = current(&store);
    assert_ne!(updated, created);
    // The published object is not locked by the dead writer, and only its
    // own tag matches.
    let fresh = LocalStore::open(&root).unwrap();
    let stale = put(&fresh, b"stale", PutMode::Update(version(Some(created))));
    assert_eq!(outcome(stale), Outcome::Precondition);
    put(&fresh, b"next", PutMode::Update(version(Some(updated)))).unwrap();
    assert_eq!(read(&store, &root), b"next");

    killed("delete", "REMOVED", None);
    assert!(!root.join("namespaces/ns/head.json").exists());
    assert!(matches!(
        store.get_blocking(object(&store), GetOptions::default()),
        Err(Error::NotFound { .. })
    ));
    put(&fresh, b"again", PutMode::Create).unwrap();
    assert_eq!(read(&store, &root), b"again");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn local_store_e_tags_never_repeat_and_conditions_fail_closed() {
    let root = scratch("tags");
    let store = LocalStore::open(&root).unwrap();
    let path = object(&store);
    let missing = store
        .put_opts(
            &path,
            b"absent".to_vec().into(),
            PutMode::Update(version(Some("\"0-0-0\"".into()))).into(),
        )
        .await;
    assert!(
        matches!(missing, Err(Error::Precondition { .. })),
        "{missing:?}"
    );
    let first = store
        .put_opts(&path, payload(0, 0).into(), PutMode::Create.into())
        .await
        .unwrap();
    let again = store
        .put_opts(&path, payload(0, 1).into(), PutMode::Create.into())
        .await;
    assert!(
        matches!(again, Err(Error::AlreadyExists { .. })),
        "{again:?}"
    );
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        payload(0, 0)
    );

    // A tight loop of same-size rewrites: inodes are recycled and many writes
    // share a timestamp tick, yet no tag repeats.
    let mut seen = BTreeSet::from([first.e_tag.clone().unwrap()]);
    let mut previous = first.e_tag.clone();
    let mut stale = Vec::new();
    for round in 1..=400 {
        let mode = if round % 2 == 0 {
            PutMode::Update(version(previous.clone()))
        } else {
            PutMode::Overwrite
        };
        let written = store
            .put_opts(&path, payload(round, 0).into(), mode.into())
            .await
            .unwrap();
        let head = store.head(&path).await.unwrap();
        assert_eq!(head.e_tag, written.e_tag, "reads report the written e-tag");
        let get = store.get(&path).await.unwrap();
        assert_eq!(get.meta.e_tag, written.e_tag);
        assert_eq!(get.bytes().await.unwrap(), payload(round, 0));
        assert!(
            seen.insert(written.e_tag.clone().unwrap()),
            "round {round} repeated"
        );
        stale.extend(previous.replace(written.e_tag.unwrap()));
    }
    // Every superseded version is refused, the object unchanged.
    for tag in stale.iter().step_by(37) {
        let refused = store
            .put_opts(
                &path,
                b"stale".to_vec().into(),
                PutMode::Update(version(Some(tag.clone()))).into(),
            )
            .await;
        assert!(
            matches!(refused, Err(Error::Precondition { .. })),
            "{refused:?}"
        );
    }
    assert_eq!(store.head(&path).await.unwrap().e_tag, previous);

    // Timestamp changes and identical payloads cannot reuse a version.
    let file = store.inner.path_to_filesystem(&path).unwrap();
    for _ in 0..50 {
        File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(UNIX_EPOCH)
            .unwrap();
        let written = store
            .put_opts(
                &path,
                payload(400, 0).into(),
                PutMode::Update(version(previous.clone())).into(),
            )
            .await
            .unwrap();
        assert!(seen.insert(written.e_tag.clone().unwrap()));
        previous = written.e_tag;
    }
    let untagged = store
        .put_opts(
            &path,
            b"untagged".to_vec().into(),
            PutMode::Update(version(None)).into(),
        )
        .await;
    assert!(
        matches!(untagged, Err(Error::Generic { .. })),
        "{untagged:?}"
    );

    // A tag is only compared for equality, so one issued by a release that
    // numbered versions from a counter stays valid for its holder.
    let numbered = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file)
        .unwrap();
    set_version(&numbered, 7).unwrap();
    assert_eq!(store.head(&path).await.unwrap().e_tag, Some(version_tag(7)));
    previous = store
        .put_opts(
            &path,
            payload(401, 0).into(),
            PutMode::Update(version(Some(version_tag(7)))).into(),
        )
        .await
        .unwrap()
        .e_tag;
    assert!(seen.insert(previous.clone().unwrap()));
    assert_eq!(store.head(&path).await.unwrap().e_tag, previous);
    // A control object whose tag disappeared is not an untagged Lance file;
    // reads must fail rather than invent an e-tag.
    let file = store.inner.path_to_filesystem(&path).unwrap();
    let control = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file)
        .unwrap();
    rustix::fs::fremovexattr(&control, VERSION).unwrap();
    assert!(matches!(store.get(&path).await, Err(Error::Generic { .. })));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn local_store_root_is_private_and_behaves_like_s3() {
    let root = scratch("root").join("nested/storage");
    let store = LocalStore::open(&root).unwrap();
    for directory in [&root, root.parent().unwrap()] {
        let mode = std::fs::metadata(directory).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "{}", directory.display());
    }
    // An earlier release's first opener, killed after it initialized its
    // version counter and before it wrote its marker, left an empty lock file.
    let initial = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join(LOCK))
        .unwrap();
    set_version(&initial, 0).unwrap();
    initial.sync_all().unwrap();
    drop(initial);
    for name in ["b/2", "a/1", "c", "b/1"] {
        let path = at(&store, &format!("namespaces/ns/{name}"));
        store
            .put(&path, name.as_bytes().to_vec().into())
            .await
            .unwrap();
        let file = store.inner.path_to_filesystem(&path).unwrap();
        assert_eq!(
            std::fs::metadata(file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let names = |objects: Vec<ObjectMeta>| -> Vec<String> {
        objects
            .iter()
            .map(|o| crate::storage::relative_path(&o.location, store.root()))
            .collect()
    };
    let listed = names(store.list(None).try_collect().await.unwrap());
    assert_eq!(
        listed,
        [
            "namespaces/ns/a/1",
            "namespaces/ns/b/1",
            "namespaces/ns/b/2",
            "namespaces/ns/c"
        ],
        "sorted like S3, without the lock file"
    );
    let delimited = store
        .list_with_delimiter(Some(&at(&store, "namespaces/ns")))
        .await
        .unwrap();
    assert_eq!(names(delimited.objects), ["namespaces/ns/c"]);
    assert_eq!(
        delimited
            .common_prefixes
            .iter()
            .map(|prefix| crate::storage::relative_path(prefix, store.root()))
            .collect::<Vec<_>>(),
        ["namespaces/ns/a", "namespaces/ns/b"]
    );
    // The root lists like a bucket: the lock file and the staging directory
    // are neither objects nor prefixes.
    assert!(root.join(STAGES).is_dir() && root.join(LOCK).is_file());
    let top = store.list_with_delimiter(None).await.unwrap();
    assert!(top.objects.is_empty());
    assert_eq!(
        top.common_prefixes
            .iter()
            .map(|prefix| crate::storage::relative_path(prefix, store.root()))
            .collect::<Vec<_>>(),
        ["namespaces"]
    );
    let empty: Vec<_> = store
        .list(Some(&at(&store, "namespaces/other")))
        .try_collect()
        .await
        .unwrap();
    assert!(empty.is_empty());
    let gone = at(&store, "namespaces/ns/c");
    // Abandoned staging files: one of this layout, one of the single-file
    // layout of earlier releases.
    for abandoned in [root.join(STAGES).join("abandoned"), root.join(LEGACY_STAGE)] {
        std::fs::write(&abandoned, b"retired private content").unwrap();
        std::fs::set_permissions(&abandoned, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let staged_listing: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert!(
        staged_listing
            .iter()
            .all(|meta| !store.is_internal(&meta.location))
    );
    store.delete(&gone).await.unwrap();
    assert!(
        staged(&root).is_empty() && !root.join(LEGACY_STAGE).exists(),
        "purge must reap abandoned staged content"
    );
    store.delete(&gone).await.unwrap();
    assert!(matches!(
        store.get(&gone).await,
        Err(Error::NotFound { .. })
    ));
    assert!(
        store
            .copy(&at(&store, "namespaces/ns/a/1"), &gone)
            .await
            .is_err()
    );
    assert!(store.put_multipart(&gone).await.is_err());

    let external = root.with_file_name("external");
    std::fs::write(&external, b"unchanged").unwrap();
    let link_path = at(&store, "namespaces/ns/link");
    std::os::unix::fs::symlink(&external, root.join("namespaces/ns/link")).unwrap();
    assert!(store.get(&link_path).await.is_err());
    assert!(
        store
            .put(&link_path, b"changed".to_vec().into())
            .await
            .is_err()
    );
    assert!(store.delete(&link_path).await.is_err());
    std::os::unix::fs::symlink(root.parent().unwrap(), root.join("redirect")).unwrap();
    assert!(
        store
            .put(&at(&store, "redirect/escaped"), b"changed".to_vec().into())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&external).unwrap(), b"unchanged");
    // Neither a redirected staging directory nor a foreign entry in it is
    // written through, followed or removed.
    let stages = root.join(STAGES);
    std::os::unix::fs::symlink(&external, stages.join("link")).unwrap();
    assert!(store.delete(&gone).await.is_err());
    assert!(stages.join("link").is_symlink());
    std::fs::remove_file(stages.join("link")).unwrap();
    std::fs::remove_dir(&stages).unwrap();
    std::os::unix::fs::symlink(root.parent().unwrap(), &stages).unwrap();
    assert!(store.put(&gone, b"changed".to_vec().into()).await.is_err());
    assert!(store.delete(&gone).await.is_err());
    assert_eq!(std::fs::read(&external).unwrap(), b"unchanged");
    std::fs::remove_file(&stages).unwrap();
    std::fs::remove_file(&external).unwrap();

    // Native Lance files have no tag xattr and retain LocalFileSystem's
    // metadata. That exception must not apply to a control object.
    let native_path = at(&store, "namespaces/ns/dataset.lance/data/native.lance");
    let native = LocalFileSystem::new();
    native
        .put(&native_path, b"native data".to_vec().into())
        .await
        .unwrap();
    let native_get = store.get(&native_path).await.unwrap();
    assert_eq!(
        native_get.meta.e_tag,
        native.head(&native_path).await.unwrap().e_tag
    );
    assert_eq!(native_get.bytes().await.unwrap(), b"native data".as_slice());

    // A retained handle must not validate an absolute path through a replaced root.
    let moved = root.with_extension("moved");
    std::fs::rename(&root, &moved).unwrap();
    std::fs::create_dir(&root).unwrap();
    assert!(store.get(&native_path).await.is_err());
    std::fs::remove_dir(&root).unwrap();
    std::os::unix::fs::symlink(&moved, &root).unwrap();
    assert!(store.get(&native_path).await.is_err());
    std::fs::remove_file(&root).unwrap();
    std::fs::rename(&moved, &root).unwrap();
    assert!(store.get(&native_path).await.is_ok());

    // An existing root must be private, owned and not a symlink.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(LocalStore::open(&root).is_err());
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let link = root.with_file_name("link");
    std::os::unix::fs::symlink(&root, &link).unwrap();
    assert!(LocalStore::open(&link).is_err());
    // So must the lock file; a symlink there is never followed.
    let lock = root.join(LOCK);
    std::fs::remove_file(&lock).unwrap();
    std::os::unix::fs::symlink(root.join("elsewhere"), &lock).unwrap();
    let refused = store.put(&gone, b"c".to_vec().into()).await;
    assert!(refused.is_err());
    assert!(!root.join("elsewhere").exists());
    std::fs::remove_dir_all(scratch_root(&root)).unwrap();
}

/// A listing re-reads every object it found. One that a concurrent delete
/// removed in between is gone, as on S3; it must not fail the whole listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_store_listing_tolerates_concurrent_deletes() {
    let root = scratch("listing");
    let store = LocalStore::open(&root).unwrap();
    let prefix = at(&store, "namespaces/ns");
    let kept = object(&store);
    store.put(&kept, b"kept".to_vec().into()).await.unwrap();
    for round in 0..3 {
        // Native Lance files, which Lance also deletes without this store
        // (rounds 0 and 1), then control objects (round 2).
        let (directory, objects) = match round {
            0 | 1 => ("namespaces/ns/dataset.lance/data", 400),
            _ => ("namespaces/ns/envelopes", 40),
        };
        std::fs::create_dir_all(root.join(directory)).unwrap();
        for index in 0..objects {
            let name = format!("{directory}/{index:04}");
            if round < 2 {
                std::fs::write(root.join(&name), b"native").unwrap();
            } else {
                let path = at(&store, &name);
                store.put(&path, b"envelope".to_vec().into()).await.unwrap();
            }
        }
        let deleter = {
            let (store, root) = (store.clone(), root.clone());
            std::thread::spawn(move || {
                for index in 0..objects {
                    let name = format!("{directory}/{index:04}");
                    if round == 1 {
                        std::fs::remove_file(root.join(name)).unwrap();
                    } else {
                        store.delete_blocking(&at(&store, &name)).unwrap();
                    }
                }
            })
        };
        let mut listings = 0;
        while !deleter.is_finished() {
            let listed: Vec<ObjectMeta> = store
                .list(Some(&prefix))
                .try_collect()
                .await
                .unwrap_or_else(|error| panic!("round {round}, listing {listings}: {error}"));
            assert!(listed.iter().any(|object| object.location == kept));
            let delimited = store
                .list_with_delimiter(Some(&at(&store, directory)))
                .await
                .unwrap_or_else(|error| panic!("round {round}, delimited {listings}: {error}"));
            assert!(delimited.objects.len() <= objects);
            listings += 1;
        }
        deleter.join().unwrap();
        assert!(listings > 0);
    }
    let listed: Vec<ObjectMeta> = store.list(Some(&prefix)).try_collect().await.unwrap();
    assert_eq!(listed.len(), 1);
    // Any other failure still fails the listing: a control object that lost
    // its version is refused, not skipped.
    let damaged = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("namespaces/ns/head.json"))
        .unwrap();
    rustix::fs::fremovexattr(&damaged, VERSION).unwrap();
    assert!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// Every write draws a new random tag, so an object deleted and created
/// again never matches a tag of an earlier incarnation, whatever inode it
/// reuses and whatever bytes it holds.
#[tokio::test]
async fn local_store_recreated_objects_never_match_earlier_tags() {
    let root = scratch("recreate");
    let store = LocalStore::open(&root).unwrap();
    let path = object(&store);
    let mut seen = BTreeSet::new();
    let mut earlier: Vec<String> = Vec::new();
    for round in 0..60 {
        // The same bytes every time: same size, often the same inode and tick.
        let created = store
            .put_opts(&path, payload(0, 0).into(), PutMode::Create.into())
            .await
            .unwrap()
            .e_tag
            .unwrap();
        assert!(seen.insert(created.clone()), "round {round} repeated");
        for tag in earlier.iter().rev().take(8) {
            let refused = store
                .put_opts(
                    &path,
                    b"stale".to_vec().into(),
                    PutMode::Update(version(Some(tag.clone()))).into(),
                )
                .await;
            assert!(
                matches!(refused, Err(Error::Precondition { .. })),
                "round {round}: {refused:?}"
            );
        }
        assert_eq!(
            store.head(&path).await.unwrap().e_tag,
            Some(created.clone())
        );
        store.delete(&path).await.unwrap();
        // A deleted object matches nothing, not even its last tag.
        let missing = store
            .put_opts(
                &path,
                b"stale".to_vec().into(),
                PutMode::Update(version(Some(created.clone()))).into(),
            )
            .await;
        assert!(
            matches!(missing, Err(Error::Precondition { .. })),
            "{missing:?}"
        );
        assert!(matches!(
            store.get(&path).await,
            Err(Error::NotFound { .. })
        ));
        earlier.push(created);
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// What a release that numbered versions from a counter on the lock file
/// does before every mutation: a non-empty lock file must carry the counter.
fn counter_release_refuses(root: &std::path::Path) -> bool {
    let lock = File::open(root.join(LOCK)).unwrap();
    lock.metadata().unwrap().len() > 0 && read_version(&lock).is_err()
}

/// A root written by a release with a version counter is taken over by the
/// first mutation: the counter is removed, so that release fails closed from
/// then on, and the objects and tags it wrote stay valid.
#[tokio::test]
async fn local_store_takes_a_root_over_from_the_version_counter() {
    let root = scratch("handover");
    let store = LocalStore::open(&root).unwrap();
    let path = object(&store);
    // The earlier release's root: a marked lock file with its counter at 5,
    // and an object carrying version 5.
    let lock = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join(LOCK))
        .unwrap();
    std::io::Write::write_all(&mut &lock, b"fyrr local version counter\n").unwrap();
    set_version(&lock, 5).unwrap();
    drop(lock);
    let file = store.inner.path_to_filesystem(&path).unwrap();
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let old = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&file)
        .unwrap();
    std::io::Write::write_all(&mut &old, b"written before").unwrap();
    set_version(&old, 5).unwrap();
    drop(old);
    assert!(!counter_release_refuses(&root));

    // Reads need no handover and report the old version as the tag.
    let held = store.head(&path).await.unwrap().e_tag;
    assert_eq!(held, Some(version_tag(5)));
    assert!(!counter_release_refuses(&root));

    // The first mutation hands the root over; the old tag still matches once.
    let other = at(&store, "namespaces/ns/other.json");
    store.put(&other, b"other".to_vec().into()).await.unwrap();
    assert!(counter_release_refuses(&root));
    let updated = store
        .put_opts(
            &path,
            b"written after".to_vec().into(),
            PutMode::Update(version(held.clone())).into(),
        )
        .await
        .unwrap()
        .e_tag;
    assert_ne!(updated, held);
    let stale = store
        .put_opts(
            &path,
            b"stale holder".to_vec().into(),
            PutMode::Update(version(held)).into(),
        )
        .await;
    assert!(
        matches!(stale, Err(Error::Precondition { .. })),
        "{stale:?}"
    );

    // A root that lost its lock file needs no counter to stay safe: writes
    // continue with new tags, and the lock file is marked again.
    std::fs::remove_file(root.join(LOCK)).unwrap();
    for store in [store.clone(), LocalStore::open(&root).unwrap()] {
        let written = store
            .put(&other, b"again".to_vec().into())
            .await
            .unwrap()
            .e_tag;
        assert_ne!(written, updated);
        assert!(counter_release_refuses(&root));
    }
    assert_eq!(store.head(&path).await.unwrap().e_tag, updated);
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        b"written after".as_slice()
    );

    // A new root is marked by its first mutation, before any object exists.
    let fresh = scratch("handover-fresh");
    let store = LocalStore::open(&fresh).unwrap();
    store
        .put(&object(&store), b"first".to_vec().into())
        .await
        .unwrap();
    assert!(counter_release_refuses(&fresh));
    std::fs::remove_dir_all(fresh).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// A staging file is abandoned exactly when nobody holds its lock. The
/// reaper removes those and leaves a live writer's file alone.
#[test]
fn local_store_reaps_only_unlocked_staging_files() {
    let root = scratch("reap");
    let store = LocalStore::open(&root).unwrap();
    put(&store, b"object", PutMode::Create).unwrap();
    let directory = store.root_directory().unwrap();
    let live = store.stage(&directory).unwrap();
    let abandoned = root.join(STAGES).join("abandoned");
    std::fs::write(&abandoned, b"retired private content").unwrap();
    std::fs::set_permissions(&abandoned, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(staged(&root).len(), 2);

    store.reap(&directory).unwrap();
    assert_eq!(
        staged(&root),
        std::slice::from_ref(&live.path),
        "only the locked file stays"
    );
    // A delete reaps every time, and still leaves the live writer's file.
    store
        .delete_blocking(&at(&store, "namespaces/ns/missing"))
        .unwrap();
    assert_eq!(staged(&root), std::slice::from_ref(&live.path));

    // The writer is gone without publishing: its file is abandoned now.
    drop(live);
    store.reap(&directory).unwrap();
    assert!(staged(&root).is_empty());
    assert_eq!(read(&store, &root), b"object");
    std::fs::remove_dir_all(root).unwrap();
}

const LANCE_CHILD: &str = "storage::local::tests::lance_append_child_process";

/// `appends` single-row versions from one writer, each a separate commit.
async fn lance_append(uri: &str, writer: i64, appends: i64) {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{WriteMode, WriteParams};
    let schema = Arc::new(Schema::new(vec![
        Field::new("writer", DataType::Int64, false),
        Field::new("row", DataType::Int64, false),
    ]));
    for row in 0..appends {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![writer])),
                Arc::new(Int64Array::from(vec![row])),
            ],
        )
        .unwrap();
        let params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };
        // Lance rebases a lost append itself; a commit that still fails wrote
        // no version, so the row is appended again.
        let mut attempts = 0;
        while let Err(error) = crate::storage::Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
            uri,
            Some(params.clone()),
        )
        .await
        {
            attempts += 1;
            assert!(attempts < 50, "writer {writer} row {row}: {error}");
        }
    }
}

/// One Lance writer in its own process, started by the commit race below.
#[tokio::test]
#[ignore = "child process of the Lance commit race"]
async fn lance_append_child_process() {
    let uri = std::env::var("FYRR_TEST_LANCE_URI").unwrap();
    let go = std::env::var_os("FYRR_TEST_LOCAL_GO").unwrap();
    while !std::path::Path::new(&go).exists() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let writer = std::env::var("FYRR_TEST_LANCE_WRITER")
        .unwrap()
        .parse()
        .unwrap();
    let appends = std::env::var("FYRR_TEST_LANCE_APPENDS")
        .unwrap()
        .parse()
        .unwrap();
    lance_append(&uri, writer, appends).await;
}

/// Lance commits a version of a local dataset by creating its manifest
/// exclusively. Writers in several processes and tasks, racing with this
/// store's own writes and listings in the same root, each get their own
/// version: none is lost, none is committed twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lance_commits_on_a_local_root_are_exclusive() {
    use arrow_array::Int64Array;
    const PROCESSES: i64 = 4;
    const TASKS: i64 = 4;
    const APPENDS: i64 = 8;
    let root = scratch("lance");
    let store = LocalStore::open(&root).unwrap();
    let dataset_root = root.join("namespaces/ns/dataset.lance");
    let uri = format!("file-object-store://{}", dataset_root.display());
    // As in a namespace: the serving head exists before the dataset.
    let head = object(&store);
    store.put(&head, payload(0, 0).into()).await.unwrap();
    lance_append(&uri, -1, 1).await;

    let go = root.join("go-lance");
    let mut children: Vec<Child> = (0..PROCESSES)
        .map(|writer| {
            Command::new(std::env::current_exe().unwrap())
                .args([
                    LANCE_CHILD,
                    "--exact",
                    "--ignored",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("FYRR_TEST_LANCE_URI", &uri)
                .env("FYRR_TEST_LOCAL_GO", &go)
                .env("FYRR_TEST_LANCE_WRITER", writer.to_string())
                .env("FYRR_TEST_LANCE_APPENDS", APPENDS.to_string())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    std::fs::write(&go, b"").unwrap();
    let tasks: Vec<_> = (PROCESSES..PROCESSES + TASKS)
        .map(|writer| {
            let uri = uri.clone();
            tokio::spawn(async move { lance_append(&uri, writer, APPENDS).await })
        })
        .collect();
    // The store's control writes and listings share the root with Lance.
    let listed = at(&store, "namespaces/ns/dataset.lance");
    let mut control = 0;
    while tasks.iter().any(|task| !task.is_finished()) {
        store.put(&head, payload(control, 0).into()).await.unwrap();
        let objects: Vec<ObjectMeta> = store.list(Some(&listed)).try_collect().await.unwrap();
        assert!(
            objects
                .iter()
                .all(|object| !object.location.as_ref().contains('#')),
            "a staged Lance file is never listed"
        );
        control += 1;
    }
    for task in tasks {
        task.await.unwrap();
    }
    for child in &mut children {
        assert!(child.wait().unwrap().success());
    }
    assert!(control > 0);

    let commits = (PROCESSES + TASKS) * APPENDS;
    let dataset = crate::storage::Dataset::open(&uri).await.unwrap();
    assert_eq!(
        dataset.version().version,
        1 + commits as u64,
        "every append is exactly one version"
    );
    let versions: Vec<u64> = dataset
        .versions()
        .await
        .unwrap()
        .iter()
        .map(|version| version.version)
        .collect();
    assert_eq!(versions, (1..=1 + commits as u64).collect::<Vec<_>>());
    let manifests = std::fs::read_dir(dataset_root.join("_versions"))
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".manifest")
        })
        .count();
    assert_eq!(manifests as i64, 1 + commits);
    let batch = dataset.scan().try_into_batch().await.unwrap();
    let column = |name: &str| -> Vec<i64> {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    };
    let rows: BTreeSet<(i64, i64)> = column("writer").into_iter().zip(column("row")).collect();
    assert_eq!(batch.num_rows() as i64, 1 + commits, "no row twice");
    let expected: BTreeSet<(i64, i64)> = (0..PROCESSES + TASKS)
        .flat_map(|writer| (0..APPENDS).map(move |row| (writer, row)))
        .chain([(-1, 0)])
        .collect();
    assert_eq!(rows, expected, "no append lost");
    std::fs::remove_dir_all(root).unwrap();
}

fn scratch_root(root: &std::path::Path) -> &std::path::Path {
    root.parent().and_then(std::path::Path::parent).unwrap()
}

#[tokio::test]
async fn local_cleanup_process_restart_converges_and_preserves_referenced_objects() {
    let root = scratch("cleanup-crash");
    let store = LocalStore::open(&root).unwrap();
    put(&store, b"referenced", PutMode::Create).unwrap();
    let retired = at(&store, "namespaces/ns/retired");
    store
        .put_opts(&retired, b"retired".to_vec().into(), PutMode::Create.into())
        .await
        .unwrap();
    let marker = root.join("unlinked");
    let mut process = child(
        &root,
        "cleanup",
        &[("FYRR_TEST_LOCAL_PAUSE_UNLINKED", marker.as_os_str())],
    );
    let started = std::time::Instant::now();
    while !marker.exists() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "unlink boundary not reached"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let batch = LocalStore::open(&root).unwrap().cleanup_batch();
    batch.delete_blocking(&retired).unwrap();
    batch.finish_cleanup().await.unwrap();
    assert_eq!(read(&store, &root), b"referenced");
    assert!(matches!(
        store.get(&retired).await,
        Err(Error::NotFound { .. })
    ));
    // Several deletions in one directory produce one durability obligation.
    for key in ["a", "b"] {
        let path = at(&store, &format!("namespaces/ns/{key}"));
        store
            .put_opts(&path, b"retired".to_vec().into(), PutMode::Create.into())
            .await
            .unwrap();
        batch.delete_blocking(&path).unwrap();
    }
    assert_eq!(
        batch
            .cleanup_directories
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .len(),
        1
    );
    batch.finish_cleanup().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn local_native_process_restart_preserves_unpublished_reference() {
    let root = scratch("native-durability");
    let store = LocalStore::open(&root).unwrap();
    put(&store, b"old-reference", PutMode::Create).unwrap();
    let native = root.join("namespaces/ns/dataset.lance/data/new.lance");
    std::fs::create_dir_all(native.parent().unwrap()).unwrap();
    std::fs::write(&native, b"native").unwrap();
    let marker = root.join("native-persisted");
    let mut process = child(
        &root,
        "native",
        &[("FYRR_TEST_LOCAL_PAUSE_NATIVE", marker.as_os_str())],
    );
    let start = std::time::Instant::now();
    while !marker.exists() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "native persistence call boundary"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let restarted = LocalStore::open(&root).unwrap();
    assert_eq!(read(&restarted, &root), b"old-reference");
    let dataset = at(&restarted, "namespaces/ns/dataset.lance");
    restarted
        .persist_native(
            &dataset,
            &[fyrr_core::envelope::FileRef {
                path: "data/new.lance".into(),
                size_bytes: 6,
            }],
        )
        .unwrap();
    put(&restarted, b"new-reference", PutMode::Overwrite).unwrap();
    assert_eq!(read(&restarted, &root), b"new-reference");
    assert_eq!(std::fs::read(native).unwrap(), b"native");
    std::fs::remove_dir_all(root).unwrap();
}

/// A writer stopped while it holds an object's lock blocks replacements and
/// removals of that object only, and those fail within a bound without
/// writing. Every other object and every create proceeds at once.
#[test]
fn local_store_frozen_holder_bounds_lock_acquisition() {
    let root = scratch("frozen-holder");
    let store = LocalStore::open(&root).unwrap();
    put(&store, b"original", PutMode::Create).unwrap();
    put_at(&store, "namespaces/ns/other", b"other", PutMode::Create).unwrap();
    put_at(&store, "namespaces/ns/retired", b"retired", PutMode::Create).unwrap();
    let marker = root.join("locked");
    let expected = current(&store);
    let result = root.join("holder-result");
    let mut holder = child(
        &root,
        "update",
        &[
            ("FYRR_TEST_LOCAL_PAUSE_LOCKED", marker.as_os_str()),
            ("FYRR_TEST_LOCAL_PAYLOAD", "holder".as_ref()),
            ("FYRR_TEST_LOCAL_EXPECTED", expected.as_ref()),
            ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
        ],
    );
    reached(&marker, &mut holder);

    // Other objects: create, replace, compare-and-swap and remove. One that
    // waited for the held object's lock would fail at the deadline, so their
    // success shows that none did, however slow the disk is.
    let unrelated = || -> Result<()> {
        let fresh = LocalStore::open(&root).unwrap();
        put_at(&fresh, "namespaces/ns/created", b"created", PutMode::Create)?;
        put_at(
            &fresh,
            "namespaces/ns/other",
            b"replaced",
            PutMode::Overwrite,
        )?;
        let (_, tag) = read_at(&fresh, "namespaces/ns/other");
        let swap = PutMode::Update(version(Some(tag)));
        put_at(&fresh, "namespaces/ns/other", b"swapped", swap)?;
        fresh.delete_blocking(&at(&fresh, "namespaces/ns/retired"))
    };
    let others = unrelated();

    // The held object: a replacement and a removal each give up in time.
    let contender = store.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let replaced = put(&contender, b"contender", PutMode::Overwrite);
        let removed = contender.delete_blocking(&object(&contender));
        send.send((replaced, removed)).unwrap();
    });
    // The bound applies to the lock; staging the replacement first flushes,
    // which a busy disk can stretch. This guard is against waiting forever.
    let bounded = receive.recv_timeout(Duration::from_secs(120));
    // A stopped writer's staging file is not abandoned: its lock is held.
    let kept = staged(&root).len();
    // Release the holder even when a regression blocks someone forever.
    holder.kill().unwrap();
    holder.wait().unwrap();
    thread.join().unwrap();
    others.expect("a held object does not block other objects");
    assert_eq!(read_at(&store, "namespaces/ns/other").0, b"swapped");
    assert_eq!(read_at(&store, "namespaces/ns/created").0, b"created");
    assert!(!root.join("namespaces/ns/retired").exists());
    assert_eq!(kept, 1, "the reaper left the stopped writer's staging file");
    let (replaced, removed) = bounded.expect("lock acquisition exceeded its bound");
    for error in [
        replaced
            .expect_err("a frozen holder retains exclusion")
            .to_string(),
        removed
            .expect_err("a frozen holder retains exclusion")
            .to_string(),
    ] {
        assert!(
            error.contains("storage lock acquisition deadline exceeded"),
            "unexpected storage failure: {error}"
        );
    }
    assert_eq!(read(&store, &root), b"original");
    assert_eq!(current(&store), expected);
    put(
        &LocalStore::open(&root).unwrap(),
        b"recovered",
        PutMode::Overwrite,
    )
    .unwrap();
    assert_eq!(read(&store, &root), b"recovered");
    assert!(staged(&root).is_empty(), "the dead writer's file is reaped");

    // Stopped before it took the lock, a writer blocks nobody at all.
    let marker = root.join("staged");
    let expected = current(&store);
    let mut holder = child(
        &root,
        "update",
        &[
            ("FYRR_TEST_LOCAL_PAUSE_STAGED", marker.as_os_str()),
            ("FYRR_TEST_LOCAL_PAYLOAD", "holder".as_ref()),
            ("FYRR_TEST_LOCAL_EXPECTED", expected.as_ref()),
            ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
        ],
    );
    reached(&marker, &mut holder);
    let replaced = put(&store, b"unblocked", PutMode::Overwrite);
    holder.kill().unwrap();
    holder.wait().unwrap();
    replaced.expect("a writer that holds no lock blocks nobody");
    assert_eq!(read(&store, &root), b"unblocked");
    std::fs::remove_dir_all(root).unwrap();
}

/// A writer that waits for an object's lock holds the inode the name meant
/// when it arrived. If the object is removed and created again meanwhile,
/// the lock it finally gets is on an inode the name no longer means. It must
/// notice and act on the new object, never on the strength of the old lock.
#[test]
fn local_store_waiter_across_delete_and_recreate_acts_on_the_new_object() {
    let root = scratch("waiter");
    let store = LocalStore::open(&root).unwrap();
    for (round, (action, recreate, outcome, bytes)) in [
        // A stale tag is refused against the new object, which is untouched.
        ("update", true, "precondition", b"second".as_slice()),
        // An unconditional write replaces the new object, not the old inode.
        ("overwrite", true, "won", b"waiter".as_slice()),
        // With no new object, the update finds nothing and the overwrite
        // creates one.
        ("update", false, "precondition", b"".as_slice()),
        ("overwrite", false, "won", b"waiter".as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        let _ = store.delete_blocking(&object(&store));
        put(&store, b"first", PutMode::Create).unwrap();
        let first = current(&store);
        let marker = root.join(format!("opened-{round}"));
        let result = root.join(format!("waiter-{round}"));
        let mut waiter = child(
            &root,
            action,
            &[
                ("FYRR_TEST_LOCAL_HOLD_OPENED", marker.as_os_str()),
                ("FYRR_TEST_LOCAL_PAYLOAD", "waiter".as_ref()),
                ("FYRR_TEST_LOCAL_EXPECTED", first.as_ref()),
                ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
            ],
        );
        // The waiter has the first inode open and has not locked it yet.
        reached(&marker, &mut waiter);
        // However long the writes below take, the waiter spends none of that
        // time waiting for a lock, so its lock wait does not run out.
        std::thread::sleep(LOCK_WAIT + Duration::from_secs(1));
        store.delete_blocking(&object(&store)).unwrap();
        let second = recreate.then(|| {
            put(&store, b"second", PutMode::Create).unwrap();
            current(&store)
        });
        std::fs::write(marker.with_extension("go"), b"").unwrap();
        assert!(waiter.wait().unwrap().success(), "round {round}");
        assert_eq!(
            std::fs::read_to_string(&result).unwrap(),
            outcome,
            "round {round}"
        );
        if bytes.is_empty() {
            assert!(!root.join("namespaces/ns/head.json").exists());
            continue;
        }
        assert_eq!(read(&store, &root), bytes, "round {round}");
        let now = current(&store);
        assert_ne!(now, first, "round {round}");
        assert_eq!(second.is_some_and(|second| second == now), outcome != "won");
    }
    assert!(staged(&root).is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

/// Several processes write the same and different objects at once, while
/// one of them is stopped and resumed repeatedly and another is killed. No
/// compare-and-swap is lost or applied twice, each contested create has one
/// winner, every acknowledged object keeps its bytes, and nothing is left
/// in the staging directory.
#[test]
fn local_store_processes_stress_the_same_and_different_objects() {
    use rustix::process::{Pid, Signal, kill_process};
    const PROCESSES: usize = 7;
    const KILLED: usize = PROCESSES - 1;
    let root = scratch("stress");
    let store = LocalStore::open(&root).unwrap();
    put_at(&store, "namespaces/ns/counter", b"0", PutMode::Create).unwrap();
    let go = root.join("go");
    let mut children: Vec<_> = (0..PROCESSES)
        .map(|id| {
            let result = root.join(format!("stress-{id}"));
            let name = id.to_string();
            let process = child(
                &root,
                "stress",
                &[
                    ("FYRR_TEST_LOCAL_GO", go.as_os_str()),
                    ("FYRR_TEST_LOCAL_ID", name.as_ref()),
                    ("FYRR_TEST_LOCAL_RESULT", result.as_os_str()),
                ],
            );
            (process, result)
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500));
    std::fs::write(&go, b"").unwrap();
    // SIGKILL one contender once it is writing: wherever it dies, its locks
    // go with it and its unfinished write is not acknowledged.
    let started = std::time::Instant::now();
    let first = root.join(format!("namespaces/ns/own/{KILLED}-0"));
    while !first.exists() && started.elapsed() < Duration::from_secs(30) {
        std::thread::sleep(Duration::from_millis(1));
    }
    children[KILLED].0.kill().unwrap();
    // SIGSTOP and SIGCONT another, for less than the lock bound each time:
    // the others wait for it at most that long, and a reaper leaves the
    // stopped writer's staging file alone.
    let stopped = Pid::from_child(&children[0].0);
    for _ in 0..10 {
        let _ = kill_process(stopped, Signal::STOP);
        std::thread::sleep(Duration::from_millis(100));
        let _ = kill_process(stopped, Signal::CONT);
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut swapped = BTreeSet::new();
    let mut winners = std::collections::BTreeMap::new();
    let mut finished = Vec::new();
    for (id, (process, result)) in children.iter_mut().enumerate() {
        let status = process.wait().unwrap();
        // The killed contender may have finished first on a fast disk.
        let Ok(log) = std::fs::read_to_string(result) else {
            assert!(id == KILLED && !status.success(), "contender {id}");
            continue;
        };
        assert!(status.success() || id == KILLED, "contender {id}");
        finished.push(id);
        for line in log.lines() {
            let (event, value) = line.split_once(' ').unwrap();
            match event {
                "swapped" => assert!(
                    swapped.insert(value.to_owned()),
                    "two updates succeeded against the tag {value}"
                ),
                "raced" => assert!(
                    winners
                        .insert(value.parse::<usize>().unwrap(), id)
                        .is_none(),
                    "round {value} has two winners"
                ),
                other => panic!("{other}"),
            }
        }
    }
    assert!(finished.len() >= PROCESSES - 1);
    let acknowledged = finished.len() * STRESS_ROUNDS;
    let (bytes, last) = read_at(&store, "namespaces/ns/counter");
    let counter: usize = String::from_utf8(bytes).unwrap().parse().unwrap();
    // Every acknowledged increment is there. The killed contender adds the
    // increments it published, acknowledged or not, and never more.
    assert!(
        (acknowledged..=PROCESSES * STRESS_ROUNDS).contains(&counter),
        "{counter} increments for {acknowledged} acknowledged"
    );
    assert_eq!(swapped.len(), acknowledged);
    assert!(!swapped.contains(&last));
    for round in 0..STRESS_ROUNDS {
        // A round without a logged winner was won by the killed contender.
        let winner = winners.get(&round).copied().unwrap_or(KILLED);
        let raced = read_at(&store, &format!("namespaces/ns/raced/{round}")).0;
        assert_eq!(raced, payload(round, winner), "round {round}");
        for id in &finished {
            let own = read_at(&store, &format!("namespaces/ns/own/{id}-{round}")).0;
            assert_eq!(own, payload(round, *id));
        }
    }
    LocalStore::open(&root)
        .unwrap()
        .delete_blocking(&at(&store, "namespaces/ns/missing"))
        .unwrap();
    assert!(staged(&root).is_empty());
    std::fs::remove_dir_all(root).unwrap();
}
