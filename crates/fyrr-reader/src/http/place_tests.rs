use super::*;

fn names(open: &Open<u8>) -> Vec<&str> {
    open.iter().map(|(name, _)| name.as_str()).collect()
}

#[test]
fn a_place_is_made_by_closing_an_idle_namespace_only() {
    let mut open = Open::<u8>::new();
    let a = place(&mut open, "a", true, 2).unwrap().unwrap();
    assert!(a.set(Arc::new(1)).is_ok());
    // `b` is held by nothing once its place is dropped.
    place(&mut open, "b", true, 2).unwrap();
    assert_eq!(names(&open), ["a", "b"]);
    // `a` is least recently used but a request holds its place: `b` closes.
    let c = place(&mut open, "c", true, 2).unwrap().unwrap();
    assert_eq!(names(&open), ["a", "c"]);
    // Both are held, so no place is made, and nothing is closed.
    assert_eq!(place(&mut open, "d", true, 2).unwrap_err(), Busy);
    assert_eq!(names(&open), ["a", "c"]);
    // A namespace that a request still serves is not idle either.
    let serving = a.get().unwrap().clone();
    drop((a, c));
    assert!(place(&mut open, "d", true, 2).unwrap().is_some());
    assert_eq!(names(&open), ["a", "d"]);
    drop(serving);
    // A lookup without `create` finds a place and makes it most recent; one
    // that is gone is opened again as a new place.
    assert!(place(&mut open, "a", false, 2).unwrap().is_some());
    assert_eq!(names(&open), ["d", "a"]);
    assert!(place(&mut open, "b", false, 2).unwrap().is_none());
    let b = place(&mut open, "b", true, 2).unwrap().unwrap();
    assert!(b.get().is_none());
    assert_eq!(names(&open), ["a", "b"]);
}

#[test]
fn a_slot_takes_its_mirror_with_it() {
    let mirror = std::env::temp_dir().join(format!("fyrr-slot-{}", std::process::id()));
    std::fs::create_dir_all(mirror.join("dataset")).unwrap();
    let slots = Arc::new(Semaphore::new(1));
    let slot = Slot {
        mirror: mirror.clone(),
        _permit: slots.clone().try_acquire_owned().unwrap(),
    };
    assert_eq!(slots.available_permits(), 0);
    drop(slot);
    assert!(!mirror.exists());
    assert_eq!(slots.available_permits(), 1);
}

#[test]
fn a_process_owns_a_directory_for_the_mirrors_of_path_namespaces() {
    let configured = std::env::temp_dir().join(format!("fyrr-owned-{}", std::process::id()));
    let root = std::path::PathBuf::from(format!("{}.paths", configured.display()));
    let _ = std::fs::remove_dir_all(&root);
    // Owners coexist.
    let (first, first_lock) = own_mirrors(configured.clone()).unwrap();
    let (second, second_lock) = own_mirrors(configured.clone()).unwrap();
    assert_ne!(first, second);
    std::fs::create_dir_all(first.join("path-0")).unwrap();
    std::fs::create_dir_all(second.join("path-0")).unwrap();
    // A new owner removes the directory of one that ended, not of a live one.
    drop(first_lock);
    let (_, third_lock) = own_mirrors(configured.clone()).unwrap();
    assert!(!first.exists());
    assert!(second.join("path-0").exists());
    // Only directories of owners are swept: not a symlink, not other files.
    let target = root.join("target");
    std::fs::create_dir_all(target.join("path-0")).unwrap();
    crate::mirror::MirrorStore::private_file(&target.join("owner.lock"), true).unwrap();
    std::os::unix::fs::symlink(&target, root.join("1-2")).unwrap();
    let (_, fourth_lock) = own_mirrors(configured).unwrap();
    assert!(target.join("path-0").exists());
    drop((second_lock, third_lock, fourth_lock));
    std::fs::remove_dir_all(&root).unwrap();
}
