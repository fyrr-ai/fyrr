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
