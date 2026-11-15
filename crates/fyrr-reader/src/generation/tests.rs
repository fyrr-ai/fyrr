#[test]
fn serving_minimum_funds_two_warm_claims_and_native_caches() {
    let mib = 1024 * 1024;
    // 320 MiB startup envelope + two 56 MiB claims + 48 MiB cache.
    assert_eq!(
        PreparationReservation::minimum_bytes(320 * mib, 56 * mib, (0, 48 * mib)),
        480 * mib
    );
    assert_eq!(PreparationReservation::minimum_bytes(1, 1, (1, 1)), 16385);
    assert_eq!(
        PreparationReservation::minimum_bytes(u64::MAX, 1, (1, 1)),
        u64::MAX
    );
}
use super::*;
#[test]
fn repeated_publications_reserve_overlap_and_reuse_full_or_capped_cache() {
    let mib = 1024 * 1024u64;
    for (capacity, expected, full) in [(1700, 740, false), (1856, 896, true), (1300, 340, false)] {
        let pool = Arc::new(tokio::sync::Semaphore::new(
            (capacity * mib / 4096) as usize,
        ));
        let mut old = pool
            .clone()
            .try_acquire_many_owned(u32::try_from(480 * mib / 4096).unwrap())
            .unwrap();
        let session = NativeSession::acquire(
            crate::generation::MemoryPool {
                cap: capacity * mib,
                permits: pool.clone(),
            },
            (576 * mib, 320 * mib),
            480 * mib,
            None,
        )
        .unwrap();
        assert_eq!(session.bytes(), expected * mib);
        assert_eq!(
            session.capacity.0 >= 576 * mib && session.capacity.1 >= 320 * mib,
            full
        );
        drop(old.split(((480 - 153) * mib / 4096) as usize));
        for _ in 0..5 {
            let mut next = pool
                .clone()
                .try_acquire_many_owned(u32::try_from(480 * mib / 4096).unwrap())
                .unwrap();
            let same = NativeSession::acquire(
                crate::generation::MemoryPool {
                    cap: capacity * mib,
                    permits: pool.clone(),
                },
                (576 * mib, 320 * mib),
                480 * mib,
                Some(session.clone()),
            )
            .unwrap();
            assert!(
                Arc::ptr_eq(&session, &same),
                "no handover or replacement for identical publication"
            );
            drop(old);
            drop(next.split(((480 - 153) * mib / 4096) as usize));
            old = next;
        }
        drop(old);
        drop(session);
        assert_eq!(pool.available_permits() as u64 * 4096, capacity * mib);
    }
}

#[test]
fn asymmetric_resize_cure_and_allocation_use_identical_lane_rounding() {
    let mib = 1024 * 1024u64;
    let pool = Arc::new(tokio::sync::Semaphore::new((400 * mib / 4096) as usize));
    let old_claim = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(56 * mib / 4096).unwrap())
        .unwrap();
    let old = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 400 * mib,
            permits: pool.clone(),
        },
        (64 * mib, 64 * mib),
        56 * mib,
        None,
    )
    .unwrap();
    let next = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(80 * mib / 4096).unwrap())
        .unwrap();
    let needed = (32 * mib + 1, 120 * mib + 1);
    assert!(
        NativeSession::acquire(
            crate::generation::MemoryPool {
                cap: 400 * mib,
                permits: pool.clone()
            },
            needed,
            80 * mib,
            Some(old.clone())
        )
        .is_err()
    );
    drop(next);
    drop(old_claim);
    drop(old);
    let claim = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(80 * mib / 4096).unwrap())
        .unwrap();
    let session = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 400 * mib,
            permits: pool.clone(),
        },
        needed,
        80 * mib,
        None,
    )
    .unwrap();
    assert_eq!(
        session.capacity,
        NativeSession::allocation(needed, 240 * mib)
    );
    assert!(session.capacity.0 >= needed.0 && session.capacity.1 >= needed.1);
    let shrunk = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 400 * mib,
            permits: pool.clone(),
        },
        (32 * mib, 32 * mib),
        80 * mib,
        Some(session.clone()),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&session, &shrunk));
    drop(shrunk);
    drop(session);
    drop(claim);
    assert_eq!(pool.available_permits() as u64 * 4096, 400 * mib);
}

#[test]
fn permanently_cold_publications_need_overlap_or_minimum_claim_handover() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n[reader]\nconcurrency=2\n",
    )
    .unwrap()
    .config;
    let mut manifest: ContentManifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    let vector = manifest
        .indexes
        .iter()
        .find(|i| i.name == "vector_idx")
        .unwrap()
        .uuid
        .clone();
    manifest.files[0].path = format!("_indices/{vector}/cold-fixture");
    manifest.files[0].size_bytes = 200 * 1024 * 1024;
    let mib = 1024 * 1024u64;
    let rows = 152_917; // rounded cold claim 60 MiB; warm exceeds either pool.
    for capacity in [150, 100] {
        let pool = Arc::new(tokio::sync::Semaphore::new(
            (capacity * mib / 4096) as usize,
        ));
        let mut old =
            PreparationReservation::acquire(&config, &manifest, rows, pool.clone()).unwrap();
        assert!(!old.warm());
        assert_eq!(old.bytes(), 60 * mib);
        let cache = NativeSession::acquire(
            crate::generation::MemoryPool {
                cap: capacity * mib,
                permits: pool.clone(),
            },
            PreparationReservation::cache_bytes(&manifest),
            old.warm_bytes(),
            None,
        )
        .unwrap();
        assert_eq!(
            cache.bytes(),
            0,
            "an unaffordable warm reserve leaves no cache allowance"
        );
        for _ in 0..5 {
            let next = PreparationReservation::acquire(&config, &manifest, rows, pool.clone());
            if capacity == 150 {
                let next = next.unwrap();
                assert!(!next.warm());
                let same = NativeSession::acquire(
                    crate::generation::MemoryPool {
                        cap: capacity * mib,
                        permits: pool.clone(),
                    },
                    PreparationReservation::cache_bytes(&manifest),
                    next.warm_bytes(),
                    Some(cache.clone()),
                )
                .unwrap();
                assert!(Arc::ptr_eq(&cache, &same));
                drop(old);
                old = next;
            } else {
                assert!(next.is_err(), "minimum serving overlap requires a drain");
                drop(old);
                old = PreparationReservation::acquire(&config, &manifest, rows, pool.clone())
                    .unwrap();
                assert!(!old.warm());
            }
        }
        drop(old);
        drop(cache);
        assert_eq!(pool.available_permits() as u64 * 4096, capacity * mib);
    }
}
#[test]
fn handover_reclaims_session_cache_and_admits_a_larger_publication() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n[reader]\nconcurrency=2\n",
    )
    .unwrap()
    .config;
    let mut manifest: ContentManifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    manifest.indexes.clear();
    manifest.files.clear();
    let mib = 1024 * 1024u64;
    let startup = (256 * mib / 4096) as usize;
    let pool = Arc::new(tokio::sync::Semaphore::new(startup));
    let old = PreparationReservation::acquire(&config, &manifest, 131_072, pool.clone()).unwrap();
    assert_eq!(old.bytes(), 56 * mib);
    let cache = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 256 * mib,
            permits: pool.clone(),
        },
        (186 * mib * 4 / 5, 0),
        old.warm_bytes(),
        None,
    )
    .unwrap();
    assert_eq!(cache.bytes(), 144 * mib);
    let weak = Arc::downgrade(&cache);
    assert_eq!(pool.available_permits() as u64 * 4096, 56 * mib);
    assert!(PreparationReservation::acquire(&config, &manifest, 262_144, pool.clone()).is_err());
    // No context owns a strong session reference across the drain.
    drop(old);
    drop(cache);
    assert!(weak.upgrade().is_none());
    assert_eq!(pool.available_permits(), startup);
    let new = PreparationReservation::acquire(&config, &manifest, 262_144, pool.clone()).unwrap();
    assert_eq!(new.bytes(), 80 * mib);
    let cache = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 256 * mib,
            permits: pool.clone(),
        },
        (200 * mib, 0),
        new.warm_bytes(),
        weak.upgrade(),
    )
    .unwrap();
    assert_eq!(cache.bytes(), 96 * mib);
    assert!(new.warm(), "an undersized cache does not select exact KNN");
    assert!(cache.capacity.0 < 200 * mib);
    let worker = cache.clone();
    drop(cache);
    drop(new);
    assert_eq!(pool.available_permits() as u64 * 4096, 160 * mib);
    drop(worker);
    assert_eq!(
        pool.available_permits(),
        startup,
        "all generation/session permits return"
    );
}

#[test]
fn cache_resize_handover_recovers_space_without_leaking_the_old_session() {
    let mib = 1024 * 1024;
    let startup = 480 * mib / 4096;
    let pool = Arc::new(tokio::sync::Semaphore::new(startup));
    let old_generation = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(56 * mib / 4096).unwrap())
        .unwrap();
    let old = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 480 * mib as u64,
            permits: pool.clone(),
        },
        (200 * mib as u64, 0),
        56 * mib as u64,
        None,
    )
    .unwrap();
    assert_eq!(old.bytes(), 250 * mib as u64);
    let new_generation = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(80 * mib / 4096).unwrap())
        .unwrap();
    let remaining = pool.available_permits();
    assert!(
        NativeSession::acquire(
            crate::generation::MemoryPool {
                cap: 480 * mib as u64,
                permits: pool.clone()
            },
            (256 * mib as u64, 0),
            80 * mib as u64,
            Some(old.clone())
        )
        .is_err()
    );
    assert_eq!(
        pool.available_permits(),
        remaining,
        "failed resize consumes no permits"
    );
    drop(new_generation);
    drop(old_generation);
    drop(old);
    assert_eq!(pool.available_permits(), startup);
    let new_generation = pool
        .clone()
        .try_acquire_many_owned(u32::try_from(80 * mib / 4096).unwrap())
        .unwrap();
    let session = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 480 * mib as u64,
            permits: pool.clone(),
        },
        (256 * mib as u64, 0),
        80 * mib as u64,
        None,
    )
    .unwrap();
    assert_eq!(session.capacity.0, 320 * mib as u64);
    for _ in 0..5 {
        let overlap = pool
            .clone()
            .try_acquire_many_owned(u32::try_from(80 * mib / 4096).unwrap())
            .unwrap();
        let same = NativeSession::acquire(
            crate::generation::MemoryPool {
                cap: 480 * mib as u64,
                permits: pool.clone(),
            },
            (256 * mib as u64, 0),
            80 * mib as u64,
            Some(session.clone()),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&session, &same));
        drop(overlap);
    }
    drop(session);
    drop(new_generation);
    assert_eq!(pool.available_permits(), startup);
}

#[test]
fn cache_growth_reuses_entries_then_resizes_with_owned_permits() {
    let mib = 1024 * 1024;
    let pool = Arc::new(tokio::sync::Semaphore::new(1024 * mib / 4096));
    let startup = pool.available_permits();
    let old = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 1024 * mib as u64,
            permits: pool.clone(),
        },
        (64 * mib as u64, 64 * mib as u64),
        0,
        None,
    )
    .unwrap();
    assert_eq!(old.capacity, (80 * mib as u64, 80 * mib as u64));
    let same = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 1024 * mib as u64,
            permits: pool.clone(),
        },
        (72 * mib as u64, 72 * mib as u64),
        0,
        Some(old.clone()),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&old, &same));
    let bigger = NativeSession::acquire(
        crate::generation::MemoryPool {
            cap: 1024 * mib as u64,
            permits: pool.clone(),
        },
        (128 * mib as u64, 128 * mib as u64),
        0,
        Some(old.clone()),
    )
    .unwrap();
    assert_eq!(bigger.capacity, (160 * mib as u64, 160 * mib as u64));
    assert!(!Arc::ptr_eq(&old, &bigger));
    drop(old);
    drop(same);
    drop(bigger);
    assert_eq!(pool.available_permits(), startup);
}

#[test]
fn identical_pool_claims_are_admitted_independently_of_rss() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n[reader]\nmemory_max_bytes='256MiB'\n",
    )
    .unwrap()
    .config;
    let manifest: ContentManifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    // Model an already sized startup pool, independent of later allocator
    // retention or other changes to observed process residency.
    let pool = Arc::new(tokio::sync::Semaphore::new(1024 * 1024));
    let first = PreparationReservation::acquire(&config, &manifest, 1, pool.clone()).unwrap();
    let claim = first.bytes();
    let warm = first.warm();
    drop(first);
    let before = PreparationReservation::rss().unwrap();
    let retained = vec![0x7bu8; 128 * 1024 * 1024];
    std::hint::black_box(&retained);
    let after = PreparationReservation::rss().unwrap();
    assert!(after > before + 64 * 1024 * 1024);
    assert!(
        after + claim + config.reader.memory_headroom_bytes.as_u64()
            > config.reader.memory_max_bytes.as_u64(),
        "the former RSS gate would reject this claim"
    );
    let second = PreparationReservation::acquire(&config, &manifest, 1, pool).unwrap();
    assert_eq!(second.bytes(), claim);
    assert_eq!(second.warm(), warm);
    drop(retained);
}

#[test]
fn underestimated_roots_degrade_and_do_not_retry_the_bad_estimate() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n",
    )
    .unwrap()
    .config;
    let manifest: ContentManifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(1024 * 1024));
    let reservation = PreparationReservation::acquire(&config, &manifest, 1, pool.clone()).unwrap();
    assert!(reservation.warm());
    let held = reservation.clone();
    assert!(!reservation.settle(reservation.bytes() + 1).unwrap());
    assert!(!held.warm(), "all workers observe cold degradation");
    assert!(
        held.upgrade(pool).is_err(),
        "a known bad estimate cannot allocate the same roots on a later warming retry"
    );
}
#[test]
fn non_vector_indexes_use_the_shared_cache_envelope() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n",
    )
    .unwrap()
    .config;
    let mut manifest: ContentManifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(1024 * 1024));
    let original = PreparationReservation::acquire(&config, &manifest, 1, pool.clone()).unwrap();
    assert!(original.warm());
    let uuid = "00000000-0000-0000-0000-000000000123";
    manifest
        .indexes
        .push(fyrr_core::publication::IndexIdentity {
            name: "text_idx".into(),
            kind: "fts".into(),
            column: "searchable_text".into(),
            uuid: uuid.into(),
        });
    manifest.files.push(fyrr_core::envelope::FileRef {
        path: format!("_indices/{uuid}/index.idx"),
        size_bytes: 4 * 1024 * 1024 * 1024,
    });
    let with_text = PreparationReservation::acquire(&config, &manifest, 1, pool).unwrap();
    assert!(with_text.warm());
    assert_eq!(original.bytes(), with_text.bytes());
}
#[test]
fn reservation_falls_back_and_stays_owned_until_last_reference() {
    let config = fyrr_core::config::parse(
        "[namespace]\nname='budget'\n[storage]\nuri='s3://budget'\n[embedding]\n\
            endpoint='https://cache.invalid'\n",
    )
    .unwrap()
    .config;
    let mut manifest: ContentManifest = serde_json::from_str(include_str!(
        "../../../../fixtures/contracts/content-manifest/manifest.json"
    ))
    .unwrap();
    manifest.files.push(fyrr_core::envelope::FileRef {
        path: format!(
            "_indices/{}/index.idx",
            manifest
                .indexes
                .iter()
                .find(|index| index.name == "vector_idx")
                .unwrap()
                .uuid
        ),
        size_bytes: 32 * 1024 * 1024,
    });
    let cold = usize::try_from(
        (32 * 1024 * 1024 + 64 * (config.reader.concurrency as u64 + 1)).div_ceil(4096),
    )
    .unwrap();
    let pool = Arc::new(tokio::sync::Semaphore::new(cold));
    let reservation = PreparationReservation::acquire(&config, &manifest, 1, pool.clone()).unwrap();
    assert!(!reservation.warm());
    assert_eq!(pool.available_permits(), 0);
    let held = reservation.clone();
    drop(reservation);
    assert_eq!(
        pool.available_permits(),
        0,
        "held old requests keep their allocation claim"
    );
    assert!(held.upgrade(pool.clone()).is_err());
    let additional =
        usize::try_from(held.warm_bytes.saturating_sub(held.bytes()).div_ceil(4096)).unwrap();
    // Model the release of another generation's transient/held claim.
    pool.add_permits(additional);
    let warmed = held.upgrade(pool.clone()).unwrap();
    assert!(warmed.warm());
    assert_eq!(
        pool.available_permits(),
        0,
        "an upgrade charges only the delta"
    );
    drop(held);
    assert_eq!(
        pool.available_permits(),
        0,
        "warm readers retain the shared minimum"
    );
    drop(warmed);
    assert_eq!(pool.available_permits(), cold + additional);
}
