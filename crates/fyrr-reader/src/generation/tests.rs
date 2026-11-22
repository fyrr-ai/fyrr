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
            spent: Default::default(),
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
            spent: Default::default(),
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

#[test]
fn default_workers_reserve_private_row_state_and_share_native_capacity() {
    let mut config = fyrr_core::config::parse(
        "[namespace]\nname='sizing'\n[storage]\nuri='s3://sizing'\n[embedding]\nendpoint='https://cache.invalid'\n",
    ).unwrap().config;
    assert_eq!(config.reader.concurrency, 8);
    let rows = 20_000;
    let base = 32 * 1024 * 1024;
    assert_eq!(PreparationReservation::worker_count(&config), 8);
    assert_eq!(
        PreparationReservation::cold_bytes(&config, rows),
        base + 9 * rows * 64
    );
    config.embedding.reader_live = true;
    assert_eq!(PreparationReservation::worker_count(&config), 16);
    assert_eq!(
        PreparationReservation::cold_bytes(&config, rows),
        base + 17 * rows * 64
    );
}
