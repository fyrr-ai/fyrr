use super::*;
#[test]
fn directory_deadline_is_exclusive() {
    let mut config = fyrr_core::config::load(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/fyrr.example.toml"
    )))
    .unwrap()
    .config;
    config.identity.directory.provider = fyrr_core::config::DirectoryProvider::Snapshot;
    let now = chrono::Utc::now();
    let deadline =
        chrono::Duration::from_std(config.identity.validity_deadline.as_duration()).unwrap();
    assert!(directory_fresh(&config, Some(now), now));
    assert!(directory_fresh(
        &config,
        Some(now - deadline + chrono::Duration::nanoseconds(1)),
        now
    ));
    assert!(!directory_fresh(&config, Some(now - deadline), now));
    assert!(!directory_fresh(
        &config,
        Some(now + chrono::Duration::nanoseconds(1)),
        now
    ));
    assert!(!directory_fresh(&config, None, now));
}
#[test]
fn only_an_absent_directory_needs_no_freshness_proof() {
    use fyrr_core::config::DirectoryProvider;
    let mut config = fyrr_core::config::parse(concat!(
        "[namespace]\nname='freshness'\n",
        "[storage]\nuri='s3://freshness'\n",
        "[embedding]\nendpoint='https://embed.invalid'\n"
    ))
    .unwrap()
    .config;
    let now = chrono::Utc::now();
    for provider in [
        DirectoryProvider::None,
        DirectoryProvider::Scim,
        DirectoryProvider::Keycloak,
        DirectoryProvider::Snapshot,
    ] {
        config.identity.directory.provider = provider;
        for freshness in [
            None,
            Some(now + chrono::Duration::seconds(1)),
            Some(now - chrono::Duration::days(1)),
        ] {
            assert_eq!(
                directory_fresh(&config, freshness, now),
                provider == DirectoryProvider::None,
                "{provider:?}: {freshness:?}"
            );
        }
        assert!(directory_fresh(&config, Some(now), now));
    }
}
