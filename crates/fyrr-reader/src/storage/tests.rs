use super::*;
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
    let deadline =
        chrono::Duration::from_std(config.identity.validity_deadline.as_duration()).unwrap();
    for provider in [
        DirectoryProvider::None,
        DirectoryProvider::Scim,
        DirectoryProvider::Keycloak,
        DirectoryProvider::Snapshot,
    ] {
        config.identity.directory.provider = provider;
        for (freshness, valid) in [
            (None, false),
            (Some(now), true),
            (Some(now + chrono::Duration::nanoseconds(1)), false),
            (Some(now - chrono::Duration::days(1)), false),
            (Some(now - deadline), false),
            (
                Some(now - deadline + chrono::Duration::nanoseconds(1)),
                true,
            ),
        ] {
            assert_eq!(
                directory_fresh(&config, freshness, now),
                valid || provider == DirectoryProvider::None,
                "{provider:?}: {freshness:?}"
            );
        }
    }
}
