use super::*;
#[test]
fn the_publication_decides_whether_the_directory_clock_applies() {
    use fyrr_core::config::DirectoryProvider;
    let mut config = fyrr_core::config::parse(concat!(
        "[namespace]\nname='freshness'\n",
        "[storage]\nuri='s3://freshness'\n",
        "[embedding]\nendpoint='https://embed.invalid'\n"
    ))
    .unwrap()
    .config;
    let now = fyrr_core::clock::now();
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
            // Without a freshness timestamp only a reader without a directory
            // provider serves (a snapshot imported by hand). A timestamp is
            // the writer's attestation, and it binds every reader: the
            // reader's own provider setting cannot waive it.
            assert_eq!(
                directory_fresh(&config, freshness, now),
                valid || (freshness.is_none() && provider == DirectoryProvider::None),
                "{provider:?}: {freshness:?}"
            );
        }
    }
}

#[test]
fn ec2_metadata_opt_out_preserves_other_credential_sources() {
    use AmazonS3ConfigKey as Key;
    let cases: &[(&[(AmazonS3ConfigKey, &str)], bool)] = &[
        (&[], true),
        (&[(Key::AccessKeyId, "key")], false),
        (&[(Key::SecretAccessKey, "secret")], false),
        (&[(Key::WebIdentityTokenFile, "/token")], true),
        (&[(Key::RoleArn, "role")], true),
        (
            &[
                (Key::WebIdentityTokenFile, "/token"),
                (Key::RoleArn, "role"),
            ],
            false,
        ),
        (&[(Key::ContainerCredentialsRelativeUri, "/task")], false),
        (
            &[(Key::ContainerCredentialsFullUri, "http://container")],
            true,
        ),
        (&[(Key::ContainerAuthorizationTokenFile, "/token")], true),
        (
            &[
                (Key::ContainerCredentialsFullUri, "http://container"),
                (Key::ContainerAuthorizationTokenFile, "/token"),
            ],
            false,
        ),
        (&[(Key::SkipSignature, "true")], false),
        (&[(Key::SkipSignature, "false")], true),
    ];
    for (options, expected) in cases {
        let mut builder = AmazonS3Builder::new().with_bucket_name("credentials");
        for (key, value) in *options {
            builder = builder.with_config(*key, *value);
        }
        assert_eq!(
            uses_instance_credentials(&builder),
            *expected,
            "{options:?}"
        );
    }
    // A malformed explicit source must retain its configuration error instead
    // of being mistaken for a disabled instance-profile fallback.
    assert!(
        AmazonS3Builder::new()
            .with_bucket_name("credentials")
            .with_access_key_id("key")
            .build()
            .is_err()
    );
}
