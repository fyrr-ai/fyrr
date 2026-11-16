use super::*;

#[test]
fn vectors_must_be_finite_nonzero_and_profile_sized() {
    assert!(validate(&[1.0; 1024], 1024).is_ok());
    assert!(validate(&[0.0; 1024], 1024).is_err());
    assert!(validate(&[f32::NAN; 1024], 1024).is_err());
    assert!(validate(&[f32::INFINITY; 1024], 1024).is_err());
    assert!(validate(&[1.0; 1023], 1024).is_err());
    assert!(validate(&[f32::MAX; 1024], 1024).is_err());
    assert!(validate(&[f32::MIN_POSITIVE; 1024], 1024).is_err());
    assert!(validate(&[1.0; 512], 512).is_ok());
    assert!(validate(&[1.0; 512], 1024).is_err());
}

#[test]
fn profile_identity_always_records_settings_and_binds_roles() {
    let mut config = fyrr_core::config::parse(
        "[namespace]\nname='profile'\n[storage]\nuri='s3://profile'\n[embedding]\n\
            endpoint='https://cache.invalid'\n",
    )
    .unwrap()
    .config
    .embedding;
    let baseline = identity(&config);
    baseline.validate_profile().unwrap();
    assert_eq!(baseline.settings.prefix, "");
    assert_eq!(baseline.settings.roles, EmbeddingRoles::default());
    assert_eq!(
        serde_json::to_value(&baseline)
            .unwrap()
            .as_object()
            .unwrap()
            .len(),
        4
    );
    assert_ne!(
        baseline.fingerprint,
        digest(
            serde_json::json!(["BAAI/bge-m3", 1024, "", "exact-utf8"])
                .to_string()
                .as_bytes()
        )
    );
    for dimension in [512, 1024] {
        config.dimensions = dimension;
        config.roles.query.input_type = Some("query".into());
        let profile = identity(&config);
        assert_ne!(profile.fingerprint, baseline.fingerprint);
        profile.validate_profile().unwrap();
        config.roles.document.prefix = Some("document: ".into());
        assert_ne!(identity(&config).fingerprint, profile.fingerprint);
        config.roles.document.prefix = None;
    }
}
