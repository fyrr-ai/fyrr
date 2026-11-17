//! Preserve native index diagnosis across the reader/writer boundary.
use super::*;

#[tokio::test]
#[ignore = "local storage publication, native index verification and writer CLI errors"]
async fn writer_verify_preserves_incompatible_index_on_storage() {
    let (config, root, _) = super::tools::fixture();
    let original = fyrr_core::config::load(&config).unwrap().config;
    assert!(!original.namespace.fts_stem);
    let valid = super::coordination::command(&config, &["writer", "verify"]);
    assert_eq!(valid["verified"], true);
    let before = write_contract(&super::coordination::publication(&config).await.0);
    let text = std::fs::read_to_string(&config).unwrap();
    let changed = text.replacen("[namespace]", "[namespace]\nfts_stem = true", 1);
    assert_ne!(changed, text);
    let incompatible = root.join("incompatible-index.toml");
    std::fs::write(&incompatible, changed).unwrap();
    let parsed = fyrr_core::config::load(&incompatible).unwrap().config;
    assert!(parsed.namespace.fts_stem);
    let refused = fyrr_reader::search::PreparedReader::open(&parsed).await;
    assert!(
        matches!(refused, Err(fyrr_reader::ReaderError::IncompatibleIndex)),
        "the same publication must be refused by the actual reader"
    );
    let output = super::coordination::host_command(&incompatible)
        .args(["writer", "verify"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("error: incompatible_index"),
        "writer verification must preserve the native index diagnosis: {error}"
    );
    assert!(!error.contains("host_callback_failed"), "{error}");
    assert_eq!(
        write_contract(&super::coordination::publication(&config).await.0),
        before,
        "verification must not change the serving head"
    );

    // Explicit historical settings keep their published identity. A reader
    // using the new defaults must refuse it until the writer republishes.
    let historical = root.join("historical-retrieval.toml");
    std::fs::write(
        &historical,
        text.replacen(
            "[namespace]",
            "[namespace]\nretrieval = { dense_weight = 0.25, lexical_candidates = 50 }",
            1,
        ),
    )
    .unwrap();
    let old = fyrr_core::config::load(&historical).unwrap().config;
    super::coordination::command(&historical, &["writer", "commit"]);
    let (_, prior) = super::coordination::publication(&historical).await;
    assert_eq!(
        prior.retrieval_policy_digest,
        old.namespace.retrieval_digest()
    );
    assert!(
        fyrr_reader::search::PreparedReader::open(&old)
            .await
            .is_ok()
    );
    assert!(matches!(
        fyrr_reader::search::PreparedReader::open(&original).await,
        Err(fyrr_reader::ReaderError::IncompatibleIndex)
    ));
    super::coordination::command(&config, &["writer", "commit"]);
    let (_, updated) = super::coordination::publication(&config).await;
    assert_eq!(
        updated.retrieval_policy_digest,
        original.namespace.retrieval_digest()
    );
    assert_ne!(
        updated.retrieval_policy_digest,
        prior.retrieval_policy_digest
    );
    assert_eq!(updated.embedding, prior.embedding);
    assert!(updated.envelope_ids.is_empty());
    assert!(
        fyrr_reader::search::PreparedReader::open(&original)
            .await
            .is_ok()
    );
    assert_eq!(
        super::coordination::command(&config, &["writer", "verify"])["verified"],
        true
    );
    std::fs::remove_dir_all(root).unwrap();
}
