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
    std::fs::remove_dir_all(root).unwrap();
}
