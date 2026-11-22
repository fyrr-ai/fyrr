//! A namespace keeps the settings it was created with, whatever a process's
//! configuration file says.
use super::*;

#[tokio::test]
#[ignore = "local storage publication, namespace settings and writer CLI errors"]
async fn a_namespace_keeps_its_own_settings_on_storage() {
    let (config, root, _) = super::tools::fixture();
    let original = fyrr_core::config::load(&config).unwrap().config;
    assert!(!original.namespace.fts_stem);
    let (head, published) = super::coordination::publication(&config).await;
    assert_eq!(
        published.retrieval_policy_digest,
        original.namespace.retrieval_digest()
    );
    let before = write_contract(&head);
    let text = std::fs::read_to_string(&config).unwrap();
    let other = root.join("other-settings.toml");
    std::fs::write(
        &other,
        text.replacen(
            "[namespace]",
            "[namespace]\nfts_stem = true\ndense_rq_bits = 2\n\
             retrieval = { dense_weight = 0.25, lexical_candidates = 50 }",
            1,
        ),
    )
    .unwrap();
    let parsed = fyrr_core::config::load(&other).unwrap().config;
    assert!(parsed.namespace.fts_stem);
    assert_ne!(
        parsed.namespace.retrieval_digest(),
        original.namespace.retrieval_digest()
    );

    // The reader and the writer take the settings from the namespace.
    let reader = fyrr_reader::search::PreparedReader::open(&parsed)
        .await
        .expect("the namespace is served with its own settings");
    assert_eq!(reader.manifest.publication_id, published.publication_id);
    assert_eq!(
        super::coordination::command(&other, &["writer", "verify"])["verified"],
        true
    );
    super::coordination::command(&other, &["writer", "commit"]);
    assert_eq!(
        write_contract(&super::coordination::publication(&config).await.0),
        before,
        "an idle commit with other settings must not change the serving head"
    );

    // Creating the namespace again repeats its settings; other ones are refused.
    super::coordination::command(&config, &["writer", "namespace", "create"]);
    let output = super::coordination::host_command(&other)
        .args(["writer", "namespace", "create"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("error: namespace_settings_fixed"), "{error}");
    assert_eq!(
        write_contract(&super::coordination::publication(&config).await.0),
        before
    );
    std::fs::remove_dir_all(root).unwrap();
}
