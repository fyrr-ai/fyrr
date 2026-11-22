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

    // A publication that differs from the namespace's recorded settings, in
    // its analyzer or in its embedding profile of the same size, is an
    // incompatible index: diagnosed by the reader and preserved by `writer verify`.
    let (store, path) = fyrr_reader::storage::namespace_store(&original).unwrap();
    let manifest_path = object_store::path::Path::from(format!(
        "{path}/{}",
        head.content.as_ref().unwrap().manifest
    ));
    let mut other_embedding = original.embedding.clone();
    other_embedding.prefix.push_str("other: ");
    for embedding in [false, true] {
        let mut tampered = published.clone();
        if embedding {
            tampered.embedding = fyrr_reader::embedding::identity(&other_embedding);
        } else {
            tampered.fts.stem = !tampered.fts.stem;
        }
        object_store::ObjectStoreExt::put(
            store.as_ref(),
            &manifest_path,
            write_contract(&tampered).into(),
        )
        .await
        .unwrap();
        let refused = fyrr_reader::search::PreparedReader::open(&original).await;
        assert!(
            matches!(refused, Err(fyrr_reader::ReaderError::IncompatibleIndex)),
            "{:?}",
            refused.map(|_| ())
        );
        let output = super::coordination::host_command(&config)
            .args(["writer", "verify"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("error: incompatible_index"), "{error}");
        assert!(!error.contains("host_callback_failed"), "{error}");
    }
    std::fs::remove_dir_all(root).unwrap();
}

const ATTRIBUTES: [(&str, &str); 5] = [
    ("created", "datetime"),
    ("priority", "int"),
    ("score", "float"),
    ("done", "bool"),
    ("team", "string"),
];

pub(super) fn declare(config: &Path, attributes: &[(&str, &str)]) -> String {
    let declared: String = attributes
        .iter()
        .map(|(name, kind)| format!("{name} = \"{kind}\"\n"))
        .collect();
    let text = std::fs::read_to_string(config).unwrap().replacen(
        "[namespace]",
        &format!("[namespace.attributes]\n{declared}[namespace]"),
        1,
    );
    std::fs::write(config, &text).unwrap();
    text
}

async fn open(config: &fyrr_core::config::Config, version: u64) -> lance::Dataset {
    fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(config),
        config
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        version,
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "local storage publication and writer CLI"]
async fn declared_attributes_are_nullable_columns_with_an_index_each_on_storage() {
    use super::mirror_identity::{EXTRA, KEPT, sync_texts};
    use arrow_array::Array;
    let (config, root) = setup_on(Backend::Local);
    declare(&config, &ATTRIBUTES);
    success(&config, &["writer", "namespace", "create"]);
    sync_texts(&config, &root, &[("kept.txt", KEPT)]);
    success(&config, &["writer", "commit"]);

    let (head, manifest) = super::coordination::publication(&config).await;
    let kinds: Vec<_> = manifest
        .attributes
        .iter()
        .map(|(name, kind)| (name.as_str(), kind.index_kind().unwrap()))
        .collect();
    assert_eq!(
        kinds,
        [
            ("created", "btree"),
            ("done", "bitmap"),
            ("priority", "btree"),
            ("score", "btree"),
            ("team", "btree"),
        ]
    );
    for (name, kind) in &manifest.attributes {
        let index = manifest
            .indexes
            .iter()
            .find(|index| index.name == format!("attr_{name}_idx"))
            .unwrap_or_else(|| panic!("no index for {name}"));
        assert_eq!(
            (index.column.as_str(), index.kind.as_str()),
            (name.as_str(), kind.index_kind().unwrap())
        );
    }

    // Every attribute is a nullable column of its own type, null for the rows
    // that carry no value, and its index answers a comparison.
    let loaded = fyrr_core::config::load(&config).unwrap();
    let dataset = open(&loaded.config, head.content.as_ref().unwrap().lance_version).await;
    let rows = dataset.count_rows(None).await.unwrap();
    assert!(rows > 0);
    for (name, arrow) in [
        ("created", "Timestamp(µs, \"UTC\")"),
        ("priority", "Int64"),
        ("score", "Float64"),
        ("done", "Boolean"),
        ("team", "Utf8"),
    ] {
        let batch = dataset
            .scan()
            .project(&[name])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let field = batch.schema().field(0).clone();
        assert!(field.is_nullable(), "{name}");
        assert_eq!(field.data_type().to_string(), arrow, "{name}");
        assert_eq!(batch.column(0).null_count(), batch.num_rows(), "{name}");
    }
    for filter in ["priority > 3", "score < 0.5", "done = true", "team = 'a'"] {
        assert_eq!(dataset.count_rows(Some(filter.into())).await.unwrap(), 0);
    }

    // A later publication adds rows without the columns; the indexes cover them.
    sync_texts(&config, &root, &[("kept.txt", KEPT), ("extra.txt", EXTRA)]);
    success(&config, &["writer", "commit"]);
    let (head, later) = super::coordination::publication(&config).await;
    assert_ne!(later.publication_id, manifest.publication_id);
    assert_eq!(later.attributes, manifest.attributes);
    assert_eq!(
        super::coordination::command(&config, &["writer", "verify"])["verified"],
        true
    );
    let dataset = open(&loaded.config, head.content.unwrap().lance_version).await;
    assert!(dataset.count_rows(None).await.unwrap() > rows);
    assert_eq!(
        dataset
            .count_rows(Some("priority > 3".into()))
            .await
            .unwrap(),
        0
    );
    fyrr_reader::search::PreparedReader::open(&loaded.config)
        .await
        .expect("the reader verifies an index for every attribute");

    // A publication that leaves an attribute out of its schema is not the
    // namespace's: the reader refuses it, and so does `writer verify`.
    let (head, published) = super::coordination::publication(&config).await;
    let (store, path) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let mut tampered = published.clone();
    tampered.attributes.remove("priority");
    object_store::ObjectStoreExt::put(
        store.as_ref(),
        &object_store::path::Path::from(format!("{path}/{}", head.content.unwrap().manifest)),
        write_contract(&tampered).into(),
    )
    .await
    .unwrap();
    let refused = fyrr_reader::search::PreparedReader::open(&loaded.config).await;
    assert!(
        matches!(refused, Err(fyrr_reader::ReaderError::IncompatibleIndex)),
        "{:?}",
        refused.map(|_| ())
    );
    let output = super::coordination::host_command(&config)
        .args(["writer", "verify"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("error: incompatible_index"), "{error}");

    // The attributes are fixed with the namespace: another file serves it with
    // its own, and creating it again with others is refused.
    let other = root.join("other-attributes.toml");
    std::fs::write(
        &other,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("priority = \"int\"", "priority = \"float\""),
    )
    .unwrap();
    let output = super::coordination::host_command(&other)
        .args(["writer", "namespace", "create"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("error: namespace_settings_fixed"), "{error}");
    std::fs::remove_dir_all(root).unwrap();
}
