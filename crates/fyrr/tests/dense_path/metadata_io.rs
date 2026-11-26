//! A connector map lives in its rows: no object beside the dataset holds it.
use super::*;
use arrow_array::Array as _;

fn fixture() -> (PathBuf, FixtureRoot) {
    let (config, root) = setup_on(Backend::Local);
    let prefix = std::fs::read_to_string(&config)
        .unwrap()
        .split("[[sources]]")
        .next()
        .unwrap()
        .to_owned();
    std::fs::write(&config, format!("{prefix}\n[[sources]]\nid='metadata'\ntype='sentinel-docs'\npermissions='fixed'\nreaders=['grp:allow']\n[sources.scope]\nfixture='{}'\n", root.join("documents.json").display())).unwrap();
    let documents: Vec<_> = (0..32).map(|n| json!({"id":format!("doc-{n}"),"title":"document","author":"Ada","readers":["grp:allow"],"note":format!("shared-note-{}",n%8),"text":format!("fixture document {}",n%8),"sections":[{"text":format!("fixture document {}",n%8),"readers":["grp:allow"],"location":{"kind":"heading","label":"body"}}]})).collect();
    std::fs::write(
        root.join("documents.json"),
        serde_json::to_vec(&json!({"documents":documents,"cursor":"one"})).unwrap(),
    )
    .unwrap();
    coordination::command(&config, &["writer", "namespace", "create"]);
    (config, root)
}

#[tokio::test]
#[ignore = "real CLI sync, commit and fetch on local storage"]
async fn connector_maps_are_stored_in_their_rows_and_fetched_with_them() {
    let (config, _root) = fixture();
    coordination::worker_sync(&config, &["metadata", "--kind", "content", "--full"]);
    coordination::command(&config, &["writer", "commit"]);
    let loaded = fyrr_core::config::load(&config).unwrap();
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded.config).unwrap();
    let listed = store
        .list_with_delimiter(Some(&namespace.clone().join("document-metadata")))
        .await
        .unwrap();
    assert!(
        listed.objects.is_empty() && listed.common_prefixes.is_empty(),
        "connector maps were written beside the dataset"
    );
    let rows = incremental::source_rows(&config, "metadata").await;
    assert_eq!(rows.num_rows(), 32);
    let roots = rows
        .column_by_name("root_id")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let maps = rows
        .column_by_name("connector_map")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::LargeBinaryArray>()
        .unwrap();
    for row in 0..rows.num_rows() {
        let n: usize = roots.value(row)["doc-".len()..].parse().unwrap();
        let map: Value = serde_json::from_slice(maps.value(row)).unwrap();
        assert_eq!(map, json!({"note": format!("shared-note-{}", n % 8)}));
    }
    let fetched = super::tools::Mcp::open(&config).call(
        "fetch",
        json!({"source_id": "metadata", "root_id": "doc-11"}),
    );
    assert_eq!(fetched["isError"], false, "{fetched}");
    assert_eq!(
        fetched["structuredContent"]["documents"][0]["connector_map"],
        json!({"note": "shared-note-3"}),
        "{fetched}"
    );
}
