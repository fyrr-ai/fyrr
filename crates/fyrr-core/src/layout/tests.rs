use super::*;
#[test]
fn namespace_paths_preserve_names_and_native_classification() {
    assert_eq!(
        NamespaceLayout::new("s3://bucket///", "n").dataset_uri(),
        "s3://bucket/namespaces/n/dataset.lance"
    );
    for prefix in [
        "",
        "/",
        "x",
        "other/",
        "/dataset.lance/",
        "/dataset.lance/dataset.lance/",
    ] {
        for suffix in [
            "data/x",
            "_indices/x",
            "_deletions/x",
            "_versions/x",
            "_transactions/x",
            "head.json",
            "data",
            "datax/x",
            "/data/x",
        ] {
            let path = format!("{prefix}dataset.lance/{suffix}");
            let legacy = [
                "/dataset.lance/data/",
                "/dataset.lance/_indices/",
                "/dataset.lance/_deletions/",
                "/dataset.lance/_versions/",
                "/dataset.lance/_transactions/",
            ]
            .iter()
            .any(|part| path.contains(part));
            assert_eq!(
                NamespaceLayout::immutable_native_path(&path),
                legacy,
                "{path}"
            );
        }
    }
}
