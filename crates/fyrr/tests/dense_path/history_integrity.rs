//! Publication history corruption must terminate at the real storage boundary.
use super::*;
use futures::{StreamExt, TryStreamExt};

#[tokio::test]
#[ignore = "local storage publication and corrupt immutable history; scripts/ci"]
async fn history_cycles_are_refused_on_storage() {
    let (config, root, _) = super::tools::fixture();
    let loaded = fyrr_core::config::load(&config).unwrap().config;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded).unwrap();
    let (head, template) = super::coordination::publication(&config).await;
    let valid = fyrr_reader::storage::manifest_chain(
        store.clone(),
        namespace.clone(),
        Some(head.content.as_ref().unwrap().manifest.clone()),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert!(
        !valid.is_empty(),
        "the actual publication has readable history"
    );
    for sequences in [vec![2u64], vec![3, 2]] {
        for (index, sequence) in sequences.iter().enumerate() {
            let mut manifest = template.clone();
            manifest.publication_id = format!("pub-{:032x}", 777700 + sequence);
            manifest.sequence = *sequence;
            manifest.previous_publication_id = Some(format!(
                "pub-{:032x}",
                777700 + sequences[(index + 1) % sequences.len()]
            ));
            store
                .put(
                    &object_store::path::Path::from(format!(
                        "{namespace}/publications/{}.json",
                        manifest.publication_id
                    )),
                    write_contract(&manifest).into(),
                )
                .await
                .unwrap();
        }
        // Read at most one cycle plus its first repeated node. The baseline
        // exposes repeated manifests without hanging the regression itself.
        let observed = fyrr_reader::storage::manifest_chain(
            store.clone(),
            namespace.clone(),
            Some(format!(
                "publications/pub-{:032x}.json",
                777700 + sequences[0]
            )),
        )
        .take(sequences.len() + 1)
        .collect::<Vec<_>>()
        .await;
        assert_eq!(observed.len(), sequences.len() + 1);
        assert!(observed[..sequences.len()].iter().all(Result::is_ok));
        assert!(
            matches!(
                observed.last(),
                Some(Err(fyrr_reader::storage::StorageError::PublicationInvalid))
            ),
            "a repeated history node must fail closed, not yield another manifest"
        );
        let mut corrupt_head = head.clone();
        let pointer = corrupt_head.content.as_mut().unwrap();
        pointer.publication_id = format!("pub-{:032x}", 777700 + sequences[0]);
        pointer.sequence = sequences[0];
        pointer.manifest = format!("publications/{}.json", pointer.publication_id);
        let head_path = namespace.clone().join("head.json");
        store
            .put(&head_path, write_contract(&corrupt_head).into())
            .await
            .unwrap();
        let receipt = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fyrr_writer::commit::receipt(&loaded, "env-ffffffffffffffffffffffffffffffff"),
        )
        .await
        .expect("a corrupt receipt history must terminate");
        assert_eq!(
            receipt.unwrap_err(),
            fyrr_writer::WriterError::PublicationInvalid
        );
        let after: ServingHead = fyrr_reader::storage::read_contract(store.as_ref(), &head_path)
            .await
            .unwrap();
        assert_eq!(
            after, corrupt_head,
            "receipt lookup must not change the head"
        );
        store
            .put(&head_path, write_contract(&head).into())
            .await
            .unwrap();
    }
    assert_eq!(
        super::coordination::publication(&config).await.0,
        head,
        "read-only history validation must not change the serving head"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
#[ignore = "local storage history and recovery boundary; scripts/ci"]
async fn history_checks_order_before_recovery_on_storage() {
    // A restore runs only on a write API namespace.
    for command in ["maintain", "restore", "rollback"] {
        // Sequence 0 stands for an ancestor that is not there at all.
        for ancestor_sequence in [0, 2, 3] {
            history_order_case(command, ancestor_sequence).await;
        }
    }
}

async fn history_order_case(command: &str, ancestor_sequence: u64) {
    let (config, root) = if command == "maintain" {
        let (config, root, _) = super::tools::fixture();
        (config, root)
    } else {
        let (config, root) = setup_on(Backend::Local);
        let address = free_address();
        super::write_api::configure(&config, &address, "");
        super::write_api::embed(&root, &["alpha"]);
        let writer = super::write_api::Writer::start(&config, &root, address).await;
        let body = json!({"upserts": [{"id": "a", "text": "alpha", "readers": ["grp:allow"]}]});
        let key = super::write_api::WRITE_KEY;
        assert_eq!(writer.post(key, "dense-fixture", &body).await.0, 200);
        (config, root)
    };
    let loaded = fyrr_core::config::load(&config).unwrap().config;
    let (store, namespace) = fyrr_reader::storage::namespace_store(&loaded).unwrap();
    let (head, template) = super::coordination::publication(&config).await;
    let mut native = fyrr_reader::storage::open_version(
        &fyrr_reader::storage::dataset_uri(&loaded),
        loaded
            .storage
            .object_store_options(&fyrr_core::config::ProcessEnvironment)
            .unwrap(),
        template.lance_version,
        None,
    )
    .await
    .unwrap();
    native.delete("source_id IS NOT NULL").await.unwrap();
    let unpublished = native.latest_version_id().await.unwrap();
    assert!(unpublished > template.lance_version);
    let mut ancestor = template.clone();
    ancestor.publication_id = "pub-00000000000000000000000000881100".into();
    ancestor.sequence = ancestor_sequence;
    ancestor.previous_publication_id = None;
    let mut tip = template.clone();
    tip.publication_id = "pub-00000000000000000000000000881101".into();
    tip.sequence = 2;
    tip.previous_publication_id = Some(ancestor.publication_id.clone());
    for manifest in [&ancestor, &tip] {
        if ancestor_sequence == 0 && manifest.publication_id == ancestor.publication_id {
            continue;
        }
        store
            .put(
                &object_store::path::Path::from(format!(
                    "{namespace}/publications/{}.json",
                    manifest.publication_id
                )),
                write_contract(manifest).into(),
            )
            .await
            .unwrap();
    }
    let mut corrupt_head = head.clone();
    let pointer = corrupt_head.content.as_mut().unwrap();
    pointer.publication_id = tip.publication_id.clone();
    pointer.manifest = format!("publications/{}.json", tip.publication_id);
    pointer.sequence = tip.sequence;
    store
        .put(
            &namespace.clone().join("head.json"),
            write_contract(&corrupt_head).into(),
        )
        .await
        .unwrap();
    let args = if command == "restore" {
        vec!["writer", command, ancestor.publication_id.as_str()]
    } else {
        vec!["writer", command]
    };
    let output = super::coordination::host_command(&config)
        .args(args)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "{command} must reject corrupt history"
    );
    assert!(
        stderr.contains("publication_invalid"),
        "{command}: {stderr}"
    );
    let after = super::coordination::publication(&config).await.0;
    assert_eq!(
        native.latest_version_id().await.unwrap(),
        unpublished,
        "{command} must reject non-descending history before native recovery",
    );
    assert_eq!(after.content, corrupt_head.content);
    assert_eq!(after.policy, corrupt_head.policy);
    assert_eq!(after.identity, corrupt_head.identity);
    store
        .put(
            &namespace.clone().join("head.json"),
            write_contract(&head).into(),
        )
        .await
        .unwrap();
    super::coordination::command(&config, &["writer", "commit"]);
    assert!(super::coordination::publication(&config).await.1.sequence > template.sequence);
    std::fs::remove_dir_all(root).unwrap();
}
