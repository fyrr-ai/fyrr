//! Drift check: committed JSON Schemas and MCP golden files equal what the Rust
//! types generate. Regenerate with
//! `FYRR_UPDATE_GOLDEN=1 cargo test -p fyrr-core --test integration generated::` and review
//! the diff.

#[path = "support/generated.rs"]
mod artifacts;
use crate::common;

use std::collections::BTreeSet;

use artifacts::{GeneratedFile, generated_files};
use fyrr_core::connector::{ConnectorBatch, CursorState};
use fyrr_core::envelope::PreparedEnvelope;
use fyrr_core::publication::{ContentManifest, ServingHead};

fn contract_fixture<T: fyrr_core::Contract>(path: &'static str, bytes: &[u8]) -> GeneratedFile {
    let mut value: serde_json::Value =
        serde_json::from_slice(bytes).expect("contract fixture JSON");
    value["schema_version"] = T::VERSION.to_string().into();
    // These fixtures describe the default model with no text transformation.
    // Construct its complete persisted identity with the current contract.
    let embedding = if T::NAME == "content-manifest" {
        value["settings"].get_mut("embedding")
    } else {
        value["payload"].get_mut("embedding")
    };
    if let Some(embedding) = embedding {
        use fyrr_core::envelope::{EmbeddingApiStyle, EmbeddingRoles, EmbeddingSettings};
        let settings = EmbeddingSettings {
            api_style: EmbeddingApiStyle::OpenaiCompatible,
            prefix: String::new(),
            roles: EmbeddingRoles::default(),
            send_dimensions: false,
        };
        embedding["fingerprint"] = settings.fingerprint("BAAI/bge-m3", 1024).into();
        embedding["settings"] = serde_json::to_value(settings).unwrap();
    }
    if T::NAME == "prepared-envelope" && value["payload"]["type"] == "permissions" {
        use sha2::Digest;
        for field in ["documents", "container_grants"] {
            for item in value["payload"][field].as_array_mut().unwrap() {
                for token in item["readers"].as_array_mut().unwrap() {
                    let raw = token.as_str().unwrap();
                    if !(raw.starts_with("tok:") && raw.len() == 68) {
                        // Synthetic fixture tokens; production uses the namespace HMAC.
                        *token = format!(
                            "tok:{}",
                            sha2::Sha256::digest(raw.as_bytes())
                                .iter()
                                .map(|b| format!("{b:02x}"))
                                .collect::<String>()
                        )
                        .into();
                    }
                }
            }
        }
    }
    let typed: T = fyrr_core::parse_contract(&serde_json::to_vec(&value).expect("fixture JSON"))
        .expect("contract fixture");
    GeneratedFile {
        path: path.into(),
        contents: String::from_utf8(fyrr_core::write_contract(&typed)).expect("fixture UTF-8"),
    }
}

fn contract_fixtures() -> Vec<GeneratedFile> {
    macro_rules! fixture {
        ($type:ty,$path:literal) => {
            contract_fixture::<$type>($path, include_bytes!(concat!("../../../", $path)))
        };
    }
    vec![
        fixture!(
            fyrr_core::identity::IdentityArtifact,
            "fixtures/contracts/identity-artifact/artifact.json"
        ),
        fixture!(
            fyrr_core::identity::ApiKeysArtifact,
            "fixtures/contracts/api-keys-artifact/artifact.json"
        ),
        fixture!(
            ConnectorBatch,
            "fixtures/contracts/connector-batch/document-changes.json"
        ),
        fixture!(
            ConnectorBatch,
            "fixtures/contracts/connector-batch/permission-changes.json"
        ),
        fixture!(
            ConnectorBatch,
            "fixtures/contracts/connector-batch/principal-snapshot.json"
        ),
        fixture!(
            ConnectorBatch,
            "fixtures/contracts/connector-batch/listing.json"
        ),
        fixture!(
            CursorState,
            "fixtures/contracts/connector-cursor/cursor.json"
        ),
        fixture!(
            PreparedEnvelope,
            "fixtures/contracts/prepared-envelope/content.json"
        ),
        fixture!(
            PreparedEnvelope,
            "fixtures/contracts/prepared-envelope/permissions.json"
        ),
        fixture!(
            PreparedEnvelope,
            "fixtures/contracts/prepared-envelope/members.json"
        ),
        fixture!(
            PreparedEnvelope,
            "fixtures/contracts/prepared-envelope/reconcile.json"
        ),
        fixture!(
            ContentManifest,
            "fixtures/contracts/content-manifest/manifest.json"
        ),
        fixture!(ServingHead, "fixtures/contracts/serving-head/head.json"),
    ]
}

fn mcp_examples() -> Vec<GeneratedFile> {
    use fyrr_core::mcp::{FetchOutput, ListFiltersOutput, MCP_CONTRACT_VERSION, SearchOutput};
    macro_rules! example {
        ($type:ty, $path:literal) => {{
            let mut value: serde_json::Value =
                serde_json::from_slice(include_bytes!(concat!("../../../", $path)))
                    .expect("MCP example");
            value["metadata"]["schema_version"] = MCP_CONTRACT_VERSION.to_string().into();
            let typed: $type = serde_json::from_value(value).expect("MCP example contract");
            GeneratedFile {
                path: $path.into(),
                contents: format!(
                    "{}\n",
                    serde_json::to_string_pretty(&typed).expect("MCP example JSON")
                ),
            }
        }};
    }
    vec![
        example!(SearchOutput, "fixtures/mcp/examples/search-output.json"),
        example!(FetchOutput, "fixtures/mcp/examples/fetch-output.json"),
        example!(
            ListFiltersOutput,
            "fixtures/mcp/examples/list-filters-output.json"
        ),
    ]
}

#[test]
fn generated_files_match_committed_copies() {
    let root = common::repo_root();
    let update = std::env::var_os("FYRR_UPDATE_GOLDEN").is_some();
    let mut drifted = Vec::new();
    for file in generated_files()
        .into_iter()
        .chain(contract_fixtures())
        .chain(mcp_examples())
    {
        let path = root.join(&file.path);
        if update {
            std::fs::create_dir_all(path.parent().expect("parent")).expect("create directory");
            std::fs::write(&path, &file.contents).expect("write generated file");
        } else if std::fs::read_to_string(&path).ok().as_deref() != Some(file.contents.as_str()) {
            drifted.push(file.path);
        }
    }
    assert!(
        drifted.is_empty(),
        "generated files differ from the committed copies: {drifted:?}\n\
         regenerate with FYRR_UPDATE_GOLDEN=1 cargo test -p fyrr-core --test integration generated:: and review the diff"
    );
}

#[test]
fn schemas_directory_holds_only_generated_files() {
    let generated: BTreeSet<String> = generated_files()
        .into_iter()
        .filter(|file| file.path.starts_with("schemas/"))
        .map(|file| file.path.to_owned())
        .collect();
    let committed: BTreeSet<String> = common::json_files("schemas").into_iter().collect();
    assert_eq!(committed, generated, "schemas/ has files no generator owns");
}
