//! Repository artifact generation used only by the drift tests.

use schemars::JsonSchema;
use schemars::generate::SchemaSettings;
use serde_json::Value;

use fyrr_core::connector::{ConnectorBatch, CursorState, document_changes_arrow_schema};
use fyrr_core::directory::DirectorySnapshot;
use fyrr_core::envelope::{PreparedEnvelope, RejectionMarker};
use fyrr_core::identity::IdentityArtifact;
use fyrr_core::mcp::{ToolError, error_codes, tools_list};
use fyrr_core::provider::{ProviderInfo, SDK_SERIES};
use fyrr_core::publication::{ContentManifest, EnvelopeCheckpoint, PolicyArtifact, ServingHead};
use fyrr_core::schemas::config_schema;
use fyrr_core::version::Contract;

/// One generated file and its path relative to the repository root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GeneratedFile {
    pub path: &'static str,
    pub contents: String,
}

fn pretty(value: &Value) -> String {
    let mut text = serde_json::to_string_pretty(value)
        .unwrap_or_else(|error| unreachable!("JSON values serialize: {error}"));
    text.push('\n');
    text
}

fn schema<T: JsonSchema>() -> Value {
    SchemaSettings::draft2020_12()
        .into_generator()
        .into_root_schema_for::<T>()
        .to_value()
}

/// A contract schema carries its version as `x-fyrr-schema-version`.
fn contract_schema<T: Contract + JsonSchema>() -> Value {
    let mut value = schema::<T>();
    if let Some(object) = value.as_object_mut() {
        object.insert("x-fyrr-contract".into(), T::NAME.into());
        object.insert(
            "x-fyrr-schema-version".into(),
            T::VERSION.to_string().into(),
        );
    }
    value
}

/// Sorts object keys recursively: Arrow metadata is a hash map, and the
/// committed copy must not depend on its iteration order.
fn sort_keys(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.sort_keys();
            object.values_mut().for_each(sort_keys);
        }
        Value::Array(items) => items.iter_mut().for_each(sort_keys),
        _ => {}
    }
}

/// The provider schema also names the SDK series this build loads.
fn provider_schema() -> Value {
    let mut value = contract_schema::<ProviderInfo>();
    if let Some(object) = value.as_object_mut() {
        object.insert("x-fyrr-sdk-series".into(), SDK_SERIES.to_string().into());
    }
    value
}

/// Published schemas the Python SDK ships inside its package (published path,
/// packaged copy), so the provider test kit validates against them outside
/// this repository.
const SDK_COPIES: [(&str, &str); 3] = [
    (
        "schemas/connector-batch.schema.json",
        "connectors/src/fyrr_connectors/schemas/connector-batch.schema.json",
    ),
    (
        "schemas/connector-document-changes.arrow.json",
        "connectors/src/fyrr_connectors/schemas/connector-document-changes.arrow.json",
    ),
    (
        "schemas/connector-provider.schema.json",
        "connectors/src/fyrr_connectors/schemas/connector-provider.schema.json",
    ),
];

/// All generated files, in a stable order.
pub(super) fn generated_files() -> Vec<GeneratedFile> {
    let mut arrow = serde_json::to_value(document_changes_arrow_schema())
        .unwrap_or_else(|error| unreachable!("Arrow schemas serialize: {error}"));
    sort_keys(&mut arrow);
    let mut files = published_files(&arrow);
    for (published, shipped) in SDK_COPIES {
        let contents = files
            .iter()
            .find(|file| file.path == published)
            .map_or_else(
                || unreachable!("{published} is generated"),
                |file| file.contents.clone(),
            );
        files.push(GeneratedFile {
            path: shipped,
            contents,
        });
    }
    files
}

fn published_files(arrow: &Value) -> Vec<GeneratedFile> {
    vec![
        GeneratedFile {
            path: "fixtures/contracts/encrypted-secret/envelope.json",
            contents: String::from_utf8(fyrr_core::write_contract(
                &fyrr_core::secret::EncryptedSecret {
                    schema_version: <fyrr_core::secret::EncryptedSecret as Contract>::VERSION,
                    algorithm: "aes-256-gcm".into(),
                    key_id: "1".repeat(64),
                    nonce: "2".repeat(24),
                    ciphertext: "3".repeat(32),
                },
            ))
            .unwrap_or_else(|_| unreachable!("JSON serialization is UTF-8")),
        },
        GeneratedFile {
            path: "schemas/encrypted-secret.schema.json",
            contents: pretty(&contract_schema::<fyrr_core::secret::EncryptedSecret>()),
        },
        GeneratedFile {
            path: "schemas/config.schema.json",
            contents: pretty(&config_schema()),
        },
        GeneratedFile {
            path: "schemas/policy-artifact.schema.json",
            contents: pretty(&contract_schema::<PolicyArtifact>()),
        },
        GeneratedFile {
            path: "fixtures/contracts/policy-artifact/valid.json",
            contents: String::from_utf8(fyrr_core::write_contract(&PolicyArtifact {
                schema_version: <PolicyArtifact as Contract>::VERSION,
                namespace: "acme".into(),
                generation: 1,
                retrieval_policy_digest: fyrr_core::limits::digest(b"fixture-policy"),
                fixed_grants: Default::default(),
                row_acl_sequence: None,
                retired_sources: Default::default(),
            }))
            .unwrap_or_else(|_| unreachable!("JSON serialization is UTF-8")),
        },
        GeneratedFile {
            path: "schemas/envelope-checkpoint.schema.json",
            contents: pretty(&contract_schema::<EnvelopeCheckpoint>()),
        },
        GeneratedFile {
            path: "fixtures/contracts/envelope-checkpoint/valid.json",
            contents: String::from_utf8(fyrr_core::write_contract(&EnvelopeCheckpoint {
                schema_version: <EnvelopeCheckpoint as Contract>::VERSION,
                namespace: "acme".into(),
                applied_envelope_ids: vec!["env-ab0dc87cb67ccdbb62a36a806c0cd6c5".into()],
                rejected_envelope_ids: vec!["env-c0ffee00c0ffee00c0ffee00c0ffee00".into()],
            }))
            .unwrap_or_else(|_| unreachable!("JSON serialization is UTF-8")),
        },
        GeneratedFile {
            path: "schemas/connector-batch.schema.json",
            contents: pretty(&contract_schema::<ConnectorBatch>()),
        },
        GeneratedFile {
            path: "schemas/connector-cursor.schema.json",
            contents: pretty(&contract_schema::<CursorState>()),
        },
        GeneratedFile {
            path: "schemas/connector-document-changes.arrow.json",
            contents: pretty(arrow),
        },
        GeneratedFile {
            path: "schemas/connector-provider.schema.json",
            contents: pretty(&provider_schema()),
        },
        GeneratedFile {
            path: "schemas/envelope-rejection.schema.json",
            contents: pretty(&contract_schema::<RejectionMarker>()),
        },
        GeneratedFile {
            path: "schemas/prepared-envelope.schema.json",
            contents: pretty(&contract_schema::<PreparedEnvelope>()),
        },
        GeneratedFile {
            path: "schemas/content-manifest.schema.json",
            contents: pretty(&contract_schema::<ContentManifest>()),
        },
        GeneratedFile {
            path: "schemas/serving-head.schema.json",
            contents: pretty(&contract_schema::<ServingHead>()),
        },
        GeneratedFile {
            path: "fixtures/contracts/directory-snapshot/snapshot.json",
            contents: String::from_utf8(fyrr_core::write_contract(&DirectorySnapshot {
                schema_version: <DirectorySnapshot as Contract>::VERSION,
                namespace: "acme".into(),
                provider: "scim".into(),
                built_at: chrono::DateTime::parse_from_rfc3339("2026-10-04T00:00:00Z")
                    .unwrap_or_else(|_| unreachable!("valid fixture timestamp"))
                    .to_utc(),
                hashing: fyrr_core::identity::HashingScheme {
                    algorithm: "hmac-sha256".into(),
                    key_id: "namespace-v1".into(),
                },
                users: vec![fyrr_core::directory::DirectoryUser {
                    key: format!("key:{}", "1".repeat(64)),
                    token: format!("tok:{}", "2".repeat(64)),
                    active: true,
                    aliases: vec![format!("key:{}", "3".repeat(64))],
                    subjects: vec![format!("key:{}", "4".repeat(64))],
                }],
                groups: vec![fyrr_core::directory::DirectoryGroup {
                    token: format!("tok:{}", "5".repeat(64)),
                    members: vec![format!("key:{}", "1".repeat(64))],
                }],
                links: Vec::new(),
                state: None,
            }))
            .unwrap_or_else(|_| unreachable!("JSON serialization is UTF-8")),
        },
        GeneratedFile {
            path: "schemas/directory-snapshot.schema.json",
            contents: pretty(&contract_schema::<DirectorySnapshot>()),
        },
        GeneratedFile {
            path: "schemas/identity-artifact.schema.json",
            contents: pretty(&contract_schema::<IdentityArtifact>()),
        },
        GeneratedFile {
            path: "schemas/mcp-tool-error.schema.json",
            contents: pretty(&schema::<ToolError>()),
        },
        GeneratedFile {
            path: "fixtures/mcp/tools-list.json",
            contents: pretty(&tools_list()),
        },
        GeneratedFile {
            path: "fixtures/mcp/error-codes.json",
            contents: pretty(&error_codes()),
        },
    ]
}
