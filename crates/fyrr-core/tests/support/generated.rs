//! Repository artifact generation used only by the drift tests.

use schemars::generate::{Contract as SchemaContract, SchemaGenerator, SchemaSettings};
use schemars::{JsonSchema, Schema};
use serde_json::{Value, json};

use fyrr_core::connector::{ConnectorBatch, CursorState, document_changes_arrow_schema};
use fyrr_core::directory::DirectorySnapshot;
use fyrr_core::envelope::{PreparedEnvelope, RejectionMarker};
use fyrr_core::http_api::{
    AggregateRequest, DeleteNamespaceRequest, DeleteNamespaceResponse, ErrorBody, FetchRequest,
    NamespaceList, NamespaceReport, QueryRequest, WriteRequest, WriteResponse,
};
use fyrr_core::identity::{ApiKeysArtifact, IdentityArtifact};
use fyrr_core::mcp::{
    AggregateOutput, FetchOutput, MCP_CONTRACT_VERSION, SearchOutput, ToolError, error_codes,
    tools_list,
};
use fyrr_core::provider::{ProviderInfo, SDK_SERIES};
use fyrr_core::publication::{
    ContentManifest, EnvelopeCheckpoint, HistoryCut, PolicyArtifact, ServingHead,
};
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

fn api_generator(contract: SchemaContract) -> SchemaGenerator {
    let mut settings = SchemaSettings::draft2020_12();
    settings.meta_schema = None;
    settings.definitions_path = "/components/schemas".into();
    settings.contract = contract;
    settings.into_generator()
}

fn json_body(schema: impl serde::Serialize, description: &str) -> Value {
    json!({"description": description, "content": {"application/json": {"schema": schema}}})
}

/// One operation of the HTTP API: a request body schema (if it has one) and
/// the schema of its `200` answer. Every other answer is the error response
/// `error`, a name in `components.responses`.
fn operation(
    tag: &str,
    summary: &str,
    role: &str,
    error: &str,
    request: Option<Schema>,
    answer: Schema,
) -> Value {
    let mut operation = json!({
        "tags": [tag],
        "summary": summary,
        "description": format!("Needs {role}."),
        "parameters": [{"$ref": "#/components/parameters/namespace"}],
        "responses": {
            "200": json_body(answer, "The answer."),
            "default": {"$ref": format!("#/components/responses/{error}")},
        },
    });
    if let Some(schema) = request {
        let mut body = json_body(schema, "The request body.");
        body["required"] = true.into();
        operation["requestBody"] = body;
    }
    operation
}

/// The OpenAPI 3.1 description of the HTTP API of the reader and the writer:
/// the `/v1/` routes, with their bodies from the Rust request and response
/// types.
fn openapi() -> Value {
    let mut requests = api_generator(SchemaContract::Deserialize);
    let mut answers = api_generator(SchemaContract::Serialize);
    let mut list = operation(
        "reader",
        "List the namespaces the key may act in",
        "an API key with the `admin` or `list` role",
        "Error",
        None,
        answers.subschema_for::<NamespaceList>(),
    );
    list["parameters"] = json!([{
        "name": "prefix", "in": "query", "required": false,
        "description": "Keeps the names that start with it.",
        "schema": {"type": "string", "maxLength": 63, "pattern": "^[a-z0-9-]*$"},
    }]);
    let paths = json!({
        "/v1/namespaces": {"get": list},
        "/v1/namespaces/{namespace}": {
            "get": operation(
                "reader",
                "Report a namespace's schema, settings, size and index state",
                "an API key with the `admin` or `list` role",
                "Error",
                None,
                answers.subschema_for::<NamespaceReport>(),
            ),
            "delete": operation(
                "writer",
                "Delete a namespace and its data",
                "an API key with the `admin` role",
                "ScimError",
                Some(requests.subschema_for::<DeleteNamespaceRequest>()),
                answers.subschema_for::<DeleteNamespaceResponse>(),
            ),
        },
        "/v1/namespaces/{namespace}/query": {"post": operation(
            "reader",
            "Search the namespace: the `search` tool as JSON",
            "an API key with the `read` role, or an OIDC user",
            "Error",
            Some(requests.subschema_for::<QueryRequest>()),
            answers.subschema_for::<SearchOutput>(),
        )},
        "/v1/namespaces/{namespace}/fetch": {"post": operation(
            "reader",
            "Fetch documents of the namespace: the `fetch` tool as JSON",
            "an API key with the `read` role, or an OIDC user",
            "Error",
            Some(requests.subschema_for::<FetchRequest>()),
            answers.subschema_for::<FetchOutput>(),
        )},
        "/v1/namespaces/{namespace}/aggregate": {"post": operation(
            "reader",
            "Aggregate the documents the key may read: count, sum, average, minimum and maximum, optionally per group",
            "an API key with the `read` role, or an OIDC user",
            "Error",
            Some(requests.subschema_for::<AggregateRequest>()),
            answers.subschema_for::<AggregateOutput>(),
        )},
        "/v1/namespaces/{namespace}/documents": {"post": operation(
            "writer",
            "Upsert, patch and delete documents, all or none; or change the schema",
            "an API key with the `write` role",
            "Error",
            Some(requests.subschema_for::<WriteRequest>()),
            answers.subschema_for::<WriteResponse>(),
        )},
    });
    let error = answers.subschema_for::<ErrorBody>();
    let mut schemas = requests.take_definitions(false);
    schemas.extend(answers.take_definitions(false));
    // The query route also answers what `fields` asks for. The MCP schema of a
    // hit leaves these out (`schemars(skip)`) to keep `tools/list` small.
    let hit = schemas
        .get_mut("SearchHit")
        .and_then(|hit| hit.get_mut("properties"))
        .and_then(Value::as_object_mut)
        .expect("SearchHit has properties");
    for (name, schema) in [
        (
            "attributes",
            json!({"type": "object", "description": "Typed attributes and vector asked for in `fields`."}),
        ),
        (
            "lexical_score",
            json!({"type": "number", "description": "Raw lexical score, with `fields.scores`; absent when the lexical lane did not find the hit."}),
        ),
        (
            "distance",
            json!({"type": "number", "description": "Raw vector distance, with `fields.scores`; absent when the dense lane did not find the hit."}),
        ),
    ] {
        assert!(
            hit.insert(name.into(), schema).is_none(),
            "SearchHit already describes {name}: drop it from this list"
        );
    }
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "fyrr HTTP API",
            "version": MCP_CONTRACT_VERSION.to_string(),
            "description": "The /v1/ routes of the reader (query, fetch, aggregate, report, list) and of \
                the writer (documents, delete). The version is that of the MCP contract the \
                query and fetch bodies follow.",
        },
        "tags": [{"name": "reader"}, {"name": "writer"}],
        "security": [{"bearer": []}],
        "paths": paths,
        "components": {
            "securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}},
            "parameters": {"namespace": {
                "name": "namespace", "in": "path", "required": true,
                "schema": {"type": "string", "maxLength": 63, "pattern": "^[a-z0-9][a-z0-9-]*$"},
            }},
            "responses": {
                "Error": json_body(
                    error,
                    "An error: an HTTP status and `{\"error\":\"<code>\"}`; docs/http-api.md lists the codes.",
                ),
                // The writer's admin listener answers the namespace deletion
                // with a SCIM error (RFC 7644, section 3.12), as it does its
                // SCIM routes.
                "ScimError": json_body(
                    json!({
                        "type": "object",
                        "required": ["schemas", "status", "detail"],
                        "properties": {
                            "schemas": {"const": ["urn:ietf:params:scim:api:messages:2.0:Error"]},
                            "status": {"type": "string", "description": "The HTTP status."},
                            "detail": {"type": "string"},
                            "scimType": {"type": "string"},
                        },
                    }),
                    "A SCIM error (`application/scim+json`).",
                ),
            },
            "schemas": schemas,
        },
    })
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
                settings: serde_json::from_value(serde_json::json!({
                    "embedding": {
                        "model": "BAAI/bge-m3",
                        "dimensions": 1024,
                        "fingerprint": "sha256:153ebedfabc06d487f789189fc0d33b7daa637931185368c4c1d331564ad2187",
                        "settings": {
                            "api_style": "openai-compatible",
                            "prefix": "",
                            "roles": {"document": {}, "query": {}},
                            "send_dimensions": false
                        }
                    },
                    "fts": {
                        "tokenizer": "simple",
                        "max_token_length": 40,
                        "lowercase": true,
                        "stem": true,
                        "remove_stop_words": false,
                        "ascii_folding": true,
                        "with_position": false
                    },
                    "dense_rq_bits": 1,
                    "dense_partition_rows": 550,
                    "retrieval": {
                        "dense_weight": 0.5,
                        "dense_candidates": 75,
                        "lexical_candidates": 200,
                        "source_cap": 2,
                        "max_documents": 75
                    },
                    "composition": {"fusion": 50, "dense": 0, "lexical": 0},
                    "attributes": {"created": "datetime", "priority": "int"}
                }))
                .unwrap_or_else(|error| unreachable!("fixture settings parse: {error}")),
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
            path: "schemas/history-cut.schema.json",
            contents: pretty(&contract_schema::<HistoryCut>()),
        },
        GeneratedFile {
            path: "fixtures/contracts/history-cut/valid.json",
            contents: String::from_utf8(fyrr_core::write_contract(&HistoryCut {
                schema_version: <HistoryCut as Contract>::VERSION,
                namespace: "acme".into(),
                publication_id: "pub-00000000000000000000000000000041".into(),
                sequence: 41,
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
            path: "schemas/api-keys-artifact.schema.json",
            contents: pretty(&contract_schema::<ApiKeysArtifact>()),
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
            path: "schemas/openapi.json",
            contents: pretty(&openapi()),
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
