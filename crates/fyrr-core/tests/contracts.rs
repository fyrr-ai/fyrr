//! Versioned contracts: golden fixtures round-trip in canonical
//! form and match the generated JSON Schemas; an unknown major version is
//! refused before the body is read (fail closed); a minor-version addition is
//! accepted.

mod common;

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use fyrr_core::connector::{
    ARROW_VERSION_KEY, ConnectorBatch, check_document_changes_schema, document_changes_arrow_schema,
};
use fyrr_core::envelope::PreparedEnvelope;
use fyrr_core::identity::{ApiKeysArtifact, IdentityArtifact};
use fyrr_core::provider::ProviderInfo;
use fyrr_core::publication::{ContentManifest, EnvelopeCheckpoint, PolicyArtifact};
use fyrr_core::{Contract, ContractError, SchemaVersion, parse_contract, write_contract};
use serde_json::{Value, json};

const FIXTURES: &str = "fixtures/contracts";

#[test]
fn publication_lineage_refuses_gaps_and_wrong_endpoints() {
    let mut value = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    value["lineage"] = json!({"holder":"holder-test","epoch":7,"base_version":118,"recovery_version":120,
        "transactions":[{"version":120,"transaction_id":"tx-restore"},{"version":121,"transaction_id":"tx-index"}]});
    assert!(parse::<ContentManifest>(&value).is_ok());
    for (field, changed) in [
        ("epoch", json!(0)),
        ("recovery_version", json!(119)),
        ("base_version", json!(121)),
    ] {
        let mut invalid = value.clone();
        invalid["lineage"][field] = changed;
        assert!(
            parse::<ContentManifest>(&invalid).is_err(),
            "invalid lineage {field}"
        );
    }
    let mut gap = value.clone();
    gap["lineage"]["recovery_version"] = Value::Null;
    assert!(parse::<ContentManifest>(&gap).is_err());
    let mut endpoint = value;
    endpoint["lance_version"] = json!(122);
    assert!(parse::<ContentManifest>(&endpoint).is_err());
    // Evidence names each envelope once.
    for field in ["applied_since", "rejected_since"] {
        let mut duplicated = common::read_json("fixtures/contracts/content-manifest/manifest.json");
        let ids = duplicated["envelope_evidence"][field]
            .as_array_mut()
            .unwrap();
        ids.push(ids[0].clone());
        assert!(parse::<ContentManifest>(&duplicated).is_err(), "{field}");
    }
    for field in ["applied_envelope_ids", "rejected_envelope_ids"] {
        let mut duplicated = common::read_json("fixtures/contracts/envelope-checkpoint/valid.json");
        let ids = duplicated[field].as_array_mut().unwrap();
        ids.push(ids[0].clone());
        assert!(parse::<EnvelopeCheckpoint>(&duplicated).is_err(), "{field}");
    }
    // A manifest of another major is refused before its body is read.
    let mut other = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    other["schema_version"] = json!("1.0");
    assert!(matches!(
        parse::<ContentManifest>(&other),
        Err(ContractError::UnsupportedMajor { .. })
    ));
}

#[test]
fn a_manifest_and_a_policy_hold_only_valid_attribute_schemas() {
    let mut manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    manifest["attributes"] = json!({"created": "datetime", "priority": "int", "done": "bool"});
    assert!(parse::<ContentManifest>(&manifest).is_ok());
    let mut policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    for (name, kind) in [
        ("_hidden", "int"),
        ("source_id", "int"),
        ("Bad", "int"),
        ("a", "decimal"),
    ] {
        let mut invalid = manifest.clone();
        invalid["attributes"] = json!({ name: kind });
        assert!(parse::<ContentManifest>(&invalid).is_err(), "{name} {kind}");
        policy["settings"]["attributes"] = json!({ name: kind });
        assert!(parse::<PolicyArtifact>(&policy).is_err(), "{name} {kind}");
    }
}

/// Index options are read at the one version of the manifest and of the policy.
#[test]
fn attribute_index_options_are_read_at_the_one_version() {
    let options = json!({"title": {"index": false, "full_text": true,
        "analyzer": {"tokenizer": "unicode", "language": "german", "ascii_folding": true}}});
    let mut manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    manifest["attributes"] = json!({"title": "string", "count": "bytes"});
    assert!(parse::<ContentManifest>(&manifest).is_ok());
    manifest["attribute_indexes"] = options.clone();
    assert!(parse::<ContentManifest>(&manifest).is_ok());
    let mut policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    policy["settings"]["attributes"] = json!({"title": "string", "count": "bytes"});
    assert!(parse::<PolicyArtifact>(&policy).is_ok());
    policy["settings"]["attribute_indexes"] = options;
    assert!(parse::<PolicyArtifact>(&policy).is_ok());
    // An option is of a declared attribute, differs from the default and is one
    // its type can have: a `bytes` has no index and a number no text index.
    for (name, option) in [
        ("undeclared", json!({"index": false})),
        ("title", json!({})),
        ("title", json!({"index": true, "full_text": false})),
        ("title", json!({"fulltext": true})),
        ("count", json!({"index": false})),
        ("count", json!({"ngram": true})),
        // The analyzer is the full-text index's, known by its names and bounds.
        (
            "title",
            json!({"ngram": true, "analyzer": {"ascii_folding": true}}),
        ),
        (
            "title",
            json!({"full_text": true, "analyzer": {"tokenizer": "jieba"}}),
        ),
        (
            "title",
            json!({"full_text": true, "analyzer": {"language": "klingon"}}),
        ),
        (
            "title",
            json!({"full_text": true, "analyzer": {"ngram_max_length": 5}}),
        ),
        (
            "title",
            json!({"full_text": true, "analyzer": {"max_token_length": 0}}),
        ),
        (
            "title",
            json!({"full_text": true, "analyzer": {"stemming": true}}),
        ),
    ] {
        policy["settings"]["attribute_indexes"] = json!({ name: option });
        assert!(parse::<PolicyArtifact>(&policy).is_err(), "{name} {option}");
        manifest["attribute_indexes"] = json!({ name: option });
        assert!(
            parse::<ContentManifest>(&manifest).is_err(),
            "{name} {option}"
        );
    }
}

/// Named vector attributes are read at the one version of the manifest and of
/// the policy. A namespace that declares none writes no entry.
#[test]
fn vector_attributes_are_read_at_the_one_version() {
    let manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    let policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    let vectors =
        json!({"image": {"dimensions": 512}, "title_vec": {"dimensions": 1024, "from": "title"}});
    let mut manifest = manifest;
    manifest["attributes"] = json!({"title": "string"});
    manifest["vectors"] = vectors.clone();
    let kept = parse::<ContentManifest>(&manifest).unwrap();
    assert_eq!(serde_json::to_value(&kept).unwrap()["vectors"], vectors);
    // A source that is no `string` attribute, a name an attribute has and a
    // dimension the model does not have are refused whatever else is set.
    for bad in [
        json!({"v": {"dimensions": 1024, "from": "missing"}}),
        json!({"title": {"dimensions": 8}}),
        json!({"v": {"dimensions": 512, "from": "title"}}),
        json!({"v": {"dimensions": 0}}),
    ] {
        let mut manifest = manifest.clone();
        manifest["vectors"] = bad.clone();
        assert!(parse::<ContentManifest>(&manifest).is_err(), "{bad}");
    }
    // A multi-vector attribute is kept, and only the cosine metric offers it.
    let mut multi = manifest.clone();
    multi["vectors"] = json!({"tokens": {"dimensions": 16, "multi": true}});
    let kept = parse::<ContentManifest>(&multi).unwrap();
    assert_eq!(
        serde_json::to_value(&kept).unwrap()["vectors"],
        multi["vectors"]
    );
    multi["dense_metric"] = json!("dot");
    assert!(parse::<ContentManifest>(&multi).is_err());
    let mut policy = policy;
    policy["settings"]["write_api"] = json!({});
    policy["settings"]["attributes"] = json!({"title": "string"});
    policy["settings"]["vectors"] = vectors;
    assert!(parse::<PolicyArtifact>(&policy).is_ok());
    // Without any, there is no entry.
    let plain = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    let written = serde_json::to_value(parse::<ContentManifest>(&plain).unwrap()).unwrap();
    assert!(written.get("vectors").is_none());
}

/// Token positions in the full-text index are read at the one version of the
/// manifest and of the policy.
#[test]
fn token_positions_are_read_at_the_one_version() {
    let mut manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    manifest["fts"]["with_position"] = json!(true);
    assert!(parse::<ContentManifest>(&manifest).is_ok());
    let mut policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    policy["settings"]["fts"]["with_position"] = json!(true);
    assert!(parse::<PolicyArtifact>(&policy).is_ok());
}

/// A metric other than cosine or a 32-bit element type is read at the one
/// version of the manifest and of the policy. The defaults are no entry.
#[test]
fn a_vector_metric_and_element_type_are_read_at_the_one_version() {
    let manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    let policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    for (key, value) in [
        ("dense_metric", "squared_l2"),
        ("dense_metric", "dot"),
        ("dense_element", "f32"),
    ] {
        let mut manifest = manifest.clone();
        manifest[key] = json!(value);
        let kept = parse::<ContentManifest>(&manifest).unwrap();
        assert_eq!(serde_json::to_value(&kept).unwrap()[key], json!(value));
        let mut policy = policy.clone();
        policy["settings"][key] = json!(value);
        assert!(parse::<PolicyArtifact>(&policy).is_ok(), "{key} {value}");
    }
    // Only the names the pinned index supports are spelled.
    for (key, value) in [("dense_metric", "hamming"), ("dense_element", "f64")] {
        let mut manifest = manifest.clone();
        manifest[key] = json!(value);
        assert!(
            parse::<ContentManifest>(&manifest).is_err(),
            "{key} {value}"
        );
    }
    // The default is no entry at all.
    let kept = parse::<ContentManifest>(&manifest).unwrap();
    let written = serde_json::to_value(&kept).unwrap();
    assert!(written.get("dense_metric").is_none() && written.get("dense_element").is_none());
}

/// A connector namespace has its typed columns; one without them is refused.
#[test]
fn a_connector_namespace_needs_its_columns() {
    let mut policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    // A namespace written through the API declares what it likes.
    assert!(parse::<PolicyArtifact>(&policy).is_ok());
    policy["settings"]
        .as_object_mut()
        .unwrap()
        .remove("write_api");
    policy["settings"]["attributes"] = json!({"title": "string", "team": "string"});
    assert!(parse::<PolicyArtifact>(&policy).is_err(), "no columns");
    policy["settings"]["attributes"] = json!({"title": "string", "team": "string",
        "status": "string", "labels": {"array": "string"}, "created_at": "datetime"});
    assert!(parse::<PolicyArtifact>(&policy).is_ok());
}

/// A frozen policy is read and written at the one version of the policy.
#[test]
fn a_frozen_policy_is_read_at_the_one_version() {
    let mut policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    policy["read_only"] = json!(true);
    let frozen = parse::<PolicyArtifact>(&policy).unwrap();
    assert!(frozen.read_only);
    assert_eq!(frozen.schema_version, <PolicyArtifact as Contract>::VERSION);
    assert!(parse::<PolicyArtifact>(&serde_json::to_value(&frozen).unwrap()).is_ok());
}

/// The majors that earlier builds wrote for plain documents are refused, as
/// every other major but the one of the contract.
#[test]
fn the_earlier_majors_of_the_manifest_the_policy_and_the_envelope_are_refused() {
    let manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    let error = parse::<ContentManifest>(&with_version(&manifest, "4.1")).unwrap_err();
    assert!(
        matches!(error, ContractError::UnsupportedMajor { .. }),
        "{error}"
    );
    let policy = common::read_json("fixtures/contracts/policy-artifact/valid.json");
    let error = parse::<PolicyArtifact>(&with_version(&policy, "3.1")).unwrap_err();
    assert!(
        matches!(error, ContractError::UnsupportedMajor { .. }),
        "{error}"
    );
    let envelope = common::read_json("fixtures/contracts/prepared-envelope/content.json");
    let error = parse::<PreparedEnvelope>(&with_version(&envelope, "5.0")).unwrap_err();
    assert!(
        matches!(error, ContractError::UnsupportedMajor { .. }),
        "{error}"
    );
}

#[test]
fn api_keys_artifact_holds_only_distinct_hashed_keys_with_roles() {
    let base = common::read_json("fixtures/contracts/api-keys-artifact/artifact.json");
    assert!(parse::<ApiKeysArtifact>(&base).is_ok());
    for (path, value) in [
        ("/keys/0/subject", json!("usr:ada")),
        ("/keys/0/roles", json!([])),
        ("/keys/0/roles", json!(["root"])),
        ("/keys/0/namespace_prefix", json!("Team_")),
    ] {
        let mut invalid = base.clone();
        *invalid.pointer_mut(path).expect("fixture path") = value;
        assert!(parse::<ApiKeysArtifact>(&invalid).is_err(), "{path}");
    }
    let mut twice = base.clone();
    twice["keys"][1]["subject"] = base["keys"][0]["subject"].clone();
    assert!(parse::<ApiKeysArtifact>(&twice).is_err());
}

fn fixture_files(name: &str) -> Vec<String> {
    let files = common::json_files(&format!("{FIXTURES}/{name}"));
    assert!(!files.is_empty(), "no golden fixtures for {name}");
    files
}

fn parse<T: Contract>(value: &Value) -> Result<T, ContractError> {
    parse_contract(&serde_json::to_vec(value).expect("serialize"))
}

fn round_trip<T: Contract + PartialEq + std::fmt::Debug>() -> usize {
    let name = T::NAME;
    let files = fixture_files(name);
    for file in &files {
        let text = common::read(file);
        let document: T = parse_contract(text.as_bytes())
            .unwrap_or_else(|error| panic!("{file}: golden fixture refused: {error}"));
        assert_eq!(
            serde_json::to_value(&document).unwrap()["schema_version"],
            serde_json::json!(T::VERSION),
            "{file}"
        );
        let written = write_contract(&document);
        let original: Value = serde_json::from_str(&text).expect("fixture JSON");
        let rewritten: Value = serde_json::from_slice(&written).expect("written JSON");
        assert_eq!(
            rewritten, original,
            "{file}: a field was dropped, renamed or defaulted"
        );
        assert_eq!(
            String::from_utf8(written).expect("UTF-8"),
            text,
            "{file}: not in canonical form"
        );
        common::assert_valid(&format!("schemas/{name}.schema.json"), &original, file);
    }
    files.len()
}

fn with_version(value: &Value, version: &str) -> Value {
    let mut value = value.clone();
    value["schema_version"] = json!(version);
    value
}

/// Adds an unknown field to every struct-shaped object, as a newer minor
/// version would; opaque maps (connector maps, fragments, provider config
/// schemas) keep their content.
fn add_future_fields(value: &mut Value) {
    if let Value::Object(object) = value {
        for (key, inner) in object.iter_mut() {
            // Maps keyed by data (sources, connector keys) take no new fields.
            if ![
                "connector_map",
                "lance_fragment",
                "config_schema",
                "fixed_grants",
                "attributes",
            ]
            .contains(&key.as_str())
            {
                add_future_fields(inner);
            }
        }
        object.insert("added_in_a_minor".into(), json!({ "nested": [1, 2, 3] }));
    } else if let Value::Array(items) = value {
        items.iter_mut().for_each(add_future_fields);
    }
}

/// A version whose major this build does not read.
fn unread_major<T: Contract>() -> String {
    format!("{}.0", T::VERSION.major + 1)
}

fn unknown_major_rejected<T: Contract + std::fmt::Debug>() -> usize {
    let name = T::NAME;
    let files = fixture_files(name);
    for file in &files {
        let fixture = common::read_json(file);
        let next_major = unread_major::<T>();
        for version in [next_major.as_str(), "0.9", "65535.0"] {
            let error = parse::<T>(&with_version(&fixture, version)).expect_err(file);
            let found: SchemaVersion = version.parse().expect("version");
            assert_eq!(
                error,
                ContractError::UnsupportedMajor {
                    contract: T::NAME,
                    found,
                    supported: T::VERSION.major,
                },
                "{file} at {version}"
            );
        }
    }
    // The major is checked before the body: an unreadable body with an
    // unknown major still reports the version, never a body error.
    let error =
        parse::<T>(&json!({ "schema_version": unread_major::<T>(), "body": 17 })).expect_err(name);
    assert!(
        matches!(error, ContractError::UnsupportedMajor { .. }),
        "{name}: {error}"
    );
    let error = parse::<T>(&json!({ "body": 17 })).expect_err(name);
    assert_eq!(error, ContractError::MissingVersion { contract: T::NAME });
    for malformed in [json!("1"), json!("1.0.0"), json!(1.0), json!(null)] {
        let error = parse::<T>(&json!({ "schema_version": malformed })).expect_err(name);
        assert_eq!(error, ContractError::MalformedVersion { contract: T::NAME });
    }
    files.len()
}

fn minor_addition_accepted<T: Contract + PartialEq + std::fmt::Debug>() -> usize {
    let files = fixture_files(T::NAME);
    for file in &files {
        let fixture = common::read_json(file);
        let current: T = parse(&fixture).expect(file);
        let mut newer = with_version(&fixture, &format!("{}.7", T::VERSION.major));
        add_future_fields(&mut newer);
        let parsed: T =
            parse(&newer).unwrap_or_else(|error| panic!("{file}: minor addition refused: {error}"));
        assert_eq!(
            serde_json::to_value(&parsed).unwrap()["schema_version"],
            serde_json::json!(SchemaVersion::new(T::VERSION.major, 7))
        );
        let mut read = serde_json::to_value(&parsed).expect("serialize");
        read["schema_version"] = json!(T::VERSION.to_string());
        assert_eq!(
            read,
            serde_json::to_value(&current).expect("serialize"),
            "{file}: a newer minor changed the data this build reads"
        );
    }
    files.len()
}

#[test]
fn golden_fixtures_round_trip_and_match_schemas() {
    let checked: usize = common::for_each_contract!(round_trip).iter().sum();
    assert!(checked >= 12, "only {checked} golden fixtures");
}

#[test]
fn unknown_major_versions_are_refused_fail_closed() {
    assert!(
        common::for_each_contract!(unknown_major_rejected)
            .iter()
            .all(|&n| n > 0)
    );
}

#[test]
fn minor_version_additions_are_accepted() {
    assert!(
        common::for_each_contract!(minor_addition_accepted)
            .iter()
            .all(|&n| n > 0)
    );
}

#[test]
fn committed_compat_fixtures_behave() {
    let unknown = common::read("fixtures/contracts/compat/connector-batch-unknown-major.json");
    let error = parse_contract::<ConnectorBatch>(unknown.as_bytes()).expect_err("unknown major");
    assert!(
        matches!(error, ContractError::UnsupportedMajor { .. }),
        "{error}"
    );

    let minor = common::read("fixtures/contracts/compat/connector-batch-minor-addition.json");
    let parsed = parse_contract::<ConnectorBatch>(minor.as_bytes()).expect("minor addition");
    let base: ConnectorBatch = parse_contract(
        common::read("fixtures/contracts/connector-batch/document-changes.json").as_bytes(),
    )
    .expect("base fixture");
    assert_eq!(parsed.body, base.body);
    assert_eq!(parsed.schema_version, SchemaVersion::new(1, 1));
}

#[test]
fn arrow_document_changes_schema_is_checked_before_rows() {
    let schema = document_changes_arrow_schema();
    assert_eq!(
        check_document_changes_schema(&schema),
        Ok(fyrr_core::connector::CONNECTOR_CONTRACT_VERSION)
    );

    let with_metadata = |schema: &Schema, version: &str| {
        let mut metadata = schema.metadata().clone();
        metadata.insert(ARROW_VERSION_KEY.into(), version.into());
        Schema::new(schema.fields().clone()).with_metadata(metadata)
    };
    for version in ["2.0", "0.1"] {
        let error =
            check_document_changes_schema(&with_metadata(&schema, version)).expect_err(version);
        assert!(
            matches!(error, ContractError::UnsupportedMajor { .. }),
            "{error}"
        );
    }

    // A newer minor adds a field: accepted, the extra column is ignored.
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new("attachment_of", DataType::Utf8, true)));
    let newer = with_metadata(
        &Schema::new(fields).with_metadata(schema.metadata().clone()),
        "1.3",
    );
    assert_eq!(
        check_document_changes_schema(&newer),
        Ok(SchemaVersion::new(1, 3))
    );

    // Removed, retyped or weakened fields are refused.
    let modify = |name: &str, change: &dyn Fn(&Field) -> Option<Field>| {
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() == name {
                    change(field)
                } else {
                    Some(field.as_ref().clone())
                }
            })
            .collect();
        Schema::new(fields).with_metadata(schema.metadata().clone())
    };
    let removed = modify("root_id", &|_| None);
    let retyped = modify("revision_order", &|field| {
        Some(field.clone().with_data_type(DataType::Utf8))
    });
    let nullable = modify("op", &|field| Some(field.clone().with_nullable(true)));
    for (case, broken) in [
        ("removed", removed),
        ("retyped", retyped),
        ("nullable", nullable),
    ] {
        let error = check_document_changes_schema(&broken).expect_err(case);
        assert!(
            matches!(error, ContractError::Invalid { .. }),
            "{case}: {error}"
        );
    }
    let unnamed = Schema::new(schema.fields().clone());
    assert!(check_document_changes_schema(&unnamed).is_err());
}

/// At any nesting: a newer minor may add nullable fields inside the
/// `sections` struct, like the JSON form accepts; non-nullable additions and
/// weakened list items are refused.
#[test]
fn arrow_nested_additions_follow_the_minor_rule() {
    let schema = document_changes_arrow_schema();
    let with_section_field = |extra: Field| {
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() != "sections" {
                    return field.as_ref().clone();
                }
                let DataType::List(item) = field.data_type() else {
                    panic!("sections is a list")
                };
                let DataType::Struct(children) = item.data_type() else {
                    panic!("section is a struct")
                };
                let mut children: Vec<Field> = children
                    .iter()
                    .map(|child| child.as_ref().clone())
                    .collect();
                children.push(extra.clone());
                let item = item
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(children.into()));
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::List(Arc::new(item)))
            })
            .collect();
        let mut metadata = schema.metadata().clone();
        metadata.insert(ARROW_VERSION_KEY.into(), "1.2".into());
        Schema::new(fields).with_metadata(metadata)
    };
    let nullable = with_section_field(Field::new("page_number", DataType::UInt32, true));
    assert_eq!(
        check_document_changes_schema(&nullable),
        Ok(SchemaVersion::new(1, 2))
    );
    let required = with_section_field(Field::new("page_number", DataType::UInt32, false));
    let error = check_document_changes_schema(&required).expect_err("non-nullable nested addition");
    assert!(error.to_string().contains("sections[][6]"), "{error}");

    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new("required_later", DataType::Utf8, false));
    let top = Schema::new(fields).with_metadata(schema.metadata().clone());
    assert!(
        check_document_changes_schema(&top).is_err(),
        "non-nullable top-level addition"
    );

    let weakened: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "readers" {
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Utf8,
                        true,
                    ))))
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    let weakened = Schema::new(weakened).with_metadata(schema.metadata().clone());
    let error = check_document_changes_schema(&weakened).expect_err("nullable list items");
    assert!(error.to_string().contains("readers[]"), "{error}");

    // List item names do not matter (pyarrow may call them "item" or "element").
    let renamed: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "labels" {
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Utf8,
                        false,
                    ))))
            } else {
                field.as_ref().clone()
            }
        })
        .collect();
    let renamed = Schema::new(renamed).with_metadata(schema.metadata().clone());
    assert!(check_document_changes_schema(&renamed).is_ok());
}

/// A change applied to a fixture document.
type Mutation = Box<dyn Fn(&mut Value)>;

/// Semantic validation refuses bad documents without echoing their values.
#[test]
fn semantic_violations_are_refused_without_echoing_values() {
    let base = common::read_json("fixtures/contracts/connector-batch/document-changes.json");
    let sentinel = "SENTINEL-c0ffee";
    let cases: Vec<(&str, Mutation)> = vec![
        (
            "upsert without revision",
            Box::new(|v| {
                v["body"]["changes"][0]
                    .as_object_mut()
                    .expect("object")
                    .remove("source_revision");
            }),
        ),
        (
            "reader without prefix",
            Box::new(move |v| {
                v["body"]["changes"][0]["readers"] = json!([sentinel]);
            }),
        ),
        (
            "attribute without equals sign",
            Box::new(move |v| {
                v["body"]["changes"][0]["attributes"] = json!([sentinel]);
            }),
        ),
        (
            "duplicate document",
            Box::new(|v| {
                let first = v["body"]["changes"][0].clone();
                v["body"]["changes"]
                    .as_array_mut()
                    .expect("array")
                    .push(first);
            }),
        ),
        (
            "kind does not match body",
            Box::new(|v| v["kind"] = json!("members")),
        ),
        (
            "too many labels",
            Box::new(|v| {
                v["body"]["changes"][0]["metadata"]["labels"] = json!(vec!["x"; 33]);
            }),
        ),
        (
            "oversized connector map",
            Box::new(move |v| {
                v["body"]["changes"][0]["metadata"]["connector_map"] =
                    json!({ "blob": sentinel.repeat(2000) });
            }),
        ),
        (
            "delete with sections",
            Box::new(|v| {
                v["body"]["changes"][1]["sections"] = json!([{ "text": "x" }]);
            }),
        ),
    ];
    for (case, mutate) in cases {
        let mut document = base.clone();
        mutate(&mut document);
        let error = parse::<ConnectorBatch>(&document).expect_err(case);
        assert!(
            matches!(error, ContractError::Invalid { .. }),
            "{case}: {error}"
        );
        assert!(
            !error.to_string().contains(sentinel),
            "{case}: error echoes a value: {error}"
        );
    }

    let mut listing = common::read_json("fixtures/contracts/connector-batch/listing.json");
    listing["body"]["last"] = json!(false);
    assert!(
        parse::<ConnectorBatch>(&listing).is_err(),
        "complete listing on a non-final part"
    );

    // A current publication/envelope always records settings, including the
    // default BGE-M3 model, and checks its fingerprint against those settings.
    for (file, pointer) in [
        (
            "fixtures/contracts/content-manifest/manifest.json",
            "/embedding",
        ),
        (
            "fixtures/contracts/prepared-envelope/content.json",
            "/payload/embedding",
        ),
    ] {
        let fixture = common::read_json(file);
        for mutation in ["missing", "prefix", "fingerprint"] {
            let mut invalid = fixture.clone();
            let embedding = invalid.pointer_mut(pointer).unwrap();
            match mutation {
                "missing" => {
                    embedding.as_object_mut().unwrap().remove("settings");
                }
                "prefix" => embedding["settings"]["prefix"] = json!(sentinel),
                _ => embedding["fingerprint"] = json!(format!("sha256:{}", "0".repeat(64))),
            }
            let error = if pointer == "/embedding" {
                parse::<ContentManifest>(&invalid).unwrap_err()
            } else {
                parse::<PreparedEnvelope>(&invalid).unwrap_err()
            };
            assert!(!error.to_string().contains(sentinel), "{error}");
        }
        let mut old = fixture;
        old["schema_version"] = json!(if pointer == "/embedding" {
            "2.2"
        } else {
            "4.0"
        });
        let error = if pointer == "/embedding" {
            parse::<ContentManifest>(&old).unwrap_err()
        } else {
            parse::<PreparedEnvelope>(&old).unwrap_err()
        };
        assert!(matches!(error, ContractError::UnsupportedMajor { .. }));
    }

    let mut envelope = common::read_json("fixtures/contracts/prepared-envelope/content.json");
    envelope["sequence"] = json!(43);
    assert!(
        parse::<PreparedEnvelope>(&envelope).is_err(),
        "envelope ID must follow its derivation"
    );

    // A condition and a write by filter are read at the one version.
    let mut conditional = common::read_json("fixtures/contracts/prepared-envelope/content.json");
    conditional["payload"]["documents"][0]["condition"] =
        json!({"field": "price", "op": "lt", "value": 1});
    assert!(parse::<PreparedEnvelope>(&conditional).is_ok());

    let mut by_filter = common::read_json("fixtures/contracts/prepared-envelope/content.json");
    by_filter["payload"]["filter_writes"] = json!({
        "source_revision": "rev-1",
        "revision_order": 1,
        "delete": {"field": "price", "op": "lt", "value": 1},
    });
    assert!(parse::<PreparedEnvelope>(&by_filter).is_ok());

    let base_artifact = common::read_json("fixtures/contracts/identity-artifact/artifact.json");
    let mut artifact = base_artifact.clone();
    artifact["principals"][0]["tokens"] = json!([0, 99]);
    assert!(
        parse::<IdentityArtifact>(&artifact).is_err(),
        "token index out of range"
    );
    // Only hashed keys and tokens: raw identities are refused without echo.
    for (field, raw) in [
        ("/tokens/0", "usr:ada.novak"),
        ("/tokens/1", "grp:security"),
        ("/tokens/2", "tok:0123abcd"),
        ("/principals/0/key", "ada.novak@acme.example"),
        ("/principals/1/key", "usr:bo"),
    ] {
        let mut artifact = base_artifact.clone();
        *artifact.pointer_mut(field).expect("fixture field") = json!(raw);
        let error = parse::<IdentityArtifact>(&artifact).expect_err(raw);
        assert!(
            matches!(error, ContractError::Invalid { .. }),
            "{field}: {error}"
        );
        assert!(
            !error.to_string().contains(raw),
            "{field}: error echoes the raw value: {error}"
        );
    }

    let mut manifest = common::read_json("fixtures/contracts/content-manifest/manifest.json");
    manifest["indexes"]
        .as_array_mut()
        .expect("array")
        .retain(|index| index["column"] != "readers");
    assert!(
        parse::<ContentManifest>(&manifest).is_err(),
        "publication without a readers index"
    );

    // Type errors report a position and category only.
    let mut typed = base.clone();
    typed["body"]["changes"][0]["root_id"] = json!({ "nested": sentinel });
    let error = parse::<ConnectorBatch>(&typed).expect_err("type error");
    assert!(matches!(error, ContractError::Malformed { .. }), "{error}");
    assert!(!error.to_string().contains(sentinel), "{error}");
}

#[test]
fn unknown_arrow_field_names_are_not_diagnostics() {
    use arrow_schema::{DataType, Field, Schema};
    use fyrr_core::connector::{check_document_changes_schema, document_changes_arrow_schema};
    let original = document_changes_arrow_schema();
    let mut fields: Vec<_> = original.fields().iter().cloned().collect();
    fields.push(std::sync::Arc::new(Field::new(
        "PRIVATE-extra-field",
        DataType::Utf8,
        false,
    )));
    let schema = Schema::new_with_metadata(fields, original.metadata().clone());
    let error = check_document_changes_schema(&schema)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!("[{}]", original.fields().len())),
        "{error}"
    );
    assert!(error.contains("must be nullable"), "{error}");
    assert!(!error.contains("PRIVATE-extra-field"), "{error}");
}

#[test]
fn identity_groups_and_subject_bindings_fail_closed() {
    let mut value = common::read_json("fixtures/contracts/identity-artifact/artifact.json");
    let artifact = parse::<IdentityArtifact>(&value).unwrap();
    value["principals"][0]["groups"] = json!([0]);
    value["subjects"] = json!([{"subject":format!("key:{}", "a".repeat(64)),
        "principal":artifact.principals[0].key}]);
    assert!(parse::<IdentityArtifact>(&value).is_ok());
    for groups in [json!([999]), json!([0, 0])] {
        let mut invalid = value.clone();
        invalid["principals"][0]["groups"] = groups;
        assert!(parse::<IdentityArtifact>(&invalid).is_err());
    }
    let mut invalid = value.clone();
    invalid["subjects"][0]["principal"] = json!(format!("key:{}", "f".repeat(64)));
    assert!(parse::<IdentityArtifact>(&invalid).is_err());
    let duplicate = value["subjects"][0].clone();
    value["subjects"].as_array_mut().unwrap().push(duplicate);
    assert!(parse::<IdentityArtifact>(&value).is_err());
}

#[test]
fn source_email_without_evidence_is_unverified_and_written_without_it() {
    use fyrr_core::connector::{EmailVerification, SourceUser};
    let unverified = json!({"id":"source-user", "email":"user@example.test", "active":true});
    let user: SourceUser = serde_json::from_value(unverified.clone()).unwrap();
    assert_eq!(user.email_verification, EmailVerification::Unverified);
    assert_eq!(serde_json::to_value(user).unwrap(), unverified);
    for evidence in ["verified_corporate", "verified_personal"] {
        let mut value = unverified.clone();
        value["email_verification"] = json!(evidence);
        let parsed: SourceUser = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);
    }
    let mut invalid = unverified;
    invalid["email_verification"] = json!("assumed");
    assert!(serde_json::from_value::<SourceUser>(invalid).is_err());
}

#[test]
fn provider_version_cases() {
    use fyrr_core::provider::{SDK_SERIES, SdkIncompatibility, SdkSeries, SdkVersion, check_sdk};
    let version = |text: &str| text.parse::<SdkVersion>().unwrap();
    for (text, installed, expected) in [
        ("1.20.3", false, Some("1.20.3")),
        ("1.0", false, None),
        ("1.0.0.0", false, None),
        ("01.0.0", false, None),
        ("1.a.0", false, None),
        ("", false, None),
        ("1..0", false, None),
        ("v1.0.0", false, None),
        ("1.2", true, Some("1.2.0")),
        ("1.2.3", true, Some("1.2.3")),
        ("1.2.3rc1", true, Some("1.2.3")),
        ("1.2.3.post1", true, Some("1.2.3")),
        ("2.0.0.dev3", true, Some("2.0.0")),
        ("dev", true, None),
        ("1", true, None),
    ] {
        let actual = if installed {
            SdkVersion::parse_installed(text)
        } else {
            text.parse()
        };
        assert_eq!(
            actual.ok().map(|v| v.to_string()).as_deref(),
            expected,
            "{text}, installed={installed}"
        );
    }
    for (text, series) in [
        ("0.1.0", SdkSeries::Initial { minor: 1 }),
        ("0.1.7", SdkSeries::Initial { minor: 1 }),
        ("0.2.0", SdkSeries::Initial { minor: 2 }),
        ("0.0.3", SdkSeries::Initial { minor: 0 }),
        ("1.0.0", SdkSeries::Stable { major: 1 }),
        ("1.4.2", SdkSeries::Stable { major: 1 }),
        ("2.0.0", SdkSeries::Stable { major: 2 }),
    ] {
        assert_eq!(version(text).series(), series, "{text}");
    }
    assert_eq!(SdkSeries::Initial { minor: 1 }.to_string(), "0.1");
    assert_eq!(SdkSeries::Stable { major: 3 }.to_string(), "3");
    assert_eq!(SDK_SERIES, SdkSeries::Initial { minor: 1 });
    let other = |minimum: &str| {
        Err(SdkIncompatibility::OtherSeries {
            required: version(minimum).series(),
            minimum: version(minimum),
        })
    };
    for (minimum, installed, expected) in [
        ("0.1.0", "0.1.0", Ok(())),
        ("0.1.0", "0.1.3", Ok(())),
        ("0.1.2", "0.1.3", Ok(())),
        ("0.2.0", "0.1.3", other("0.2.0")),
        ("0.0.9", "0.1.3", other("0.0.9")),
        ("1.0.0", "0.1.3", other("1.0.0")),
        (
            "0.1.4",
            "0.1.3",
            Err(SdkIncompatibility::TooOld {
                minimum: version("0.1.4"),
                installed: version("0.1.3"),
            }),
        ),
    ] {
        assert_eq!(
            check_sdk(version(minimum), version(installed)),
            expected,
            "{minimum}, {installed}"
        );
    }
    // Provider metadata of another major is refused.
    let mut other = common::read_json("fixtures/contracts/connector-provider/provider.json");
    other["schema_version"] = json!("1.0");
    assert!(matches!(
        parse::<ProviderInfo>(&other),
        Err(ContractError::UnsupportedMajor { .. })
    ));
}
