//! MCP tool contracts: the static `tools/list` stays within 16 KiB,
//! carries the required annotations, wording and effective limits, and the
//! example requests and responses round-trip through the Rust types and match
//! the published tool schemas.

mod common;

use fyrr_core::mcp::{
    ErrorCode, FetchInput, FetchOutput, ListFiltersInput, ListFiltersOutput, MCP_CONTRACT_VERSION,
    META_SCHEMA_VERSION_KEY, SearchInput, SearchOutput, TOOLS_LIST_MAX_BYTES, ToolError,
    tools_list,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

fn tool(name: &str) -> Value {
    tools_list()["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == name)
        .unwrap_or_else(|| panic!("tool {name} missing"))
        .clone()
}

#[test]
fn truncation_is_optional_additive_and_has_bounded_numeric_counts() {
    assert_eq!(MCP_CONTRACT_VERSION.to_string(), "4.0");
    let schema = tool("search")["outputSchema"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let legacy = common::read_json("fixtures/mcp/examples/search-output.json");
    let roundtrip: SearchOutput = serde_json::from_value(legacy.clone()).unwrap();
    assert!(roundtrip.truncated.is_none());
    assert_eq!(serde_json::to_value(roundtrip).unwrap(), legacy);
    let mut value = legacy;
    value["truncated"] = json!({"text_dropped":4294967295u64,"hits_dropped":0});
    assert!(validator.is_valid(&value));
    for invalid in [
        json!(-1),
        json!(4294967296u64),
        json!("private"),
        json!(1.5),
    ] {
        value["truncated"]["text_dropped"] = invalid;
        assert!(!validator.is_valid(&value));
        assert!(serde_json::from_value::<SearchOutput>(value.clone()).is_err());
    }
}

#[test]
fn routing_telemetry_schema_accepts_only_bounded_outcomes() {
    let schema = tool("search")["outputSchema"].clone();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let mut value = common::read_json("fixtures/mcp/examples/search-output.json");
    for strategy in ["exact", "rabitq", "mixed"] {
        for reason in [
            "absent",
            "build_failed",
            "memory_budget",
            "eligible_shortlist",
        ] {
            value["metadata"]["telemetry"]["routing_strategy"] = json!(strategy);
            value["metadata"]["telemetry"]["routing_fallback"] = json!(reason);
            assert!(validator.is_valid(&value));
        }
    }
    value["metadata"]["telemetry"]["routing_strategy"] = json!("arbitrary");
    assert!(!validator.is_valid(&value));
    value["metadata"]["telemetry"]["routing_strategy"] = json!("exact");
    value["metadata"]["telemetry"]["routing_fallback"] = json!("storage_unavailable");
    assert!(!validator.is_valid(&value));
}

#[test]
fn tools_list_fits_the_size_budget() {
    for (name, list) in [("search", "hits"), ("fetch", "documents")] {
        assert!(
            tool(name)["outputSchema"]["properties"][list]["items"]["properties"]["title"]
                .is_object()
        );
    }
    let compact = serde_json::to_vec(&tools_list()).expect("serialize");
    assert!(
        compact.len() <= TOOLS_LIST_MAX_BYTES,
        "tools/list is {} bytes, over the {TOOLS_LIST_MAX_BYTES}-byte budget",
        compact.len()
    );
    // The committed golden is the same document (drift is checked in tests/generated.rs).
    let golden = common::read_json("fixtures/mcp/tools-list.json");
    assert_eq!(
        serde_json::to_vec(&golden).expect("serialize").len(),
        compact.len()
    );
}

#[test]
fn tools_have_names_annotations_and_version() {
    let names: Vec<String> = tools_list()["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| tool["name"].as_str().expect("name").to_owned())
        .collect();
    assert_eq!(names, ["search", "fetch", "list_filters"]);
    for name in &names {
        let tool = tool(name);
        let annotations = &tool["annotations"];
        assert_eq!(annotations["readOnlyHint"], json!(true), "{name}");
        assert_eq!(annotations["idempotentHint"], json!(true), "{name}");
        assert_eq!(annotations["destructiveHint"], json!(false), "{name}");
        assert_eq!(annotations["openWorldHint"], json!(false), "{name}");
        assert!(annotations["title"].is_string(), "{name}");
        assert_eq!(
            tool["_meta"][META_SCHEMA_VERSION_KEY],
            json!(MCP_CONTRACT_VERSION.to_string())
        );
        assert_eq!(tool["inputSchema"]["type"], json!("object"), "{name}");
        assert_eq!(
            tool["inputSchema"]["additionalProperties"],
            json!(false),
            "{name}"
        );
        assert_eq!(tool["outputSchema"]["type"], json!("object"), "{name}");
    }
}

#[test]
fn schemas_state_the_effective_limits() {
    let search = tool("search")["inputSchema"].clone();
    let properties = &search["properties"];
    assert_eq!(properties["query"]["maxLength"], json!(4096));
    assert_eq!(properties["query"]["x-maxUtf8Bytes"], json!(4096));
    assert_eq!(properties["limit"]["minimum"], json!(1));
    assert_eq!(properties["limit"]["maximum"], json!(75));
    assert_eq!(properties["exclude"]["maxItems"], json!(100));
    assert_eq!(properties["deadline_ms"]["maximum"], json!(300_000));
    for filter in ["sources", "source_types", "projects", "authors"] {
        assert_eq!(properties[filter]["maxItems"], json!(64), "{filter}");
    }
    assert_eq!(properties["attributes"]["maxItems"], json!(16));
    assert!(
        search
            .get("required")
            .is_none_or(|required| required == &json!([]))
    );

    let fetch = tool("fetch")["inputSchema"].clone();
    assert_eq!(fetch["properties"]["cursor"]["maxLength"], json!(16384));
    assert_eq!(fetch["properties"]["uri"]["maxLength"], json!(4096));
}

#[test]
fn inputs_reject_unknown_fields() {
    let parse = |value: Value| serde_json::from_value::<SearchInput>(value);
    assert!(parse(json!({ "query": "x" })).is_ok());
    assert!(parse(json!({ "query": "x", "top_k": 5 })).is_err());
    assert!(serde_json::from_value::<FetchInput>(json!({ "url": "x" })).is_err());
    assert!(serde_json::from_value::<ListFiltersInput>(json!({ "limit": 1 })).is_err());
}

#[test]
fn error_codes_match_the_spec_list() {
    let codes: Vec<&str> = ErrorCode::ALL.iter().map(|code| code.as_str()).collect();
    assert_eq!(
        codes,
        [
            "invalid_argument",
            "authorization_unavailable",
            "storage_unavailable",
            "incompatible_index",
            "deadline_exceeded",
            "cancelled",
            "stale_cursor",
            "busy",
            "rate_limited",
        ]
    );
    let retryable: Vec<&str> = ErrorCode::ALL
        .iter()
        .filter(|code| code.retryable())
        .map(|code| code.as_str())
        .collect();
    assert_eq!(
        retryable,
        [
            "authorization_unavailable",
            "storage_unavailable",
            "busy",
            "rate_limited"
        ]
    );
    for code in ErrorCode::ALL {
        assert_eq!(
            serde_json::to_value(code).expect("serialize"),
            json!(code.as_str())
        );
    }
    let golden = common::read_json("fixtures/mcp/error-codes.json");
    assert_eq!(
        golden.as_array().expect("array").len(),
        ErrorCode::ALL.len()
    );
}

fn round_trip<T: Serialize + DeserializeOwned>(file: &str, schema: &Value) {
    let path = format!("fixtures/mcp/examples/{file}");
    let original = common::read_json(&path);
    let typed: T =
        serde_json::from_value(original.clone()).unwrap_or_else(|error| panic!("{path}: {error}"));
    assert_eq!(
        serde_json::to_value(&typed).expect("serialize"),
        original,
        "{path}"
    );
    let validator = jsonschema::validator_for(schema).expect("tool schema compiles");
    let errors: Vec<String> = validator
        .iter_errors(&original)
        .map(|error| error.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "{path} does not match its tool schema: {errors:?}"
    );
}

#[test]
fn examples_round_trip_and_match_tool_schemas() {
    let (search, fetch, list) = (tool("search"), tool("fetch"), tool("list_filters"));
    round_trip::<SearchInput>("search-input.json", &search["inputSchema"]);
    round_trip::<SearchInput>("search-input-filter-only.json", &search["inputSchema"]);
    round_trip::<SearchOutput>("search-output.json", &search["outputSchema"]);
    round_trip::<FetchInput>("fetch-input-window.json", &fetch["inputSchema"]);
    round_trip::<FetchOutput>("fetch-output.json", &fetch["outputSchema"]);
    round_trip::<ListFiltersInput>("list-filters-input.json", &list["inputSchema"]);
    round_trip::<ListFiltersOutput>("list-filters-output.json", &list["outputSchema"]);
    round_trip::<ToolError>(
        "tool-error.json",
        &common::read_json("schemas/mcp-tool-error.schema.json"),
    );

    let mut over_limit = common::read_json("fixtures/mcp/examples/search-input.json");
    over_limit["limit"] = json!(76);
    let validator = jsonschema::validator_for(&search["inputSchema"]).expect("compiles");
    assert!(
        !validator.is_valid(&over_limit),
        "limit 76 must be outside the schema"
    );
}

#[test]
fn validation_enforces_bytes_relations_and_bounds() {
    let search = |value: Value| {
        serde_json::from_value::<SearchInput>(value)
            .unwrap()
            .validate()
    };
    assert!(search(json!({"query":"é".repeat(2048)})).is_ok());
    assert!(search(json!({"query":"é".repeat(2049)})).is_err());
    assert!(search(json!({"order":"updated_at"})).is_ok());
    for value in [
        json!({}),
        json!({"query":" "}),
        json!({"query":"a","limit":76}),
        json!({"query":"a","authors":[]}),
        json!({"query":"a","authors":["é".repeat(257)]}),
        json!({"query":"a","updated_after":"2026-01-02T00:00:00Z","updated_before":"2026-01-01T00:00:00Z"}),
        json!({"query":"a","attributes":[{"key":"bad=key","values":["x"]}]}),
        json!({"query":"a","deadline_ms":0}),
    ] {
        assert!(search(value).is_err());
    }
    let fetch = |value: Value| {
        serde_json::from_value::<FetchInput>(value)
            .unwrap()
            .validate()
    };
    assert!(fetch(json!({"source_id":"s","root_id":"r","window":{"record_id":"r:0","revision":"t","start":1,"end":2}})).is_ok());
    for value in [
        json!({"source_id":"s"}),
        json!({"uri":"x","cursor":"x"}),
        json!({"cursor":"x","window":{"record_id":"r:0","revision":"t","start":1,"end":2}}),
        json!({"source_id":"s","root_id":"r","window":{"record_id":"r:0","revision":"t","start":2,"end":1}}),
    ] {
        assert!(fetch(value).is_err());
    }
}

#[test]
fn agent_tool_parameter_validation_and_limits() {
    let valid_search = [
        json!({"queries":["alpha","beta"],"text_hits":4,"excerpt_chars":800}),
        json!({"queries":vec!["x";16],"limit":20,"text_hits":0}),
        json!({"query":"x","limit":75,"text_hits":75,"excerpt_chars":1}),
    ];
    for value in valid_search {
        let input: SearchInput = serde_json::from_value(value).unwrap();
        assert!(input.validate().is_ok());
    }
    for value in [
        json!({"queries":["x"]}),
        json!({"queries":vec!["x";17]}),
        json!({"query":"x","queries":["a","b"]}),
        json!({"queries":["a","b"],"limit":21}),
        json!({"queries":["a","b"],"order":"updated_at"}),
        json!({"queries":["a",""]}),
        json!({"queries":["a","é".repeat(2049)]}),
        json!({"query":"x","text_hits":51}),
        json!({"query":"x","text_hits":0,"output":"metadata"}),
        json!({"query":"x","excerpt_chars":0}),
        json!({"query":"x","excerpt_chars":1201}),
        json!({"query":"x","excerpt_chars":1,"output":"content"}),
    ] {
        let input: SearchInput = serde_json::from_value(value).unwrap();
        assert!(input.validate().is_err());
    }
    let documents = json!([{"source_id":"a","root_id":"1","window":{"record_id":"r","revision":"v","start":0,"end":1}},{"source_id":"a","root_id":"2"}]);
    assert!(
        serde_json::from_value::<FetchInput>(json!({"documents":documents}))
            .unwrap()
            .validate()
            .is_ok()
    );
    for value in [
        json!({"documents":[]}),
        json!({"documents":vec![json!({"source_id":"a","root_id":"1"});6]}),
        json!({"documents":[{"source_id":"a","root_id":"1"},{"source_id":"a","root_id":"1"}]}),
        json!({"documents":documents,"uri":"x"}),
        json!({"documents":documents,"cursor":"x"}),
        json!({"documents":documents,"source_id":"a","root_id":"1"}),
        json!({"documents":[{"source_id":"a","root_id":"1","window":{"record_id":"r","revision":"v","start":0,"end":1,"context":1}}]}),
    ] {
        assert!(
            serde_json::from_value::<FetchInput>(value)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let properties = tool("search")["inputSchema"]["properties"].clone();
    assert_eq!(properties["queries"]["minItems"], 2);
    assert_eq!(properties["queries"]["maxItems"], 16);
    assert_eq!(properties["queries"]["items"]["x-maxUtf8Bytes"], 4096);
    assert_eq!(properties["excerpt_chars"]["maximum"], 1200);
    assert_eq!(
        tool("fetch")["inputSchema"]["properties"]["documents"]["maxItems"],
        5
    );
}
