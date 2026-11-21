//! Installed connector providers: validation of what the Python
//! worker discovered, the `fyrr writer providers` listing, provider checks in
//! `fyrr config check` and the effective configuration schema.
//!
//! Every provider is validated here, in Rust: its metadata as the
//! `connector-provider` contract, its config schemas as compilable JSON
//! Schemas, and its SDK requirement against the installed SDK, whose series
//! must be the one this build loads. A provider that fails any check is
//! refused fail-closed, with a reason that names fields, never values.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::LazyLock;

use fyrr_core::config::{Config, Issue, PROVIDER, PROVIDER_PATTERN, PermissionsMode, SourceConfig};
use fyrr_core::connector::SyncKind;
use fyrr_core::parse_contract;
use fyrr_core::provider::{
    ProviderInfo, SDK_DISTRIBUTION, SDK_SERIES, SECRET_MARK, SdkVersion, check_sdk,
};
use jsonschema::Validator;
use jsonschema::paths::LocationSegment;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Distribution names shown in listings.
static DISPLAYABLE: LazyLock<regex::Regex> =
    LazyLock::new(|| compiled(r"^[A-Za-z0-9][A-Za-z0-9._+!-]{0,99}$"));
/// A version of a Python distribution that fyrr will show: a release of one to
/// four dot-separated integers, then at most three of the PEP 440 pre, post and
/// dev suffixes (`a`, `b`, `rc`, `post`, `dev`, each with digits). Integers
/// are at most six digits long, so a version is at most 57 characters. Epochs
/// and local segments are not shown.
static VERSION: LazyLock<regex::Regex> = LazyLock::new(|| {
    compiled(r"^[0-9]{1,6}(\.[0-9]{1,6}){0,3}(\.?(a|b|rc|post|dev)[0-9]{1,6}){0,3}$")
});

/// Shown where a name or version is absent.
pub(crate) const NO_VALUE: &str = "-";
/// Shown in place of a name or version that is not what it should look like.
pub(crate) const INVALID_VALUE: &str = "<invalid>";

/// A version string the Python side supplied, as the text fyrr may show:
/// the string itself when it is made of verified numeric parts and a closed
/// set of suffixes (see [`VERSION`]), else [`INVALID_VALUE`]. Every version of
/// the peer enters through here.
pub(crate) fn version_text(version: &str) -> String {
    if VERSION.is_match(version) {
        version.to_owned()
    } else {
        INVALID_VALUE.to_owned()
    }
}

fn compiled(pattern: &str) -> regex::Regex {
    regex::Regex::new(pattern).unwrap_or_else(|error| unreachable!("valid pattern: {error}"))
}

#[derive(Debug, Deserialize)]
struct RawDiscovery {
    sdk_version: String,
    providers: Vec<RawEntry>,
}

#[derive(Debug, Deserialize)]
struct RawEntry {
    entry_point: String,
    distribution: Option<String>,
    version: Option<String>,
    #[serde(default)]
    info: Option<Value>,
    #[serde(default)]
    error: Option<Value>,
}

/// What the worker found, validated.
#[derive(Debug)]
pub struct Discovery {
    /// The installed SDK version as the distribution reports it.
    pub sdk_version: String,
    pub providers: Vec<Provider>,
}

/// One installed provider entry point.
#[derive(Debug)]
pub struct Provider {
    /// The entry point name; shown only if it is a valid provider name.
    pub name: String,
    pub distribution: Option<String>,
    pub version: Option<String>,
    /// The metadata when it parsed as the contract, even if refused later.
    pub info: Option<ProviderInfo>,
    pub status: Result<Schemas, String>,
}

/// The compiled config schemas of an accepted provider.
#[derive(Debug)]
pub struct Schemas {
    settings: Validator,
    scope: Validator,
}

impl Provider {
    pub fn display_name(&self) -> &str {
        if PROVIDER.is_match(&self.name) {
            &self.name
        } else {
            "<invalid name>"
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.status.is_ok()
    }
}

fn displayable(value: Option<&str>) -> &str {
    match value {
        Some(value) if DISPLAYABLE.is_match(value) => value,
        Some(_) => INVALID_VALUE,
        None => NO_VALUE,
    }
}

/// Validates the worker's discovery output (`fyrr_connectors.runtime.discover`).
pub fn parse_discovery(json: &[u8]) -> Result<Discovery, String> {
    let raw: RawDiscovery = serde_json::from_slice(json)
        .map_err(|_| "the Python worker's provider list is unreadable".to_owned())?;
    let installed = match SdkVersion::parse_installed(&raw.sdk_version) {
        Ok(version) if version.series() == SDK_SERIES => Ok(version),
        Ok(version) => Err(format!(
            "the installed {SDK_DISTRIBUTION} {version} is SDK series {}; this fyrr build loads SDK series {SDK_SERIES}",
            version.series()
        )),
        Err(_) => Err(format!(
            "the installed {SDK_DISTRIBUTION} version is unreadable"
        )),
    };
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for entry in &raw.providers {
        *counts.entry(entry.entry_point.as_str()).or_default() += 1;
    }
    let duplicated: Vec<String> = counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(name, _)| name.to_owned())
        .collect();
    let providers = raw
        .providers
        .into_iter()
        .map(|entry| {
            let duplicate = duplicated.contains(&entry.entry_point);
            validate(entry, &installed, duplicate)
        })
        .collect();
    Ok(Discovery {
        sdk_version: version_text(&raw.sdk_version),
        providers,
    })
}

fn validate(entry: RawEntry, installed: &Result<SdkVersion, String>, duplicate: bool) -> Provider {
    let mut provider = Provider {
        name: entry.entry_point,
        distribution: entry.distribution,
        version: entry.version.as_deref().map(version_text),
        info: None,
        status: Err(String::new()),
    };
    provider.status = (|| {
        if !PROVIDER.is_match(&provider.name) {
            return Err(format!(
                "the entry point name does not match {PROVIDER_PATTERN}"
            ));
        }
        if duplicate {
            return Err("more than one installed distribution registers this name".into());
        }
        if entry.error.is_some() {
            return Err("its metadata callable failed".into());
        }
        let info = entry.info.ok_or("its metadata callable returned nothing")?;
        let bytes = serde_json::to_vec(&info).map_err(|_| "its metadata is not JSON")?;
        let info: ProviderInfo =
            parse_contract(&bytes).map_err(|error| format!("invalid metadata: {error}"))?;
        provider.info = Some(info.clone());
        if info.name != provider.name {
            return Err("the metadata name differs from the entry point name".into());
        }
        let installed = installed.clone()?;
        let minimum: SdkVersion = info
            .min_sdk_version
            .parse()
            .map_err(|_| "min_sdk_version is malformed")?;
        check_sdk(minimum, installed).map_err(|error| error.to_string())?;
        Ok(Schemas {
            settings: compile("settings", &info.config_schema.settings)?,
            scope: compile("scope", &info.config_schema.scope)?,
        })
    })();
    provider
}

fn compile(part: &str, schema: &Map<String, Value>) -> Result<Validator, String> {
    jsonschema::draft202012::new(&Value::Object(schema.clone()))
        .map_err(|_| format!("config_schema.{part} is not a valid draft 2020-12 JSON Schema"))
}

impl Discovery {
    pub fn find(&self, name: &str) -> Option<&Provider> {
        self.providers.iter().find(|provider| provider.name == name)
    }

    pub fn refused(&self) -> usize {
        self.providers
            .iter()
            .filter(|provider| !provider.is_compatible())
            .count()
    }

    /// The `fyrr writer providers` table.
    pub fn render(&self) -> String {
        let mut rows = vec![[
            "PROVIDER".to_owned(),
            "DISTRIBUTION".to_owned(),
            "VERSION".to_owned(),
            "REQUIRES SDK".to_owned(),
            "CONNECTIONS".to_owned(),
            "SYNC KINDS".to_owned(),
            "STATUS".to_owned(),
        ]];
        for provider in &self.providers {
            let info = provider.info.as_ref();
            rows.push([
                provider.display_name().to_owned(),
                displayable(provider.distribution.as_deref()).to_owned(),
                provider
                    .version
                    .clone()
                    .unwrap_or_else(|| NO_VALUE.to_owned()),
                info.map_or_else(
                    || NO_VALUE.to_owned(),
                    |info| format!(">={}", info.min_sdk_version),
                ),
                info.map_or_else(
                    || NO_VALUE.to_owned(),
                    |info| {
                        join(
                            info.connection_types
                                .iter()
                                .map(|mode| permissions_str(*mode)),
                        )
                    },
                ),
                info.map_or_else(
                    || NO_VALUE.to_owned(),
                    |info| join(info.sync_kinds.iter().map(|kind| kind.as_str())),
                ),
                match &provider.status {
                    Ok(_) => "ok".to_owned(),
                    Err(reason) => format!("refused: {reason}"),
                },
            ]);
        }
        let widths: Vec<usize> = (0..6)
            .map(|column| rows.iter().map(|row| row[column].len()).max().unwrap_or(0))
            .collect();
        let mut out = format!(
            "{SDK_DISTRIBUTION} SDK {} (this fyrr build loads SDK series {SDK_SERIES}); {} provider(s), {} refused\n",
            self.sdk_version,
            self.providers.len(),
            self.refused()
        );
        for row in rows {
            for (column, width) in widths.iter().enumerate() {
                let _ = write!(out, "{:<width$}  ", row[column]);
            }
            out.push_str(&row[6]);
            out.push('\n');
        }
        out
    }
}

fn join<'a>(values: impl Iterator<Item = &'a str>) -> String {
    values.collect::<Vec<_>>().join(",")
}

pub const fn permissions_str(mode: PermissionsMode) -> &'static str {
    match mode {
        PermissionsMode::Sync => "sync",
        PermissionsMode::Fixed => "fixed",
    }
}

/// Checks every source against its provider: installed and compatible, the
/// connection type supported, `settings` and `scope` valid under the
/// provider's schemas, and secret fields only environment references.
pub fn check_sources(config: &Config, discovery: &Discovery) -> Vec<Issue> {
    let mut issues = Vec::new();
    for (index, source) in config.sources.iter().enumerate() {
        issues.extend(check_source(index, source, discovery));
    }
    issues
}

/// The issues of one source (`index` locates it in the file).
pub fn check_source(index: usize, source: &SourceConfig, discovery: &Discovery) -> Vec<Issue> {
    let at = |field: &str| format!("sources[{index}].{field}");
    let Some(provider) = discovery.find(&source.provider) else {
        return vec![Issue::error(
            at("type"),
            "no installed connector provider has this name; `fyrr writer providers` lists them",
        )];
    };
    let schemas = match &provider.status {
        Ok(schemas) => schemas,
        Err(reason) => {
            return vec![Issue::error(
                at("type"),
                format!("its connector provider is refused: {reason}"),
            )];
        }
    };
    let info = provider
        .info
        .as_ref()
        .unwrap_or_else(|| unreachable!("an accepted provider has metadata"));
    let mut issues = Vec::new();
    if !info.connection_types.contains(&source.permissions) {
        issues.push(Issue::error(
            at("permissions"),
            format!(
                "the provider supports only these connection types: {}",
                join(
                    info.connection_types
                        .iter()
                        .map(|mode| permissions_str(*mode))
                )
            ),
        ));
    }
    for (part, value, validator, schema) in [
        (
            "settings",
            &source.settings,
            &schemas.settings,
            &info.config_schema.settings,
        ),
        (
            "scope",
            &source.scope,
            &schemas.scope,
            &info.config_schema.scope,
        ),
    ] {
        let instance = Value::Object(value.clone());
        for error in validator.iter_errors(&instance) {
            let mut path = at(part);
            for segment in error.instance_path().segments() {
                match segment {
                    LocationSegment::Property(key) => {
                        path.push('.');
                        path.push_str(&key);
                    }
                    LocationSegment::Index(position) => {
                        let _ = write!(path, "[{position}]");
                    }
                }
            }
            issues.push(Issue::error(path, schema_message(error.kind())));
        }
        let root = Value::Object(schema.clone());
        let mut secrets = Vec::new();
        if secret_values(&root, &root, &instance, &at(part), 0, &mut secrets).is_err() {
            issues.push(Issue::error(
                at(part),
                "provider secret schema traversal is unsupported",
            ));
        }
        for (path, found) in secrets {
            if !is_env_reference(found) && !found.is_null() {
                issues.push(Issue::error(
                    path,
                    "a secret field holds { env = \"VAR_NAME\" }, never the secret itself",
                ));
            }
        }
    }
    issues
}

/// See [`crate::config_show_redactions`].
pub fn show_redactions(
    config: &Config,
    discovery: Option<&Discovery>,
) -> Vec<(String, &'static str)> {
    let mut rules = Vec::new();
    for (index, source) in config.sources.iter().enumerate() {
        let accepted = discovery
            .and_then(|discovery| discovery.find(&source.provider))
            .filter(|provider| provider.is_compatible())
            .and_then(|provider| provider.info.as_ref());
        match accepted {
            Some(info) => {
                for (part, schema) in [
                    ("settings", &info.config_schema.settings),
                    ("scope", &info.config_schema.scope),
                ] {
                    let root = Value::Object(schema.clone());
                    let value = Value::Object(if part == "settings" {
                        source.settings.clone()
                    } else {
                        source.scope.clone()
                    });
                    let prefix = format!("sources[{index}].{part}");
                    let mut secrets = Vec::new();
                    if secret_values(&root, &root, &value, &prefix, 0, &mut secrets).is_err() {
                        rules.push((prefix, "<redacted: unsupported provider schema>"));
                    } else {
                        rules.extend(
                            secrets
                                .into_iter()
                                .map(|(path, _)| (path, "<redacted: secret field>")),
                        );
                    }
                }
            }
            None => {
                for part in ["settings", "scope"] {
                    rules.push((
                        format!("sources[{index}].{part}"),
                        "<redacted: no provider schema>",
                    ));
                }
            }
        }
    }
    rules
}

/// A value-free description of a schema violation.
fn schema_message(kind: &jsonschema::error::ValidationErrorKind) -> String {
    use jsonschema::error::ValidationErrorKind as Kind;
    match kind {
        Kind::AdditionalProperties { unexpected } => {
            format!(
                "unknown field(s) for this provider: {}",
                unexpected.join(", ")
            )
        }
        // The property name comes from the provider's own schema, which may
        // carry anything: it is shown only when it is a plain identifier.
        Kind::Required { property } => match property.as_str() {
            Some(name)
                if name.len() <= 64
                    && !name.is_empty()
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')) =>
            {
                format!("missing field `{name}` required by the provider")
            }
            _ => "a field the provider requires is missing".to_owned(),
        },
        other => format!(
            "does not match the provider's config schema ({})",
            other.keyword()
        ),
    }
}

/// Traverse schema and actual values together, retaining array indices.
/// Unsupported schema applicators fail closed instead of missing a secret.
fn secret_values<'a>(
    root: &Value,
    schema: &Value,
    value: &'a Value,
    path: &str,
    depth: usize,
    out: &mut Vec<(String, &'a Value)>,
) -> Result<(), ()> {
    if depth > 64 {
        return Err(());
    }
    // Schemas with no secret annotations need no secret traversal.
    if depth == 0 && !contains_secret(schema) {
        return Ok(());
    }
    let Some(schema) = schema.as_object() else {
        return if schema.is_boolean() { Ok(()) } else { Err(()) };
    };
    if schema.get(SECRET_MARK) == Some(&Value::Bool(true)) {
        out.push((path.to_owned(), value));
        return Ok(());
    }
    if let Some(reference) = schema.get("$ref") {
        let pointer = reference
            .as_str()
            .and_then(|r| r.strip_prefix('#'))
            .ok_or(())?;
        let target = root.pointer(pointer).ok_or(())?;
        secret_values(root, target, value, path, depth + 1, out)?;
    }
    for keyword in [
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "dependentSchemas",
        "patternProperties",
        "prefixItems",
        "contains",
        "$dynamicRef",
    ] {
        if schema.contains_key(keyword) {
            return Err(());
        }
    }
    for keyword in [
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
    ] {
        if schema.get(keyword).is_some_and(Value::is_object) {
            return Err(());
        }
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, child) in properties {
            if let Some(value) = value.get(key) {
                secret_values(root, child, value, &format!("{path}.{key}"), depth + 1, out)?;
            }
        }
    }
    if let Some(items) = schema.get("items")
        && let Some(values) = value.as_array()
    {
        for (index, value) in values.iter().enumerate() {
            secret_values(
                root,
                items,
                value,
                &format!("{path}[{index}]"),
                depth + 1,
                out,
            )?;
        }
    }
    Ok(())
}

fn contains_secret(schema: &Value) -> bool {
    match schema {
        Value::Object(object) => {
            object.get(SECRET_MARK) == Some(&Value::Bool(true))
                || object.values().any(contains_secret)
        }
        Value::Array(values) => values.iter().any(contains_secret),
        _ => false,
    }
}

fn is_env_reference(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(fyrr_core::config::is_secret_ref_object)
}

/// The configuration schema with the installed, compatible providers joined
/// in: a source's `type` must name one of them, and its `settings`
/// and `scope` follow that provider's schemas. Each provider schema is
/// embedded as its own resource (`$id`), so its internal references resolve.
pub fn extend_config_schema(schema: &mut Value, discovery: &Discovery) {
    let mut names = Vec::new();
    let mut rules = Vec::new();
    let mut definitions = Map::new();
    for provider in discovery.providers.iter().filter(|p| p.is_compatible()) {
        let Some(info) = &provider.info else { continue };
        names.push(Value::from(info.name.clone()));
        let mut then = Map::new();
        for (part, part_schema) in [
            ("settings", &info.config_schema.settings),
            ("scope", &info.config_schema.scope),
        ] {
            let id = format!("urn:fyrr:provider:{}:{part}", info.name);
            let mut embedded = part_schema.clone();
            embedded.insert("$id".into(), Value::from(id.clone()));
            definitions.insert(
                format!("provider-{}-{part}", info.name),
                Value::Object(embedded),
            );
            then.insert(part.into(), serde_json::json!({ "$ref": id }));
        }
        rules.push(serde_json::json!({
            "if": { "properties": { "type": { "const": info.name } }, "required": ["type"] },
            "then": { "properties": then }
        }));
    }
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    let defs = root
        .entry("$defs")
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(defs) = defs.as_object_mut() else {
        return;
    };
    let Some(source) = defs.get_mut("SourceConfig").and_then(Value::as_object_mut) else {
        return;
    };
    if let Some(kind) = source
        .get_mut("properties")
        .and_then(|properties| properties.get_mut("type"))
        .and_then(Value::as_object_mut)
    {
        kind.insert("enum".into(), Value::Array(names));
        kind.insert(
            "description".into(),
            Value::from("Connector provider name: one of the installed, compatible providers."),
        );
    }
    source.insert("allOf".into(), Value::Array(rules));
    defs.extend(definitions);
}

/// The sync kinds a source runs by default: what the provider implements,
/// without permission and member reads on a fixed connection, whose readers
/// come from the configuration.
pub fn default_kinds(info: &ProviderInfo, source: &SourceConfig) -> Vec<SyncKind> {
    info.sync_kinds
        .iter()
        .copied()
        .filter(|kind| {
            (source.permissions == PermissionsMode::Sync
                || !matches!(kind, SyncKind::Permissions | SyncKind::Members))
                && (*kind != SyncKind::Members || source.intervals.members.is_some())
        })
        .collect()
}

pub(crate) fn select_kinds(
    defaults: Vec<fyrr_core::connector::SyncKind>,
    requested: &[fyrr_core::connector::SyncKind],
) -> (
    Vec<fyrr_core::connector::SyncKind>,
    Vec<fyrr_core::connector::SyncKind>,
) {
    if requested.is_empty() {
        (defaults, Vec::new())
    } else {
        requested
            .iter()
            .copied()
            .partition(|kind| defaults.contains(kind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SENTINEL: &str = "SENTINEL-prov-5a0d";

    fn info(name: &str, min_sdk: &str) -> Value {
        let mut info: Value = serde_json::from_str(include_str!(
            "../../../fixtures/contracts/connector-provider/provider.json"
        ))
        .expect("fixture");
        info["name"] = json!(name);
        info["min_sdk_version"] = json!(min_sdk);
        info
    }

    fn discovery(sdk: &str, entries: Vec<Value>) -> Discovery {
        let raw = json!({ "sdk_version": sdk, "providers": entries });
        parse_discovery(raw.to_string().as_bytes()).expect("discovery")
    }

    fn entry(name: &str, info: Value) -> Value {
        json!({ "entry_point": name, "distribution": "fyrr-provider-test", "version": "0.1.0", "info": info })
    }

    fn status(discovery: &Discovery, index: usize) -> String {
        match &discovery.providers[index].status {
            Ok(_) => "ok".to_owned(),
            Err(reason) => reason.clone(),
        }
    }

    #[test]
    fn versions_the_python_side_reports_are_shown_only_when_built_from_verified_parts() {
        for good in [
            "0.1.2",
            "1.0.0rc1",
            "1.0.post1.dev2",
            "1.2.3.4",
            "1.0a2",
            "2024.11",
            "9",
            "123456.0",
        ] {
            assert_eq!(version_text(good), good);
        }
        for bad in [
            "",
            "0.1 2",
            "0.1.2\nSENTINEL",
            "<script>",
            "SENTINEL",
            "1.0.SENTINEL",
            "1.0rcSENTINEL",
            "1.0final",
            "1.0.dev",
            "v1.0",
            "1.2.3.4.5",
            "1234567",
            "1..2",
            "2!1.0",
            "1.0+local.7",
            "1.0.post1.post2.post3.post4",
            "1_0",
            "é1",
            &"1".repeat(65),
        ] {
            assert_eq!(version_text(bad), INVALID_VALUE, "{bad:?}");
        }
        let mut entries = vec![entry("markdown-dir", info("markdown-dir", "0.1.0"))];
        entries[0]["version"] = json!(format!("1.0 {SENTINEL}"));
        let found = discovery("0.1.2", entries);
        assert_eq!(found.providers[0].version.as_deref(), Some(INVALID_VALUE));
        let table = found.render();
        assert!(!table.contains(SENTINEL), "{table}");
        let raw = json!({"sdk_version": format!("0.1.2 {SENTINEL}"), "providers": []});
        let found = parse_discovery(raw.to_string().as_bytes()).expect("discovery");
        assert_eq!(found.sdk_version, INVALID_VALUE);
        assert!(!found.render().contains(SENTINEL));
    }

    #[test]
    fn compatible_providers_are_accepted_and_others_refused() {
        let mut no_content = info("no-content", "0.1.0");
        no_content["sync_kinds"] = json!(["reconcile"]);
        let mut bad_schema = info("bad-schema", "0.1.0");
        bad_schema["config_schema"]["settings"] =
            json!({ "type": "object", "properties": { "x": { "type": 17 } } });
        let found = discovery(
            "0.1.2",
            vec![
                entry("markdown-dir", info("markdown-dir", "0.1.0")),
                entry("future", info("future", "0.2.0")),
                entry("stable", info("stable", "1.0.0")),
                entry("newer", info("newer", "0.1.3")),
                entry("renamed", info("other-name", "0.1.0")),
                entry("no-content", no_content),
                entry("bad-schema", bad_schema),
                json!({ "entry_point": "crashes", "distribution": "d", "version": "1", "error": { "type": "builtins.ValueError", "frames": ["p.py:3 in info"] } }),
                entry(&format!("Bad {SENTINEL}"), info("x", "0.1.0")),
            ],
        );
        let expected = [
            "ok",
            "requires SDK series 0.2 (min_sdk_version 0.2.0); this fyrr build loads SDK series 0.1",
            "requires SDK series 1 (min_sdk_version 1.0.0)",
            "requires fyrr-connectors >= 0.1.3; the installed SDK is 0.1.2",
            "the metadata name differs",
            "sync_kinds must include content",
            "config_schema.settings is not a valid",
            "its metadata callable failed",
            "the entry point name does not match",
        ];
        for (index, wanted) in expected.iter().enumerate() {
            assert!(
                status(&found, index).contains(wanted),
                "{index}: {}",
                status(&found, index)
            );
        }
        let table = found.render();
        assert!(!table.contains(SENTINEL), "{table}");
        assert!(table.contains("9 provider(s), 8 refused"), "{table}");
        assert!(table.contains("<invalid name>"), "{table}");
    }

    #[test]
    fn duplicates_and_other_installed_sdk_series_are_refused() {
        let found = discovery(
            "0.1.0",
            vec![
                entry("dup", info("dup", "0.1.0")),
                entry("dup", info("dup", "0.1.0")),
            ],
        );
        assert!(status(&found, 0).contains("more than one"));
        let found = discovery(
            "1.0.0",
            vec![entry("markdown-dir", info("markdown-dir", "0.1.0"))],
        );
        assert!(
            status(&found, 0).contains(
                "fyrr-connectors 1.0.0 is SDK series 1; this fyrr build loads SDK series 0.1"
            ),
            "{}",
            status(&found, 0)
        );
    }

    fn source(settings: Value, scope: Value, permissions: &str) -> Config {
        let text = format!(
            "[namespace]\nname = \"acme\"\n[storage]\nuri = \"s3://bucket/acme\"\n[embedding]\nendpoint = \"https://e.example/v1\"\n[[sources]]\nid = \"docs\"\ntype = \"markdown-dir\"\npermissions = \"{permissions}\"\nreaders = [\"grp:eng\"]\n"
        );
        let mut config = fyrr_core::config::parse(&text).expect("config").config;
        config.sources[0].settings = settings.as_object().expect("object").clone();
        config.sources[0].scope = scope.as_object().expect("object").clone();
        config
    }

    #[test]
    fn sources_are_checked_against_their_provider_without_values() {
        let found = discovery(
            "0.1.0",
            vec![entry("markdown-dir", info("markdown-dir", "0.1.0"))],
        );
        let valid = source(
            json!({ "webhook_token": { "env": "HOOK" } }),
            json!({ "root": "/docs" }),
            "fixed",
        );
        assert_eq!(check_sources(&valid, &found), Vec::new());
        let invalid = source(
            json!({ "webhook_token": SENTINEL, "max_file_bytes": SENTINEL, "extra": SENTINEL }),
            json!({}),
            "sync",
        );
        let issues: Vec<String> = check_sources(&invalid, &found)
            .iter()
            .map(ToString::to_string)
            .collect();
        let all = issues.join("\n");
        assert!(!all.contains(SENTINEL), "{all}");
        for wanted in [
            "sources[0].permissions: the provider supports only these connection types: fixed",
            "sources[0].settings.webhook_token: a secret field holds",
            "sources[0].settings.max_file_bytes: does not match the provider's config schema (type)",
            "sources[0].settings: unknown field(s) for this provider: extra",
            "sources[0].scope: missing field `root` required by the provider",
        ] {
            assert!(all.contains(wanted), "{wanted} missing from:\n{all}");
        }
        let mut unknown = valid.clone();
        unknown.sources[0].provider = "jira".into();
        assert!(
            check_sources(&unknown, &found)[0]
                .message
                .contains("no installed connector provider")
        );
    }

    #[test]
    fn the_effective_schema_joins_provider_schemas() {
        let found = discovery(
            "0.1.0",
            vec![
                entry("markdown-dir", info("markdown-dir", "0.1.0")),
                entry("future", info("future", "0.2.0")),
            ],
        );
        let mut schema = fyrr_core::schemas::config_schema();
        extend_config_schema(&mut schema, &found);
        let validator = jsonschema::draft202012::new(&schema).expect("valid schema");
        let config = |settings: Value| {
            let mut value =
                serde_json::to_value(source(settings, json!({ "root": "/d" }), "fixed"))
                    .expect("json");
            value["sources"][0]["type"] = json!("markdown-dir");
            value
        };
        assert!(validator.is_valid(&config(json!({ "webhook_token": { "env": "HOOK" } }))));
        assert!(!validator.is_valid(&config(json!({ "webhook_token": "literal" }))));
        let mut future = config(json!({}));
        future["sources"][0]["type"] = json!("future");
        assert!(
            !validator.is_valid(&future),
            "a refused provider is not a valid type"
        );
    }
    #[test]
    fn secret_traversal_resolves_array_references_and_fails_closed() {
        let root = json!({"type": "object", "$defs": {"account": {"type": "object", "properties": {"access": {"x-fyrr-secret": true}}}},
            "properties": {"accounts": {"type": "array", "items": {"$ref": "#/$defs/account"}}}});
        let value =
            json!({"accounts": [{"access": "PRIVATE-access"}, {"access": {"env": "ACCESS"}}]});
        let mut found = Vec::new();
        secret_values(&root, &root, &value, "settings", 0, &mut found).unwrap();
        assert_eq!(
            found.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(),
            ["settings.accounts[0].access", "settings.accounts[1].access"]
        );
        assert!(!is_env_reference(found[0].1));
        assert!(is_env_reference(found[1].1));
        let unsupported = json!({"anyOf": [root]});
        assert!(
            secret_values(
                &unsupported,
                &unsupported,
                &value,
                "settings",
                0,
                &mut Vec::new()
            )
            .is_err()
        );
    }
}
