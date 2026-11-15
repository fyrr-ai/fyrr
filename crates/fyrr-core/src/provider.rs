//! Connector provider metadata, the Airflow-style contract between fyrr
//! and the Python distributions that register connectors.
//!
//! A provider is a distribution with an entry point in the group
//! [`ENTRY_POINT_GROUP`]; the entry point names a callable that returns a
//! [`ProviderInfo`] document. fyrr validates it against the published schema
//! (`schemas/connector-provider.schema.json`, generated from this type) and
//! takes the provider's version from the distribution, never from the
//! document. The SDK (`fyrr-connectors`) follows Semantic Versioning, and its
//! compatibility series ([`SdkSeries`]) is the compatibility promise. This
//! build loads SDK series [`SDK_SERIES`]; a provider whose minimum SDK version
//! is in another series, or newer than the installed SDK, is refused
//! fail-closed.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::config::{PROVIDER_PATTERN, PermissionsMode};
use crate::connector::SyncKind;
use crate::limits::check_text;
use crate::version::{Contract, SchemaVersion};

/// The entry point group providers register under.
pub const ENTRY_POINT_GROUP: &str = "fyrr.connectors";
/// The provider metadata contract version written by this build's SDK.
pub const PROVIDER_CONTRACT_VERSION: SchemaVersion = SchemaVersion::new(2, 0);
/// The `fyrr-connectors` SDK series this build loads.
pub const SDK_SERIES: SdkSeries = SdkSeries::Initial { minor: 1 };
/// The distribution name of the SDK.
pub const SDK_DISTRIBUTION: &str = "fyrr-connectors";
/// Marks a property of a provider config schema as a secret: its value must be
/// an environment reference `{ env = "VAR" }`.
pub const SECRET_MARK: &str = "x-fyrr-secret";
/// `module.path:attribute` of the connector class.
pub const CONNECTOR_PATTERN: &str = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$";
/// `major.minor.patch` with canonical decimal numbers.
pub const SDK_VERSION_PATTERN: &str = r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$";
/// Longest provider description.
pub const MAX_DESCRIPTION_BYTES: usize = 1024;

fn compiled(pattern: &str) -> regex::Regex {
    regex::Regex::new(pattern).unwrap_or_else(|error| unreachable!("valid pattern: {error}"))
}

static PROVIDER: LazyLock<regex::Regex> = LazyLock::new(|| compiled(PROVIDER_PATTERN));
static CONNECTOR: LazyLock<regex::Regex> = LazyLock::new(|| compiled(CONNECTOR_PATTERN));

/// What a provider's metadata callable returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(title = "fyrr connector provider metadata")]
pub struct ProviderInfo {
    pub schema_version: SchemaVersion,
    /// Provider name: the value of a source's `type`. Equals the entry point
    /// name.
    #[schemars(regex(pattern = PROVIDER_PATTERN))]
    pub name: String,
    /// One line about the provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Oldest `fyrr-connectors` SDK version (`major.minor.patch`) the provider
    /// works with; fyrr refuses the provider when its series is not the one
    /// fyrr loads or the installed SDK is older.
    #[schemars(regex(pattern = SDK_VERSION_PATTERN))]
    pub min_sdk_version: String,
    /// Connection types the provider supports: the permissions modes a
    /// source may configure (`sync` maps the source's permissions, `fixed`
    /// uses the source's configured readers).
    pub connection_types: Vec<PermissionsMode>,
    /// The sync kinds the connector implements; always includes `content`.
    pub sync_kinds: Vec<SyncKind>,
    /// The connector class as `module.path:Class`.
    #[schemars(regex(pattern = CONNECTOR_PATTERN))]
    pub connector: String,
    pub config_schema: ProviderConfigSchema,
}

/// JSON Schemas (draft 2020-12) of a source's `settings` and `scope` tables.
/// They join fyrr's generated configuration schema, and `fyrr config check`
/// validates sources against them. A property marked `"x-fyrr-secret": true`
/// must hold `{ env = "VAR" }`, never the secret itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderConfigSchema {
    pub settings: Map<String, Value>,
    pub scope: Map<String, Value>,
}

impl Contract for ProviderInfo {
    const NAME: &'static str = "connector-provider";
    const VERSION: SchemaVersion = PROVIDER_CONTRACT_VERSION;

    /// Messages name fields, never values.
    fn validate(&self) -> Result<(), String> {
        if !PROVIDER.is_match(&self.name) {
            return Err(format!("name does not match {PROVIDER_PATTERN}"));
        }
        if let Some(description) = &self.description {
            check_text("description", description, MAX_DESCRIPTION_BYTES)?;
        }
        self.min_sdk_version
            .parse::<SdkVersion>()
            .map_err(|_| "min_sdk_version must be major.minor.patch".to_owned())?;
        unique_non_empty("connection_types", &self.connection_types)?;
        unique_non_empty("sync_kinds", &self.sync_kinds)?;
        if !self.sync_kinds.contains(&SyncKind::Content) {
            return Err("sync_kinds must include content".into());
        }
        if !CONNECTOR.is_match(&self.connector) {
            return Err("connector must be module.path:Class".into());
        }
        for (field, schema) in [
            ("config_schema.settings", &self.config_schema.settings),
            ("config_schema.scope", &self.config_schema.scope),
        ] {
            if schema.get("type").and_then(Value::as_str) != Some("object") {
                return Err(format!(
                    "{field} must be a schema with \"type\": \"object\""
                ));
            }
        }
        Ok(())
    }
}

fn unique_non_empty<T: Ord>(field: &str, values: &[T]) -> Result<(), String> {
    if values.is_empty() {
        return Err(format!("{field} is empty"));
    }
    if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(format!("{field} has duplicates"));
    }
    Ok(())
}

/// A `major.minor.patch` SDK version. Installed versions may carry a PEP 440
/// suffix (`1.2.0rc1`, `1.2.0.dev3`); only the numeric release is compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SdkVersion {
    pub major: u64,
    pub minor: u64,
    pub patch: u64,
}

impl fmt::Display for SdkVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// The text is not `major.minor.patch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("an SDK version is major.minor.patch")]
pub struct MalformedSdkVersion;

impl FromStr for SdkVersion {
    type Err = MalformedSdkVersion;

    /// Strict `major.minor.patch`, as in provider metadata.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut parts = text.split('.');
        let mut next = || -> Result<u64, MalformedSdkVersion> {
            let part = parts.next().ok_or(MalformedSdkVersion)?;
            let canonical = !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part == "0" || !part.starts_with('0'));
            if !canonical {
                return Err(MalformedSdkVersion);
            }
            part.parse().map_err(|_| MalformedSdkVersion)
        };
        let version = Self {
            major: next()?,
            minor: next()?,
            patch: next()?,
        };
        if parts.next().is_some() {
            return Err(MalformedSdkVersion);
        }
        Ok(version)
    }
}

impl SdkVersion {
    /// The compatibility series this version belongs to.
    pub const fn series(self) -> SdkSeries {
        if self.major == 0 {
            SdkSeries::Initial { minor: self.minor }
        } else {
            SdkSeries::Stable { major: self.major }
        }
    }

    /// The release part of an installed distribution version (`1.2`,
    /// `1.2.3`, `1.2.3rc1`, `1.2.3.post1`).
    pub fn parse_installed(text: &str) -> Result<Self, MalformedSdkVersion> {
        let numbers: Vec<u64> = text
            .split('.')
            .map_while(|part| {
                let digits: String = part.chars().take_while(char::is_ascii_digit).collect();
                (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
            })
            .take(3)
            .collect();
        match numbers.as_slice() {
            [major, minor] => Ok(Self {
                major: *major,
                minor: *minor,
                patch: 0,
            }),
            [major, minor, patch] => Ok(Self {
                major: *major,
                minor: *minor,
                patch: *patch,
            }),
            _ => Err(MalformedSdkVersion),
        }
    }
}

/// The versions of the SDK that are compatible with each other, decided by
/// the leading non-zero component as Semantic Versioning does for `0.y.z`:
/// `0.y.z` is series `0.y`, and `x.y.z` with `x > 0` is series `x`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SdkSeries {
    /// `0.y.z`: every minor is its own series.
    Initial { minor: u64 },
    /// `x.y.z` with `x > 0`: the major is the series.
    Stable { major: u64 },
}

impl fmt::Display for SdkSeries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initial { minor } => write!(f, "0.{minor}"),
            Self::Stable { major } => write!(f, "{major}"),
        }
    }
}

/// Why a provider cannot be loaded with the installed SDK.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SdkIncompatibility {
    #[error(
        "requires SDK series {required} (min_sdk_version {minimum}); this fyrr build loads SDK series {SDK_SERIES}"
    )]
    OtherSeries {
        required: SdkSeries,
        minimum: SdkVersion,
    },
    #[error("requires fyrr-connectors >= {minimum}; the installed SDK is {installed}")]
    TooOld {
        minimum: SdkVersion,
        installed: SdkVersion,
    },
}

/// Checks a provider's minimum SDK version against the installed SDK, whose
/// own series the caller has checked against [`SDK_SERIES`].
pub fn check_sdk(minimum: SdkVersion, installed: SdkVersion) -> Result<(), SdkIncompatibility> {
    if minimum.series() != SDK_SERIES {
        return Err(SdkIncompatibility::OtherSeries {
            required: minimum.series(),
            minimum,
        });
    }
    if installed < minimum {
        return Err(SdkIncompatibility::TooOld { minimum, installed });
    }
    Ok(())
}
