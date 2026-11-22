//! Explicit `major.minor` schema versions and fail-closed contract parsing.
//!
//! Upgrade rules, shared by every versioned contract:
//! - An additive change (a new optional field, a new tagged variant that old
//!   readers never receive) bumps the minor version. Readers accept any minor
//!   of their major and ignore fields they do not know, so contract structs never
//!   use `deny_unknown_fields`. Fields added in a minor version need a default.
//! - Any other change bumps the major version. Readers reject an unknown major
//!   before looking at the rest of the document, and data is republished
//!   instead of migrated.
//!
//! Parse errors never echo document content: they carry the contract name, a
//! position and an error category only, because contract documents hold
//! tokens, identities and source text.

use std::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A contract's `major.minor` schema version, written as a string such as
/// `"1.0"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaVersion {
    pub major: u16,
    pub minor: u16,
}

impl SchemaVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// The text is not `<major>.<minor>` with canonical decimal numbers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("schema version must be <major>.<minor>, e.g. \"1.0\"")]
pub struct MalformedVersion;

impl FromStr for SchemaVersion {
    type Err = MalformedVersion;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (major, minor) = text.split_once('.').ok_or(MalformedVersion)?;
        Ok(Self::new(
            canonical_number(major)?,
            canonical_number(minor)?,
        ))
    }
}

fn canonical_number(text: &str) -> Result<u16, MalformedVersion> {
    crate::limits::canonical_number(text).ok_or(MalformedVersion)
}

impl Serialize for SchemaVersion {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for SchemaVersion {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SchemaVersion".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "Contract schema version `<major>.<minor>`. Readers accept any minor of their major and reject other majors.",
            "type": "string",
            "pattern": "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$"
        })
    }
}

/// A versioned contract document with a top-level `schema_version` field.
pub trait Contract: Serialize + DeserializeOwned {
    /// Stable contract name, used in errors and golden-fixture paths.
    const NAME: &'static str;
    /// The version this build writes. Its major is the only major it reads.
    const VERSION: SchemaVersion;
    /// Other majors this build reads besides `VERSION`'s: a document that an
    /// older build must refuse, written only where it needs them.
    const ALSO_READS: &'static [u16] = &[];

    /// Semantic checks beyond the JSON shape. Messages must not contain
    /// document values (identities, tokens, text).
    fn validate(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Why a contract document was refused. No variant carries document content.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContractError {
    #[error("{contract}: document is not a JSON object")]
    NotAnObject { contract: &'static str },
    #[error("{contract}: schema_version is missing")]
    MissingVersion { contract: &'static str },
    #[error("{contract}: schema_version is malformed")]
    MalformedVersion { contract: &'static str },
    #[error(
        "{contract}: unsupported schema major version {found} (this build reads {supported}.x); refusing the document"
    )]
    UnsupportedMajor {
        contract: &'static str,
        found: SchemaVersion,
        supported: u16,
    },
    #[error("{contract}: invalid document at line {line}, column {column} ({category})")]
    Malformed {
        contract: &'static str,
        line: usize,
        column: usize,
        category: &'static str,
    },
    #[error("{contract}: {message}")]
    Invalid {
        contract: &'static str,
        message: String,
    },
}

/// Checks that `found` is readable by a build that writes `current`.
pub fn check_compatible(
    contract: &'static str,
    current: SchemaVersion,
    found: SchemaVersion,
) -> Result<(), ContractError> {
    if found.major == current.major {
        Ok(())
    } else {
        Err(ContractError::UnsupportedMajor {
            contract,
            found,
            supported: current.major,
        })
    }
}

/// Parses a contract document, refusing an unknown major version before the
/// body is interpreted (fail closed), then validates it.
pub fn parse_contract<T: Contract>(bytes: &[u8]) -> Result<T, ContractError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| malformed::<T>(&error))?;
    let object = value
        .as_object()
        .ok_or(ContractError::NotAnObject { contract: T::NAME })?;
    let version = match object.get("schema_version") {
        None => return Err(ContractError::MissingVersion { contract: T::NAME }),
        Some(serde_json::Value::String(text)) => text
            .parse::<SchemaVersion>()
            .map_err(|_| ContractError::MalformedVersion { contract: T::NAME })?,
        Some(_) => return Err(ContractError::MalformedVersion { contract: T::NAME }),
    };
    if !T::ALSO_READS.contains(&version.major) {
        check_compatible(T::NAME, T::VERSION, version)?;
    }
    let document: T = serde_json::from_value(value).map_err(|error| malformed::<T>(&error))?;
    document
        .validate()
        .map_err(|message| ContractError::Invalid {
            contract: T::NAME,
            message,
        })?;
    Ok(document)
}

/// Serializes a contract document as pretty JSON with a trailing newline, the
/// canonical form of the golden fixtures.
pub fn write_contract<T: Contract>(document: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(document)
        .unwrap_or_else(|error| unreachable!("contract types serialize to JSON: {error}"));
    bytes.push(b'\n');
    bytes
}

fn malformed<T: Contract>(error: &serde_json::Error) -> ContractError {
    use serde_json::error::Category;
    ContractError::Malformed {
        contract: T::NAME,
        line: error.line(),
        column: error.column(),
        category: match error.classify() {
            Category::Io => "io",
            Category::Syntax => "syntax",
            Category::Data => "unexpected field type or value",
            Category::Eof => "truncated",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_versions_only() {
        assert_eq!("1.0".parse(), Ok(SchemaVersion::new(1, 0)));
        assert_eq!("12.34".parse(), Ok(SchemaVersion::new(12, 34)));
        for bad in [
            "", "1", "1.", ".1", "01.0", "1.00", "1.0.0", "+1.0", "1.-1", "a.b",
        ] {
            assert_eq!(bad.parse::<SchemaVersion>(), Err(MalformedVersion), "{bad}");
        }
    }
}
