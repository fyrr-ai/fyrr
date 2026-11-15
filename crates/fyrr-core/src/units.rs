//! Human-readable durations (`"30s"`, `"24h"`) and byte sizes (`"64MiB"`) for
//! the configuration file. Both serialize back to their canonical form: the
//! largest unit that represents the value exactly.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const DURATION_UNITS: [(&str, u64); 5] = [
    ("d", 86_400_000),
    ("h", 3_600_000),
    ("m", 60_000),
    ("s", 1_000),
    ("ms", 1),
];

const SIZE_UNITS: [(&str, u64); 5] = [
    ("TiB", 1 << 40),
    ("GiB", 1 << 30),
    ("MiB", 1 << 20),
    ("KiB", 1 << 10),
    ("B", 1),
];

/// A configured duration with millisecond resolution: an integer followed by
/// `ms`, `s`, `m`, `h` or `d`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConfigDuration(u64);

impl ConfigDuration {
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs * 1_000)
    }
    pub const fn from_mins(mins: u64) -> Self {
        Self(mins * 60_000)
    }
    pub const fn from_hours(hours: u64) -> Self {
        Self(hours * 3_600_000)
    }
    pub const fn from_days(days: u64) -> Self {
        Self(days * 86_400_000)
    }
    pub const fn as_millis(self) -> u64 {
        self.0
    }
    pub const fn as_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

/// A configured byte size: an integer optionally followed by `B`, `KiB`,
/// `MiB`, `GiB` or `TiB`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    pub const fn bytes(bytes: u64) -> Self {
        Self(bytes)
    }
    pub const fn kib(kib: u64) -> Self {
        Self(kib << 10)
    }
    pub const fn mib(mib: u64) -> Self {
        Self(mib << 20)
    }
    pub const fn gib(gib: u64) -> Self {
        Self(gib << 30)
    }
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// The text is not an integer with a supported unit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("expected {expected}")]
pub struct UnitParseError {
    expected: &'static str,
}

// No quote characters: configuration errors cut messages at the first quote,
// because serde quotes file values there (config::without_values).
const DURATION_EXPECTED: &str = "a duration such as 250ms, 30s, 5m, 1h or 7d";
const SIZE_EXPECTED: &str = "a byte size such as 4096, 64KiB, 16MiB or 1GiB";

fn parse_scaled(
    text: &str,
    units: &[(&str, u64)],
    default_unit: Option<u64>,
    expected: &'static str,
) -> Result<u64, UnitParseError> {
    let error = UnitParseError { expected };
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    let (number, unit) = text.split_at(digits);
    let value: u64 = crate::limits::canonical_number(number).ok_or(error.clone())?;
    let scale = if unit.is_empty() {
        default_unit.ok_or(error.clone())?
    } else {
        units
            .iter()
            .find(|(name, _)| *name == unit)
            .map(|(_, scale)| *scale)
            .ok_or(error.clone())?
    };
    value.checked_mul(scale).ok_or(error)
}

fn format_scaled(value: u64, units: &[(&str, u64)], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let (name, scale) = units
        .iter()
        .find(|(_, scale)| value != 0 && value.is_multiple_of(*scale))
        .unwrap_or(&units[units.len() - 1]);
    write!(f, "{}{}", value / scale, name)
}

impl FromStr for ConfigDuration {
    type Err = UnitParseError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_scaled(text, &DURATION_UNITS, None, DURATION_EXPECTED).map(Self)
    }
}

impl FromStr for ByteSize {
    type Err = UnitParseError;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_scaled(text, &SIZE_UNITS, Some(1), SIZE_EXPECTED).map(Self)
    }
}

impl fmt::Display for ConfigDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_scaled(self.0, &DURATION_UNITS, f)
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_scaled(self.0, &SIZE_UNITS, f)
    }
}

macro_rules! string_serde {
    ($type:ty, $name:literal, $pattern:literal, $description:literal) => {
        impl Serialize for $type {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $type {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = String::deserialize(deserializer)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }

        impl JsonSchema for $type {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                $name.into()
            }
            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({
                    "description": $description,
                    "type": "string",
                    "pattern": $pattern
                })
            }
        }
    };
}

string_serde!(
    ConfigDuration,
    "Duration",
    "^(0|[1-9][0-9]*)(ms|s|m|h|d)$",
    "Duration: an integer followed by ms, s, m, h or d."
);
string_serde!(
    ByteSize,
    "ByteSize",
    "^(0|[1-9][0-9]*)(B|KiB|MiB|GiB|TiB)?$",
    "Byte size: an integer followed by B, KiB, MiB, GiB or TiB (bytes when omitted)."
);
