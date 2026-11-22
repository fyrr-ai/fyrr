//! Typed attributes: the schema a namespace keeps for them and the one
//! function that infers and validates a value against it.
//!
//! Every attribute is nullable and is stored as its own column of the table.
//! An undeclared attribute takes the type of its first value; `datetime` is
//! never inferred, only declared.

use std::collections::BTreeMap;

use chrono::DateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::limits::schema::{MAX_ATTRIBUTES, MAX_NAME_BYTES, MAX_VALUE_BYTES};

/// The columns of the table that are not attributes. An attribute cannot take
/// one of these names; `prepare::schema` in the writer is held to this list.
pub const BUILT_IN_COLUMNS: [&str; 16] = [
    "source_id",
    "root_id",
    "record_id",
    "source_revision",
    "readers",
    "vector",
    "searchable_text",
    "metadata_json",
    "record_order",
    "connector_map_ref",
    "source_uri",
    "source_type",
    "project",
    "author",
    "updated_at",
    "attributes",
];

/// The type of an attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttributeType {
    String,
    /// 64-bit signed integer.
    Int,
    /// 64-bit float.
    Float,
    Bool,
    /// An RFC 3339 string in requests, stored as UTC microseconds.
    Datetime,
}

impl AttributeType {
    /// The type of the attribute's column.
    pub fn arrow_type(self) -> arrow_schema::DataType {
        use arrow_schema::{DataType, TimeUnit};
        match self {
            Self::String => DataType::Utf8,
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Datetime => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        }
    }

    /// The kind of the attribute's scalar index: a bitmap for `bool`, a B-tree
    /// for every other type.
    pub fn index_kind(self) -> &'static str {
        match self {
            Self::Bool => "bitmap",
            _ => "btree",
        }
    }
}

/// The name of an attribute's index. The prefix keeps it clear of the indexes
/// of the built-in columns.
pub fn index_name(attribute: &str) -> String {
    format!("attr_{attribute}_idx")
}

/// A namespace's attributes: name to type.
pub type AttributeSchema = BTreeMap<String, AttributeType>;

/// A value checked against its attribute's type.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    Null,
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// UTC microseconds.
    Datetime(i64),
}

/// Why an attribute or a value was refused. The code is the named error; no
/// message carries a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AttributeError {
    #[error("attribute_name_invalid")]
    NameInvalid,
    #[error("attribute_name_reserved")]
    NameReserved,
    #[error("attribute_limit_exceeded")]
    TooMany,
    #[error("attribute_value_too_large")]
    ValueTooLarge,
    #[error("attribute_type_mismatch")]
    TypeMismatch,
    #[error("attribute_value_invalid")]
    ValueInvalid,
}

impl AttributeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::NameInvalid => "attribute_name_invalid",
            Self::NameReserved => "attribute_name_reserved",
            Self::TooMany => "attribute_limit_exceeded",
            Self::ValueTooLarge => "attribute_value_too_large",
            Self::TypeMismatch => "attribute_type_mismatch",
            Self::ValueInvalid => "attribute_value_invalid",
        }
    }
}

/// A name is a lowercase letter followed by lowercase letters, digits and `_`,
/// at most [`MAX_NAME_BYTES`]: a Lance column name that needs
/// no quoting. A built-in column name, `id` (the key of a document written
/// through the HTTP API) or a leading `_` is reserved.
pub fn check_name(name: &str) -> Result<(), AttributeError> {
    if name.starts_with('_') || name == "id" || BUILT_IN_COLUMNS.contains(&name) {
        return Err(AttributeError::NameReserved);
    }
    let mut bytes = name.bytes();
    if name.len() > MAX_NAME_BYTES
        || !bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        || !bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(AttributeError::NameInvalid);
    }
    Ok(())
}

/// Checks a schema as a whole: names and the bound on its size.
pub fn check_schema(schema: &AttributeSchema) -> Result<(), AttributeError> {
    if schema.len() > MAX_ATTRIBUTES {
        return Err(AttributeError::TooMany);
    }
    schema.keys().try_for_each(|name| check_name(name))
}

/// Infers and validates in one step. A declared attribute holds only its type;
/// an undeclared one is added to `schema` with the type of its value
/// (`string`, `int`, `float` or `bool`). A `null` is valid for every attribute
/// and infers nothing. On an error `schema` is unchanged.
pub fn resolve(
    schema: &mut AttributeSchema,
    name: &str,
    value: &serde_json::Value,
) -> Result<AttributeValue, AttributeError> {
    use serde_json::Value;
    check_name(name)?;
    if value.is_null() {
        return Ok(AttributeValue::Null);
    }
    let inferred = match value {
        Value::String(_) => AttributeType::String,
        Value::Bool(_) => AttributeType::Bool,
        Value::Number(number) if number.is_f64() => AttributeType::Float,
        Value::Number(_) => AttributeType::Int,
        _ => return Err(AttributeError::ValueInvalid),
    };
    let declared = schema.get(name).copied();
    if declared.is_none() && schema.len() >= MAX_ATTRIBUTES {
        return Err(AttributeError::TooMany);
    }
    let kind = declared.unwrap_or(inferred);
    let typed = match (kind, value) {
        (AttributeType::String, Value::String(text)) => {
            if text.len() > MAX_VALUE_BYTES {
                return Err(AttributeError::ValueTooLarge);
            }
            AttributeValue::String(text.clone())
        }
        (AttributeType::Int, Value::Number(number)) if !number.is_f64() => {
            // An integer above `i64::MAX` is a number of this type that does not fit.
            AttributeValue::Int(number.as_i64().ok_or(AttributeError::ValueInvalid)?)
        }
        (AttributeType::Float, Value::Number(number)) => {
            AttributeValue::Float(number.as_f64().ok_or(AttributeError::ValueInvalid)?)
        }
        (AttributeType::Bool, Value::Bool(flag)) => AttributeValue::Bool(*flag),
        (AttributeType::Datetime, Value::String(text)) => {
            if text.len() > MAX_VALUE_BYTES {
                return Err(AttributeError::ValueTooLarge);
            }
            let time =
                DateTime::parse_from_rfc3339(text).map_err(|_| AttributeError::ValueInvalid)?;
            AttributeValue::Datetime(time.timestamp_micros())
        }
        _ => return Err(AttributeError::TypeMismatch),
    };
    schema.entry(name.to_owned()).or_insert(kind);
    Ok(typed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema(entries: &[(&str, AttributeType)]) -> AttributeSchema {
        entries.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect()
    }

    #[test]
    fn an_undeclared_attribute_takes_the_type_of_its_first_value() {
        let mut schema = AttributeSchema::new();
        for (name, value, kind, typed) in [
            (
                "a",
                json!("x"),
                AttributeType::String,
                AttributeValue::String("x".into()),
            ),
            ("b", json!(-7), AttributeType::Int, AttributeValue::Int(-7)),
            (
                "c",
                json!(1.5),
                AttributeType::Float,
                AttributeValue::Float(1.5),
            ),
            (
                "d",
                json!(true),
                AttributeType::Bool,
                AttributeValue::Bool(true),
            ),
            // A datetime is declared, never inferred.
            (
                "e",
                json!("2026-01-02T03:04:05Z"),
                AttributeType::String,
                AttributeValue::String("2026-01-02T03:04:05Z".into()),
            ),
        ] {
            assert_eq!(resolve(&mut schema, name, &value), Ok(typed));
            assert_eq!(schema[name], kind);
        }
    }

    #[test]
    fn a_declared_attribute_keeps_its_type() {
        let mut schema = schema(&[
            ("when", AttributeType::Datetime),
            ("ratio", AttributeType::Float),
            ("count", AttributeType::Int),
        ]);
        assert_eq!(
            resolve(&mut schema, "when", &json!("2026-01-02T04:04:05+01:00")),
            Ok(AttributeValue::Datetime(1_767_323_045_000_000))
        );
        // A whole number is a float when the attribute says so.
        assert_eq!(
            resolve(&mut schema, "ratio", &json!(2)),
            Ok(AttributeValue::Float(2.0))
        );
        for (name, value, error) in [
            ("when", json!("yesterday"), AttributeError::ValueInvalid),
            ("when", json!(5), AttributeError::TypeMismatch),
            ("count", json!(1.5), AttributeError::TypeMismatch),
            ("count", json!("1"), AttributeError::TypeMismatch),
            ("count", json!(u64::MAX), AttributeError::ValueInvalid),
            ("ratio", json!(true), AttributeError::TypeMismatch),
        ] {
            assert_eq!(
                resolve(&mut schema, name, &value),
                Err(error),
                "{name} {value}"
            );
        }
        assert_eq!(schema.len(), 3);
    }

    #[test]
    fn a_null_is_valid_and_infers_nothing() {
        let mut schema = AttributeSchema::new();
        assert_eq!(
            resolve(&mut schema, "a", &json!(null)),
            Ok(AttributeValue::Null)
        );
        assert!(schema.is_empty());
    }

    #[test]
    fn structured_values_are_refused() {
        let mut schema = AttributeSchema::new();
        for value in [json!([1]), json!({"a": 1})] {
            assert_eq!(
                resolve(&mut schema, "a", &value),
                Err(AttributeError::ValueInvalid)
            );
        }
        assert!(schema.is_empty());
    }

    #[test]
    fn reserved_and_malformed_names_are_refused() {
        for name in ["_x", "_", "id", "source_id", "updated_at", "attributes", "vector"] {
            assert_eq!(
                check_name(name),
                Err(AttributeError::NameReserved),
                "{name}"
            );
        }
        for name in ["", "A", "1a", "a-b", "a.b", "a b", "é"] {
            assert_eq!(check_name(name), Err(AttributeError::NameInvalid), "{name}");
        }
        for name in ["a", "a1", "a_b", "created_at"] {
            assert_eq!(check_name(name), Ok(()), "{name}");
        }
    }

    #[test]
    fn the_name_bound_holds_at_its_edge() {
        let at = "a".repeat(MAX_NAME_BYTES);
        assert_eq!(check_name(&at), Ok(()));
        assert_eq!(
            check_name(&format!("{at}a")),
            Err(AttributeError::NameInvalid)
        );
    }

    #[test]
    fn the_value_bound_holds_at_its_edge() {
        let mut schema = schema(&[("a", AttributeType::String)]);
        let at = "x".repeat(MAX_VALUE_BYTES);
        assert!(resolve(&mut schema, "a", &json!(at)).is_ok());
        assert_eq!(
            resolve(&mut schema, "a", &json!(format!("{at}x"))),
            Err(AttributeError::ValueTooLarge)
        );
    }

    #[test]
    fn the_attribute_count_holds_at_its_edge() {
        let mut full: AttributeSchema = (0..MAX_ATTRIBUTES)
            .map(|n| (format!("a{n}"), AttributeType::Int))
            .collect();
        assert_eq!(check_schema(&full), Ok(()));
        // A declared attribute still takes values at the bound; a new one does not.
        assert!(resolve(&mut full, "a0", &json!(1)).is_ok());
        assert_eq!(
            resolve(&mut full, "extra", &json!(1)),
            Err(AttributeError::TooMany)
        );
        assert_eq!(full.len(), MAX_ATTRIBUTES);
        full.insert("extra".into(), AttributeType::Int);
        assert_eq!(check_schema(&full), Err(AttributeError::TooMany));
    }

    #[test]
    fn every_error_has_a_distinct_code() {
        let errors = [
            AttributeError::NameInvalid,
            AttributeError::NameReserved,
            AttributeError::TooMany,
            AttributeError::ValueTooLarge,
            AttributeError::TypeMismatch,
            AttributeError::ValueInvalid,
        ];
        let codes: std::collections::BTreeSet<_> = errors.iter().map(|e| e.code()).collect();
        assert_eq!(codes.len(), errors.len());
        assert!(errors.iter().all(|e| e.to_string() == e.code()));
    }
}
