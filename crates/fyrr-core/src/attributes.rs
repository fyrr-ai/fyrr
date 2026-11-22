//! Typed attributes: the schema a namespace keeps for them and the one
//! function that infers and validates a value against it.
//!
//! Every attribute is nullable and is stored as its own column of the table.
//! An undeclared attribute takes the type of its first value (`string`, `int`,
//! `float` or `bool`, or an array of one of them); `uint`, `datetime`, `uuid`
//! and `bytes` are never inferred, only declared.

use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::STANDARD};
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
    /// 64-bit unsigned integer.
    Uint,
    /// A UUID as 8-4-4-4-12 hexadecimal digits in requests, stored in 16 bytes.
    Uuid,
    /// Base64 in requests. Stored, never filtered, sorted or indexed.
    Bytes,
    /// A list of values of one scalar type, written `{"array": "string"}`.
    Array(ElementType),
}

/// The type of the elements of an array: every scalar type but `bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ElementType {
    String,
    Int,
    Float,
    Bool,
    Datetime,
    Uint,
    Uuid,
}

impl ElementType {
    /// The scalar type of a single element.
    pub fn scalar(self) -> AttributeType {
        match self {
            Self::String => AttributeType::String,
            Self::Int => AttributeType::Int,
            Self::Float => AttributeType::Float,
            Self::Bool => AttributeType::Bool,
            Self::Datetime => AttributeType::Datetime,
            Self::Uint => AttributeType::Uint,
            Self::Uuid => AttributeType::Uuid,
        }
    }
}

impl AttributeType {
    /// The type of the attribute's column.
    pub fn arrow_type(self) -> arrow_schema::DataType {
        use arrow_schema::{DataType, Field, TimeUnit};
        match self {
            Self::String => DataType::Utf8,
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Datetime => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Uint => DataType::UInt64,
            Self::Uuid => DataType::FixedSizeBinary(16),
            Self::Bytes => DataType::Binary,
            Self::Array(element) => DataType::List(std::sync::Arc::new(Field::new(
                "item",
                element.scalar().arrow_type(),
                false,
            ))),
        }
    }

    /// The kind of the attribute's index: a bitmap for `bool`, a label list for
    /// an array, a B-tree for every other type but `bytes`, which has none.
    pub fn index_kind(self) -> Option<&'static str> {
        match self {
            Self::Bytes => None,
            Self::Bool => Some("bitmap"),
            Self::Array(_) => Some("label_list"),
            _ => Some("btree"),
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
    Uint(u64),
    Uuid([u8; 16]),
    Bytes(Vec<u8>),
    /// The elements of an array, each a scalar.
    Array(Vec<AttributeValue>),
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
/// no quoting. A built-in column name or a leading `_` is reserved.
pub fn check_name(name: &str) -> Result<(), AttributeError> {
    if name.starts_with('_') || BUILT_IN_COLUMNS.contains(&name) {
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

/// The 16 bytes of a UUID written as 8-4-4-4-12 hexadecimal digits, either case.
pub fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let groups: Vec<&str> = text.split('-').collect();
    if groups.iter().map(|group| group.len()).ne([8, 4, 4, 4, 12]) {
        return None;
    }
    hex::decode(groups.concat()).ok()?.try_into().ok()
}

/// The type of the elements an undeclared attribute takes from its first value.
fn element_of(value: &serde_json::Value) -> Option<ElementType> {
    use serde_json::Value;
    match value {
        Value::String(_) => Some(ElementType::String),
        Value::Bool(_) => Some(ElementType::Bool),
        Value::Number(number) if number.is_f64() => Some(ElementType::Float),
        Value::Number(_) => Some(ElementType::Int),
        _ => None,
    }
}

/// The type of an undeclared attribute's first value; none for an empty array.
fn inferred(value: &serde_json::Value) -> Result<Option<AttributeType>, AttributeError> {
    let invalid = AttributeError::ValueInvalid;
    match value {
        serde_json::Value::Array(items) => items
            .first()
            .map(|first| element_of(first).map(AttributeType::Array).ok_or(invalid))
            .transpose(),
        _ => element_of(value)
            .map(|element| Some(element.scalar()))
            .ok_or(invalid),
    }
}

/// One scalar value of the type `kind`.
fn scalar(
    kind: AttributeType,
    value: &serde_json::Value,
) -> Result<AttributeValue, AttributeError> {
    use serde_json::Value;
    let bounded = |text: &str| {
        if text.len() > MAX_VALUE_BYTES {
            Err(AttributeError::ValueTooLarge)
        } else {
            Ok(())
        }
    };
    Ok(match (kind, value) {
        (AttributeType::String, Value::String(text)) => {
            bounded(text)?;
            AttributeValue::String(text.clone())
        }
        (AttributeType::Int, Value::Number(number)) if !number.is_f64() => {
            // An integer above `i64::MAX` is a number of this type that does not fit.
            AttributeValue::Int(number.as_i64().ok_or(AttributeError::ValueInvalid)?)
        }
        (AttributeType::Uint, Value::Number(number)) if !number.is_f64() => {
            AttributeValue::Uint(number.as_u64().ok_or(AttributeError::ValueInvalid)?)
        }
        (AttributeType::Float, Value::Number(number)) => {
            AttributeValue::Float(number.as_f64().ok_or(AttributeError::ValueInvalid)?)
        }
        (AttributeType::Bool, Value::Bool(flag)) => AttributeValue::Bool(*flag),
        (AttributeType::Datetime, Value::String(text)) => {
            bounded(text)?;
            let time =
                DateTime::parse_from_rfc3339(text).map_err(|_| AttributeError::ValueInvalid)?;
            AttributeValue::Datetime(time.timestamp_micros())
        }
        (AttributeType::Uuid, Value::String(text)) => {
            AttributeValue::Uuid(parse_uuid(text).ok_or(AttributeError::ValueInvalid)?)
        }
        (AttributeType::Bytes, Value::String(text)) => {
            bounded(text)?;
            AttributeValue::Bytes(
                STANDARD
                    .decode(text)
                    .map_err(|_| AttributeError::ValueInvalid)?,
            )
        }
        _ => return Err(AttributeError::TypeMismatch),
    })
}

/// Infers and validates in one step. A declared attribute holds only its type;
/// an undeclared one is added to `schema` with the type of its value
/// (`string`, `int`, `float` or `bool`, or an array of one of them, by its
/// first element). A `null` is valid for every attribute and infers nothing,
/// and neither does an empty array of an undeclared one. On an error `schema`
/// is unchanged.
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
    let declared = schema.get(name).copied();
    let Some(kind) = declared.map_or_else(|| inferred(value), |kind| Ok(Some(kind)))? else {
        return Ok(AttributeValue::Null);
    };
    if declared.is_none() && schema.len() >= MAX_ATTRIBUTES {
        return Err(AttributeError::TooMany);
    }
    let typed = match (kind, value) {
        (AttributeType::Array(element), Value::Array(items)) => AttributeValue::Array(
            items
                .iter()
                .map(|item| scalar(element.scalar(), item))
                .collect::<Result<_, _>>()?,
        ),
        _ => scalar(kind, value)?,
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
        for value in [
            json!({"a": 1}),
            json!([[1]]),
            json!([null]),
            json!([{"a": 1}]),
        ] {
            assert_eq!(
                resolve(&mut schema, "a", &value),
                Err(AttributeError::ValueInvalid),
                "{value}"
            );
        }
        assert!(schema.is_empty());
    }

    #[test]
    fn an_undeclared_array_takes_its_type_from_its_first_element() {
        let mut schema = AttributeSchema::new();
        for (name, value, element) in [
            ("a", json!(["x", "y"]), ElementType::String),
            ("b", json!([1, 2]), ElementType::Int),
            ("c", json!([1.5]), ElementType::Float),
            ("d", json!([true, false]), ElementType::Bool),
        ] {
            assert!(matches!(
                resolve(&mut schema, name, &value),
                Ok(AttributeValue::Array(items)) if items.len() == value.as_array().unwrap().len()
            ));
            assert_eq!(schema[name], AttributeType::Array(element));
        }
        // The first element decides: a later one of another type does not fit.
        assert_eq!(
            resolve(&mut schema, "e", &json!([1, "x"])),
            Err(AttributeError::TypeMismatch)
        );
        assert!(!schema.contains_key("e"));
        // An empty array tells nothing of its elements.
        assert_eq!(
            resolve(&mut schema, "f", &json!([])),
            Ok(AttributeValue::Null)
        );
        assert!(!schema.contains_key("f"));
    }

    #[test]
    fn the_new_types_are_declared_and_checked() {
        let uuid = "0191e5c8-7b3a-7c2e-9f10-0123456789ab";
        let bytes = [0x01, 0x91, 0xe5, 0xc8, 0x7b, 0x3a, 0x7c, 0x2e];
        let mut schema = schema(&[
            ("big", AttributeType::Uint),
            ("id", AttributeType::Uuid),
            ("blob", AttributeType::Bytes),
            ("ids", AttributeType::Array(ElementType::Uuid)),
            ("when", AttributeType::Array(ElementType::Datetime)),
            ("tags", AttributeType::Array(ElementType::String)),
        ]);
        assert_eq!(
            resolve(&mut schema, "big", &json!(u64::MAX)),
            Ok(AttributeValue::Uint(u64::MAX))
        );
        assert_eq!(
            resolve(&mut schema, "id", &json!(uuid.to_uppercase())),
            Ok(AttributeValue::Uuid(parse_uuid(uuid).unwrap()))
        );
        assert_eq!(
            parse_uuid(uuid).unwrap()[..8],
            bytes,
            "the digits are the bytes in order"
        );
        assert_eq!(
            resolve(&mut schema, "blob", &json!("AZHlyHs6fC4=")),
            Ok(AttributeValue::Bytes(bytes.to_vec()))
        );
        assert_eq!(
            resolve(&mut schema, "ids", &json!([uuid, uuid])),
            Ok(AttributeValue::Array(vec![
                AttributeValue::Uuid(
                    parse_uuid(uuid).unwrap()
                );
                2
            ]))
        );
        // A declared array takes an empty array, stored as one.
        assert_eq!(
            resolve(&mut schema, "tags", &json!([])),
            Ok(AttributeValue::Array(vec![]))
        );
        for (name, value, error) in [
            ("big", json!(-1), AttributeError::ValueInvalid),
            ("big", json!(u64::MAX as f64), AttributeError::TypeMismatch),
            ("big", json!("1"), AttributeError::TypeMismatch),
            (
                "id",
                json!(uuid.replace('-', "")),
                AttributeError::ValueInvalid,
            ),
            (
                "id",
                json!(format!("{{{uuid}}}")),
                AttributeError::ValueInvalid,
            ),
            (
                "id",
                json!("0191e5c8-7b3a-7c2e-9f10-0123456789ag"),
                AttributeError::ValueInvalid,
            ),
            ("blob", json!("not base64!"), AttributeError::ValueInvalid),
            ("blob", json!(1), AttributeError::TypeMismatch),
            ("ids", json!([uuid, "x"]), AttributeError::ValueInvalid),
            ("ids", json!(uuid), AttributeError::TypeMismatch),
            ("tags", json!(["a", 1]), AttributeError::TypeMismatch),
            ("tags", json!(["a", null]), AttributeError::TypeMismatch),
            ("tags", json!("a"), AttributeError::TypeMismatch),
            ("when", json!(["yesterday"]), AttributeError::ValueInvalid),
            ("id", json!(["x"]), AttributeError::TypeMismatch),
        ] {
            assert_eq!(
                resolve(&mut schema, name, &value),
                Err(error),
                "{name} {value}"
            );
        }
    }

    #[test]
    fn an_array_element_and_a_bytes_value_are_bounded() {
        let mut schema = schema(&[
            ("tags", AttributeType::Array(ElementType::String)),
            ("blob", AttributeType::Bytes),
        ]);
        let over = "x".repeat(MAX_VALUE_BYTES + 1);
        for (name, value) in [("tags", json!(["a", over])), ("blob", json!(over))] {
            assert_eq!(
                resolve(&mut schema, name, &value),
                Err(AttributeError::ValueTooLarge),
                "{name}"
            );
        }
    }

    #[test]
    fn bytes_have_no_index_and_no_array_type() {
        assert_eq!(AttributeType::Bytes.index_kind(), None);
        assert_eq!(AttributeType::Uuid.index_kind(), Some("btree"));
        assert_eq!(AttributeType::Uint.index_kind(), Some("btree"));
        assert_eq!(
            AttributeType::Array(ElementType::Bool).index_kind(),
            Some("label_list")
        );
        // The old forms keep their text, and an array is a table.
        let parsed: AttributeSchema =
            serde_json::from_value(json!({"a": "int", "b": {"array": "uuid"}, "c": "bytes"}))
                .unwrap();
        assert_eq!(parsed["a"], AttributeType::Int);
        assert_eq!(parsed["b"], AttributeType::Array(ElementType::Uuid));
        assert_eq!(parsed["c"], AttributeType::Bytes);
        assert!(serde_json::from_value::<AttributeType>(json!({"array": "bytes"})).is_err());
    }

    #[test]
    fn reserved_and_malformed_names_are_refused() {
        for name in ["_x", "_", "source_id", "updated_at", "attributes", "vector"] {
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
