use super::filters::{field_type, scalar_type};
use super::*;
use crate::search::column;
use arrow_array::{
    BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use fyrr_core::attributes::{AttributeSchema, AttributeType, format_uuid};
use fyrr_core::limits::mcp::MAX_AGGREGATE_GROUPS;
use serde_json::{Number, Value};
use std::cmp::Ordering;

/// An `f64` in the IEEE total order, so that a float group key and a float
/// minimum have one.
#[derive(Clone, Copy, Debug)]
struct Float(f64);
impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Float {}
impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Float {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// A scalar read from a column. A column holds one type, so the derived order
/// is the order of that type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Int(i64),
    Uint(u64),
    Float(Float),
    Bool(bool),
    Text(String),
    Time(i64),
    Uuid([u8; 16]),
}

impl Cell {
    /// The value of `name` in `row`, or `None` when it is null. The caller
    /// has checked that `kind` is a scalar.
    fn read(
        batch: &RecordBatch,
        name: &str,
        kind: AttributeType,
        row: usize,
    ) -> Result<Option<Self>> {
        let array = batch.column_by_name(name).ok_or(PublicationRowsInvalid)?;
        if array.is_null(row) {
            return Ok(None);
        }
        Ok(Some(match kind {
            AttributeType::String => Self::Text(
                column::<StringArray>(batch, name, PublicationRowsInvalid)?
                    .value(row)
                    .to_owned(),
            ),
            AttributeType::Int => {
                Self::Int(column::<Int64Array>(batch, name, PublicationRowsInvalid)?.value(row))
            }
            AttributeType::Uint => {
                Self::Uint(column::<UInt64Array>(batch, name, PublicationRowsInvalid)?.value(row))
            }
            AttributeType::Float => Self::Float(Float(
                column::<Float64Array>(batch, name, PublicationRowsInvalid)?.value(row),
            )),
            AttributeType::Bool => {
                Self::Bool(column::<BooleanArray>(batch, name, PublicationRowsInvalid)?.value(row))
            }
            AttributeType::Datetime => Self::Time(
                column::<TimestampMicrosecondArray>(batch, name, PublicationRowsInvalid)?
                    .value(row),
            ),
            AttributeType::Uuid => Self::Uuid(
                column::<FixedSizeBinaryArray>(batch, name, PublicationRowsInvalid)?
                    .value(row)
                    .try_into()
                    .map_err(|_| PublicationRowsInvalid)?,
            ),
            AttributeType::Bytes | AttributeType::Array(_) => return Err(InvalidArgument),
        }))
    }

    fn json(&self) -> Value {
        match self {
            Self::Int(value) => (*value).into(),
            Self::Uint(value) => (*value).into(),
            Self::Float(value) => float_json(value.0),
            Self::Bool(value) => (*value).into(),
            Self::Text(value) => value.clone().into(),
            Self::Time(micros) => chrono::DateTime::from_timestamp_micros(*micros)
                .map_or(Value::Null, |time| McpTime(time).to_string().into()),
            Self::Uuid(bytes) => format_uuid(*bytes).into(),
        }
    }
}

/// A float as JSON; `null` when JSON cannot hold it (NaN and infinity).
fn float_json(value: f64) -> Value {
    Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// One aggregate of the request, checked against the namespace's schema.
struct Measure {
    op: AggregateOp,
    column: Option<(String, AttributeType)>,
}

impl Measure {
    fn new(aggregation: &Aggregation, attributes: &AttributeSchema) -> Result<Self> {
        let column = match &aggregation.field {
            None => None,
            Some(field) => {
                let kind = match aggregation.op {
                    // Documents that hold the field, of whatever type.
                    AggregateOp::Count => field_type(field, attributes)?,
                    AggregateOp::Min | AggregateOp::Max => scalar_type(field, attributes)?,
                    AggregateOp::Sum | AggregateOp::Avg => match scalar_type(field, attributes)? {
                        kind
                        @ (AttributeType::Int | AttributeType::Uint | AttributeType::Float) => kind,
                        _ => return Err(InvalidArgument),
                    },
                };
                Some((field.clone(), kind))
            }
        };
        Ok(Self {
            op: aggregation.op,
            column,
        })
    }

    /// Adds the document of `row`; a null value is skipped.
    fn add(&self, acc: &mut Acc, batch: &RecordBatch, row: usize) -> Result<()> {
        let Some((name, kind)) = &self.column else {
            acc.count += 1;
            return Ok(());
        };
        if self.op == AggregateOp::Count {
            let array = batch.column_by_name(name).ok_or(PublicationRowsInvalid)?;
            acc.count += u64::from(!array.is_null(row));
            return Ok(());
        }
        let Some(cell) = Cell::read(batch, name, *kind, row)? else {
            return Ok(());
        };
        acc.count += 1;
        match (self.op, cell) {
            (_, Cell::Int(value)) if self.sums() => acc.int += i128::from(value),
            (_, Cell::Uint(value)) if self.sums() => acc.int += i128::from(value),
            (_, Cell::Float(value)) if self.sums() => acc.float += value.0,
            (AggregateOp::Min, cell) if acc.best.as_ref().is_none_or(|best| cell < *best) => {
                acc.best = Some(cell);
            }
            (AggregateOp::Max, cell) if acc.best.as_ref().is_none_or(|best| cell > *best) => {
                acc.best = Some(cell);
            }
            _ => {}
        }
        Ok(())
    }

    fn sums(&self) -> bool {
        matches!(self.op, AggregateOp::Sum | AggregateOp::Avg)
    }

    #[allow(clippy::cast_precision_loss)] // shortcut: an average is a float; exact sums are integers
    fn finish(&self, acc: &Acc) -> Value {
        match self.op {
            AggregateOp::Count => acc.count.into(),
            _ if acc.count == 0 => Value::Null,
            AggregateOp::Sum => match self.column {
                Some((_, AttributeType::Float)) => float_json(acc.float),
                // shortcut: a sum past 64 bits is a float; exact sums wait for a request that needs them
                _ => i64::try_from(acc.int)
                    .map(Value::from)
                    .or_else(|_| u64::try_from(acc.int).map(Value::from))
                    .unwrap_or_else(|_| float_json(acc.int as f64)),
            },
            AggregateOp::Avg => float_json((acc.int as f64 + acc.float) / acc.count as f64),
            AggregateOp::Min | AggregateOp::Max => {
                acc.best.as_ref().map_or(Value::Null, Cell::json)
            }
        }
    }
}

/// What one group has seen of one aggregate. `count` is of documents with a
/// value, so `sum` and `avg` of no value are `null`.
#[derive(Clone, Default)]
struct Acc {
    count: u64,
    int: i128,
    float: f64,
    best: Option<Cell>,
}

impl PreparedReader {
    /// The aggregates of the documents the caller may read and `filter`
    /// selects, per group of `group_by` values, in key order. A document with
    /// no value in a `group_by` field is in no group. More than
    /// `MAX_AGGREGATE_GROUPS` groups is `invalid_argument`.
    pub async fn aggregate(&mut self, args: &AggregateInput) -> Result<AggregateOutput> {
        args.validate().map_err(|_| InvalidArgument)?;
        let attributes = &self.manifest.attributes;
        let group_by = args
            .group_by
            .iter()
            .flatten()
            .map(|field| scalar_type(field, attributes).map(|kind| (field.clone(), kind)))
            .collect::<Result<Vec<_>>>()?;
        let measures = args
            .aggregations
            .iter()
            .map(|aggregation| Measure::new(aggregation, attributes))
            .collect::<Result<Vec<_>>>()?;
        let columns: Vec<&str> = group_by
            .iter()
            .map(|(name, _)| name.as_str())
            .chain(
                measures
                    .iter()
                    .filter_map(|measure| measure.column.as_ref())
                    .map(|(name, _)| name.as_str()),
            )
            .collect();
        let mut groups: BTreeMap<Vec<Cell>, Vec<Acc>> = BTreeMap::new();
        if group_by.is_empty() {
            // Without `group_by` the answer is one group, even over no document.
            groups.insert(vec![], vec![Acc::default(); measures.len()]);
        }
        let metadata = self
            .scan_documents(args.filter.as_ref(), &columns, |batch, row| {
                let mut key = Vec::with_capacity(group_by.len());
                for (name, kind) in &group_by {
                    match Cell::read(batch, name, *kind, row)? {
                        Some(cell) => key.push(cell),
                        None => return Ok(()),
                    }
                }
                if !groups.contains_key(&key) && groups.len() >= MAX_AGGREGATE_GROUPS {
                    return Err(InvalidArgument);
                }
                let accs = groups
                    .entry(key)
                    .or_insert_with(|| vec![Acc::default(); measures.len()]);
                for (measure, acc) in measures.iter().zip(accs) {
                    measure.add(acc, batch, row)?;
                }
                Ok(())
            })
            .await?;
        Ok(AggregateOutput {
            groups: groups
                .into_iter()
                .map(|(key, accs)| AggregateGroup {
                    key: key.iter().map(Cell::json).collect(),
                    values: measures
                        .iter()
                        .zip(&accs)
                        .map(|(measure, acc)| measure.finish(acc))
                        .collect(),
                })
                .collect(),
            metadata,
        })
    }
}
