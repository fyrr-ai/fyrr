use super::*;
use crate::search::FilterSelection;
use chrono::{DateTime, Utc};
use fyrr_core::attributes::analyzer::Tokenizer;
use fyrr_core::attributes::{AttributeSchema, AttributeType, parse_uuid};
use fyrr_core::limits::mcp::{MAX_REGEX_BYTES, MAX_REGEX_PROGRAM_BYTES};
use lance_index::scalar::{
    FullTextSearchQuery,
    inverted::query::{FtsQuery, MatchQuery, Operator},
};
use serde_json::Value;

/// The scalar type of a built-in field or of an attribute the namespace
/// declares. An attribute name is a plain column name, and only a declared one
/// gets here.
pub(super) fn field_type(field: &str, attributes: &AttributeSchema) -> Result<AttributeType> {
    match field {
        "source_id" | "root_id" | "source_type" | "project" | "author" => Ok(AttributeType::String),
        "updated_at" => Ok(AttributeType::Datetime),
        name => attributes.get(name).copied().ok_or(InvalidArgument),
    }
}

/// The type of a field that holds one ordered scalar: not the labels, an array
/// or a `bytes` attribute. `order_by` and `aggregate` take these only.
pub(super) fn scalar_type(field: &str, attributes: &AttributeSchema) -> Result<AttributeType> {
    match field_type(field, attributes)? {
        AttributeType::Array(_) | AttributeType::Bytes => Err(InvalidArgument),
        kind => Ok(kind),
    }
}

fn comparison_sql(comparison: &Comparison, attributes: &AttributeSchema) -> Result<String> {
    let Comparison { field, op, value } = comparison;
    let kind = match field.as_str() {
        // The connector labels `key=value`: a set test and nothing else.
        "attributes" if *op == Op::In => {
            return Ok(format!(
                "array_has_any(attributes,[{}])",
                literals(AttributeType::String, value)?
            ));
        }
        name => field_type(name, attributes)?,
    };
    // A `bytes` attribute is stored and nothing else.
    if kind == AttributeType::Bytes {
        return Err(InvalidArgument);
    }
    // Backticks make the name a column whatever it spells: an attribute can be
    // called `true`, `null`, `case` or `not`.
    let column = format!("`{field}`");
    if value.is_null() {
        return match op {
            Op::Eq => Ok(format!("{column} IS NULL")),
            Op::Ne => Ok(format!("{column} IS NOT NULL")),
            _ => Err(InvalidArgument),
        };
    }
    let symbol = match op {
        Op::StartsWith | Op::NotStartsWith | Op::Matches | Op::NotMatches => {
            return text_sql(&column, kind, *op, value);
        }
        Op::Contains | Op::NotContains if kind == AttributeType::String => {
            return text_sql(&column, kind, *op, value);
        }
        Op::Contains | Op::NotContains | Op::ContainsAny | Op::ContainsAll | Op::ContainsNone => {
            return array_sql(&column, kind, *op, value);
        }
        Op::In => return Ok(format!("{column} IN ({})", literals(kind, value)?)),
        Op::NotIn => return Ok(format!("{column} NOT IN ({})", literals(kind, value)?)),
        Op::Eq => "=",
        Op::Ne => "<>",
        Op::Lt => "<",
        Op::Lte => "<=",
        Op::Gt => ">",
        Op::Gte => ">=",
    };
    Ok(format!("{column} {symbol} {}", literal(kind, value)?))
}

/// A test of the text of a `string`: a prefix, a substring or a regular
/// expression, or its negation. Lance answers `starts_with` from a B-tree and
/// `contains` and `regexp_like` from an n-gram index; a missing value matches
/// neither side, as in every other comparison.
fn text_sql(column: &str, kind: AttributeType, op: Op, value: &Value) -> Result<String> {
    let (AttributeType::String, Value::String(pattern)) = (kind, value) else {
        return Err(InvalidArgument);
    };
    let (function, negated) = match op {
        Op::StartsWith => ("starts_with", false),
        Op::NotStartsWith => ("starts_with", true),
        Op::Contains => ("contains", false),
        Op::NotContains => ("contains", true),
        Op::Matches => ("regexp_like", false),
        Op::NotMatches => ("regexp_like", true),
        _ => return Err(InvalidArgument),
    };
    // DataFusion rewrites a test with an empty prefix or substring, or an
    // expression that matches everything, into `IS NOT NULL`, which breaks the
    // rule that a missing value fails every test and its negation.
    if pattern.is_empty() {
        return Err(InvalidArgument);
    }
    let test = if function == "regexp_like" {
        // The pattern is untrusted. The regex engine runs in time linear in the
        // text, so the compile bounds are what keeps a request cheap; Lance
        // compiles it with the engine's own, larger limit. The `(?-m)` prefix
        // and the `m` flag change nothing for the engine, and keep
        // DataFusion's simplifier from turning a plain pattern into a `LIKE`
        // that does not escape a backslash. What is checked is what runs.
        if pattern.len() > MAX_REGEX_BYTES {
            return Err(InvalidArgument);
        }
        let expression = format!("(?-m){pattern}");
        regex::RegexBuilder::new(&expression)
            .multi_line(true)
            .size_limit(MAX_REGEX_PROGRAM_BYTES)
            .build()
            .map_err(|_| InvalidArgument)?;
        format!("regexp_like({column},{},'m')", sql(&expression))
    } else {
        format!("{function}({column},{})", sql(pattern))
    };
    Ok(if negated { format!("NOT {test}") } else { test })
}

/// A test of the elements of an array attribute. A scalar attribute has none,
/// and a value is a literal of the element type.
fn array_sql(column: &str, kind: AttributeType, op: Op, value: &Value) -> Result<String> {
    let AttributeType::Array(element) = kind else {
        return Err(InvalidArgument);
    };
    let element = element.scalar();
    Ok(match op {
        Op::Contains => format!("array_has({column},{})", literal(element, value)?),
        Op::NotContains => format!("NOT array_has({column},{})", literal(element, value)?),
        Op::ContainsAny => format!("array_has_any({column},[{}])", literals(element, value)?),
        Op::ContainsAll => format!("array_has_all({column},[{}])", literals(element, value)?),
        Op::ContainsNone => format!(
            "NOT array_has_any({column},[{}])",
            literals(element, value)?
        ),
        _ => return Err(InvalidArgument),
    })
}

/// The values of `in` and `not_in`: one or more literals of `kind`.
fn literals(kind: AttributeType, values: &Value) -> Result<String> {
    let values = values.as_array().ok_or(InvalidArgument)?;
    if values.is_empty() {
        return Err(InvalidArgument);
    }
    Ok(values
        .iter()
        .map(|value| literal(kind, value))
        .collect::<Result<Vec<_>>>()?
        .join(","))
}

fn literal(kind: AttributeType, value: &Value) -> Result<String> {
    match (kind, value) {
        (AttributeType::String, Value::String(text)) => Ok(sql(text)),
        (AttributeType::Int, Value::Number(number)) => number
            .as_i64()
            .map(|number| number.to_string())
            .ok_or(InvalidArgument),
        (AttributeType::Float, Value::Number(number)) => number
            .as_f64()
            .map(|number| format!("{number:?}"))
            .ok_or(InvalidArgument),
        // The casts keep the literal of the column's type, which an array
        // function needs and Lance's indexes answer (see `lance_pin`).
        (AttributeType::Uint, Value::Number(number)) => number
            .as_u64()
            .map(|number| format!("CAST({number} AS BIGINT UNSIGNED)"))
            .ok_or(InvalidArgument),
        (AttributeType::Bool, Value::Bool(flag)) => Ok(flag.to_string()),
        (AttributeType::Uuid, Value::String(text)) => {
            let bytes = parse_uuid(text).ok_or(InvalidArgument)?;
            Ok(format!(
                "arrow_cast(X'{}', 'FixedSizeBinary(16)')",
                hex::encode(bytes)
            ))
        }
        (AttributeType::Datetime, Value::String(text)) => {
            let time = DateTime::parse_from_rfc3339(text).map_err(|_| InvalidArgument)?;
            Ok(format!(
                // Microseconds, as stored: a bare literal is nanoseconds and
                // overflows past the year 2262.
                "timestamp(6) '{}'",
                time.with_timezone(&Utc).format("%Y-%m-%d %H:%M:%S%.6f")
            ))
        }
        _ => Err(InvalidArgument),
    }
}

/// The SQL of a filter tree as a stored document is tested against it, or
/// `None` when it does not hold for the schema: the check of a write condition.
pub fn filter_sql(filter: &Filter, attributes: &AttributeSchema) -> Option<String> {
    PreparedReader::filter_sql(filter, attributes).ok()
}

impl PreparedReader {
    pub(crate) fn metadata(&self, telemetry: Telemetry) -> ResponseMetadata {
        ResponseMetadata {
            schema_version: MCP_CONTRACT_VERSION,
            publication_id: self.manifest.publication_id.clone(),
            content_version: self.manifest.lance_version,
            committed_at: McpTime(self.manifest.committed_at),
            degraded: vec![],
            stale_version: false,
            omitted_bytes: 0,
            telemetry,
        }
    }
    pub(crate) fn finish_metadata(
        &self,
        metadata: &mut ResponseMetadata,
        start: Instant,
        before: crate::mirror::IoSnapshot,
    ) {
        let after = self.mirror.counters();
        metadata.stale_version = self
            .head
            .content
            .as_ref()
            .is_some_and(|p| p.sequence != self.manifest.sequence);
        metadata.telemetry.s3_requests = after.requests - before.requests;
        metadata.telemetry.s3_bytes = after.bytes - before.bytes;
        metadata.telemetry.cache = if after.hits > before.hits {
            CacheState::Warm
        } else {
            CacheState::Hot
        };
        metadata
            .telemetry
            .stage_us
            .insert("total".into(), micros(start.elapsed()));
    }
    /// The rows of `dataset` that satisfy `predicate`, which is the admission
    /// predicate and the request filter. The scan carries no row mask, so
    /// Lance answers both sides from their scalar indexes; an external mask
    /// would demote the whole filter to a read of every admitted row. The
    /// predicate was built from the same state as `admitted`, and every row it
    /// selects is checked against `admitted`, so a row outside the admission
    /// fails closed.
    pub(crate) async fn filter_rows(
        dataset: &lance::Dataset,
        predicate: &str,
        admitted: &RowAddrMask,
    ) -> Result<RowAddrTreeMap> {
        let scan = Self::filter_scan(dataset, predicate)?;
        let mut allowed = RowAddrTreeMap::new();
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| StorageUnavailable)?;
        while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
            for id in ids(&batch)?.values() {
                if !admitted.selected(*id) {
                    return Err(AuthorizationUnavailable);
                }
                allowed.insert(*id);
            }
        }
        Ok(allowed)
    }
    /// The scan of `filter_rows`: no projection and no row mask.
    pub(crate) fn filter_scan(
        dataset: &lance::Dataset,
        predicate: &str,
    ) -> Result<lance::dataset::scanner::Scanner> {
        let mut scan = dataset.scan();
        scan.project::<&str>(&[]).map_err(|_| IncompatibleIndex)?;
        scan.filter(predicate)
            .map_err(|_| IncompatibleIndex)?
            .with_row_id();
        Ok(scan)
    }
    /// The SQL of a filter tree. Every comparison names a built-in field or an
    /// attribute of the namespace and holds values of that field's type, or the
    /// request is `invalid_argument`. A comparison with a missing (null) value
    /// is never true, `ne` and `not_in` included, and `not` keeps it that way:
    /// SQL three-valued logic, which Lance's scalar indexes follow.
    pub(crate) fn filter_sql(filter: &Filter, attributes: &AttributeSchema) -> Result<String> {
        let joined = |nodes: &[Filter], keyword: &str| -> Result<String> {
            let parts = nodes
                .iter()
                .map(|node| Self::filter_sql(node, attributes).map(|sql| format!("({sql})")))
                .collect::<Result<Vec<_>>>()?;
            Ok(parts.join(keyword))
        };
        match filter {
            Filter::And { and } => joined(and, " AND "),
            Filter::Or { or } => joined(or, " OR "),
            Filter::Not { not } => Ok(format!("NOT ({})", Self::filter_sql(not, attributes)?)),
            Filter::Compare(comparison) => comparison_sql(comparison, attributes),
        }
    }
    pub(crate) async fn apply_filters(&mut self, tree: Option<&Filter>) -> Result<()> {
        let Some(tree) = tree else {
            return Ok(());
        };
        let filter = Self::filter_sql(tree, &self.manifest.attributes)?;
        let predicate = format!("({}) AND ({filter})", self.admission_predicate()?);
        #[cfg(test)]
        self.test_hooks.filter_scan.wait_once().await;
        let allowed =
            Self::filter_rows(&self.dataset, &predicate, &self.admission.eligible).await?;
        self.filter_selection = Some(FilterSelection {
            predicate: filter,
            eligible: RowAddrMask::from_allowed(allowed),
            masked: false,
        });
        Ok(())
    }
    /// Narrows the selection to the rows whose full-text attributes meet every
    /// `contains` condition. Each scan takes the selection so far as its row
    /// mask: the caller's admission and the filter bound it, and every row it
    /// returns is checked against it. No SQL describes the result, so the
    /// selection is marked `masked`.
    pub(crate) async fn apply_contains(&mut self, contains: Option<&[Contains]>) -> Result<()> {
        let Some(contains) = contains else {
            return Ok(());
        };
        let mut selected = self.selected_rows().clone();
        for condition in contains {
            let query = self.contains_query(condition)?;
            let mut scan = self.dataset.scan();
            scan.project::<&str>(&[]).map_err(|_| IncompatibleIndex)?;
            scan.prefilter(true)
                .with_row_id_prefilter(selected.clone())
                .with_row_id();
            scan.full_text_search(FullTextSearchQuery::new_query(query))
                .map_err(|_| IncompatibleIndex)?;
            let mut stream = scan
                .try_into_stream()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut allowed = RowAddrTreeMap::new();
            while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
                for id in ids(&batch)?.values() {
                    if !selected.selected(*id) {
                        return Err(AuthorizationUnavailable);
                    }
                    allowed.insert(*id);
                }
            }
            selected = RowAddrMask::from_allowed(allowed);
        }
        self.filter_selection = Some(FilterSelection {
            predicate: self
                .filter_selection
                .as_ref()
                .map_or_else(|| "true".into(), |filter| filter.predicate.clone()),
            eligible: selected,
            masked: true,
        });
        Ok(())
    }
    /// The full-text query of one condition: a match query on the attribute's
    /// index, joined by AND or OR. The attribute must have a full-text index as
    /// the manifest records it, and not one of n-grams: Lance gives every n-gram
    /// of a value the same position and takes them as alternatives, so `all`
    /// would find a value with any one of them (the pin test records it).
    /// shortcut: no prefix matching, which waits for #170.
    fn contains_query(&self, condition: &Contains) -> Result<FtsQuery> {
        let index = self
            .manifest
            .attribute_indexes
            .get(&condition.attribute)
            .filter(|index| index.full_text)
            .ok_or(InvalidArgument)?;
        if index
            .analyzer
            .as_ref()
            .is_some_and(|a| a.tokenizer == Tokenizer::Ngram)
        {
            return Err(InvalidArgument);
        }
        let (text, all) = condition.text().ok_or(InvalidArgument)?;
        Ok(FtsQuery::Match(
            MatchQuery::new(text.into())
                .with_column(Some(condition.attribute.clone()))
                .with_operator(if all { Operator::And } else { Operator::Or }),
        ))
    }
    pub(crate) async fn listing(
        &self,
        args: &SearchInput,
        telemetry: Telemetry,
    ) -> Result<SearchOutput> {
        // No row mask: Lance selects the rows from the scalar indexes of the
        // predicate, and every row it returns is checked against the selection.
        let mut scan = self.dataset.scan();
        scan.project(&["source_id", "root_id", "record_id"])
            .map_err(|_| IncompatibleIndex)?;
        scan.filter(&self.request_predicate()?)
            .map_err(|_| IncompatibleIndex)?
            .with_row_id();
        // `order_by` names scalar fields only: the labels, an array attribute
        // and a `bytes` attribute are not orderable here. The document key
        // makes the order total.
        let mut order = match &args.order_by {
            Some(keys) => keys
                .iter()
                .map(|key| {
                    scalar_type(&key.field, &self.manifest.attributes)?;
                    Ok(ordering(&key.field, key.direction == SortDirection::Asc))
                })
                .collect::<Result<Vec<_>>>()?,
            None => vec![ordering("updated_at", false)],
        };
        order.extend([
            ordering("source_id", true),
            ordering("root_id", true),
            ordering("record_id", true),
        ]);
        scan.order_by(Some(order)).map_err(|_| IncompatibleIndex)?;
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| StorageUnavailable)?;
        let mut selected = Vec::new();
        let mut roots = BTreeSet::new();
        let limit = args.effective_limit() as usize;
        let offset = args.offset.unwrap_or(0) as usize;
        'scan: while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
            for row in 0..batch.num_rows() {
                if !self.selected_rows().selected(ids(&batch)?.value(row)) {
                    return Err(AuthorizationUnavailable);
                }
                let source = strings(&batch, "source_id")?.value(row);
                let root = strings(&batch, "root_id")?.value(row);
                // The offset counts documents, not rows: a connector document
                // has several records, so Lance's own offset would skip rows.
                if !roots.insert((source.to_owned(), root.to_owned())) || roots.len() <= offset {
                    continue;
                }
                selected.push(Candidate {
                    row_id: ids(&batch)?.value(row),
                    source: source.into(),
                    root: root.into(),
                    record: strings(&batch, "record_id")?.value(row).into(),
                    score: 0.0,
                    lexical: None,
                    distance: None,
                });
                if selected.len() == limit {
                    break 'scan;
                }
            }
        }
        self.hydrate_selected(
            selected
                .into_iter()
                .enumerate()
                .map(|(rank, candidate)| crate::search::SelectedHit {
                    candidate,
                    query: "",
                    query_index: None,
                    output: if rank < args.text_hits.unwrap_or(args.effective_limit()) as usize {
                        args.output.unwrap_or(OutputMode::Excerpt)
                    } else {
                        OutputMode::Metadata
                    },
                    excerpt_chars: args
                        .excerpt_chars
                        .unwrap_or(fyrr_core::limits::mcp::EXCERPT_CODEPOINTS),
                })
                .collect(),
            vec![],
            telemetry,
            None,
            args.fields.as_ref(),
        )
        .await
    }
    /// Scans once the documents the caller may read and `filter` selects, and
    /// calls `visit` with the first row of each, which holds `columns`. A
    /// document has several rows; the document key is `source_id` and
    /// `root_id`. Counts are of documents, and the scan stops at `busy` when
    /// more than `reader.filter_count_max_rows` readable rows match. Every row
    /// is checked against the selection, so `visit` never sees a hidden one.
    pub(crate) async fn scan_documents(
        &mut self,
        filter: Option<&Filter>,
        columns: &[&str],
        mut visit: impl FnMut(&RecordBatch, usize) -> Result<()>,
    ) -> Result<ResponseMetadata> {
        let (start, before, mut telemetry, _pin) = self.request_prologue().await?;
        let outcome = async {
            let stage = Instant::now();
            let counters = self.mirror.counters();
            self.apply_filters(filter).await?;
            let projection: BTreeSet<&str> = ["source_id", "root_id"]
                .into_iter()
                .chain(columns.iter().copied())
                .collect();
            let mut scan = self.dataset.scan();
            scan.project(&projection.into_iter().collect::<Vec<_>>())
                .map_err(|_| IncompatibleIndex)?;
            // No row mask, as in `listing`; rows are checked below.
            scan.filter(&self.request_predicate()?)
                .map_err(|_| IncompatibleIndex)?
                .with_row_id();
            scan.limit(
                Some((self.config.reader.filter_count_max_rows + 1) as i64),
                None,
            )
            .map_err(|_| IncompatibleIndex)?;
            let mut stream = scan
                .try_into_stream()
                .await
                .map_err(|_| StorageUnavailable)?;
            let mut seen = BTreeSet::new();
            let mut cost = 0;
            while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
                cost += batch.num_rows();
                if cost > self.config.reader.filter_count_max_rows {
                    return Err(Busy);
                }
                for row in 0..batch.num_rows() {
                    if !self.selected_rows().selected(ids(&batch)?.value(row)) {
                        return Err(AuthorizationUnavailable);
                    }
                    let key = (
                        strings(&batch, "source_id")?.value(row).to_owned(),
                        strings(&batch, "root_id")?.value(row).to_owned(),
                    );
                    if seen.insert(key) {
                        visit(&batch, row)?;
                    }
                }
            }
            telemetry.filter_rows_scanned = cost as u64;
            self.stage(&mut telemetry, "filter_counts", stage, counters);
            Ok(self.metadata(telemetry))
        }
        .await;
        let stage = Instant::now();
        let counters = self.mirror.counters();
        if self.interrupted().await? {
            return Err(AuthorizationUnavailable);
        }
        let mut metadata = outcome?;
        self.stage(
            &mut metadata.telemetry,
            "output_authorization",
            stage,
            counters,
        );
        self.finish_metadata(&mut metadata, start, before);
        Ok(metadata)
    }
    pub async fn list_filters(&mut self, args: &ListFiltersInput) -> Result<ListFiltersOutput> {
        args.validate().map_err(|_| InvalidArgument)?;
        let mut counts: [BTreeMap<String, u64>; 3] = Default::default();
        let columns = ["project", "source_type", "author"];
        let metadata = self
            .scan_documents(None, &columns, |batch, row| {
                for (map, column) in counts.iter_mut().zip(columns) {
                    let values = strings(batch, column)?;
                    if !values.is_null(row) && map.len() <= LIST_FILTERS_MAX_VALUES {
                        *map.entry(values.value(row).into()).or_default() += 1;
                    }
                }
                Ok(())
            })
            .await?;
        let values = |map: BTreeMap<String, u64>| {
            if map.len() > LIST_FILTERS_MAX_VALUES {
                FilterValues {
                    status: FilterStatus::TooMany,
                    values: vec![],
                }
            } else {
                FilterValues {
                    status: FilterStatus::Values,
                    values: map
                        .into_iter()
                        .map(|(value, count)| FilterValue { value, count })
                        .collect(),
                }
            }
        };
        let [projects, source_types, authors] = counts;
        Ok(ListFiltersOutput {
            projects: values(projects),
            source_types: values(source_types),
            authors: values(authors),
            metadata,
        })
    }
}
