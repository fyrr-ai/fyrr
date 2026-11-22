use super::*;
use arrow_array::{ArrayRef, BooleanArray, FixedSizeListArray, Float16Array, Float64Array};
use arrow_array::{
    BinaryArray, FixedSizeBinaryArray, Int64Array, ListArray, TimestampMicrosecondArray,
    UInt64Array,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use fyrr_core::attributes::{AttributeType, CONNECTOR_COLUMNS, format_uuid};
use serde_json::Value;

/// Common metadata with the record revision and its text length in bytes.
type RowMetadata = (fyrr_core::connector::DocumentMetadata, (String, u64));
type ExcerptJob = (usize, usize, Arc<crate::excerpt::Terms>, u32);
struct HydrationRows {
    metadata: BTreeMap<u64, RowMetadata>,
    /// The attributes `fields` asks for, by row.
    attributes: BTreeMap<u64, serde_json::Map<String, Value>>,
    text_batch: Option<RecordBatch>,
    text_rows: BTreeMap<u64, usize>,
}
struct RenderedHits {
    hits: Vec<SearchHit>,
    omitted_bytes: u64,
    excerpt_jobs: Vec<ExcerptJob>,
    revision_us: u64,
    selection_us: u64,
}

#[derive(serde::Deserialize)]
struct StoredText {
    #[serde(rename = "_fyrr_record_revision")]
    revision: String,
    #[serde(rename = "_fyrr_text_utf8_bytes")]
    bytes: u64,
}

impl PreparedReader {
    pub(crate) async fn rows_by_id(
        &self,
        row_ids: &[u64],
        columns: &[&str],
        projection_error: ReaderError,
    ) -> Result<RecordBatch> {
        self.authorization()?;
        if row_ids.iter().any(|id| !self.selected_rows().selected(*id)) {
            return Err(AuthorizationUnavailable);
        }
        let columns: Vec<_> = columns
            .iter()
            .copied()
            .chain(std::iter::once("_rowid"))
            .collect();
        let projection = self
            .dataset
            .schema()
            .project_preserve_system_columns(&columns)
            .map_err(|_| projection_error)?;
        // Validate the projection before performing the native take.
        let batch = self
            .dataset
            .take_builder(row_ids, projection)
            .map_err(|_| projection_error)?
            .execute()
            .await
            .map_err(|_| StorageUnavailable)?;
        if batch.num_rows() != row_ids.len() {
            return Err(StorageUnavailable);
        }
        Ok(batch)
    }

    /// The columns `fields` returns: the attributes it includes, or every one
    /// of the namespace but those it excludes. An included name the namespace
    /// does not declare is invalid; `vector` is always one to name.
    pub(super) fn returned_columns(&self, fields: Option<&ReturnFields>) -> Result<Vec<String>> {
        let declared = &self.manifest.attributes;
        match fields {
            Some(ReturnFields {
                include: Some(names),
                ..
            }) => {
                let mut columns: Vec<String> = Vec::new();
                for name in names {
                    if name != "vector" && !declared.contains_key(name) {
                        return Err(InvalidArgument);
                    }
                    if !columns.contains(name) {
                        columns.push(name.clone());
                    }
                }
                Ok(columns)
            }
            Some(ReturnFields {
                exclude: Some(names),
                ..
            }) => Ok(declared
                .keys()
                .filter(|name| !names.contains(name))
                .cloned()
                .collect()),
            _ => Ok(Vec::new()),
        }
    }

    /// The columns that hold the metadata fields of a connector document: none
    /// in a namespace written through the API.
    pub(crate) fn connector_columns(&self) -> Vec<&'static str> {
        if self.config.namespace.write_api.is_some() {
            return Vec::new();
        }
        CONNECTOR_COLUMNS.iter().map(|(name, _)| *name).collect()
    }

    /// `metadata` with the fields that its namespace stores as typed columns
    /// taken from `row` of `batch`, which holds [`Self::connector_columns`].
    pub(crate) fn with_connector_columns(
        &self,
        batch: &RecordBatch,
        row: usize,
        mut metadata: fyrr_core::connector::DocumentMetadata,
    ) -> Result<fyrr_core::connector::DocumentMetadata> {
        let names: Vec<String> = self
            .connector_columns()
            .into_iter()
            .map(str::to_owned)
            .collect();
        if names.is_empty() {
            return Ok(metadata);
        }
        let mut values = self.row_attributes(batch, row, &names)?;
        let mut take = |name: &str| values.remove(name).unwrap_or(Value::Null);
        let invalid = |_| PublicationRowsInvalid;
        metadata.title = serde_json::from_value(take("title")).map_err(invalid)?;
        metadata.team = serde_json::from_value(take("team")).map_err(invalid)?;
        metadata.status = serde_json::from_value(take("status")).map_err(invalid)?;
        metadata.labels = serde_json::from_value::<Option<_>>(take("labels"))
            .map_err(invalid)?
            .unwrap_or_default();
        metadata.created_at = serde_json::from_value(take("created_at")).map_err(invalid)?;
        Ok(metadata)
    }

    /// One row's values of the returned columns; a missing value is `null`.
    fn row_attributes(
        &self,
        batch: &RecordBatch,
        row: usize,
        columns: &[String],
    ) -> Result<serde_json::Map<String, Value>> {
        columns
            .iter()
            .map(|name| {
                let array = batch.column_by_name(name).ok_or(PublicationRowsInvalid)?;
                let value = if array.is_null(row) {
                    Value::Null
                } else if name == "vector" {
                    vector_json(array, row)?
                } else {
                    let kind = self.manifest.attributes.get(name);
                    scalar_json(array, *kind.ok_or(PublicationRowsInvalid)?, row)?
                };
                Ok((name.clone(), value))
            })
            .collect()
    }

    pub(crate) async fn hydrate_selected(
        &self,
        selected: Vec<SelectedHit<'_>>,
        degraded: Vec<Lane>,
        mut telemetry: Telemetry,
        recency_text: Option<(u32, OutputMode)>,
        fields: Option<&ReturnFields>,
    ) -> Result<SearchOutput> {
        let start = Instant::now();
        let before = self.mirror.counters();
        // The widest request is a listing's window (16 queries of 20 hits is less).
        if selected.len() > fyrr_core::limits::mcp::MAX_LISTING_WINDOW as usize
            || selected
                .iter()
                .any(|r| !self.selected_rows().selected(r.candidate.row_id))
        {
            return Err(AuthorizationUnavailable);
        }
        let mut hits = Vec::new();
        let mut omitted_bytes = 0u64;
        if !selected.is_empty() {
            let columns = self.returned_columns(fields)?;
            let (selected, mut rows) = self
                .hydration_rows(selected, recency_text, &columns)
                .await?;
            self.stage(&mut telemetry, "hydration", start, before);
            let start = Instant::now();
            let scores = fields.is_some_and(|fields| fields.scores);
            let mut rendered = self.render_hits(selected, &mut rows, scores)?;
            if !rendered.excerpt_jobs.is_empty() {
                let selection_start = Instant::now();
                let text = strings(
                    rows.text_batch.as_ref().ok_or(StorageUnavailable)?,
                    "searchable_text",
                )?;
                let pin = Arc::new(self.mirror_pin.request()?);
                let scratch = self.response_claim.clone();
                // Share Arrow buffers and query terms; each CPU task only reads
                // already admitted text. Small agent outputs stay inline. No
                // waiting or source I/O runs on Lance's compute pool.
                let batches =
                    futures::future::join_all(rendered.excerpt_jobs.chunks(8).map(|jobs| {
                        let jobs = jobs.to_vec();
                        let text = text.clone();
                        // A cancelled async caller cannot cancel CPU work already
                        // queued. Retain its execution and memory claims until that
                        // work ends, including batch scratch and retired pins.
                        let claims = (
                            pin.clone(),
                            self.execution_slot.clone(),
                            self.preparation_reservation.clone(),
                            scratch.clone(),
                        );
                        lance_core::utils::tokio::spawn_cpu(move || {
                            let _claims = claims;
                            Ok::<_, std::convert::Infallible>(
                                jobs.into_iter()
                                    .map(|(hit, row, terms, cap)| {
                                        (hit, terms.select(text.value(row), cap))
                                    })
                                    .collect::<Vec<_>>(),
                            )
                        })
                    }))
                    .await;
                for batch in batches {
                    for (hit, excerpt) in batch.unwrap_or_else(|error| match error {}) {
                        // The serial loop counted the entire record as omitted;
                        // account for the returned bytes once selection finishes.
                        rendered.omitted_bytes = rendered
                            .omitted_bytes
                            .checked_sub(excerpt.text.len() as u64)
                            .ok_or(PublicationRowsInvalid)?;
                        rendered.hits[hit].excerpt = Some(excerpt);
                    }
                }
                rendered.selection_us += micros(selection_start.elapsed());
            }
            telemetry
                .stage_us
                .insert("excerpts_revision".into(), rendered.revision_us);
            telemetry
                .stage_us
                .insert("excerpts_selection".into(), rendered.selection_us);
            self.stage(&mut telemetry, "excerpts", start, self.mirror.counters());
            hits = rendered.hits;
            omitted_bytes = rendered.omitted_bytes;
        }
        Ok(SearchOutput {
            hits,
            truncated: None,
            metadata: {
                let mut metadata = self.metadata(telemetry);
                metadata.degraded = degraded;
                metadata.omitted_bytes = omitted_bytes;
                metadata
            },
        })
    }

    async fn hydration_rows<'q>(
        &self,
        mut selected: Vec<SelectedHit<'q>>,
        recency_text: Option<(u32, OutputMode)>,
        returned: &[String],
    ) -> Result<(Vec<SelectedHit<'q>>, HydrationRows)> {
        let row_ids: Vec<_> = selected.iter().map(|r| r.candidate.row_id).collect();
        // Private per-record metadata permits projection without any text.
        let all_text = recency_text.map_or_else(
            || {
                selected
                    .iter()
                    .all(|hit| hit.output != OutputMode::Metadata)
            },
            |(count, output)| count as usize >= selected.len() && output != OutputMode::Metadata,
        );
        // Candidates already carry identity from this pinned generation.
        // rows_by_id rechecks authorization and admission before native take;
        // only payload columns need to be read again for selected rows.
        let mut columns = vec!["metadata_json"];
        if all_text {
            columns.push("searchable_text");
        }
        columns.extend(returned.iter().map(String::as_str));
        columns.extend(
            self.connector_columns()
                .into_iter()
                .filter(|name| !returned.iter().any(|r| r == name)),
        );
        let batch = self
            .rows_by_id(&row_ids, &columns, PublicationInvalid)
            .await?;
        let mut metadata = BTreeMap::new();
        let mut attributes = BTreeMap::new();
        let rows: BTreeMap<_, _> = ids(&batch)?
            .values()
            .iter()
            .enumerate()
            .map(|(row, id)| (*id, row))
            .collect();
        for hit in &selected {
            let candidate = &hit.candidate;
            let row = *rows.get(&candidate.row_id).ok_or(StorageUnavailable)?;
            let raw = strings(&batch, "metadata_json")?.value(row);

            // Typed parsing skips potentially large private permission state.
            let stored: StoredText =
                serde_json::from_str(raw).map_err(|_| PublicationRowsInvalid)?;
            fyrr_core::limits::check_digest("record_revision", &stored.revision)
                .map_err(|_| PublicationRowsInvalid)?;
            let private = (stored.revision, stored.bytes);
            let common: Option<fyrr_core::connector::DocumentMetadata> =
                serde_json::from_str(raw).map_err(|_| PublicationRowsInvalid)?;
            let common = self.with_connector_columns(
                &batch,
                row,
                common.ok_or(PublicationRowsInvalid)?,
            )?;
            metadata.insert(candidate.row_id, (common, private));
            if !returned.is_empty() {
                attributes.insert(
                    candidate.row_id,
                    self.row_attributes(&batch, row, returned)?,
                );
            }
        }
        if recency_text.is_some() {
            selected.sort_by(|a, b| {
                metadata[&b.candidate.row_id]
                    .0
                    .updated_at
                    .cmp(&metadata[&a.candidate.row_id].0.updated_at)
                    .then(
                        (&a.candidate.source, &a.candidate.root, &a.candidate.record).cmp(&(
                            &b.candidate.source,
                            &b.candidate.root,
                            &b.candidate.record,
                        )),
                    )
            });
        }
        let selected: Vec<_> = selected
            .into_iter()
            .enumerate()
            .map(|(rank, mut hit)| {
                if let Some((count, output)) = recency_text {
                    hit.output = if rank < count as usize {
                        output
                    } else {
                        OutputMode::Metadata
                    };
                }
                hit
            })
            .collect();
        let text_ids: Vec<_> = selected
            .iter()
            .filter(|hit| hit.output != OutputMode::Metadata)
            .map(|hit| hit.candidate.row_id)
            .collect();
        let text_batch = if all_text {
            Some(batch.clone())
        } else if text_ids.is_empty() {
            None
        } else {
            Some(
                self.rows_by_id(&text_ids, &["searchable_text"], PublicationInvalid)
                    .await?,
            )
        };
        let text_rows: BTreeMap<_, _> = text_batch
            .as_ref()
            .map(|batch| {
                Ok::<BTreeMap<u64, usize>, ReaderError>(
                    ids(batch)?
                        .values()
                        .iter()
                        .enumerate()
                        .map(|(row, id)| (*id, row))
                        .collect(),
                )
            })
            .transpose()?
            .unwrap_or_default();
        Ok((
            selected,
            HydrationRows {
                metadata,
                attributes,
                text_batch,
                text_rows,
            },
        ))
    }

    fn render_hits(
        &self,
        selected: Vec<SelectedHit<'_>>,
        rows: &mut HydrationRows,
        scores: bool,
    ) -> Result<RenderedHits> {
        let HydrationRows {
            metadata,
            attributes,
            text_batch,
            text_rows,
        } = rows;
        let mut hits = Vec::new();
        let mut omitted_bytes = 0u64;
        let mut excerpt_terms = BTreeMap::new();
        let parallel_excerpts = selected
            .iter()
            .filter(|hit| hit.output == OutputMode::Excerpt)
            .count()
            > 8;
        let mut excerpt_jobs = Vec::new();
        let (mut revision_us, mut selection_us) = (0u64, 0u64);
        for hit in selected {
            let candidate = hit.candidate;
            let (metadata, private) = metadata
                .remove(&candidate.row_id)
                .ok_or(StorageUnavailable)?;
            let text = if let Some(row) = text_rows.get(&candidate.row_id) {
                Some(
                    strings(
                        text_batch.as_ref().ok_or(StorageUnavailable)?,
                        "searchable_text",
                    )?
                    .value(*row),
                )
            } else {
                None
            };
            let revision_start = Instant::now();
            let (revision, length) = private;
            if let Some(text) = text
                && (length != text.len() as u64
                    || revision
                        != record_revision(
                            &candidate.source,
                            &candidate.root,
                            &candidate.record,
                            text,
                        ))
            {
                return Err(PublicationRowsInvalid);
            }
            revision_us += micros(revision_start.elapsed());
            let selection_start = Instant::now();
            let excerpt = if hit.output == OutputMode::Excerpt {
                let terms = excerpt_terms
                    .entry(hit.query)
                    .or_insert_with(|| Arc::new(crate::excerpt::Terms::new(hit.query)));
                if parallel_excerpts {
                    let row = *text_rows.get(&candidate.row_id).ok_or(StorageUnavailable)?;
                    excerpt_jobs.push((hits.len(), row, terms.clone(), hit.excerpt_chars));
                    None
                } else {
                    Some(terms.select(text.ok_or(StorageUnavailable)?, hit.excerpt_chars))
                }
            } else {
                None
            };
            selection_us += micros(selection_start.elapsed());
            let content = if hit.output == OutputMode::Content {
                Some(text.ok_or(StorageUnavailable)?.to_owned())
            } else {
                None
            };
            let returned = content
                .as_ref()
                .map_or_else(|| excerpt.as_ref().map_or(0, |e| e.text.len()), |t| t.len())
                as u64;
            omitted_bytes = omitted_bytes
                .checked_add(length.saturating_sub(returned))
                .ok_or(PublicationRowsInvalid)?;
            hits.push(SearchHit {
                query_index: hit.query_index,
                source_id: candidate.source,
                root_id: candidate.root,
                record_id: candidate.record,
                revision,
                source_uri: metadata.source_uri,
                source_type: metadata.source_type,
                title: metadata.title,
                author: metadata.author,
                team: metadata.team,
                project: metadata.project,
                status: metadata.status,
                labels: metadata.labels,
                created_at: metadata.created_at.map(McpTime),
                updated_at: metadata.updated_at.map(McpTime),
                score: candidate.score,
                excerpt,
                content,
                attributes: attributes.remove(&candidate.row_id),
                lexical_score: candidate.lexical.filter(|_| scores),
                distance: candidate.distance.filter(|_| scores),
            });
        }
        Ok(RenderedHits {
            hits,
            omitted_bytes,
            excerpt_jobs,
            revision_us,
            selection_us,
        })
    }
}

/// A vector as numbers; every 16-bit value is exact as a 64-bit one.
fn vector_json(array: &ArrayRef, row: usize) -> Result<Value> {
    let list = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or(PublicationRowsInvalid)?;
    let items = list.value(row);
    let items = items
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or(PublicationRowsInvalid)?;
    Ok(Value::Array(
        items
            .values()
            .iter()
            .map(|item| item.to_f64().into())
            .collect(),
    ))
}

/// A typed attribute's value, as the JSON type of its kind.
fn scalar_json(array: &ArrayRef, kind: AttributeType, row: usize) -> Result<Value> {
    let any = array.as_any();
    Ok(match kind {
        AttributeType::String => Value::String(
            any.downcast_ref::<StringArray>()
                .ok_or(PublicationRowsInvalid)?
                .value(row)
                .to_owned(),
        ),
        AttributeType::Int => any
            .downcast_ref::<Int64Array>()
            .ok_or(PublicationRowsInvalid)?
            .value(row)
            .into(),
        AttributeType::Float => any
            .downcast_ref::<Float64Array>()
            .ok_or(PublicationRowsInvalid)?
            .value(row)
            .into(),
        AttributeType::Bool => any
            .downcast_ref::<BooleanArray>()
            .ok_or(PublicationRowsInvalid)?
            .value(row)
            .into(),
        AttributeType::Datetime => {
            let micros = any
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or(PublicationRowsInvalid)?
                .value(row);
            let time =
                chrono::DateTime::from_timestamp_micros(micros).ok_or(PublicationRowsInvalid)?;
            Value::String(McpTime(time).to_string())
        }
        AttributeType::Uint => any
            .downcast_ref::<UInt64Array>()
            .ok_or(PublicationRowsInvalid)?
            .value(row)
            .into(),
        AttributeType::Uuid => Value::String(format_uuid(
            any.downcast_ref::<FixedSizeBinaryArray>()
                .ok_or(PublicationRowsInvalid)?
                .value(row)
                .try_into()
                .map_err(|_| PublicationRowsInvalid)?,
        )),
        AttributeType::Bytes => Value::String(
            STANDARD.encode(
                any.downcast_ref::<BinaryArray>()
                    .ok_or(PublicationRowsInvalid)?
                    .value(row),
            ),
        ),
        AttributeType::Array(element) => {
            let items = any
                .downcast_ref::<ListArray>()
                .ok_or(PublicationRowsInvalid)?
                .value(row);
            Value::Array(
                (0..items.len())
                    .map(|item| scalar_json(&items, element.scalar(), item))
                    .collect::<Result<_>>()?,
            )
        }
    })
}
