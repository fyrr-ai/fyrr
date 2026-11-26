use super::*;
use std::fmt::Write as _;

impl PreparedReader {
    pub(super) async fn fetch_page(
        &self,
        args: &FetchInput,
        cursor: LookupCursor,
        mut telemetry: Telemetry,
        limits: FetchPageLimits,
        lookup: &RowAddrMask,
        shared_keys: Option<&RecordBatch>,
    ) -> Result<FetchOutput> {
        let (keys, stage, counters) = {
            let mut predicate = self.request_predicate()?;
            match &cursor.selector {
                FetchSelector::Uri(uri) => {
                    let _ = write!(predicate, " AND source_uri={}", sql(uri));
                }
                FetchSelector::SourceRoot { source, root } => {
                    let _ = write!(
                        predicate,
                        " AND source_id={} AND root_id={}",
                        sql(source),
                        sql(root)
                    );
                }
            }
            if let Some(window) = &args.window {
                let _ = write!(predicate, " AND record_id={}", sql(&window.record_id));
            }
            let stage = Instant::now();
            let counters = self.mirror.counters();
            if args.cursor.is_some() {
                self.append_cursor_boundary(&cursor, &mut predicate).await?;
            }

            let keys = if let Some(keys) = shared_keys {
                if args.cursor.is_some() {
                    return Err(StaleCursor);
                }
                let sources = strings(keys, "source_id")?;
                let roots = strings(keys, "root_id")?;
                let records = strings(keys, "record_id")?;
                let selected = arrow_array::BooleanArray::from(
                    (0..keys.num_rows())
                        .map(|row| match &cursor.selector {
                            FetchSelector::SourceRoot { source, root } => {
                                sources.value(row) == source
                                    && roots.value(row) == root
                                    && args
                                        .window
                                        .as_ref()
                                        .is_none_or(|window| records.value(row) == window.record_id)
                            }
                            FetchSelector::Uri(_) => false,
                        })
                        .collect::<Vec<_>>(),
                );
                let keys = arrow_select::filter::filter_record_batch(keys, &selected)
                    .map_err(|_| IncompatibleIndex)?;
                keys.slice(0, keys.num_rows().min(limits.records + 1))
            } else {
                // Look ahead using identities only; full text is fetched for at most 32
                // authorized rows.
                let mut scan = self.dataset.scan();
                scan.project(&["source_id", "root_id", "record_id", "record_order"])
                    .map_err(|_| IncompatibleIndex)?;
                scan.filter(&predicate)
                    .map_err(|_| IncompatibleIndex)?
                    .with_row_id()
                    .with_row_id_prefilter(lookup.clone())
                    // The explicit locator mask supplies the scan input. Scalar
                    // planning would build broad grant/source index expressions that
                    // this masked scan later discards; retain the exact SQL refinement.
                    .use_scalar_index(false);
                // Refine current grants, exact source/root/window and cursor over only
                // admitted index locators; authorization still precedes sort and limits.
                scan.order_by(Some(vec![
                    ordering("source_id", true),
                    ordering("root_id", true),
                    ordering("record_order", true),
                    ordering("record_id", true),
                ]))
                .map_err(|_| IncompatibleIndex)?;
                scan.limit(Some((limits.records + 1) as i64), None)
                    .map_err(|_| IncompatibleIndex)?;
                scan.try_into_batch()
                    .await
                    .map_err(|_| StorageUnavailable)?
            };
            (keys, stage, counters)
        };
        let positions = column::<UInt64Array>(&keys, "record_order", IncompatibleIndex)?;
        if positions.null_count() != 0 {
            return Err(IncompatibleIndex);
        }
        let mut row_ids = Vec::new();
        let mut roots = BTreeSet::new();
        for row in 0..keys.num_rows() {
            let key = (
                strings(&keys, "source_id")?.value(row).to_owned(),
                strings(&keys, "root_id")?.value(row).to_owned(),
            );
            if row_ids.len() == limits.records
                || (!roots.contains(&key) && roots.len() == FETCH_MAX_DOCUMENTS as usize)
            {
                break;
            }
            roots.insert(key);
            let id = ids(&keys)?.value(row);
            if !self.selected_rows().selected(id) {
                return Err(AuthorizationUnavailable);
            }
            row_ids.push(id);
        }
        self.stage(&mut telemetry, "fetch_lookup", stage, counters);
        let mut result = FetchOutput {
            documents: vec![],
            next_cursor: None,
            document_changed: false,
            window: None,
            metadata: self.metadata(telemetry),
        };
        if row_ids.is_empty() {
            return Ok(result);
        }
        let stage = Instant::now();
        let counters = self.mirror.counters();
        let mut columns = vec!["searchable_text", "metadata_json", "connector_map"];
        columns.extend(self.connector_columns());
        let batch = self
            .rows_by_id(&row_ids, &columns, IncompatibleIndex)
            .await?;
        self.stage(
            &mut result.metadata.telemetry,
            "fetch_hydration",
            stage,
            counters,
        );
        self.format_fetch_page(args, cursor, limits, (keys, batch, row_ids), result)
            .await
    }

    async fn append_cursor_boundary(
        &self,
        cursor: &LookupCursor,
        predicate: &mut String,
    ) -> Result<()> {
        let (source, root, record) = &cursor.position.after;
        // The authenticated cursor names a record already returned to this subject.
        // Resolve only its internal order, even if that record is now denied. This
        // permits continuation under new grants without exposing sibling ordinals.
        let mut lookup = self.dataset.scan();
        lookup
            .project(&["record_order"])
            .map_err(|_| IncompatibleIndex)?;
        lookup
            .filter(&format!(
                "source_id={} AND root_id={} AND record_id={}",
                sql(source),
                sql(root),
                sql(record)
            ))
            .map_err(|_| IncompatibleIndex)?;
        lookup.limit(Some(1), None).map_err(|_| IncompatibleIndex)?;
        let key = lookup
            .try_into_batch()
            .await
            .map_err(|_| StorageUnavailable)?;
        let positions = column::<UInt64Array>(&key, "record_order", IncompatibleIndex)?;
        if positions.len() != 1 || positions.is_null(0) {
            return Err(StaleCursor);
        }
        let op = if cursor.position.offset > 0 {
            ">="
        } else {
            ">"
        };
        let _ = write!(
            predicate,
            " AND (source_id>{s} OR (source_id={s} AND root_id>{r}) OR (source_id={s} AND \
                root_id={r} AND record_order{op}{i}))",
            s = sql(source),
            r = sql(root),
            i = positions.value(0)
        );

        Ok(())
    }

    async fn format_fetch_page(
        &self,
        args: &FetchInput,
        mut cursor: LookupCursor,
        limits: FetchPageLimits,
        (keys, batch, row_ids): (RecordBatch, RecordBatch, Vec<u64>),
        mut result: FetchOutput,
    ) -> Result<FetchOutput> {
        let rows: BTreeMap<_, _> = ids(&batch)?
            .values()
            .iter()
            .enumerate()
            .map(|(row, id)| (*id, row))
            .collect();
        let format_start = Instant::now();
        let format_counters = self.mirror.counters();
        let mut used = 0;
        let mut more = keys.num_rows() > row_ids.len();
        for id in row_ids {
            let row = *rows.get(&id).ok_or(StorageUnavailable)?;
            // Identities came from the authorized lookup at the same immutable
            // pin. Do not read their columns a second time during payload take.
            let key = ids(&keys)?
                .values()
                .iter()
                .position(|candidate| *candidate == id)
                .ok_or(StorageUnavailable)?;
            let source = strings(&keys, "source_id")?.value(key);
            let root = strings(&keys, "root_id")?.value(key);
            let record = strings(&keys, "record_id")?.value(key);
            let full = strings(&batch, "searchable_text")?.value(row);
            let token = revision(source, root, record, full);
            let (text, span_start) = if let Some(window) = &args.window {
                if token.as_str() != window.revision.as_str() {
                    result.document_changed = true;
                    if !limits.list {
                        return Ok(result);
                    }
                }
                let length = full.chars().count();
                if !result.document_changed
                    && (window.start as usize > length
                        || window.end.is_some_and(|end| end as usize > length))
                {
                    return Err(InvalidArgument);
                }
                let (begin, end) = if result.document_changed {
                    (0, 0)
                } else {
                    match (window.end, window.context) {
                        (Some(end), None) => (window.start as usize, end as usize),
                        (None, Some(context)) => (
                            (window.start as usize).saturating_sub(context as usize),
                            (window.start as usize + context as usize).min(length),
                        ),
                        _ => return Err(InvalidArgument),
                    }
                };
                let begin = if limits.list
                    && !result.document_changed
                    && !cursor.position.after.0.is_empty()
                {
                    begin.max(cursor.position.offset)
                } else {
                    begin
                };
                let begin_byte = full
                    .char_indices()
                    .nth(begin)
                    .map_or(full.len(), |(i, _)| i);
                let end_byte = full.char_indices().nth(end).map_or(full.len(), |(i, _)| i);
                (&full[begin_byte..end_byte], begin)
            } else {
                let offset = if cursor.position.after == (source.into(), root.into(), record.into())
                {
                    cursor.position.offset
                } else {
                    0
                };
                let begin = full
                    .char_indices()
                    .nth(offset)
                    .map_or(full.len(), |(i, _)| i);
                (&full[begin..], offset)
            };

            let served = clip(text, limits.bytes - used);
            if served.is_empty() && !text.is_empty() {
                more = true;
                break;
            }
            self.append_fetch_document(source, root, row, &batch, &mut result)
                .await?;
            if !result.document_changed {
                result
                    .documents
                    .last_mut()
                    .ok_or(StorageUnavailable)?
                    .records
                    .push(FetchedRecord {
                        record_id: record.into(),
                        revision: token,
                        text: served.into(),
                    });
            }
            used += served.len();
            cursor.position.after = (source.into(), root.into(), record.into());
            cursor.position.offset = if served.len() < text.len() {
                span_start + served.chars().count()
            } else {
                0
            };
            result.metadata.omitted_bytes += (text.len() - served.len()) as u64;
            if args.window.is_some() {
                if !result.document_changed {
                    result.window = Some(ServedWindow {
                        record_id: record.into(),
                        start: u32::try_from(span_start).map_err(|_| StorageUnavailable)?,
                        end: u32::try_from(span_start + served.chars().count())
                            .map_err(|_| StorageUnavailable)?,
                    });
                }
                if limits.list && served.len() < text.len() {
                    result.next_cursor = Some(self.encode_cursor(&cursor)?);
                }
                self.stage(
                    &mut result.metadata.telemetry,
                    "fetch_records",
                    format_start,
                    format_counters,
                );
                return Ok(result);
            }
            if served.len() < text.len() {
                more = true;
                break;
            }
        }
        if more {
            result.next_cursor = Some(self.encode_cursor(&cursor)?);
        }
        self.stage(
            &mut result.metadata.telemetry,
            "fetch_records",
            format_start,
            format_counters,
        );
        Ok(result)
    }

    async fn append_fetch_document(
        &self,
        source: &str,
        root: &str,
        row: usize,
        batch: &RecordBatch,
        result: &mut FetchOutput,
    ) -> Result<()> {
        let common: Option<DocumentMetadata> =
            serde_json::from_str(strings(batch, "metadata_json")?.value(row))
                .map_err(|_| StorageUnavailable)?;
        let common = self.with_connector_columns(batch, row, common.ok_or(StorageUnavailable)?)?;
        if result
            .documents
            .last()
            .is_none_or(|d| d.source_id != source || d.root_id != root)
        {
            let maps: &arrow_array::BinaryArray =
                column(batch, "connector_map", PublicationRowsInvalid)?;
            let connector_map = if maps.is_null(row) {
                None
            } else {
                Some(serde_json::from_slice(maps.value(row)).map_err(|_| StorageUnavailable)?)
            };
            result.documents.push(FetchedDocument {
                document_changed: None,
                window: None,
                source_id: source.into(),
                root_id: root.into(),
                source_uri: common.source_uri,
                source_type: common.source_type,
                title: common.title,
                author: common.author,
                team: common.team,
                project: common.project,
                status: common.status,
                labels: common.labels,
                created_at: common.created_at.map(McpTime),
                updated_at: common.updated_at.map(McpTime),
                connector_map,
                records: vec![],
            });
        }

        Ok(())
    }
}
