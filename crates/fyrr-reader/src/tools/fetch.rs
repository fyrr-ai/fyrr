use super::*;

impl PreparedReader {
    pub(super) fn encode_cursor(&self, cursor: &impl Serialize) -> Result<String> {
        let payload = serde_json::to_string(cursor).map_err(|_| StorageUnavailable)?;
        let mac = hash_principal(&self.config, "cursor", &format!("fyrr-fetch-1:{payload}"))?;
        let encoded = format!("{payload}.{}", &mac[7..]);
        if encoded.len() > fyrr_core::limits::mcp::MAX_CURSOR_BYTES {
            return Err(InvalidArgument);
        }
        Ok(encoded)
    }
    pub(super) fn decode_cursor(&self, value: &str) -> Result<FetchCursor> {
        let (payload, signature) = value.rsplit_once('.').ok_or(StaleCursor)?;
        crate::storage::verify_hash(
            &self.config,
            "cursor",
            &format!("fyrr-fetch-1:{payload}"),
            &hex::decode(signature).map_err(|_| StaleCursor)?,
        )?;
        let cursor: FetchCursor = serde_json::from_str(payload).map_err(|_| StaleCursor)?;
        let subject = self.principal()?;
        if cursor.scope().namespace != self.config.namespace.name
            || cursor.scope().subject != subject
            || cursor.scope().content_version != self.manifest.lance_version
            || cursor.scope().expires <= Utc::now().timestamp()
            || self
                .head
                .content
                .as_ref()
                .is_none_or(|p| p.lance_version != cursor.scope().content_version)
        {
            return Err(StaleCursor);
        }
        Ok(cursor)
    }
    pub async fn fetch(&mut self, args: &FetchInput) -> Result<FetchOutput> {
        args.validate().map_err(|_| InvalidArgument)?;
        let (start, before, telemetry, _pin) = self.request_prologue().await?;
        let outcome = self.fetch_admitted(args, telemetry).await;
        let stage = Instant::now();
        let counters = self.mirror.counters();
        if self.refresh_authorization().await? {
            return Err(AuthorizationUnavailable);
        }
        let mut result = outcome?;
        self.stage(
            &mut result.metadata.telemetry,
            "output_authorization",
            stage,
            counters,
        );
        self.finish_metadata(&mut result.metadata, start, before);
        Ok(result)
    }
    pub(super) async fn fetch_admitted(
        &self,
        args: &FetchInput,
        mut telemetry: Telemetry,
    ) -> Result<FetchOutput> {
        let cursor = if let Some(value) = &args.cursor {
            self.decode_cursor(value)?
        } else {
            let scope = CursorScope {
                namespace: self.config.namespace.name.clone(),
                subject: self.principal()?,
                content_version: self.manifest.lance_version,
                expires: Utc::now().timestamp() + 900,
            };
            let position = CursorPosition {
                after: (String::new(), String::new(), String::new()),
                offset: 0,
            };
            if let Some(documents) = args.documents.clone() {
                FetchCursor::Documents(DocumentsCursor {
                    scope,
                    documents,
                    document_index: 0,
                    position,
                })
            } else {
                let selector = if let Some(uri) = &args.uri {
                    FetchSelector::Uri(uri.trim().to_owned())
                } else {
                    FetchSelector::SourceRoot {
                        source: args.source_id.clone().ok_or(InvalidArgument)?,
                        root: args.root_id.clone().ok_or(InvalidArgument)?,
                    }
                };
                FetchCursor::Lookup(LookupCursor {
                    scope,
                    selector,
                    position,
                })
            }
        };
        let cursor = match cursor {
            FetchCursor::Documents(cursor) => return self.fetch_documents(cursor, telemetry).await,
            FetchCursor::Lookup(cursor) => cursor,
        };
        let lookup = match (&cursor.selector, &args.window) {
            (FetchSelector::SourceRoot { root, .. }, Some(window)) => format!(
                "root_id={} AND record_id={}",
                sql(root),
                sql(&window.record_id)
            ),
            (FetchSelector::Uri(uri), None) => format!("source_uri={}", sql(uri)),
            (FetchSelector::SourceRoot { root, .. }, None) => format!("root_id={}", sql(root)),
            (FetchSelector::Uri(_), Some(_)) => return Err(StaleCursor),
        };
        let lookup = self.fetch_index_mask(&lookup, &mut telemetry).await?;
        self.fetch_page(
            args,
            cursor,
            telemetry,
            FetchPageLimits {
                records: FETCH_MAX_RECORDS as usize,
                bytes: FETCH_MAX_CONTENT_BYTES as usize,
                list: false,
            },
            &lookup,
            None,
        )
        .await
    }

    pub(super) async fn fetch_documents(
        &self,
        mut cursor: DocumentsCursor,
        mut telemetry: Telemetry,
    ) -> Result<FetchOutput> {
        let documents = cursor.documents.clone();
        let validation = FetchInput {
            documents: Some(documents.clone()),
            uri: None,
            source_id: None,
            root_id: None,
            cursor: None,
            window: None,
            deadline_ms: None,
        };
        validation.validate().map_err(|_| StaleCursor)?;
        if cursor.document_index >= documents.len() {
            return Err(StaleCursor);
        }
        // Resolve only opaque IDs from selective root/record indexes once for
        // the list. Broad source/grant indexes otherwise materialize hundreds
        // of thousands of IDs for every document, even with a root index.
        let lookup = documents[cursor.document_index..]
            .iter()
            .map(|document| {
                document.window.as_ref().map_or_else(
                    || format!("root_id={}", sql(&document.root_id)),
                    |window| {
                        format!(
                            "(root_id={} AND record_id={})",
                            sql(&document.root_id),
                            sql(&window.record_id)
                        )
                    },
                )
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let lookup = self.fetch_index_mask(&lookup, &mut telemetry).await?;
        // Share one authorized identity query for small first-page lists. The
        // bound keeps identity scratch small; wide roots and cursors retain the
        // ordinary sorted, limited scan path.
        let shared_keys = self
            .shared_document_keys(&cursor, &lookup, &mut telemetry)
            .await?;
        let mut result = FetchOutput {
            documents: vec![],
            next_cursor: None,
            document_changed: false,
            window: None,
            metadata: self.metadata(telemetry),
        };
        let mut records = 0;
        let mut bytes = 0;
        while cursor.document_index < documents.len() {
            if records == FETCH_MAX_RECORDS as usize || bytes == FETCH_MAX_CONTENT_BYTES as usize {
                break;
            }
            let document = &documents[cursor.document_index];
            let mut child = LookupCursor {
                scope: cursor.scope.clone(),
                selector: FetchSelector::SourceRoot {
                    source: document.source_id.clone(),
                    root: document.root_id.clone(),
                },
                position: cursor.position.clone(),
            };
            let continuation = !child.position.after.0.is_empty();
            let args = FetchInput {
                documents: None,
                uri: None,
                source_id: Some(document.source_id.clone()),
                root_id: Some(document.root_id.clone()),
                window: document.window.clone(),
                deadline_ms: None,
                cursor: if continuation {
                    Some(self.encode_cursor(&child)?)
                } else {
                    None
                },
            };
            // Each lookup uses the current request's authorization predicate and mask,
            // including documents first reached on a continuation page.
            let page = self
                .fetch_page(
                    &args,
                    child.clone(),
                    result.metadata.telemetry.clone(),
                    FetchPageLimits {
                        records: FETCH_MAX_RECORDS as usize - records,
                        bytes: FETCH_MAX_CONTENT_BYTES as usize - bytes,
                        list: true,
                    },
                    &lookup,
                    shared_keys.as_ref(),
                )
                .await?;
            let omitted = result.metadata.omitted_bytes;
            result.metadata = page.metadata;
            result.metadata.omitted_bytes += omitted;
            for mut fetched in page.documents {
                records += fetched.records.len();
                bytes += fetched.records.iter().map(|r| r.text.len()).sum::<usize>();
                fetched.document_changed = Some(page.document_changed);
                fetched.window.clone_from(&page.window);
                result.documents.push(fetched);
            }
            if let Some(next) = page.next_cursor {
                child = match self.decode_cursor(&next)? {
                    FetchCursor::Lookup(cursor) => cursor,
                    FetchCursor::Documents(_) => return Err(StaleCursor),
                };
                cursor.position.after = child.position.after;
                cursor.position.offset = child.position.offset;
                break;
            }
            cursor.document_index += 1;
            cursor.position.after = (String::new(), String::new(), String::new());
            cursor.position.offset = 0;
        }
        if cursor.document_index < documents.len() {
            result.next_cursor = Some(self.encode_cursor(&cursor)?);
        }
        Ok(result)
    }

    pub(super) async fn fetch_index_mask(
        &self,
        predicate: &str,
        telemetry: &mut Telemetry,
    ) -> Result<RowAddrMask> {
        self.authorization()?;
        let start = Instant::now();
        let counters = self.mirror.counters();
        let mut scan = self.dataset.scan();
        // No document columns, sort or limit before row admission. An index
        // match is only a locator, never permission to read the row.
        scan.empty_project().map_err(|_| IncompatibleIndex)?;
        scan.filter(predicate)
            .map_err(|_| IncompatibleIndex)?
            .with_row_id()
            .batch_size(4096)
            .batch_readahead(1);
        let mut stream = scan
            .try_into_stream()
            .await
            .map_err(|_| StorageUnavailable)?;
        let mut allowed = RowAddrTreeMap::new();
        let mut matches = 0;
        while let Some(batch) = stream.try_next().await.map_err(|_| StorageUnavailable)? {
            matches += batch.num_rows();
            if matches > 65_536 {
                // A broad locator falls back to the resident admitted mask.
                // This is slower, but bounds the new mask without truncating
                // raw matches before authorization or starving allowed rows.
                self.stage(telemetry, "fetch_lookup", start, counters);
                return Ok(self.selected_rows().clone());
            }
            for id in ids(&batch)?.values() {
                if self.selected_rows().selected(*id) {
                    allowed.insert(*id);
                }
            }
        }
        self.stage(telemetry, "fetch_lookup", start, counters);
        Ok(RowAddrMask::from_allowed(allowed))
    }
    async fn shared_document_keys(
        &self,
        cursor: &DocumentsCursor,
        lookup: &RowAddrMask,
        telemetry: &mut Telemetry,
    ) -> Result<Option<RecordBatch>> {
        Ok(
            if cursor.document_index == 0
                && cursor.position.after.0.is_empty()
                && lookup
                    .iter_addrs()
                    .is_some_and(|rows| rows.take(129).count() <= 128)
            {
                let start = Instant::now();
                let counters = self.mirror.counters();
                let mut scan = self.dataset.scan();
                scan.project(&["source_id", "root_id", "record_id", "record_order"])
                    .map_err(|_| IncompatibleIndex)?;
                scan.filter(&self.request_predicate()?)
                    .map_err(|_| IncompatibleIndex)?
                    .with_row_id()
                    .with_row_id_prefilter(lookup.clone())
                    .use_scalar_index(false);
                scan.order_by(Some(vec![
                    ordering("source_id", true),
                    ordering("root_id", true),
                    ordering("record_order", true),
                    ordering("record_id", true),
                ]))
                .map_err(|_| IncompatibleIndex)?;
                let keys = scan
                    .try_into_batch()
                    .await
                    .map_err(|_| StorageUnavailable)?;
                self.stage(telemetry, "fetch_lookup", start, counters);
                Some(keys)
            } else {
                None
            },
        )
    }
}
