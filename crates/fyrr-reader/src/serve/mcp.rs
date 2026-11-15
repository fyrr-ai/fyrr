use super::*;

pub(super) fn tool_error(code: ReaderError) -> CallToolResult {
    let code = code.mcp_code();
    CallToolResult::structured_error(json!(ToolError {
        code,
        message: code.meaning().into(),
        retryable: code.retryable(),
    }))
}
#[derive(Clone, Debug)]
pub(crate) enum ToolCall {
    Search(SearchInput),
    Fetch(FetchInput),
    ListFilters(ListFiltersInput),
}
impl ToolCall {
    pub(crate) fn parse(name: &str, args: Value) -> Result<Self> {
        let call = match name {
            "search" => Self::Search(serde_json::from_value(args).map_err(|_| InvalidArgument)?),
            "fetch" => Self::Fetch(serde_json::from_value(args).map_err(|_| InvalidArgument)?),
            "list_filters" => {
                Self::ListFilters(serde_json::from_value(args).map_err(|_| InvalidArgument)?)
            }
            _ => return Err(InvalidArgument),
        };
        match &call {
            Self::Search(args) => args.validate().map_err(|_| InvalidArgument)?,
            Self::Fetch(args) => args.validate().map_err(|_| InvalidArgument)?,
            Self::ListFilters(args) => args.validate().map_err(|_| InvalidArgument)?,
        }
        Ok(call)
    }
    pub(crate) fn deadline_ms(&self) -> u32 {
        match self {
            Self::Search(args) => args.deadline_ms,
            Self::Fetch(args) => args.deadline_ms,
            Self::ListFilters(args) => args.deadline_ms,
        }
        .unwrap_or(fyrr_core::limits::mcp::DEFAULT_DEADLINE_MS)
    }
}
pub(super) async fn execute(
    mut reader: PreparedReader,
    job: Job,
    context: RequestTiming,
) -> (
    PreparedReader,
    Option<(crate::search::DenseCompletion, Option<i64>)>,
) {
    let mut dense = None;
    let request = &job.request;
    let wait = u64::try_from(context.arrived.elapsed().as_millis()).unwrap_or(u64::MAX);
    let outcome = if context.cancelled.is_cancelled() {
        Err(Cancelled)
    } else if Instant::now() >= context.deadline {
        Err(DeadlineExceeded)
    } else {
        reader.mirror.start_request();
        let work = async {
            #[cfg(test)]
            reader.test_hooks.native_work.wait_once().await;
            match request {
                ToolCall::Search(args) => reader
                    .search_input(
                        args,
                        Some((&mut dense, job.recall_query_index)),
                        Some(context.deadline.into_std()),
                    )
                    .await
                    .and_then(|output| {
                        serde_json::to_value(output).map_err(|_| StorageUnavailable)
                    }),
                ToolCall::Fetch(args) => reader
                    .fetch(args)
                    .await
                    .and_then(|r| serde_json::to_value(r).map_err(|_| StorageUnavailable)),
                ToolCall::ListFilters(args) => reader
                    .list_filters(args)
                    .await
                    .and_then(|r| serde_json::to_value(r).map_err(|_| StorageUnavailable)),
            }
        };
        let result = bounded(&context, work).await;
        settle(&mut reader, result).await
    };
    let outcome = if job
        .subject
        .as_ref()
        .is_some_and(|s| s.expires <= chrono::Utc::now().timestamp())
    {
        Err(AuthorizationUnavailable)
    } else {
        outcome
    };
    #[cfg(test)]
    reader.test_hooks.completion.wait().await;
    let successful = outcome.is_ok() && !context.cancelled.is_cancelled();
    let expires = job.subject.as_ref().map(|subject| subject.expires);
    *crate::sync::lock(&job.response_claims.scratch) = reader.response_claim.take();
    let result = match outcome {
        Ok(mut value) => {
            if job.subject.is_some() {
                value["metadata"]["telemetry"]["stage_us"]["authorization_cache_build"] =
                    json!(context.authorization_build_us);
            }
            value["metadata"]["telemetry"]["queue_wait_ms"] = json!(wait);
            value["metadata"]["telemetry"]["stage_us"]["total"] =
                json!(micros(context.arrived.elapsed()));
            if matches!(request, ToolCall::Search(_)) {
                if let Some(max) = reader.config.reader.max_response_bytes {
                    let fitted = serde_json::from_value(value)
                        .map_err(|_| StorageUnavailable)
                        .and_then(|mut output| {
                            crate::response_budget::fit_response(&mut output, max)?;
                            serde_json::to_value(output).map_err(|_| StorageUnavailable)
                        });
                    match fitted {
                        Ok(value) => CallToolResult::structured(value),
                        Err(code) => tool_error(code),
                    }
                } else {
                    CallToolResult::structured(value)
                }
            } else {
                CallToolResult::structured(value)
            }
        }
        Err(code) => tool_error(code),
    };
    #[cfg(test)]
    reader.test_hooks.observe_admission(&reader, false);
    let sent = job.reply.send(result).is_ok();
    reader.execution_slot.take();
    (
        reader,
        if successful && sent {
            dense.map(|completion| (completion, expires))
        } else {
            None
        },
    )
}

impl McpService {
    pub(crate) fn cancel(&self, subject: &str, id: &RequestId) {
        if let Ok(registry) = self.cancellations.lock()
            && let Some(token) = registry.get(&(subject.to_owned(), id.clone()))
        {
            token.cancel();
        }
    }
    pub(crate) async fn publication(&self) -> Value {
        let mut readiness = self.publication.read().await.clone();
        if self.sender.is_closed() {
            readiness.ready = false;
        }
        readiness.into_wire()
    }
    pub(crate) async fn refresh(&self) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        self.reload.try_send(tx).map_err(|_| Busy)?;
        rx.await.map_err(|_| StorageUnavailable)?
    }
}
impl ServerHandler for McpService {
    fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
        std::borrow::Cow::Owned(vec![
            ProtocolVersion::V_2025_11_25,
            ProtocolVersion::V_2025_06_18,
            ProtocolVersion::V_2025_03_26,
        ])
    }

    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.protocol_version = ProtocolVersion::V_2025_11_25;
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info = Implementation::new("fyrr", env!("CARGO_PKG_VERSION"));
        info
    }
    async fn on_cancelled(
        &self,
        notification: CancelledNotificationParam,
        context: rmcp::service::NotificationContext<RoleServer>,
    ) {
        let subject = from_http::<crate::auth::AuthenticatedSubject>(&context.extensions)
            .map_or_else(|| "stdio".into(), |s| s.key);
        if let Some(id) = notification.request_id {
            self.cancel(&subject, &id);
        }
    }
    async fn on_custom_notification(
        &self,
        notification: CustomNotification,
        context: rmcp::service::NotificationContext<RoleServer>,
    ) {
        if notification.method == "fyrr-internal-cancellation"
            && let Some(cancel) = context
                .extensions
                .get::<CancelledNotificationParam>()
                .cloned()
        {
            self.on_cancelled(cancel, context).await;
        }
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        serde_json::from_value(fyrr_core::mcp::tools_list())
            .map_err(|_| rmcp::ErrorData::internal_error("storage_unavailable", None))
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        let subject = from_http::<crate::auth::AuthenticatedSubject>(&context.extensions);
        if self.http && subject.is_none() {
            return tool_response(tool_error(AuthorizationUnavailable));
        }
        let arrived = from_http::<Instant>(&context.extensions)
            .or_else(|| context.extensions.get::<Instant>().copied())
            .unwrap_or_else(Instant::now);
        let parsed = context
            .extensions
            .get::<Result<ToolCall>>()
            .cloned()
            .or_else(|| from_http::<Result<ToolCall>>(&context.extensions));
        let call = match parsed.unwrap_or_else(|| {
            ToolCall::parse(
                &request.name,
                Value::Object(request.arguments.unwrap_or_default()),
            )
        }) {
            Ok(call) => call,
            Err(code) => return tool_response(tool_error(code)),
        };
        let ms = call.deadline_ms();
        let permit = if let Some(slot) = context.extensions.get::<StdioAdmissionSlot>() {
            match &slot.permit {
                Some(p) => p.clone(),
                None => {
                    return tool_response(tool_error(Busy));
                }
            }
        } else {
            match self.slots.clone().try_acquire_owned() {
                Ok(p) => Arc::new(p),
                Err(_) => {
                    return tool_response(tool_error(Busy));
                }
            }
        };
        let response_claims = context
            .extensions
            .get::<Arc<ResponseClaims>>()
            .cloned()
            .or_else(|| from_http::<Arc<ResponseClaims>>(&context.extensions))
            .unwrap_or_default();
        *crate::sync::lock(&response_claims.admission) = Some(permit.clone());
        let key = (
            subject
                .as_ref()
                .map_or_else(|| "stdio".into(), |s| s.key.clone()),
            context.id.clone(),
        );
        let cancellation = if let Some(slot) = context.extensions.get::<StdioAdmissionSlot>() {
            slot.cancellation.clone()
        } else {
            let mut registry = self
                .cancellations
                .lock()
                .map_err(|_| rmcp::ErrorData::internal_error("authorization_unavailable", None))?;
            if registry.contains_key(&key) {
                return tool_response(tool_error(InvalidArgument));
            }
            registry.insert(key.clone(), context.ct.clone());
            context.ct.clone()
        };
        let _registration = CancellationRegistration {
            key,
            registry: self.cancellations.clone(),
            token: cancellation.clone(),
        };
        let (reply, response) = oneshot::channel();
        let job = Job {
            request: call,
            reply,
            timing: RequestTiming {
                arrived,
                deadline: arrived + Duration::from_millis(u64::from(ms)),
                cancelled: cancellation,
                version_wait_until: arrived,
                authorization_build_us: 0,
            },
            subject,
            _permit: permit,
            response_claims: response_claims.clone(),
            recall_query_index: 0,
        };
        if self.sender.try_send(job).is_err() {
            return tool_response(tool_error(Busy));
        }
        tool_response(
            response
                .await
                .unwrap_or_else(|_| tool_error(StorageUnavailable)),
        )
    }
}
/// A value the HTTP transport stored on the request parts; stdio has none.
pub(super) fn from_http<T: Clone + Send + Sync + 'static>(
    extensions: &rmcp::model::Extensions,
) -> Option<T> {
    extensions
        .get::<axum::http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<T>())
        .cloned()
}

pub(super) fn tool_response(
    mut value: CallToolResult,
) -> Result<CallToolResponse, rmcp::ErrorData> {
    // Preserve the original wire contract; SDK builders default to an explicit result type.
    value.result_type = None;
    Ok(value.into())
}
