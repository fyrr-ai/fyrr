use super::*;

/// Only physical line bounds: JSON-RPC parsing and error responses stay in rmcp.
struct BoundedStdin<R: tokio::io::AsyncRead + Unpin> {
    reader: tokio::io::BufReader<R>,
    line_bytes: usize,
}
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for BoundedStdin<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use tokio::io::AsyncBufRead;
        let this = self.get_mut();
        let available = futures::ready!(std::pin::Pin::new(&mut this.reader).poll_fill_buf(cx))?;
        let count = available
            .iter()
            .position(|b| *b == b'\n')
            .map_or(available.len(), |i| i + 1)
            .min(out.remaining());
        if this.line_bytes + count > 1_048_576 {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame_too_large",
            )));
        }
        let newline = count > 0 && available[count - 1] == b'\n';
        out.put_slice(&available[..count]);
        std::pin::Pin::new(&mut this.reader).consume(count);
        this.line_bytes = if newline { 0 } else { this.line_bytes + count };
        std::task::Poll::Ready(Ok(()))
    }
}
pub(super) struct StdioTransport<R: tokio::io::AsyncRead, W: tokio::io::AsyncWrite> {
    pub(super) inner: rmcp::transport::async_rw::AsyncRwTransport<RoleServer, R, W>,
    pub(super) slots: Arc<tokio::sync::Semaphore>,
    pub(super) cancellations: CancellationRegistry,
    pub(super) response_claims: StdioResponseClaims,
}
impl<R, W> rmcp::transport::Transport<RoleServer> for StdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    type Error = std::io::Error;
    fn send(
        &mut self,
        message: ServerJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let key = match &message {
            ServerJsonRpcMessage::Response(response) => Some(&response.id),
            ServerJsonRpcMessage::Error(error) => error.id.as_ref(),
            _ => None,
        }
        .cloned();
        let claims = key.and_then(|key| crate::sync::lock(&self.response_claims).remove(&key));
        let send = self.inner.send(message);
        async move {
            let _claims = claims;
            send.await
        }
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        crate::sync::lock(&self.response_claims).clear();
        self.inner.close().await
    }
    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        loop {
            let mut message = self.inner.receive().await?;
            if let ClientJsonRpcMessage::Request(request) = &mut message {
                let key = request.id.clone();
                let duplicate = crate::sync::lock(&self.response_claims).contains_key(&key);
                if duplicate {
                    // rmcp tracks requests by ID too; never let a duplicate overwrite
                    // an executing request's cancellation or response ownership.
                    let mut result = tool_error(Busy);
                    result.result_type = None;
                    self.inner
                        .send(ServerJsonRpcMessage::Response(JsonRpcResponse {
                            jsonrpc: Default::default(),
                            id: request.id.clone(),
                            result: ServerResult::CallToolResult(result),
                        }))
                        .await
                        .ok()?;
                    continue;
                }
                if let ClientRequest::CallToolRequest(call) = &request.request {
                    let parsed = ToolCall::parse(
                        &call.params.name,
                        Value::Object(call.params.arguments.clone().unwrap_or_default()),
                    );
                    let valid = parsed.is_ok();
                    request.request.extensions_mut().insert(parsed);
                    if valid {
                        let cancellation = CancellationToken::new();
                        let mut permit = self.slots.clone().try_acquire_owned().ok().map(Arc::new);
                        if permit.is_some() {
                            if let Ok(mut registry) = self.cancellations.lock() {
                                let key = ("stdio".into(), request.id.clone());
                                match registry.entry(key) {
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        entry.insert(cancellation.clone());
                                    }
                                    std::collections::hash_map::Entry::Occupied(_) => {
                                        permit = None;
                                    }
                                }
                            } else {
                                permit = None;
                            }
                        }
                        if let Some(permit) = &permit {
                            let claims = Arc::new(ResponseClaims::default());
                            *crate::sync::lock(&claims.admission) = Some(permit.clone());
                            crate::sync::lock(&self.response_claims)
                                .insert(request.id.clone(), claims.clone());
                            request.request.extensions_mut().insert(claims);
                        }
                        request.request.extensions_mut().insert(StdioAdmissionSlot {
                            permit,
                            cancellation,
                        });
                    }
                }
                request.request.extensions_mut().insert(Instant::now());
            }
            if let ClientJsonRpcMessage::Notification(notification) = &mut message
                && let ClientNotification::CancelledNotification(cancel) =
                    &notification.notification
            {
                // rmcp deliberately discards cancelled responses. fyrr's contract requires
                // a bounded `cancelled` result; route the SDK-parsed notification internally.
                let mut internal = CustomNotification::new("fyrr-internal-cancellation", None);
                internal.extensions.insert(cancel.params.clone());
                notification.notification = ClientNotification::CustomNotification(internal);
            }
            return Some(message);
        }
    }
}

pub(crate) async fn serve(reader: PreparedReader) -> Result<()> {
    if reader.config.reader.http.is_some() {
        return crate::http::serve(reader).await;
    }
    let service = start(reader).await?;
    let transport = StdioTransport {
        inner: rmcp::transport::async_rw::AsyncRwTransport::new_server(
            BoundedStdin {
                reader: tokio::io::BufReader::new(tokio::io::stdin()),
                line_bytes: 0,
            },
            tokio::io::stdout(),
        ),
        slots: service.slots.clone(),
        cancellations: service.cancellations.clone(),
        response_claims: Arc::default(),
    };
    service.run_stdio(transport).await
}

impl McpService {
    pub(super) async fn run_stdio<R, W>(self, transport: StdioTransport<R, W>) -> Result<()>
    where
        R: tokio::io::AsyncRead + Send + Unpin + 'static,
        W: tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let scheduler = self.sender.clone();
        tokio::select! {
            biased;
            result = async move {
                let running = self.serve(transport).await.map_err(|_| TransportUnavailable)?;
                running.waiting().await.map_err(|_| TransportUnavailable)?;
                Ok(())
            } => result,
            () = scheduler.closed() => Err(TransportUnavailable),
        }
    }
}
