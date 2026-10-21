// SPDX-License-Identifier: Apache-2.0
//! Authenticated finite Flight service. Payload work starts only after request EOF.

use super::{
    attempts::{Attempt, Connection, Principal, Registry},
    payload::{Sending, native},
    queue::Queue,
    transport::{Call, IDLE, call, control, status},
    *,
};
use crate::{
    resident_memory_source::{ResidentMemorySource, exchange_bridge::Fixture},
    resident_session::ResidentVortexSession,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    flight_descriptor::DescriptorType,
    flight_service_server::FlightService,
    sql::{
        CommandStatementQuery, ProstMessageExt as _, SqlInfo, TicketStatementQuery,
        server::FlightSqlService,
    },
};
use futures::{Stream, stream};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub(crate) struct Service(Arc<Shared>);

struct Shared {
    session: ResidentVortexSession,
    fixture: Arc<Fixture>,
    source: ResidentMemorySource,
    registry: Registry,
    queue: Queue,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    reports: Mutex<Vec<Value>>,
}

type DataResult = std::result::Result<FlightData, Status>;

struct DataStream {
    receiver: mpsc::Receiver<DataResult>,
    attempt: Arc<Attempt>,
    connection: Connection,
    progress: watch::Sender<Progress>,
    ended: bool,
}

#[derive(Clone, Copy)]
struct Progress {
    last: Instant,
    finished: bool,
    failed: bool,
}

impl DataStream {
    fn progress(&self, finished: bool, failed: bool) {
        self.progress.send_replace(Progress {
            last: Instant::now(),
            finished,
            failed,
        });
    }
}

impl Stream for DataStream {
    type Item = DataResult;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if let Err(error) = self.attempt.check() {
            self.receiver.close();
            while self.receiver.try_recv().is_ok() {}
            self.connection.abort();
            self.ended = true;
            self.progress(true, true);
            return Poll::Ready(Some(Err(status(error))));
        }
        let next = self.receiver.poll_recv(context);
        if matches!(next, Poll::Ready(None)) {
            self.ended = true;
            if let Err(error) = self.attempt.complete() {
                self.progress(true, true);
                self.connection.abort();
                return Poll::Ready(Some(Err(status(error))));
            }
            self.progress(true, false);
        } else if matches!(next, Poll::Ready(Some(_))) {
            self.progress(false, false);
        }
        next
    }
}

impl Drop for DataStream {
    fn drop(&mut self) {
        if !self.ended {
            self.attempt.cancel();
            self.connection.abort();
            self.progress(true, true);
        }
    }
}

impl Service {
    pub(crate) fn new(
        session: ResidentVortexSession,
        fixture: Arc<Fixture>,
        source: ResidentMemorySource,
        registry: Registry,
    ) -> Self {
        let queue = Queue::new(session.shared_memory_pool());
        Self(Arc::new(Shared {
            session,
            fixture,
            source,
            registry,
            queue,
            tasks: Mutex::new(Vec::new()),
            reports: Mutex::new(Vec::new()),
        }))
    }

    pub(crate) fn snapshot(&self) -> Result<Value> {
        let (active, attempts, version) = self.0.registry.snapshot()?;
        let (queue_bytes, queue_frames) = self.0.queue.snapshot();
        let memory = self.0.session.snapshot().memory;
        let reports = self
            .0
            .reports
            .lock()
            .map_err(|_| invalid("service reports are poisoned"))?;
        Ok(
            json!({"active_attempts": active, "attempt_ids": attempts, "policy_version": version,
            "queued_bytes": queue_bytes, "queued_frames": queue_frames, "reserved_bytes": memory.reserved_bytes,
            "peak_reserved_bytes": memory.peak_reserved_bytes, "transfers": *reports}),
        )
    }

    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> Result<()> {
        let mut tasks = self
            .0
            .tasks
            .lock()
            .map_err(|_| invalid("owned task registry is poisoned"))?;
        if tasks.len() >= 256 {
            return Err(limit("owned task registry is full"));
        }
        tasks.push(tokio::spawn(future));
        Ok(())
    }

    pub(crate) async fn drain(&self) -> Result<()> {
        let mut tasks = {
            let mut tasks = self
                .0
                .tasks
                .lock()
                .map_err(|_| invalid("owned task registry is poisoned"))?;
            std::mem::take(&mut *tasks)
        };
        let result = tokio::time::timeout(IDLE, futures::future::join_all(tasks.iter_mut())).await;
        match result {
            Ok(joined) if joined.iter().all(std::result::Result::is_ok) => Ok(()),
            _ => {
                for task in &tasks {
                    task.abort();
                }
                // Timeout is always a failed drain, even if abort subsequently finishes.
                let _ = tokio::time::timeout(IDLE, futures::future::join_all(tasks)).await;
                Err(Error {
                    code: "SLX_DRAIN",
                    detail: "owned payload tasks did not drain cleanly",
                })
            }
        }
    }

    fn response(
        &self,
        sending: Sending,
        attempt: Arc<Attempt>,
        call: Call,
        request_bytes: u64,
    ) -> Result<DataStream> {
        let (sender, receiver) = mpsc::channel(4);
        let (progress, observed) = watch::channel(Progress {
            last: Instant::now(),
            finished: false,
            failed: false,
        });
        let queue = self.0.queue.clone();
        let service = self.clone();
        let stream = DataStream {
            receiver,
            attempt: attempt.clone(),
            connection: call.connection.clone(),
            progress,
            ended: false,
        };
        self.spawn(async move {
            let mut sending = sending;
            let produced = produce(&mut sending, &queue, &attempt, &sender).await;
            let send_report = sending.report.clone();
            if produced.is_err() { attempt.cancel(); }
            drop((sending, sender));
            let outcome = match produced {
                Ok(()) => wait_consumed(observed, &attempt).await,
                Err(error) => Err(error),
            };
            if outcome.is_err() { attempt.cancel(); }
            let report = json!({"principal": call.principal.name(), "source_id": attempt.binding.source_id,
                "query_id": attempt.binding.query_id, "policy_version": attempt.binding.policy_version,
                "local_socket": call.local.to_string(), "remote_socket": call.remote.to_string(),
                "sender_pid": std::process::id(), "request_application_wire_bytes": request_bytes,
                "send": send_report, "success": outcome.is_ok(),
                "error": outcome.as_ref().err().map(|error| json!({"code": error.code, "detail": error.detail}))});
            if let Ok(mut reports) = service.0.reports.lock() { reports.push(report); }
            if outcome.is_err() {
                // Closing IO wakes Tonic even when HTTP/2 flow control stopped polling
                // the body; cancellation must not depend on the slow peer reading again.
                call.connection.abort();
            }
        })?;
        Ok(stream)
    }
}

async fn produce(
    sending: &mut Sending,
    queue: &Queue,
    attempt: &Arc<Attempt>,
    sender: &mpsc::Sender<DataResult>,
) -> Result<()> {
    loop {
        let next = tokio::select! {
            () = attempt.cancelled() => return Err(Error { code: "SLX_CANCELLED", detail: "payload production cancelled" }),
            () = sender.closed() => return Err(Error { code: "SLX_CANCELLED", detail: "response consumer closed" }),
            next = tokio::time::timeout(IDLE, sending.next(queue, attempt)) => next
                .map_err(|_| Error { code: "SLX_IDLE", detail: "payload production exceeded idle limit" })??,
        };
        let Some(frame) = next else {
            return Ok(());
        };
        tokio::select! {
            () = attempt.cancelled() => return Err(Error { code: "SLX_CANCELLED", detail: "queued payload cancelled" }),
            sent = tokio::time::timeout(IDLE, sender.send(Ok(frame))) => sent
                .map_err(|_| Error { code: "SLX_IDLE", detail: "response consumer exceeded idle limit" })?
                .map_err(|_| Error { code: "SLX_CANCELLED", detail: "response consumer closed" })?,
        }
    }
}

async fn wait_consumed(
    mut progress: watch::Receiver<Progress>,
    attempt: &Arc<Attempt>,
) -> Result<()> {
    loop {
        let state = *progress.borrow_and_update();
        if state.finished {
            return if state.failed {
                Err(Error {
                    code: "SLX_CANCELLED",
                    detail: "response body closed before clean completion",
                })
            } else {
                Ok(())
            };
        }
        tokio::select! {
            () = attempt.cancelled() => return Err(Error { code: "SLX_CANCELLED", detail: "response drain cancelled" }),
            () = tokio::time::sleep_until((state.last + IDLE).into()) => return Err(Error { code: "SLX_IDLE", detail: "response body stopped consuming queued frames" }),
            changed = progress.changed() => {
                if changed.is_err() { return Err(Error { code: "SLX_CANCELLED", detail: "response body closed" }); }
            }
        }
    }
}

async fn message(
    stream: &mut Streaming<FlightData>,
) -> std::result::Result<Option<FlightData>, Status> {
    tokio::time::timeout(IDLE, stream.message())
        .await
        .map_err(|_| {
            status(Error {
                code: "SLX_IDLE",
                detail: "request stream exceeded idle limit",
            })
        })?
}

fn open(data: &FlightData) -> Result<(Binding, Mode)> {
    if data.encoded_len() > MAX_CONTROL
        || !data.data_header.is_empty()
        || !data.data_body.is_empty()
        || !data.app_metadata.is_empty()
    {
        return Err(invalid(
            "first request must contain only a bounded Open descriptor",
        ));
    }
    let descriptor = data
        .flight_descriptor
        .as_ref()
        .ok_or_else(|| invalid("missing Open descriptor"))?;
    if descriptor.r#type != DescriptorType::Cmd as i32 || !descriptor.path.is_empty() {
        return Err(invalid("Open requires CMD descriptor and no path"));
    }
    let open: Open = strict_decode(descriptor.cmd.clone(), MAX_CONTROL)?;
    let mode = Mode::try_from(open.mode).map_err(|_| invalid("unknown explicit mode"))?;
    let binding = open
        .binding
        .clone()
        .ok_or_else(|| invalid("Open requires operation binding"))?;
    validate_open(&open, &binding, mode)?;
    Ok((binding, mode))
}

#[tonic::async_trait]
impl FlightSqlService for Service {
    type FlightService = Self;
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let call = call(&request)?;
        if !matches!(call.principal, Principal::Reader | Principal::Reader2) {
            return Err(Status::permission_denied(
                "SLX_PERMISSION: principal has no data grant",
            ));
        }
        if request.get_ref().encoded_len() > MAX_CONTROL
            || query.transaction_id.is_some()
            || query.query != self.0.fixture.sql
            || self.0.fixture.authority.id != "dictionary_large"
        {
            return Err(Status::unimplemented(
                "SLX_UNSUPPORTED: only the exact registered SQL projection is available",
            ));
        }
        let schema = native(self.0.fixture.sql_schema(&self.0.source)).map_err(status)?;
        let binding = self.0.registry.issue_sql(call.principal).map_err(status)?;
        let statement_handle = wire::SqlTicket {
            binding: Some(binding),
        }
        .encode_to_vec()
        .into();
        let ticket = TicketStatementQuery { statement_handle }
            .as_any()
            .encode_to_vec();
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|_| Status::internal("SLX_SQL: result schema encoding failed"))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket)))
            .with_descriptor(request.into_inner())
            .with_total_records(
                i64::try_from(self.0.fixture.authority.rows)
                    .map_err(|_| Status::internal("SLX_SQL: row count overflow"))?,
            )
            .with_ordered(true);
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        query: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let call = call(&request)?;
        if request.get_ref().encoded_len() > MAX_CONTROL {
            return Err(status(limit("SQL ticket exceeds control limit")));
        }
        let ticket: wire::SqlTicket =
            strict_decode(query.statement_handle, MAX_CONTROL).map_err(status)?;
        let binding = ticket
            .binding
            .ok_or_else(|| status(invalid("SQL ticket requires binding")))?;
        if binding.source_id != self.0.fixture.authority.id {
            return Err(status(mismatch("SQL source is not resident")));
        }
        let attempt = self
            .0
            .registry
            .begin(call.principal, &binding, &call.connection, true)
            .map_err(status)?;
        let sending = Sending::sql(
            &self.0.source,
            &self.0.fixture,
            binding,
            &attempt.cancellation,
        )
        .map_err(status)?;
        attempt.check().map_err(status)?;
        let output = self
            .response(
                sending,
                attempt,
                call,
                (request.get_ref().encoded_len() + 5) as u64,
            )
            .map_err(status)?;
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let call = call(&request)?;
        let body = control(&self.0.registry, &call, request.get_ref())?;
        Ok(Response::new(Box::pin(stream::iter([Ok(
            arrow_flight::Result { body },
        )]))))
    }

    async fn do_exchange_fallback(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        let call = call(&request)?;
        if !matches!(call.principal, Principal::Reader | Principal::Reader2) {
            return Err(Status::permission_denied(
                "SLX_PERMISSION: principal has no data grant",
            ));
        }
        // Covers the bounded Tonic message and strict canonical re-encoding before
        // parsing. Provider/socket allocations remain separately reported.
        let _input =
            native(self.0.session.reserve_input_scratch((MAX_RPC * 2) as u64)).map_err(status)?;
        let mut input = request.into_inner();
        let first = message(&mut input)
            .await?
            .ok_or_else(|| status(invalid("missing Open request")))?;
        let request_bytes = (first.encoded_len() + 5) as u64;
        let (binding, mode) = open(&first).map_err(status)?;
        if binding.source_id != self.0.fixture.authority.id {
            return Err(status(mismatch(
                "source is not resident in this finite peer",
            )));
        }
        let attempt = self
            .0
            .registry
            .begin(call.principal, &binding, &call.connection, false)
            .map_err(status)?;
        let extra = tokio::select! {
            () = attempt.cancelled() => return Err(status(Error { code: "SLX_CANCELLED", detail: "Open attempt was cancelled" })),
            next = message(&mut input) => next?,
        };
        if extra.is_some() {
            return Err(status(invalid(
                "Open must be followed by clean request EOF",
            )));
        }
        attempt.check().map_err(status)?;
        let sending = Sending::new(
            &self.0.source,
            &self.0.fixture,
            binding,
            mode,
            &attempt.cancellation,
        )
        .map_err(status)?;
        attempt.check().map_err(status)?;
        let output = self
            .response(sending, attempt, call, request_bytes)
            .map_err(status)?;
        Ok(Response::new(Box::pin(output)))
    }
}

#[cfg(test)]
#[path = "native_exchange_service_tests.rs"]
mod tests;
