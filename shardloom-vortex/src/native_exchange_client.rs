// SPDX-License-Identifier: Apache-2.0
//! Raw native-extension receiving and the independent ordinary Flight SQL client.

use super::*;
use crate::native_exchange_protocol::payload::Receiving;
use crate::resident_memory_source::{
    MemoryBatchSourceBuilder, exchange_bridge::PROVIDER_ALLOWANCE,
};
use arrow_flight::{
    Action, FlightDescriptor, flight_service_client::FlightServiceClient,
    sql::client::FlightSqlServiceClient,
};
use futures::{TryStreamExt as _, stream};
use tonic::{Request, transport::Channel};

struct Received {
    source: ResidentMemorySource,
    report: Value,
    connection_seconds: f64,
    transfer_and_admission_seconds: f64,
}

async fn connect(
    prepared: &Prepared,
    address: SocketAddr,
) -> Result<(FlightServiceClient<Channel>, f64)> {
    let started = Instant::now();
    let channel = transport::endpoint(
        address,
        &Pem::load(&prepared.config.tls_root, "reader")?,
        "localhost",
        true,
    )?
    .connect()
    .await
    .map_err(|_| invalid("verified loopback connection failed"))?;
    Ok((
        FlightServiceClient::new(channel)
            .max_decoding_message_size(MAX_RPC)
            .max_encoding_message_size(MAX_RPC),
        started.elapsed().as_secs_f64(),
    ))
}

async fn exchange(
    prepared: &Prepared,
    address: SocketAddr,
    cancellation: &CancellationToken,
    mut pause_after_ack: Option<u32>,
) -> Result<Received> {
    let temporary = native(prepared.session.reserve_input_scratch((MAX_RPC * 2) as u64))?;
    let (mut client, connection_seconds) = connect(prepared, address).await?;
    let started = Instant::now();
    let binding = prepared.binding()?;
    let mode = prepared.mode()?;
    let mut receiving =
        Receiving::new(&prepared.session, &prepared.fixture, binding.clone(), mode)?;
    let first = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_cmd(
            Open {
                binding: Some(binding),
                profile: Some(profile()),
                mode: mode as i32,
            }
            .encode_to_vec(),
        )),
        ..Default::default()
    };
    let request_bytes = first.encoded_len() + 5;
    let mut request = Request::new(stream::iter([first]));
    request.set_timeout(Duration::from_secs(60));
    let mut stream = client
        .do_exchange(request)
        .await
        .inspect_err(|error| {
            eprintln!("SLX_RPC_FAILURE: {:?}: {}", error.code(), error.message());
        })
        .map_err(|_| invalid("native extension request was refused"))?
        .into_inner();
    loop {
        native(cancellation.check())?;
        let next = tokio::time::timeout(IDLE, stream.message())
            .await
            .map_err(|_| Error {
                code: "SLX_IDLE",
                detail: "receiver saw no stream progress",
            })?
            .map_err(|_| invalid("response did not end with successful RPC status"))?;
        let Some(frame) = next else {
            break;
        };
        receiving.accept(frame)?;
        if let Some(sender_pid) = pause_after_ack.take() {
            emit(
                &json!({"event": "partial", "pid": std::process::id(), "sender_pid": sender_pid,
                "completed_executions": prepared.session.snapshot().completed_executions,
                "configuration_sha256": prepared.config_sha256,
                "published_sources": 0, "retained_native_bytes": prepared.session.snapshot().memory.reserved_bytes}),
            )?;
            if !matches!(command::<Command>()?, Command::Continue) {
                return Err(invalid(
                    "paused receiver requires an explicit continue command",
                ));
            }
        }
    }
    drop((stream, client));
    let (source, report) = receiving
        .finish(true, &prepared.fixture, cancellation)
        .await?;
    let mut report =
        serde_json::to_value(report).map_err(|_| invalid("cannot encode receiving report"))?;
    report["request_application_wire_bytes"] = json!(request_bytes);
    report["representation"] = json!(mode.label());
    report["native_intake"] = intake(&source);
    drop(temporary);
    Ok(Received {
        source,
        report,
        connection_seconds,
        transfer_and_admission_seconds: started.elapsed().as_secs_f64(),
    })
}

async fn refuse_unknown_sql_commands(client: &mut FlightSqlServiceClient<Channel>) -> Result<()> {
    let unsupported = client
        .execute("SELECT unsupported FROM missing".to_owned(), None)
        .await
        .err()
        .ok_or_else(|| invalid("unsupported SQL unexpectedly succeeded"))?;
    if !unsupported.to_string().contains("SLX_UNSUPPORTED") {
        return Err(invalid("unsupported SQL lacks explicit refusal"));
    }
    let unsupported_action = client
        .do_action(Action::new("shardloom.unknown", Bytes::new()))
        .await
        .err()
        .ok_or_else(|| invalid("unsupported command unexpectedly succeeded"))?;
    if !unsupported_action.to_string().contains("SLX_UNSUPPORTED") {
        return Err(invalid("unsupported command lacks explicit refusal"));
    }
    Ok(())
}

async fn sql(
    prepared: &Prepared,
    address: SocketAddr,
    cancellation: &CancellationToken,
) -> Result<Received> {
    let decode = native(prepared.session.reserve_input_scratch(PROVIDER_ALLOWANCE))?;
    let (client, connection_seconds) = connect(prepared, address).await?;
    let mut client = FlightSqlServiceClient::new_from_inner(client);
    let started = Instant::now();
    refuse_unknown_sql_commands(&mut client).await?;
    let info = client
        .execute(prepared.authority.sql.clone(), None)
        .await
        .map_err(|_| invalid("ordinary Flight SQL statement failed"))?;
    if info.endpoint.len() != 1
        || !info.ordered
        || info.total_records
            != i64::try_from(prepared.fixture.authority.rows)
                .map_err(|_| limit("SQL row count overflow"))?
    {
        return Err(mismatch("ordinary Flight SQL metadata differs"));
    }
    let schema = info
        .clone()
        .try_decode_schema()
        .map_err(|_| invalid("ordinary SQL schema is invalid"))?;
    let ticket = info.endpoint[0]
        .ticket
        .clone()
        .ok_or_else(|| invalid("ordinary SQL ticket missing"))?;
    let mut request = Request::new(ticket.clone());
    request.set_timeout(Duration::from_secs(60));
    let mut output = client
        .do_get(request)
        .await
        .map_err(|_| invalid("ordinary SQL result request failed"))?;
    let mut builder = native(MemoryBatchSourceBuilder::new(
        &prepared.session,
        cancellation.clone(),
    ))?;
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    loop {
        native(cancellation.check())?;
        let next = tokio::time::timeout(IDLE, output.try_next())
            .await
            .map_err(|_| Error {
                code: "SLX_IDLE",
                detail: "SQL receiver saw no progress",
            })?
            .map_err(|_| invalid("ordinary SQL result stream failed"))?;
        let Some(batch) = next else {
            break;
        };
        if batch.schema().as_ref() != &schema {
            return Err(mismatch("SQL batch schema changed"));
        }
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .filter(|count| *count <= prepared.fixture.authority.rows)
            .ok_or_else(|| limit("SQL result exceeds frozen rows"))?;
        batches += 1;
        if batches > MAX_FRAMES {
            return Err(limit("SQL result exceeds frozen batch ceiling"));
        }
        native(builder.push_arrow(batch))?;
    }
    if rows != prepared.fixture.authority.rows || batches == 0 {
        return Err(mismatch("SQL result is incomplete"));
    }
    drop(output);
    let source = native(builder.finish())?;
    let replay = client
        .do_get(ticket)
        .await
        .err()
        .ok_or_else(|| invalid("completed SQL ticket replay succeeded"))?;
    if !replay.to_string().contains("SLX_REPLAY") {
        return Err(invalid("completed SQL ticket lacks replay refusal"));
    }
    drop((client, decode));
    let report = json!({"client": "arrow-flight-58.3.0 FlightSqlServiceClient", "sql": prepared.authority.sql,
        "rows": rows, "batches": batches, "native_intake": intake(&source),
        "ordinary_arrow_results": true, "unsupported_sql_refused": true,
        "unsupported_command_refused": true, "completed_ticket_replay_refused": true});
    Ok(Received {
        source,
        report,
        connection_seconds,
        transfer_and_admission_seconds: started.elapsed().as_secs_f64(),
    })
}

fn intake(source: &ResidentMemorySource) -> Value {
    let report = source.intake_transfer_report();
    json!({"encoded_array_nodes_copied": report.encoded_array_nodes_copied,
        "encoded_buffer_copies": report.encoded_buffer_copies,
        "encoded_buffer_bytes_copied": report.encoded_buffer_bytes_copied,
        "reused_array_references": report.reused_array_references,
        "reused_buffer_references": report.reused_buffer_references,
        "materialized_batches": report.materialized_batches, "materialized_rows": report.materialized_rows,
        "retained_metadata_bytes": report.retained_metadata_bytes,
        "provider_scratch_peak_reserved_bytes": report.provider_scratch_peak_reserved_bytes,
        "ipc_transport_bytes": report.ipc_transport_bytes, "ipc_buffer_bytes_copied": report.ipc_buffer_bytes_copied,
        "ipc_decode_workspace_peak_reserved_bytes": report.ipc_decode_workspace_peak_reserved_bytes,
        "ipc_coordinate_arrays_executed": report.ipc_coordinate_arrays_executed,
        "ipc_coordinate_values_checked": report.ipc_coordinate_values_checked,
        "physical_decoder_bytes": null, "provider_internal_allocations_and_copies": null})
}

pub(super) async fn receiver(prepared: Prepared) -> Result<()> {
    let Command::Connect {
        address,
        sender_pid,
    } = command::<Command>()?
    else {
        return Err(invalid("receiver requires concrete sender connection"));
    };
    if sender_pid == 0 || sender_pid == std::process::id() {
        return Err(invalid("receiver requires a distinct sender process"));
    }
    let cancellation =
        CancellationToken::default().with_deadline(Instant::now() + Duration::from_secs(60));
    let received = if prepared.config.mode == "sql" {
        sql(&prepared, address, &cancellation).await?
    } else {
        exchange(&prepared, address, &cancellation, None).await?
    };
    if prepared.session.snapshot().completed_executions != 0 {
        return Err(invalid("receiver executed before sender-exit barrier"));
    }
    emit(
        &json!({"event": "received", "pid": std::process::id(), "sender_pid": sender_pid,
        "completed_executions": 0, "configuration_sha256": prepared.config_sha256,
        "retained_native_bytes": prepared.session.snapshot().memory.reserved_bytes}),
    )?;
    let Command::Consume {
        sender_pid: exited_pid,
        sender_exit_code: 0,
    } = command::<Command>()?
    else {
        return Err(invalid(
            "native consumer requires successful sender-exit confirmation",
        ));
    };
    if sender_pid != exited_pid {
        return Err(invalid("sender-exit confirmation has a different PID"));
    }
    let consumer = if prepared.config.mode == "sql" {
        native(
            prepared
                .fixture
                .consume_sql_result(&received.source, &cancellation),
        )?
    } else {
        native(prepared.fixture.consume(&received.source, &cancellation))?
    };
    let cleanup = Instant::now();
    let mut report = prepared.report_identity();
    report["role"] = json!("receiver");
    report["sender_pid"] = json!(sender_pid);
    report["sender_exited_before_consumer"] = json!(true);
    report["connection_seconds"] = json!(received.connection_seconds);
    report["transfer_and_admission_seconds"] = json!(received.transfer_and_admission_seconds);
    report["receive"] = received.report;
    report["consumer"] = consumer;
    report["completed_executions"] = json!(prepared.session.snapshot().completed_executions);
    drop((received.source, prepared.fixture));
    let memory = prepared.session.snapshot().memory;
    if memory.reserved_bytes != 0 {
        return Err(invalid("receiver native credits remain live"));
    }
    report["final_reserved_bytes"] = json!(memory.reserved_bytes);
    report["peak_reserved_bytes"] = json!(memory.peak_reserved_bytes);
    report["cleanup_seconds"] = json!(cleanup.elapsed().as_secs_f64());
    emit(&json!({"event": "final", "report": report}))
}

// These two roles are failure probes only. The ordinary receiver above never
// pauses after ACK and never interprets a refused transfer as successful work.
pub(super) async fn refusal_probe(prepared: Prepared, role: &str) -> Result<()> {
    if prepared.config.mode != "native_fragments" {
        return Err(invalid(
            "process failure probes require the native fragment mode",
        ));
    }
    let Command::Connect {
        address,
        sender_pid,
    } = command::<Command>()?
    else {
        return Err(invalid(
            "failure probe requires a concrete sender connection",
        ));
    };
    if sender_pid == 0 || sender_pid == std::process::id() {
        return Err(invalid("failure probe requires a distinct sender process"));
    }
    let cancellation =
        CancellationToken::default().with_deadline(Instant::now() + Duration::from_secs(60));
    let failure = if role == "receiver-hold" {
        exchange(&prepared, address, &cancellation, Some(sender_pid))
            .await
            .err()
            .ok_or_else(|| invalid("interrupted transfer unexpectedly produced a source"))?
            .code
    } else if role == "receiver-stale" {
        let (mut client, _) = connect(&prepared, address).await?;
        let first = FlightData {
            flight_descriptor: Some(FlightDescriptor::new_cmd(
                Open {
                    binding: Some(prepared.binding()?),
                    profile: Some(profile()),
                    mode: Mode::NativeFragments as i32,
                }
                .encode_to_vec(),
            )),
            ..Default::default()
        };
        let error = tokio::time::timeout(IDLE, client.do_exchange(stream::iter([first])))
            .await
            .map_err(|_| invalid("stale binding probe did not receive a refusal"))?
            .err()
            .ok_or_else(|| invalid("restarted peer accepted the old epoch"))?;
        if !error.message().contains("SLX_IDENTITY") {
            return Err(invalid("old epoch was not rejected by identity validation"));
        }
        drop(client);
        "SLX_IDENTITY"
    } else {
        return Err(invalid("unknown process failure probe"));
    };
    let mut report = prepared.report_identity();
    drop(prepared.fixture);
    let snapshot = prepared.session.snapshot();
    if snapshot.completed_executions != 0 || snapshot.memory.reserved_bytes != 0 {
        return Err(invalid(
            "refused receiver retained memory or executed a partial source",
        ));
    }
    report["role"] = json!(role);
    report["sender_pid"] = json!(sender_pid);
    report["expected_refusal"] = json!(failure);
    report["published_sources"] = json!(0);
    report["completed_executions"] = json!(0);
    report["final_reserved_bytes"] = json!(snapshot.memory.reserved_bytes);
    report["peak_reserved_bytes"] = json!(snapshot.memory.peak_reserved_bytes);
    emit(&json!({"event": "final", "report": report}))
}
