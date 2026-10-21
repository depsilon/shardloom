// SPDX-License-Identifier: Apache-2.0
//! A finite child process controlled by a local supervisor over bounded pipes.
//! Results stay private until the parent reaps the sender and opens the barrier.

use super::{
    attempts::Registry,
    authority::{Authority, SUITE_QUERY, unhex},
    payload::native,
    service::Service,
    transport::{self, Admission, IDLE, Pem},
    *,
};
use crate::{
    resident_memory_source::{
        ResidentMemorySource,
        exchange_bridge::{Fixture, GRANT},
    },
    resident_session::ResidentVortexSession,
};
use arrow_flight::flight_service_server::FlightServiceServer;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    io::{BufRead as _, Read as _, Write as _},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::service::interceptor::InterceptedService;

#[path = "native_exchange_client.rs"]
mod client;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Configuration {
    version: u32,
    fixture_root: PathBuf,
    tls_root: PathBuf,
    certificate_manifest_sha256: String,
    source_id: String,
    mode: String,
    server_epoch: String,
    attempt_id: String,
}

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Command {
    Connect {
        address: SocketAddr,
        sender_pid: u32,
    },
    Stop,
    Continue,
    Consume {
        sender_pid: u32,
        sender_exit_code: i32,
    },
}

pub(super) struct Prepared {
    config: Configuration,
    config_sha256: String,
    authority: Arc<Authority>,
    fixture: Arc<Fixture>,
    session: ResidentVortexSession,
    epoch: Bytes,
    attempt_id: Bytes,
    metadata_preparation_seconds: f64,
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}

fn bounded_input() -> Result<Vec<u8>> {
    let mut input = Vec::with_capacity(MAX_CONTROL);
    std::io::stdin()
        .lock()
        .take((MAX_CONTROL + 1) as u64)
        .read_until(b'\n', &mut input)
        .map_err(|_| invalid("supervisor input could not be read"))?;
    if input.len() > MAX_CONTROL || input.last() != Some(&b'\n') {
        return Err(limit("supervisor input must be one bounded complete line"));
    }
    input.pop();
    Ok(input)
}

fn command<T: DeserializeOwned>() -> Result<T> {
    serde_json::from_slice(&bounded_input()?)
        .map_err(|_| invalid("supervisor command does not match the finite schema"))
}

fn emit(value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid("cannot encode peer event"))?;
    if bytes.len() > 1 << 20 {
        return Err(limit("peer report exceeds one MiB"));
    }
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    output
        .write_all(&bytes)
        .and_then(|()| output.write_all(b"\n"))
        .and_then(|()| output.flush())
        .map_err(|_| invalid("supervisor output closed"))
}

impl Prepared {
    fn load(bytes: &[u8]) -> Result<Self> {
        let started = Instant::now();
        let config: Configuration = serde_json::from_slice(bytes)
            .map_err(|_| invalid("invalid finite peer configuration"))?;
        if config.version != 1
            || !config.fixture_root.is_absolute()
            || !config.tls_root.is_absolute()
            || config.source_id.len() > 64
        {
            return Err(invalid(
                "unsupported peer version, paths or source identifier",
            ));
        }
        transport::verify_certificate_manifest(
            &config.tls_root,
            &config.certificate_manifest_sha256,
        )?;
        let epoch = unhex(&config.server_epoch)?;
        let attempt_id = unhex(&config.attempt_id)?;
        if epoch.len() != 16
            || epoch.iter().all(|byte| *byte == 0)
            || attempt_id.len() != 16
            || attempt_id.iter().all(|byte| *byte == 0)
        {
            return Err(invalid(
                "peer epoch and attempt require nonzero sixteen-byte identities",
            ));
        }
        let authority = Arc::new(Authority::load()?);
        let session = native(ResidentVortexSession::new(GRANT, 1))?;
        let fixture = Arc::new(native(Fixture::load(
            &config.fixture_root.join("fixture-manifest.json"),
            &config.source_id,
            &authority,
            &session,
        ))?);
        let prepared = Self {
            config,
            config_sha256: hex(&hash(bytes)),
            authority,
            fixture,
            session,
            epoch,
            attempt_id,
            metadata_preparation_seconds: started.elapsed().as_secs_f64(),
        };
        if prepared.config.mode == "sql" {
            if prepared.config.source_id != "dictionary_large" {
                return Err(mismatch("SQL source is outside finite grant"));
            }
        } else {
            prepared.mode()?;
        }
        Ok(prepared)
    }

    fn mode(&self) -> Result<Mode> {
        match self.config.mode.as_str() {
            "native_fragments" => Ok(Mode::NativeFragments),
            "native_assembled" => Ok(Mode::NativeAssembled),
            "arrow_none" => Ok(Mode::ArrowNone),
            "arrow_lz4" => Ok(Mode::ArrowLz4),
            "arrow_zstd" => Ok(Mode::ArrowZstd),
            _ => Err(mismatch("unknown explicit data representation")),
        }
    }

    fn binding(&self) -> Result<Binding> {
        self.fixture.authority.binding(
            "reader",
            &self.epoch,
            self.attempt_id.clone(),
            1,
            SUITE_QUERY,
        )
    }

    fn report_identity(&self) -> Value {
        json!({"version": 1, "pid": std::process::id(), "source_id": self.config.source_id,
            "mode": self.config.mode, "configuration_sha256": self.config_sha256,
            "server_epoch": self.config.server_epoch, "attempt_id": self.config.attempt_id,
            "frozen_suite_operation_sha256": hex(&self.fixture.authority.operation_sha256),
            "certificate_manifest_sha256": self.config.certificate_manifest_sha256,
            "metadata_preparation_seconds": self.metadata_preparation_seconds,
            "native_grant_bytes": GRANT, "provider_allocation_bytes": null,
            "provider_copy_bytes": null, "tls_runtime_kernel_allocation_bytes": null,
            "encrypted_tcp_bytes": null, "fallback_attempted": false,
            "external_engine_invoked": false,
            "coverage": "application credits and explicit copies only; provider/TLS/runtime/kernel costs remain unobserved; process resources are collected by the parent"})
    }
}

pub(crate) fn run(role: &str) -> Result<()> {
    if !matches!(
        role,
        "sender" | "receiver" | "receiver-hold" | "receiver-stale"
    ) {
        return Err(invalid("unknown peer role"));
    }
    let bytes = bounded_input()?;
    let prepared = Prepared::load(&bytes)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| invalid("cannot create finite peer runtime"))?;
    runtime.block_on(async {
        match role {
            "sender" => sender(prepared).await,
            "receiver" => client::receiver(prepared).await,
            _ => client::refusal_probe(prepared, role).await,
        }
    })
}

async fn sender(prepared: Prepared) -> Result<()> {
    let started = Instant::now();
    let cancellation =
        CancellationToken::default().with_deadline(Instant::now() + Duration::from_secs(60));
    let source = native(prepared.fixture.original_source(
        &prepared.config.fixture_root,
        &prepared.session,
        &cancellation,
    ))?;
    let source_preparation_seconds = started.elapsed().as_secs_f64();
    let registry = Registry::new(prepared.authority.clone(), prepared.epoch.clone())?;
    let service = Service::new(
        prepared.session.clone(),
        prepared.fixture.clone(),
        source,
        registry,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| invalid("cannot bind finite loopback listener"))?;
    let address = listener
        .local_addr()
        .map_err(|_| invalid("listener has no local address"))?;
    let admission = Admission::new();
    let authentication = transport::authentication(&prepared.config.tls_root)?;
    let flight = FlightServiceServer::new(service.clone())
        .max_decoding_message_size(MAX_RPC)
        .max_encoding_message_size(MAX_RPC);
    let intercepted =
        InterceptedService::new(flight, transport::authenticate(authentication.clone()));
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let serving = transport::server(&Pem::load(&prepared.config.tls_root, "server")?)?
        .add_service(intercepted)
        .serve_with_incoming_shutdown(transport::incoming(listener, admission.clone()), async {
            let _ = stop.await;
        });
    let mut task = tokio::spawn(serving);
    emit(
        &json!({"event": "ready", "pid": std::process::id(), "address": address.to_string(),
        "configuration_sha256": prepared.config_sha256}),
    )?;
    let stop_command = tokio::task::spawn_blocking(command::<Command>)
        .await
        .map_err(|_| invalid("supervisor command reader stopped"))??;
    if !matches!(stop_command, Command::Stop) {
        return Err(invalid("sender requires stop command"));
    }
    let cleanup = Instant::now();
    shutdown
        .send(())
        .map_err(|()| invalid("server stopped before supervised shutdown"))?;
    if let Ok(result) = tokio::time::timeout(IDLE, &mut task).await {
        result
            .map_err(|_| invalid("server task failed"))?
            .map_err(|_| invalid("server transport failed"))?;
    } else {
        task.abort();
        let _ = task.await;
        return Err(Error {
            code: "SLX_IDLE",
            detail: "server connections did not drain in five seconds",
        });
    }
    service.drain().await?;
    let state = service.snapshot()?;
    let (live, peak) = admission.snapshot();
    if live != 0
        || state["active_attempts"] != 0
        || state["queued_bytes"] != 0
        || state["queued_frames"] != 0
    {
        return Err(invalid(
            "sender retains a connection, attempt or queued owner",
        ));
    }
    let mut report = prepared.report_identity();
    report["role"] = json!("sender");
    report["source_preparation_seconds"] = json!(source_preparation_seconds);
    report["service"] = state;
    report["final_live_connections"] = json!(live);
    report["peak_live_connections"] = json!(peak);
    report["accepted_connections"] = json!(admission.accepted());
    report["authenticated_rpc_lookups"] = json!(authentication.lookups());
    drop((service, prepared.fixture));
    let memory = prepared.session.snapshot().memory;
    if memory.reserved_bytes != 0 {
        return Err(invalid("sender native credits remain live"));
    }
    report["final_reserved_bytes"] = json!(memory.reserved_bytes);
    report["peak_reserved_bytes"] = json!(memory.peak_reserved_bytes);
    report["cleanup_seconds"] = json!(cleanup.elapsed().as_secs_f64());
    emit(&json!({"event": "final", "report": report}))
}
