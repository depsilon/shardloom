// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::native_exchange_protocol::{
    authority::{Authority, SUITE_QUERY},
    payload::Receiving,
    transport::{self, Admission, Pem},
};
use crate::resident_memory_source::exchange_bridge::{Fixture, GRANT};
use arrow_flight::{
    FlightDescriptor, flight_service_client::FlightServiceClient,
    flight_service_server::FlightServiceServer,
};
use shardloom_exec::compute_pool::CancellationToken;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tokio::sync::oneshot;
use tonic::{Code, service::interceptor::InterceptedService, transport::Channel};

#[path = "native_exchange_sql_tests.rs"]
mod sql;

#[path = "native_exchange_fault_tests.rs"]
mod faults;

#[path = "native_exchange_response_fault_tests.rs"]
mod response_faults;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn fixtures() -> (PathBuf, PathBuf) {
    (
        std::env::var_os("SHARDLOOM_EXCHANGE_FIXTURES")
            .map(PathBuf::from)
            .expect("independent fixture root required"),
        std::env::var_os("SHARDLOOM_EXCHANGE_TLS_FIXTURES")
            .map(PathBuf::from)
            .expect("private TLS fixtures required"),
    )
}

struct Server {
    service: Service,
    session: ResidentVortexSession,
    authority: Arc<Authority>,
    registry: Registry,
    epoch: Bytes,
    address: SocketAddr,
    admission: Admission,
    baseline: u64,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}

impl Server {
    async fn start(case: &str) -> Self {
        let (fixtures, tls) = fixtures();
        let session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let authority = Arc::new(Authority::load().unwrap());
        let fixture = Arc::new(
            Fixture::load(
                &fixtures.join("fixture-manifest.json"),
                case,
                &authority,
                &session,
            )
            .unwrap(),
        );
        let source = fixture
            .original_source(&fixtures, &session, &CancellationToken::default())
            .unwrap();
        let baseline = session.snapshot().memory.reserved_bytes;
        let epoch = Bytes::from_static(&[7; 16]);
        let registry = Registry::new(authority.clone(), epoch.clone()).unwrap();
        let service = Service::new(session.clone(), fixture, source, registry.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let admission = Admission::new();
        let flight = FlightServiceServer::new(service.clone())
            .max_decoding_message_size(MAX_RPC)
            .max_encoding_message_size(MAX_RPC);
        let intercepted = InterceptedService::new(
            flight,
            transport::authenticate(transport::authentication(&tls).unwrap()),
        );
        let (shutdown, stop) = oneshot::channel();
        let serving = transport::server(&Pem::load(&tls, "server").unwrap())
            .unwrap()
            .add_service(intercepted)
            .serve_with_incoming_shutdown(
                transport::incoming(listener, admission.clone()),
                async {
                    let _ = stop.await;
                },
            );
        let task = tokio::spawn(serving);
        Self {
            service,
            session,
            authority,
            registry,
            epoch,
            address,
            admission,
            baseline,
            shutdown,
            task,
        }
    }

    fn binding(&self, case: &str, id: u8) -> Binding {
        self.authority
            .case(case)
            .unwrap()
            .binding(
                "reader",
                &self.epoch,
                Bytes::copy_from_slice(&[id; 16]),
                1,
                SUITE_QUERY,
            )
            .unwrap()
    }

    async fn client(&self, role: &str) -> FlightServiceClient<Channel> {
        let (_, tls) = fixtures();
        let channel = transport::endpoint(
            self.address,
            &Pem::load(&tls, role).unwrap(),
            "localhost",
            true,
        )
        .unwrap()
        .connect()
        .await
        .unwrap();
        FlightServiceClient::new(channel)
            .max_decoding_message_size(MAX_RPC)
            .max_encoding_message_size(MAX_RPC)
    }

    async fn idle(&self) {
        tokio::time::timeout(IDLE, async {
            loop {
                let state = self.service.snapshot().unwrap();
                if state["active_attempts"] == 0
                    && state["queued_bytes"] == 0
                    && state["queued_frames"] == 0
                    && self.session.snapshot().memory.reserved_bytes == self.baseline
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn stop(self) -> Value {
        self.shutdown.send(()).unwrap();
        tokio::time::timeout(IDLE, self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.service.drain().await.unwrap();
        assert_eq!(self.admission.snapshot().0, 0);
        assert_eq!(self.registry.snapshot().unwrap().0, 0);
        let report = self.service.snapshot().unwrap();
        assert_eq!(report["queued_bytes"], 0);
        assert_eq!(report["queued_frames"], 0);
        drop(self.service);
        assert_eq!(self.session.snapshot().memory.reserved_bytes, 0);
        report
    }
}

fn request(binding: Binding, mode: Mode) -> FlightData {
    FlightData {
        flight_descriptor: Some(FlightDescriptor::new_cmd(
            Open {
                binding: Some(binding),
                profile: Some(profile()),
                mode: mode as i32,
            }
            .encode_to_vec(),
        )),
        ..Default::default()
    }
}

async fn transfer(
    client: &mut FlightServiceClient<Channel>,
    session: &ResidentVortexSession,
    fixture: &Fixture,
    binding: Binding,
    mode: Mode,
) -> (ResidentMemorySource, Value) {
    let _temporary = session.reserve_input_scratch((MAX_RPC * 2) as u64).unwrap();
    let mut receiving = Receiving::new(session, fixture, binding.clone(), mode).unwrap();
    let mut input = Request::new(stream::iter([request(binding, mode)]));
    input.set_timeout(Duration::from_secs(60));
    let mut output = client.do_exchange(input).await.unwrap().into_inner();
    while let Some(frame) = tokio::time::timeout(IDLE, output.message())
        .await
        .unwrap()
        .unwrap()
    {
        receiving.accept(frame).unwrap();
    }
    drop(output);
    let (source, report) = receiving
        .finish(true, fixture, &CancellationToken::default())
        .await
        .unwrap();
    (source, serde_json::to_value(report).unwrap())
}

#[test]
#[ignore = "requires independent data, private TLS fixtures and the sequential process/storage guard"]
fn real_mutual_tls_payloads_match_all_five_modes_and_thirteen_sources() {
    let (fixtures, _) = fixtures();
    let authority = Authority::load().unwrap();
    let mut workflows = Vec::new();
    runtime().block_on(async {
        for case in &authority.cases {
            for mode in [
                Mode::NativeFragments,
                Mode::NativeAssembled,
                Mode::ArrowNone,
                Mode::ArrowLz4,
                Mode::ArrowZstd,
            ] {
                let server = Server::start(&case.id).await;
                let receiver = ResidentVortexSession::new(GRANT, 1).unwrap();
                let fixture = Fixture::load(
                    &fixtures.join("fixture-manifest.json"),
                    &case.id,
                    &authority,
                    &receiver,
                )
                .unwrap();
                let mut client = server.client("reader").await;
                let (source, receive) = transfer(
                    &mut client,
                    &receiver,
                    &fixture,
                    server.binding(&case.id, 2),
                    mode,
                )
                .await;
                drop(client);
                server.idle().await;
                let sender = server.stop().await;
                assert_eq!(sender["transfers"].as_array().unwrap().len(), 1);
                assert_eq!(sender["transfers"][0]["success"], true);
                let consumer = fixture
                    .consume(&source, &CancellationToken::default())
                    .unwrap();
                drop((source, fixture));
                assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
                workflows.push(
                    json!({"case": case.id, "mode": mode.label(), "sender": sender,
                    "receive": receive, "consumer": consumer, "final_sender_reserved_bytes": 0,
                    "final_receiver_reserved_bytes": 0, "final_live_connections": 0}),
                );
            }
        }
    });
    let path =
        std::env::var_os("SHARDLOOM_EXCHANGE_SOCKET_REPORT").expect("fresh report path required");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"version": 1,
        "scope": "actual loopback mTLS data sockets inside one process; no separate-process or timing acceptance claim",
        "workflows": workflows})).unwrap();
}

#[test]
#[ignore = "requires independent data, private TLS fixtures and the sequential process/storage guard"]
fn actual_data_requests_refuse_bad_open_extra_input_and_replay_then_allow_fresh_retry() {
    runtime().block_on(async {
        let server = Server::start("dictionary_large").await;
        let mut client = server.client("reader").await;
        let binding = server.binding("dictionary_large", 3);
        let mut bad_profile = request(binding.clone(), Mode::NativeFragments);
        let mut open: Open = strict_decode(
            bad_profile.flight_descriptor.as_ref().unwrap().cmd.clone(),
            MAX_CONTROL,
        )
        .unwrap();
        open.profile.as_mut().unwrap().extension_version += 1;
        bad_profile.flight_descriptor.as_mut().unwrap().cmd = open.encode_to_vec().into();
        let error = client
            .do_exchange(stream::iter([bad_profile]))
            .await
            .err()
            .unwrap();
        assert!(error.message().contains("SLX_IDENTITY"));
        let error = client
            .do_exchange(stream::iter([
                request(binding.clone(), Mode::NativeFragments),
                FlightData::default(),
            ]))
            .await
            .err()
            .unwrap();
        assert!(error.message().contains("SLX_INVALID_WIRE"));
        server.idle().await;
        let error = client
            .do_exchange(stream::iter([request(
                binding.clone(),
                Mode::NativeFragments,
            )]))
            .await
            .err()
            .unwrap();
        assert!(error.message().contains("SLX_REPLAY"));
        for role in ["denied", "administrator"] {
            let mut unauthorized = server.client(role).await;
            let error = unauthorized
                .do_exchange(stream::iter([request(
                    binding.clone(),
                    Mode::NativeFragments,
                )]))
                .await
                .err()
                .unwrap();
            assert_eq!(error.code(), Code::PermissionDenied);
            drop(unauthorized);
            tokio::time::timeout(IDLE, async {
                while server.admission.snapshot().0 != 1 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
        }
        let (fixtures, _) = fixtures();
        let receiver = ResidentVortexSession::new(GRANT, 1).unwrap();
        let fixture = Fixture::load(
            &fixtures.join("fixture-manifest.json"),
            "dictionary_large",
            &server.authority,
            &receiver,
        )
        .unwrap();
        let (source, _) = transfer(
            &mut client,
            &receiver,
            &fixture,
            server.binding("dictionary_large", 4),
            Mode::NativeFragments,
        )
        .await;
        drop((source, fixture, client));
        assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
        server.idle().await;
        server.stop().await;
    });
}

#[test]
#[ignore = "requires independent data, private TLS fixtures and the sequential process/storage guard"]
fn slow_socket_consumer_is_closed_and_all_native_and_queue_owners_drain() {
    runtime().block_on(async {
        let server = Server::start("wide_text").await;
        let mut client = server.client("reader").await;
        let mut output = client
            .do_exchange(stream::iter([request(
                server.binding("wide_text", 6),
                Mode::NativeFragments,
            )]))
            .await
            .unwrap()
            .into_inner();
        // Keep the response alive without polling its data. The service must wake
        // and close its IO without depending on another read by this peer.
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let report = server.service.snapshot().unwrap();
                if report["transfers"]
                    .as_array()
                    .is_some_and(|reports| !reports.is_empty())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let report = server.service.snapshot().unwrap();
        assert_eq!(report["transfers"][0]["success"], false);
        assert_eq!(report["transfers"][0]["error"]["code"], "SLX_IDLE");
        let failed = tokio::time::timeout(IDLE, async {
            loop {
                match output.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) => return false,
                    Err(_) => return true,
                }
            }
        })
        .await
        .unwrap();
        assert!(failed);
        drop((output, client));
        server.idle().await;
        server.stop().await;
    });
}
