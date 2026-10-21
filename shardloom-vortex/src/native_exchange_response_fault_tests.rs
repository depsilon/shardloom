// SPDX-License-Identifier: Apache-2.0
//! Deliberately invalid TLS responses test the private receiver before publication.

use super::*;
use crate::native_exchange_protocol::attempts::{Connection, Principal};
use futures::StreamExt as _;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug)]
enum Fault {
    MissingAck,
    Truncated,
    Duplicate,
    Gap,
    Reordered,
    BadCoordinates,
    BadRegistry,
    Trailing,
    FailedAfterComplete,
    AbruptSocketClose,
    ArrowFingerprint,
    ArrowHeaderLimit,
    RpcMessageLimit,
}

#[derive(Clone)]
struct FaultService {
    frames: Arc<Vec<FlightData>>,
    fault: Fault,
    after_complete: Arc<Notify>,
    connection: Arc<Mutex<Option<Connection>>>,
}

#[tonic::async_trait]
impl FlightSqlService for FaultService {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn do_exchange_fallback(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        let call = transport::call(&request)?;
        *self.connection.lock().unwrap() = Some(call.connection);
        let mut input = request.into_inner();
        assert!(input.message().await?.is_some());
        assert!(input.message().await?.is_none());
        let frames = self.frames.as_ref().clone();
        let fault = self.fault;
        let gate = self.after_complete.clone();
        let output = stream::iter(frames.into_iter().map(Ok)).chain(
            stream::once(async move {
                match fault {
                    Fault::FailedAfterComplete => {
                        gate.notified().await;
                        Some(Err(Status::data_loss("injected failure after Complete")))
                    }
                    Fault::AbruptSocketClose => std::future::pending().await,
                    _ => None,
                }
            })
            .filter_map(std::future::ready),
        );
        Ok(Response::new(Box::pin(output)))
    }
}

async fn frames(
    session: &ResidentVortexSession,
    fixture: &Fixture,
    root: &std::path::Path,
    registry: &Registry,
    binding: &Binding,
    mode: Mode,
) -> Vec<FlightData> {
    let source = fixture
        .original_source(root, session, &CancellationToken::default())
        .unwrap();
    let attempt = registry
        .begin(Principal::Reader, binding, &Connection::new(), false)
        .unwrap();
    let queue = Queue::new(session.shared_memory_pool());
    let mut sending = Sending::new(
        &source,
        fixture,
        binding.clone(),
        mode,
        &attempt.cancellation,
    )
    .unwrap();
    let mut frames = Vec::new();
    while let Some(frame) = sending.next(&queue, &attempt).await.unwrap() {
        // These deliberate test copies are covered by the caller's separate
        // reservation and are never counted as candidate transport performance.
        frames.push(FlightData::decode(Bytes::from(frame.encode_to_vec())).unwrap());
        drop(frame);
        assert_eq!(queue.snapshot(), (0, 0));
    }
    attempt.complete().unwrap();
    frames
}

fn alter(frames: &mut Vec<FlightData>, fault: Fault) {
    use crate::native_exchange_protocol::tests::change;
    match fault {
        Fault::MissingAck => {
            frames.remove(0);
        }
        Fault::Truncated => {
            frames.pop();
        }
        Fault::Duplicate => frames.insert(1, frames[0].clone()),
        Fault::Gap => frames[1] = change(&frames[1], |frame| frame.sequence += 1),
        Fault::Reordered => frames.swap(1, 2),
        Fault::BadCoordinates => {
            frames[1] = change(&frames[1], |frame| {
                let Some(Body::Native(fragment)) = frame.body.as_mut() else {
                    panic!("native fragment required")
                };
                fragment.message_offset += 1;
            });
        }
        Fault::BadRegistry => {
            frames[0] = change(&frames[0], |frame| {
                let Some(Body::Ack(ack)) = frame.body.as_mut() else {
                    panic!("ack required")
                };
                ack.profile.as_mut().unwrap().physical_registry.swap(0, 1);
            });
        }
        Fault::Trailing => frames.push(frames.last().unwrap().clone()),
        Fault::FailedAfterComplete => {}
        Fault::AbruptSocketClose => frames.truncate(2),
        Fault::ArrowFingerprint => {
            let frame = frames
                .iter_mut()
                .find(|frame| !frame.data_body.is_empty())
                .unwrap();
            let mut body = frame.data_body.to_vec();
            body[0] ^= 1;
            frame.data_body = body.into();
        }
        Fault::ArrowHeaderLimit => frames[0].data_header = Bytes::from(vec![0; (64 << 10) + 1]),
        Fault::RpcMessageLimit => frames[0].data_body = Bytes::from(vec![0; MAX_RPC + 1]),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "keep the malicious peer, private receiver and complete cleanup proof in one sequential test"
)]
async fn fault(fault: Fault) -> Value {
    let (root, tls) = fixtures();
    let mode = match fault {
        Fault::ArrowFingerprint | Fault::ArrowHeaderLimit => Mode::ArrowZstd,
        _ => Mode::NativeFragments,
    };
    let source_id = "dictionary_small";
    let authority = Arc::new(Authority::load().unwrap());
    let sender = ResidentVortexSession::new(GRANT, 1).unwrap();
    let fixture = Fixture::load(
        &root.join("fixture-manifest.json"),
        source_id,
        &authority,
        &sender,
    )
    .unwrap();
    let epoch = Bytes::from_static(&[19; 16]);
    let binding = fixture
        .authority
        .binding(
            "reader",
            &epoch,
            Bytes::from_static(&[20; 16]),
            1,
            SUITE_QUERY,
        )
        .unwrap();
    let registry = Registry::new(authority.clone(), epoch).unwrap();
    let held_payload = sender.reserve_input_scratch(2 * MAX_PAYLOAD).unwrap();
    let mut frames = frames(&sender, &fixture, &root, &registry, &binding, mode).await;
    let original_frames = frames.len();
    alter(&mut frames, fault);
    let service = FaultService {
        frames: Arc::new(frames),
        fault,
        after_complete: Arc::new(Notify::new()),
        connection: Arc::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let admission = Admission::new();
    // The malicious test producer can exceed the real service's encoding limit;
    // the normal client's decoding ceiling must still reject that response.
    let flight = FlightServiceServer::new(service.clone())
        .max_decoding_message_size(MAX_RPC)
        .max_encoding_message_size(MAX_RPC + (64 << 10));
    let intercepted = InterceptedService::new(
        flight,
        transport::authenticate(transport::authentication(&tls).unwrap()),
    );
    let (shutdown, stop) = oneshot::channel();
    let serving = transport::server(&Pem::load(&tls, "server").unwrap())
        .unwrap()
        .add_service(intercepted)
        .serve_with_incoming_shutdown(transport::incoming(listener, admission.clone()), async {
            let _ = stop.await;
        });
    let task = tokio::spawn(serving);
    let channel = transport::endpoint(
        address,
        &Pem::load(&tls, "reader").unwrap(),
        "localhost",
        true,
    )
    .unwrap()
    .connect()
    .await
    .unwrap();
    let mut client = FlightServiceClient::new(channel)
        .max_decoding_message_size(MAX_RPC)
        .max_encoding_message_size(MAX_RPC);
    let receiver = ResidentVortexSession::new(GRANT, 1).unwrap();
    let receiver_fixture = Fixture::load(
        &root.join("fixture-manifest.json"),
        source_id,
        &authority,
        &receiver,
    )
    .unwrap();
    let transport_credit = receiver
        .reserve_input_scratch((MAX_RPC * 2) as u64)
        .unwrap();
    let mut receiving =
        Receiving::new(&receiver, &receiver_fixture, binding.clone(), mode).unwrap();
    let mut stream = client
        .do_exchange(stream::iter([request(binding, mode)]))
        .await
        .unwrap()
        .into_inner();
    let mut received = 0;
    let error = loop {
        match tokio::time::timeout(IDLE, stream.message()).await.unwrap() {
            Ok(Some(frame)) => {
                received += 1;
                if let Err(error) = receiving.accept(frame) {
                    break Some(error.code.to_owned());
                }
                if matches!(fault, Fault::FailedAfterComplete) && received == original_frames {
                    service.after_complete.notify_one();
                }
                if matches!(fault, Fault::AbruptSocketClose) && received == 2 {
                    service.connection.lock().unwrap().as_ref().unwrap().abort();
                }
            }
            Ok(None) => break None,
            Err(error) => break Some(format!("RPC {:?}", error.code())),
        }
    };
    let failure = if let Some(error) = error {
        drop(receiving);
        error
    } else {
        receiving
            .finish(true, &receiver_fixture, &CancellationToken::default())
            .await
            .err()
            .unwrap()
            .code
            .to_owned()
    };
    if matches!(fault, Fault::FailedAfterComplete) {
        assert_eq!(received, original_frames);
    }
    assert_eq!(receiver.snapshot().completed_executions, 0);
    drop((stream, client, receiver_fixture, transport_credit));
    assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
    shutdown.send(()).unwrap();
    tokio::time::timeout(IDLE, task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(admission.snapshot().0, 0);
    drop((service, fixture, held_payload));
    assert_eq!(sender.snapshot().memory.reserved_bytes, 0);
    json!({"fault": format!("{fault:?}"), "mode": mode.label(), "received_frames": received,
        "failure": failure, "published_sources": 0, "native_executions": 0,
        "final_sender_reserved_bytes": 0, "final_receiver_reserved_bytes": 0,
        "final_live_connections": 0, "server_task_joined": true})
}

#[test]
#[ignore = "requires independent fixtures, private TLS and sequential workload supervision"]
fn malformed_real_tls_responses_never_publish_partial_native_sources() {
    let reports = runtime().block_on(async {
        let mut reports = Vec::new();
        for selected in [
            Fault::MissingAck,
            Fault::Truncated,
            Fault::Duplicate,
            Fault::Gap,
            Fault::Reordered,
            Fault::BadCoordinates,
            Fault::BadRegistry,
            Fault::Trailing,
            Fault::FailedAfterComplete,
            Fault::AbruptSocketClose,
            Fault::ArrowFingerprint,
            Fault::ArrowHeaderLimit,
            Fault::RpcMessageLimit,
        ] {
            reports.push(fault(selected).await);
        }
        reports
    });
    let path = std::env::var_os("SHARDLOOM_EXCHANGE_RESPONSE_FAULT_REPORT")
        .expect("fresh response fault report required");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"version": 1, "scope": "actual TLS malformed responses inside one process", "cases": reports})).unwrap();
}
