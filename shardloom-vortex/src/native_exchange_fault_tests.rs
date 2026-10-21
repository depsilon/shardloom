// SPDX-License-Identifier: Apache-2.0
//! Real TLS refusals against the same finite service used by the process peers.

use super::*;
use arrow_flight::Action;
use futures::StreamExt as _;

async fn refuse(client: &mut FlightServiceClient<Channel>, input: FlightData) {
    let error = client
        .do_exchange(stream::iter([input]))
        .await
        .err()
        .unwrap();
    assert!(
        error.message().contains("SLX_"),
        "refusal must keep its deterministic diagnostic: {error}"
    );
}

#[test]
#[ignore = "requires independent fixtures, private TLS and sequential workload supervision"]
fn actual_tls_rejects_the_complete_open_identity_and_wire_mismatch_matrix() {
    runtime().block_on(async {
        let server = Server::start("tiny").await;
        let mut client = server.client("reader").await;
        let valid = Open {
            binding: Some(server.binding("tiny", 20)),
            profile: Some(profile()),
            mode: Mode::NativeFragments as i32,
        };
        let mut invalid = Vec::new();
        for mode in [0, 99, -1] {
            let mut changed = valid.clone();
            changed.mode = mode;
            invalid.push(changed);
        }
        for field in 0..6 {
            let mut changed = valid.clone();
            let value = changed.profile.as_mut().unwrap();
            match field {
                0 => value.extension_version += 1,
                1 => value.vortex_version = "0.0.0".into(),
                2 => value.vortex_message_version += 1,
                3 => value.physical_registry.swap(0, 1),
                4 => value.limits.as_mut().unwrap().native_memory_bytes += 1,
                _ => changed.profile = None,
            }
            invalid.push(changed);
        }
        for field in 0..13 {
            let mut changed = valid.clone();
            let value = changed.binding.as_mut().unwrap();
            match field {
                0 => value.server_epoch = Bytes::from_static(&[4; 16]),
                1 => value.principal = "reader2".into(),
                2 => value.source_id = "not_authorized".into(),
                3 => value.source_sha256 = hash(b"changed source"),
                4 => value.dtype_sha256 = hash(b"changed dtype"),
                5 => value.operation_sha256 = hash(b"changed operation"),
                6 => value.attempt_id = Bytes::new(),
                7 => value.partition_id = 1,
                8 => value.first_row = 1,
                9 => value.rows += 1,
                10 => value.policy_version += 1,
                11 => value.query_id = "not_authorized".into(),
                _ => changed.binding = None,
            }
            invalid.push(changed);
        }
        for open in invalid {
            refuse(
                &mut client,
                FlightData {
                    flight_descriptor: Some(FlightDescriptor::new_cmd(open.encode_to_vec())),
                    ..Default::default()
                },
            )
            .await;
        }
        for suffix in [&[0x78, 1][..], &[0x18, 1], &[0xff]] {
            let mut bytes = valid.encode_to_vec();
            bytes.extend_from_slice(suffix);
            refuse(
                &mut client,
                FlightData {
                    flight_descriptor: Some(FlightDescriptor::new_cmd(bytes)),
                    ..Default::default()
                },
            )
            .await;
        }
        for field in 0..4 {
            let mut first = request(server.binding("tiny", 21), Mode::NativeFragments);
            match field {
                0 => first.data_body = Bytes::from_static(b"extra body"),
                1 => first.data_header = Bytes::from_static(b"extra header"),
                2 => first.app_metadata = Bytes::from_static(b"extra metadata"),
                _ => first
                    .flight_descriptor
                    .as_mut()
                    .unwrap()
                    .path
                    .push("path".into()),
            }
            refuse(&mut client, first).await;
        }
        // All incompatibilities fail before producing a payload. No alternate
        // representation or external evaluator can satisfy these requests.
        assert_eq!(
            server.service.snapshot().unwrap()["transfers"],
            serde_json::json!([])
        );
        server.idle().await;
        fresh_retry(&server, &mut client, 22, 1, "tiny").await;
        drop(client);
        let report = server.stop().await;
        assert_eq!(report["transfers"].as_array().unwrap().len(), 1);
    });
}

async fn fresh_retry(
    server: &Server,
    client: &mut FlightServiceClient<Channel>,
    id: u8,
    version: u64,
    case: &str,
) {
    let (root, _) = fixtures();
    let session = ResidentVortexSession::new(GRANT, 1).unwrap();
    let fixture = Fixture::load(
        &root.join("fixture-manifest.json"),
        case,
        &server.authority,
        &session,
    )
    .unwrap();
    let mut binding = server.binding(case, id);
    binding.policy_version = version;
    let (source, _) = transfer(client, &session, &fixture, binding, Mode::NativeFragments).await;
    fixture
        .consume(&source, &CancellationToken::default())
        .unwrap();
    drop((source, fixture));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    server.idle().await;
}

async fn stream_fails(output: &mut tonic::Streaming<FlightData>) {
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
    assert!(failed, "an interrupted response cannot have successful EOF");
}

#[test]
#[ignore = "requires independent fixtures, private TLS and sequential workload supervision"]
fn active_data_cancellation_and_policy_revocation_abort_io_and_allow_full_fresh_retry() {
    runtime().block_on(async {
        for replace in [false, true] {
            let server = Server::start("wide_text").await;
            let mut client = server.client("reader").await;
            let binding = server.binding("wide_text", 30);
            let mut output = client
                .do_exchange(stream::iter([request(
                    binding.clone(),
                    Mode::NativeFragments,
                )]))
                .await
                .unwrap()
                .into_inner();
            assert!(output.message().await.unwrap().is_some());
            assert_eq!(server.registry.snapshot().unwrap().0, 1);
            let mut control = server
                .client(if replace { "administrator" } else { "reader" })
                .await;
            let action = if replace {
                Action::new(
                    "shardloom.replace-policy.v1",
                    wire::ReplacePolicy {
                        server_epoch: server.epoch.clone(),
                        expected_version: 1,
                        new_version: 2,
                    }
                    .encode_to_vec(),
                )
            } else {
                Action::new(
                    "shardloom.cancel.v1",
                    wire::Cancel {
                        binding: Some(binding.clone()),
                    }
                    .encode_to_vec(),
                )
            };
            let mut receipt = control.do_action(action).await.unwrap().into_inner();
            assert!(receipt.message().await.unwrap().is_some());
            assert!(receipt.message().await.unwrap().is_none());
            drop((receipt, control));
            stream_fails(&mut output).await;
            drop((output, client));
            server.idle().await;
            let report = server.service.snapshot().unwrap();
            assert_eq!(report["transfers"][0]["success"], false);
            assert_eq!(report["transfers"][0]["error"]["code"], "SLX_CANCELLED");
            let mut retry = server.client("reader").await;
            refuse(&mut retry, request(binding, Mode::NativeFragments)).await;
            fresh_retry(
                &server,
                &mut retry,
                31,
                if replace { 2 } else { 1 },
                "wide_text",
            )
            .await;
            drop(retry);
            server.stop().await;
        }
    });
}

#[test]
#[ignore = "requires independent fixtures, private TLS and sequential workload supervision"]
fn rpc_deadline_and_disconnected_consumer_release_native_owners() {
    runtime().block_on(async {
        let server = Server::start("wide_text").await;
        let mut client = server.client("reader").await;
        let binding = server.binding("wide_text", 40);
        let mut pending = Request::new(
            stream::iter([request(binding.clone(), Mode::NativeFragments)])
                .chain(stream::pending()),
        );
        pending.set_timeout(Duration::from_millis(100));
        let failure = tokio::time::timeout(IDLE, client.do_exchange(pending))
            .await
            .unwrap()
            .err()
            .unwrap();
        assert!(matches!(
            failure.code(),
            Code::Cancelled | Code::DeadlineExceeded
        ));
        server.idle().await;
        refuse(&mut client, request(binding, Mode::NativeFragments)).await;
        let mut output = client
            .do_exchange(stream::iter([request(
                server.binding("wide_text", 41),
                Mode::NativeFragments,
            )]))
            .await
            .unwrap()
            .into_inner();
        assert!(output.message().await.unwrap().is_some());
        drop((output, client));
        server.idle().await;
        assert_eq!(
            server.service.snapshot().unwrap()["transfers"][0]["success"],
            false
        );
        let mut client = server.client("reader").await;
        fresh_retry(&server, &mut client, 42, 1, "wide_text").await;
        drop(client);
        server.stop().await;
    });
}

#[test]
#[ignore = "requires independent fixtures, private TLS and sequential workload supervision"]
fn real_rpc_sender_and_receiver_grant_denial_release_partial_owners() {
    runtime().block_on(async {
        let server = Server::start("wide_text").await;
        let mut client = server.client("reader").await;
        let denied = server
            .session
            .reserve_input_scratch(GRANT - server.baseline)
            .unwrap();
        refuse(
            &mut client,
            request(server.binding("wide_text", 50), Mode::NativeFragments),
        )
        .await;
        drop(denied);
        server.idle().await;
        let output = client
            .do_exchange(stream::iter([request(
                server.binding("wide_text", 51),
                Mode::NativeFragments,
            )]))
            .await
            .unwrap()
            .into_inner();
        let (root, _) = fixtures();
        let session = ResidentVortexSession::new(GRANT, 1).unwrap();
        let fixture = Fixture::load(
            &root.join("fixture-manifest.json"),
            "wide_text",
            &server.authority,
            &session,
        )
        .unwrap();
        let baseline = session.snapshot().memory.reserved_bytes;
        let denied = session.reserve_input_scratch(GRANT - baseline).unwrap();
        assert!(
            Receiving::new(
                &session,
                &fixture,
                server.binding("wide_text", 51),
                Mode::NativeFragments
            )
            .is_err()
        );
        drop((denied, fixture, output, client));
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        server.idle().await;
        let mut client = server.client("reader").await;
        fresh_retry(&server, &mut client, 52, 1, "wide_text").await;
        drop(client);
        server.stop().await;
    });
}
