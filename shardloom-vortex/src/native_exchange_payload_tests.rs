// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::native_exchange_protocol::{
    attempts::{Connection, Principal, Registry},
    authority::{Authority, SUITE_QUERY},
};
use serde_json::json;
use std::path::PathBuf;

fn root() -> PathBuf {
    std::env::var_os("SHARDLOOM_EXCHANGE_FIXTURES")
        .map(PathBuf::from)
        .expect("independent fixture root required")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap()
}

fn attempt(authority: Arc<Authority>, source: &str) -> (Registry, Binding, Arc<Attempt>) {
    let epoch = Bytes::from_static(&[1; 16]);
    let binding = authority
        .case(source)
        .unwrap()
        .binding(
            "reader",
            &epoch,
            Bytes::from_static(&[2; 16]),
            1,
            SUITE_QUERY,
        )
        .unwrap();
    let registry = Registry::new(authority, epoch).unwrap();
    let attempt = registry
        .begin(Principal::Reader, &binding, &Connection::new(), false)
        .unwrap();
    (registry, binding, attempt)
}

#[test]
#[ignore = "requires independent frozen fixtures and the sequential storage/process guard"]
fn framed_provider_payloads_feed_private_receivers_for_all_65_frozen_workflows() {
    let authority = Arc::new(Authority::load().unwrap());
    let root = root();
    let runtime = runtime();
    let mut workflows = Vec::new();
    for case in &authority.cases {
        for mode in [
            Mode::NativeFragments,
            Mode::NativeAssembled,
            Mode::ArrowNone,
            Mode::ArrowLz4,
            Mode::ArrowZstd,
        ] {
            let sender = ResidentVortexSession::new(bridge::GRANT, 1).unwrap();
            let receiver = ResidentVortexSession::new(bridge::GRANT, 1).unwrap();
            let fixture = Fixture::load(
                &root.join("fixture-manifest.json"),
                &case.id,
                &authority,
                &sender,
            )
            .unwrap();
            let receiving_fixture = Fixture::load(
                &root.join("fixture-manifest.json"),
                &case.id,
                &authority,
                &receiver,
            )
            .unwrap();
            let cancellation = CancellationToken::default();
            let source = fixture
                .original_source(&root, &sender, &cancellation)
                .unwrap();
            let (registry, binding, attempt) = attempt(authority.clone(), &case.id);
            let queue = Queue::new(sender.shared_memory_pool());
            let mut sending = Sending::new(
                &source,
                &fixture,
                binding.clone(),
                mode,
                &attempt.cancellation,
            )
            .unwrap();
            let mut receiving =
                Receiving::new(&receiver, &receiving_fixture, binding, mode).unwrap();
            let transient = receiver
                .reserve_input_scratch((MAX_RPC * 2) as u64)
                .unwrap();
            runtime.block_on(async {
                while let Some(frame) = sending.next(&queue, &attempt).await.unwrap() {
                    let private = FlightData::decode(Bytes::from(frame.encode_to_vec())).unwrap();
                    drop(frame);
                    assert_eq!(queue.snapshot(), (0, 0));
                    receiving.accept(private).unwrap();
                }
            });
            let sent = sending.report.clone();
            assert_eq!(
                sent.complete_message_assembly_bytes == 0,
                mode != Mode::NativeAssembled
            );
            attempt.complete().unwrap();
            drop((sending, attempt, source, fixture, transient));
            assert_eq!(registry.snapshot().unwrap().0, 0);
            assert_eq!(sender.snapshot().memory.reserved_bytes, 0);
            let (received, receipt) = runtime
                .block_on(receiving.finish(true, &receiving_fixture, &cancellation))
                .unwrap();
            assert_eq!(receipt.frames, sent.frames);
            assert_eq!(receipt.application_wire_bytes, sent.application_wire_bytes);
            let consumer = receiving_fixture.consume(&received, &cancellation).unwrap();
            drop((received, receiving_fixture));
            assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
            workflows.push(
                json!({"source": case.id, "mode": mode.label(), "send": sent,
                "receive": receipt, "consumer": consumer, "sender_final_reserved_bytes": 0,
                "receiver_final_reserved_bytes": 0}),
            );
        }
    }
    let path =
        std::env::var_os("SHARDLOOM_EXCHANGE_PAYLOAD_REPORT").expect("fresh report path required");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(
        file,
        &json!({"version": 1,
        "scope": "in-process protocol payload and receiving owner; no socket or two-process claim",
        "workflows": workflows}),
    )
    .unwrap();
}

#[test]
#[ignore = "requires independent frozen fixtures and the sequential storage/process guard"]
fn private_receiving_refusals_release_buffers_before_retry_and_never_expose_partial_sources() {
    let authority = Arc::new(Authority::load().unwrap());
    let root = root();
    let runtime = runtime();
    for mode in [Mode::NativeFragments, Mode::ArrowZstd] {
        let sender = ResidentVortexSession::new(bridge::GRANT, 1).unwrap();
        let receiver = ResidentVortexSession::new(bridge::GRANT, 1).unwrap();
        let fixture = Fixture::load(
            &root.join("fixture-manifest.json"),
            "dictionary_large",
            &authority,
            &sender,
        )
        .unwrap();
        let cancellation = CancellationToken::default();
        let source = fixture
            .original_source(&root, &sender, &cancellation)
            .unwrap();
        let (_, binding, attempt) = attempt(authority.clone(), "dictionary_large");
        let mut sending =
            Sending::new(&source, &fixture, binding.clone(), mode, &cancellation).unwrap();
        let queue = Queue::new(sender.shared_memory_pool());
        let mut frames = Vec::new();
        let all_wire = receiver
            .reserve_input_scratch(bridge::PROVIDER_ALLOWANCE)
            .unwrap();
        runtime.block_on(async {
            while let Some(frame) = sending.next(&queue, &attempt).await.unwrap() {
                frames.push(FlightData::decode(Bytes::from(frame.encode_to_vec())).unwrap());
            }
        });
        attempt.complete().unwrap();
        drop((sending, attempt, source));
        let before = receiver.snapshot().memory.reserved_bytes;
        let mut receiving = Receiving::new(&receiver, &fixture, binding.clone(), mode).unwrap();
        receiving.accept(frames[0].clone()).unwrap();
        assert!(receiving.accept(frames[0].clone()).is_err());
        assert_eq!(receiver.snapshot().memory.reserved_bytes, before);
        assert!(receiving.accept(frames[1].clone()).is_err());
        assert!(
            runtime
                .block_on(receiving.finish(true, &fixture, &cancellation))
                .is_err()
        );
        for (limit, success) in [(frames.len() - 1, true), (frames.len(), false)] {
            let mut receiving = Receiving::new(&receiver, &fixture, binding.clone(), mode).unwrap();
            for frame in frames.iter().take(limit) {
                receiving.accept(frame.clone()).unwrap();
            }
            assert!(
                runtime
                    .block_on(receiving.finish(success, &fixture, &cancellation))
                    .is_err()
            );
            assert_eq!(receiver.snapshot().memory.reserved_bytes, before);
        }
        let deny = receiver
            .reserve_input_scratch(bridge::GRANT - before - 1)
            .unwrap();
        assert!(Receiving::new(&receiver, &fixture, binding.clone(), mode).is_err());
        drop(deny);
        let mut receiving = Receiving::new(&receiver, &fixture, binding, mode).unwrap();
        for frame in &frames {
            receiving.accept(frame.clone()).unwrap();
        }
        let (source, _) = runtime
            .block_on(receiving.finish(true, &fixture, &cancellation))
            .unwrap();
        assert_eq!(source.row_count() as u64, fixture.authority.rows);
        drop((source, frames, all_wire, fixture));
        assert_eq!(sender.snapshot().memory.reserved_bytes, 0);
        assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
    }
}
