// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::native_exchange_protocol::arrow::inspect;
use prost::Message as _;
use vortex::array::expr::stats::Stat;

fn manifest() -> std::path::PathBuf {
    std::env::var_os("SHARDLOOM_EXCHANGE_FIXTURES")
        .map(std::path::PathBuf::from)
        .expect("independently generated fixture root required")
        .join("fixture-manifest.json")
}

fn native_wire(encoding: &NativeEncoding, case: &Case) -> [Vec<u8>; 2] {
    let messages = encoding.messages.each_ref().map(|parts| parts.concat());
    let mut digest = Sha256::new();
    for (message, expected) in messages.iter().zip(&case.native.messages) {
        assert_eq!(message.len() as u64, expected.0);
        assert_eq!(hash(message), expected.1);
        digest.update((message.len() as u64).to_le_bytes());
        digest.update(message);
    }
    assert_eq!(&digest.finalize()[..], case.native.ordered_sha256.as_ref());
    messages
}

#[test]
#[ignore = "requires independent frozen fixtures and the sequential storage/process guard"]
fn bridge_run_end_source_statistics_match_release_and_debug_with_bounded_preparation() {
    fn run_end_child(array: &ArrayRef) -> Option<ArrayRef> {
        if array.as_opt::<RunEnd>().is_some() {
            Some(array.clone())
        } else {
            array.children_iter().find_map(run_end_child)
        }
    }

    let authority = Authority::load().unwrap();
    let manifest = manifest();
    let session = ResidentVortexSession::new(GRANT, 1).unwrap();
    let fixture = Fixture::load(&manifest, "run_end", &authority, &session).unwrap();
    let cancellation = CancellationToken::default();
    let source = fixture
        .original_source(manifest.parent().unwrap(), &session, &cancellation)
        .unwrap();
    let expected = native_wire(
        &encode_native(&source, Mode::NativeFragments, &cancellation).unwrap(),
        &fixture.authority,
    );
    let run_end =
        run_end_child(&source.0.array).expect("frozen source must retain its run-end child");
    for stat in [Stat::IsSorted, Stat::IsStrictSorted] {
        run_end.as_::<RunEnd>().ends().statistics().clear(stat);
    }
    // Reproduce release construction in either profile: ordinary encoding must
    // differ until common preparation recomputes the missing native statistics.
    let missing = encode_native(&source, Mode::NativeFragments, &cancellation).unwrap();
    assert_ne!(missing.messages[1].concat(), expected[1]);
    drop(missing);
    let baseline = session.snapshot().memory.reserved_bytes;
    let deny = session.reserve_input_scratch(GRANT - baseline).unwrap();
    assert!(
        session
            .with_native_execution_context(&cancellation, |context| {
                prepare_source_statistics(&source.0.array, context)
            })
            .is_err()
    );
    drop(deny);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(
        session
            .with_native_execution_context(&cancelled, |context| {
                prepare_source_statistics(&source.0.array, context)
            })
            .is_err()
    );
    session
        .with_native_execution_context(&cancellation, |context| {
            prepare_source_statistics(&source.0.array, context)
        })
        .unwrap();
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    for mode in [Mode::NativeFragments, Mode::NativeAssembled] {
        assert_eq!(
            native_wire(
                &encode_native(&source, mode, &cancellation).unwrap(),
                &fixture.authority
            ),
            expected
        );
    }
    drop((run_end, source, fixture));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

async fn arrow_wire(encoding: &mut ArrowEncoding, case: &Case, mode: Mode) -> Vec<FlightData> {
    let expected = case.arrow_expected(mode).unwrap();
    let mut frames = Vec::new();
    let mut digest = Sha256::new();
    while let Some(frame) = encoding.next().await.unwrap() {
        let expected = &expected.frames[frames.len()];
        assert_eq!(frame.data_header.len(), expected.header_bytes);
        assert_eq!(frame.data_body.len(), expected.body_bytes);
        assert_eq!(hash(&frame.data_header), expected.header_sha256);
        assert_eq!(hash(&frame.data_body), expected.body_sha256);
        assert_eq!(inspect(&frame, mode).unwrap(), expected.metadata);
        for bytes in [&frame.data_header, &frame.data_body] {
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(bytes);
        }
        // Independent receiving storage: no source/provider owner survives this copy.
        frames.push(FlightData::decode(Bytes::from(frame.encode_to_vec())).unwrap());
    }
    assert_eq!(frames.len(), expected.frames.len());
    assert_eq!(&digest.finalize()[..], expected.ordered_sha256.as_ref());
    frames
}

#[test]
#[ignore = "requires independent frozen fixtures and the sequential storage/process guard"]
fn bridge_payloads_match_all_frozen_controls_and_complete_native_consumers() {
    let authority = Authority::load().unwrap();
    let manifest = manifest();
    let root = manifest.parent().unwrap();
    let cancellation = CancellationToken::default();
    let mut reports = Vec::new();
    for case in &authority.cases {
        for mode in [
            Mode::NativeFragments,
            Mode::NativeAssembled,
            Mode::ArrowNone,
            Mode::ArrowLz4,
            Mode::ArrowZstd,
        ] {
            let sender = ResidentVortexSession::new(GRANT, 1).unwrap();
            let receiver = ResidentVortexSession::new(GRANT, 1).unwrap();
            let fixture = Fixture::load(&manifest, &case.id, &authority, &sender).unwrap();
            let receiver_fixture =
                Fixture::load(&manifest, &case.id, &authority, &receiver).unwrap();
            let source = fixture
                .original_source(root, &sender, &cancellation)
                .unwrap();
            let receive_credit = receiver.reserve_input_scratch(PROVIDER_ALLOWANCE).unwrap();
            let received = match mode {
                Mode::NativeFragments | Mode::NativeAssembled => {
                    let encoding = encode_native(&source, mode, &cancellation).unwrap();
                    let messages = native_wire(&encoding, case);
                    let payload: u64 = case.native.messages.iter().map(|part| part.0).sum();
                    assert_eq!(
                        encoding.assembly_bytes,
                        if mode == Mode::NativeAssembled {
                            payload
                        } else {
                            0
                        }
                    );
                    drop(encoding);
                    ResidentMemorySource::from_vortex_ipc(
                        &receiver,
                        &messages[0],
                        &messages[1],
                        &cancellation,
                    )
                    .unwrap()
                }
                _ => futures::executor::block_on(async {
                    let mut encoding =
                        encode_arrow(&source, &fixture, mode, &cancellation).unwrap();
                    let frames = arrow_wire(&mut encoding, case, mode).await;
                    drop(encoding);
                    decode_arrow(&receiver, frames, &cancellation)
                        .await
                        .unwrap()
                }),
            };
            drop((receive_credit, source, fixture));
            assert_eq!(sender.snapshot().memory.reserved_bytes, 0);
            let encodings = receiver_fixture.check_source(&received, mode).unwrap();
            let results = receiver_fixture.consume(&received, &cancellation).unwrap();
            drop((received, receiver_fixture));
            assert_eq!(receiver.snapshot().memory.reserved_bytes, 0);
            reports.push(json!({"source": case.id, "mode": mode.label(), "encodings": encodings,
                "consumer": results, "sender_final_reserved_bytes": 0, "receiver_final_reserved_bytes": 0,
                "sender_peak_reserved_bytes": sender.snapshot().memory.peak_reserved_bytes,
                "receiver_peak_reserved_bytes": receiver.snapshot().memory.peak_reserved_bytes}));
        }
    }
    let path =
        std::env::var_os("SHARDLOOM_EXCHANGE_BRIDGE_REPORT").expect("fresh report path required");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"version": 1,
        "scope": "in-process native bridge and independent receiving storage; no socket or process-boundary claim",
        "workflows": reports})).unwrap();
}

#[test]
#[ignore = "requires independent frozen fixtures and the sequential storage/process guard"]
fn bridge_grant_denial_and_cloned_parts_retain_allocation_owners() {
    let authority = Authority::load().unwrap();
    let manifest = manifest();
    let session = ResidentVortexSession::new(GRANT, 1).unwrap();
    let fixture = Fixture::load(&manifest, "dictionary_large", &authority, &session).unwrap();
    let cancellation = CancellationToken::default();
    let source = fixture
        .original_source(manifest.parent().unwrap(), &session, &cancellation)
        .unwrap();
    let baseline = session.snapshot().memory.reserved_bytes;
    let encoding = encode_native(&source, Mode::NativeFragments, &cancellation).unwrap();
    assert_eq!(
        session.snapshot().memory.reserved_bytes,
        baseline + PROVIDER_ALLOWANCE
    );
    let retained = encoding.messages[1]
        .iter()
        .find(|bytes| !bytes.is_empty())
        .unwrap()
        .clone();
    drop(encoding);
    assert_eq!(
        session.snapshot().memory.reserved_bytes,
        baseline + PROVIDER_ALLOWANCE
    );
    drop(retained);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let deny = session
        .reserve_input_scratch(GRANT - baseline - PROVIDER_ALLOWANCE + 1)
        .unwrap();
    assert!(encode_native(&source, Mode::NativeFragments, &cancellation).is_err());
    assert!(encode_arrow(&source, &fixture, Mode::ArrowZstd, &cancellation).is_err());
    drop(deny);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(encode_native(&source, Mode::NativeFragments, &cancellation).is_err());
    assert!(encode_arrow(&source, &fixture, Mode::ArrowZstd, &cancellation).is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    drop((fixture, source));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
