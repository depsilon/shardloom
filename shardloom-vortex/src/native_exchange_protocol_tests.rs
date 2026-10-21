// SPDX-License-Identifier: Apache-2.0
//! Wire/state tests only. Real native decoding and network failures have separate
//! acceptance obligations; these synthetic message bytes cannot establish them.

use super::*;
use wire::{
    ArrowPosition, Cancel, NativeFragment, Operations, Projection, ProjectionField, ReplacePolicy,
    SqlTicket,
};

pub(super) fn binding() -> Binding {
    Binding {
        server_epoch: Bytes::from(vec![1; 16]),
        principal: "reader".into(),
        source_id: "tiny".into(),
        source_sha256: hash(b"original source"),
        dtype_sha256: hash(b"original dtype"),
        operation_sha256: hash(b"typed operation"),
        attempt_id: Bytes::from(vec![2; 16]),
        partition_id: 0,
        first_row: 0,
        rows: 7,
        policy_version: 1,
        query_id: "fixture-projections".into(),
    }
}

fn open() -> Open {
    Open {
        binding: Some(binding()),
        profile: Some(profile()),
        mode: Mode::NativeFragments as i32,
    }
}

fn hex(value: &str) -> Bytes {
    assert_eq!(value.len() % 2, 0);
    Bytes::from(
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).unwrap())
            .collect::<Vec<_>>(),
    )
}

fn frame(sequence: u64, body: Body) -> FlightData {
    FlightData {
        app_metadata: Frame {
            binding: Some(binding()),
            sequence,
            body: Some(body),
        }
        .encode_to_vec()
        .into(),
        ..Default::default()
    }
}

fn fixture() -> (NativeExpected, Vec<FlightData>, [Bytes; 2]) {
    let parts = [
        Bytes::from_static(b"synthetic dtype bytes"),
        Bytes::from_static(b"synthetic encoded array bytes"),
    ];
    let mut ordered = Sha256::new();
    for part in &parts {
        ordered.update((part.len() as u64).to_le_bytes());
        ordered.update(part);
    }
    let expected = NativeExpected {
        messages: parts.each_ref().map(|part| (part.len() as u64, hash(part))),
        ordered_sha256: Bytes::copy_from_slice(&ordered.finalize()),
    };
    let bytes = parts.iter().map(Bytes::len).sum::<usize>() as u64;
    let frames = vec![
        frame(
            0,
            Body::Ack(Ack {
                profile: Some(profile()),
                mode: Mode::NativeFragments as i32,
                expected_payload_sha256: expected.ordered_sha256.clone(),
                expected_payload_bytes: bytes,
                expected_messages: 2,
            }),
        ),
        frame(
            1,
            Body::Native(NativeFragment {
                message_index: 0,
                message_offset: 0,
                message_bytes: parts[0].len() as u64,
                segments: vec![parts[0].slice(..4), parts[0].slice(4..)],
            }),
        ),
        frame(
            2,
            Body::Native(NativeFragment {
                message_index: 1,
                message_offset: 0,
                message_bytes: parts[1].len() as u64,
                segments: vec![parts[1].clone()],
            }),
        ),
        frame(
            3,
            Body::Complete(Complete {
                frames: 4,
                messages: 2,
                rows: binding().rows,
                payload_bytes: bytes,
                ordered_payload_sha256: expected.ordered_sha256.clone(),
            }),
        ),
    ];
    (expected, frames, parts)
}

pub(super) fn change(data: &FlightData, f: impl FnOnce(&mut Frame)) -> FlightData {
    let mut value = Frame::decode(data.app_metadata.clone()).unwrap();
    f(&mut value);
    FlightData {
        app_metadata: value.encode_to_vec().into(),
        ..data.clone()
    }
}

#[test]
fn frozen_schema_and_original_declarations_have_exact_field_correspondence() {
    let schema = include_str!("../tests/fixtures/native-exchange/exchange-v1.proto");
    let source = include_str!("native_exchange_wire.rs");
    let messages = regex::Regex::new(r"(?ms)^message (\w+) \{(.*?)^\}").unwrap();
    let proto_field = regex::Regex::new(r"(?m)^\s*(repeated )?(\w+) (\w+) = (\d+);").unwrap();
    let rust_field =
        regex::Regex::new(r"(?s)#\[prost\((.*?)\)\]\s*pub\(crate\) (\w+):\s*([^,]+),").unwrap();
    let names = messages
        .captures_iter(schema)
        .map(|row| row[1].to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names.len(), 15);
    let mut fields = 0;
    for message in messages.captures_iter(schema) {
        let name = &message[1];
        let pattern = format!(r"(?ms)^pub\(crate\) struct {name} \{{(.*?)^\}}");
        let declaration = regex::Regex::new(&pattern)
            .unwrap()
            .captures(source)
            .unwrap()[1]
            .to_owned();
        let rust = rust_field
            .captures_iter(&declaration)
            .map(|row| {
                (
                    row[2].to_owned(),
                    (row[1].replace(' ', ""), row[3].replace(' ', "")),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let proto = proto_field.captures_iter(&message[2]).collect::<Vec<_>>();
        assert_eq!(rust.len(), if name == "Frame" { 3 } else { proto.len() });
        for field in proto {
            fields += 1;
            let field_name = &field[3];
            if name == "Frame" && matches!(field_name, "ack" | "native" | "arrow" | "complete") {
                continue;
            }
            let repeated = field.get(1).is_some();
            let kind = &field[2];
            let (attribute, rust_type) = &rust[field_name];
            assert!(
                attribute.contains(&format!("tag=\"{}\"", &field[4])),
                "{name}.{field_name}"
            );
            assert_eq!(
                attribute.contains("repeated"),
                repeated,
                "{name}.{field_name}"
            );
            let (annotation, scalar) = match kind {
                "bytes" => ("bytes=\"bytes\"".to_owned(), "bytes::Bytes".to_owned()),
                "string" => ("string".to_owned(), "String".to_owned()),
                "uint64" => ("uint64".to_owned(), "u64".to_owned()),
                "uint32" => ("uint32".to_owned(), "u32".to_owned()),
                "Mode" => ("enumeration=\"Mode\"".to_owned(), "i32".to_owned()),
                other => {
                    assert!(names.iter().any(|name| name == other));
                    ("message".to_owned(), other.to_owned())
                }
            };
            assert!(attribute.contains(&annotation), "{name}.{field_name}");
            let expected = if repeated {
                format!("Vec<{scalar}>")
            } else if annotation == "message" {
                format!("Option<{scalar}>")
            } else {
                scalar
            };
            assert_eq!(rust_type, &expected, "{name}.{field_name}");
        }
    }
    assert_eq!(fields, 62);
    for (name, tag, kind) in [
        ("Ack", 3, "Ack"),
        ("Native", 4, "NativeFragment"),
        ("Arrow", 5, "ArrowPosition"),
        ("Complete", 6, "Complete"),
    ] {
        assert!(source.contains(&format!(
            "#[prost(message, tag = \"{tag}\")]\n        {name}(super::{kind})"
        )));
    }
    for (mode, value) in [
        (Mode::Unspecified, 0),
        (Mode::NativeFragments, 1),
        (Mode::NativeAssembled, 2),
        (Mode::ArrowNone, 3),
        (Mode::ArrowLz4, 4),
        (Mode::ArrowZstd, 5),
    ] {
        assert_eq!(mode as i32, value);
    }
}

#[test]
fn profile_and_operations_match_independently_frozen_protobuf_bytes() {
    let freeze: serde_json::Value = serde_json::from_str(include_str!(
        "../../docs/benchmarks/evidence/native-exchange-control-2026-10-20/protocol-freeze.json"
    ))
    .unwrap();
    let golden = hex(freeze["profile"]["protobuf_hex"].as_str().unwrap());
    assert_eq!(profile().encode_to_vec(), golden);
    assert_eq!(
        hash(&golden),
        hex(freeze["profile"]["sha256"].as_str().unwrap())
    );
    let cases = freeze["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 13);
    for case in cases {
        let bytes = hex(case["operations_protobuf_hex"].as_str().unwrap());
        let operations: Operations = strict_decode(bytes.clone(), MAX_CONTROL).unwrap();
        assert_eq!(operations.projections.len(), 3);
        assert_eq!(operations.execution_order, [0, 1, 2, 1]);
        assert_eq!(
            hash(&bytes),
            hex(case["operation_sha256"].as_str().unwrap())
        );
    }
    let field = |source: &str, alias: &str| ProjectionField {
        source: source.into(),
        alias: alias.into(),
    };
    let operation = Operations {
        projections: vec![
            Projection {
                id: "all".into(),
                fields: vec![
                    field("event_key", "event_key"),
                    field("category", "category"),
                ],
            },
            Projection {
                id: "first".into(),
                fields: vec![field("event_key", "event_key")],
            },
            Projection {
                id: "last_first_renamed".into(),
                fields: vec![
                    field("category", "last_value"),
                    field("event_key", "first_value"),
                ],
            },
        ],
        execution_order: vec![0, 1, 2, 1],
    };
    let dictionary = cases
        .iter()
        .find(|case| case["id"] == "dictionary_large")
        .unwrap();
    assert_eq!(
        operation.encode_to_vec(),
        hex(dictionary["operations_protobuf_hex"].as_str().unwrap())
    );
}

#[test]
fn strict_control_rejects_unknown_duplicate_reordered_and_noncanonical_fields() {
    let ordinary = open();
    let encoded = ordinary.encode_to_vec();
    assert_eq!(
        strict_decode::<Open>(encoded.clone().into(), MAX_CONTROL).unwrap(),
        ordinary
    );
    for tail in [&[0x98, 0x06, 1][..], &[0x18, 1][..]] {
        let mut changed = encoded.clone();
        changed.extend_from_slice(tail);
        assert_eq!(
            strict_decode::<Open>(changed.into(), MAX_CONTROL)
                .unwrap_err()
                .code,
            "SLX_INVALID_WIRE"
        );
    }
    assert!(strict_decode::<Profile>(Bytes::from_static(&[8, 0x81, 0]), MAX_CONTROL).is_err());
    assert!(strict_decode::<Profile>(Bytes::from_static(&[8, 1, 24, 0]), MAX_CONTROL).is_err());
    assert!(strict_decode::<Profile>(Bytes::from_static(&[8]), MAX_CONTROL).is_err());
    let mut changed = vec![0x18, 1];
    changed.extend_from_slice(&encoded[..encoded.len() - 2]);
    assert!(strict_decode::<Open>(changed.into(), MAX_CONTROL).is_err());
    assert_eq!(
        strict_decode::<Open>(Bytes::from(vec![0; MAX_CONTROL + 1]), MAX_CONTROL)
            .unwrap_err()
            .code,
        "SLX_RESOURCE_LIMIT"
    );
    let cancel = Cancel {
        binding: Some(binding()),
    };
    assert_eq!(
        strict_decode::<Cancel>(cancel.encode_to_vec().into(), MAX_CONTROL).unwrap(),
        cancel
    );
    let ticket = SqlTicket {
        binding: Some(binding()),
    };
    assert_eq!(
        strict_decode::<SqlTicket>(ticket.encode_to_vec().into(), MAX_CONTROL).unwrap(),
        ticket
    );
    let replace = ReplacePolicy {
        server_epoch: binding().server_epoch,
        expected_version: 1,
        new_version: 2,
    };
    assert_eq!(
        strict_decode::<ReplacePolicy>(replace.encode_to_vec().into(), MAX_CONTROL).unwrap(),
        replace
    );
}

#[test]
fn negotiation_refuses_every_profile_and_binding_mismatch_without_mode_substitution() {
    validate_open(&open(), &binding(), Mode::NativeFragments).unwrap();
    let mutations: [fn(&mut Profile); 6] = [
        |p| p.extension_version += 1,
        |p| p.vortex_version = "0.86.0".into(),
        |p| p.vortex_message_version = 1,
        |p| p.physical_registry.swap(0, 1),
        |p| p.limits.as_mut().unwrap().queued_bytes += 1,
        |p| p.limits = None,
    ];
    for mutate in mutations {
        let mut changed = open();
        mutate(changed.profile.as_mut().unwrap());
        assert_eq!(
            validate_open(&changed, &binding(), Mode::NativeFragments)
                .unwrap_err()
                .code,
            "SLX_IDENTITY"
        );
    }
    let mutations: [fn(&mut Binding); 12] = [
        |b| b.server_epoch = Bytes::from(vec![3; 16]),
        |b| b.principal = "reader2".into(),
        |b| b.source_id = "other".into(),
        |b| b.source_sha256 = hash(b"changed"),
        |b| b.dtype_sha256 = hash(b"changed"),
        |b| b.operation_sha256 = hash(b"changed"),
        |b| b.attempt_id = Bytes::from(vec![3; 16]),
        |b| b.partition_id = 1,
        |b| b.first_row = 1,
        |b| b.rows += 1,
        |b| b.policy_version += 1,
        |b| b.query_id = "different".into(),
    ];
    for mutate in mutations {
        let mut changed = open();
        mutate(changed.binding.as_mut().unwrap());
        assert!(validate_open(&changed, &binding(), Mode::NativeFragments).is_err());
    }
    for mode in [0, 2, 3, 4, 5, 99, -1] {
        let mut changed = open();
        changed.mode = mode;
        assert!(validate_open(&changed, &binding(), Mode::NativeFragments).is_err());
    }
    for name in ["", "../tiny", "tiny/file", "λ", &"x".repeat(65)] {
        let mut changed = binding();
        changed.source_id = name.into();
        assert!(validate_binding(&changed, &changed).is_err());
    }
    let mut missing = open();
    missing.binding = None;
    assert!(validate_open(&missing, &binding(), Mode::NativeFragments).is_err());
    let mut missing = open();
    missing.profile = None;
    assert!(validate_open(&missing, &binding(), Mode::NativeFragments).is_err());
}

#[test]
fn native_fragments_require_ordered_bytes_completion_and_successful_rpc_eof() {
    let (expected, frames, parts) = fixture();
    let mut state = NativeStream::new(binding(), Mode::NativeFragments, expected.clone()).unwrap();
    let mut received = [Vec::new(), Vec::new()];
    let mut completed_messages = Vec::new();
    for data in &frames {
        match state.accept(data).unwrap() {
            NativeStep::Payload {
                message,
                segments,
                message_complete,
            } => {
                for segment in segments {
                    received[message].extend_from_slice(&segment);
                }
                if message_complete {
                    completed_messages.push(message);
                }
            }
            NativeStep::Ack | NativeStep::Complete => {}
        }
    }
    state.finish(true).unwrap();
    assert_eq!(received[0], parts[0]);
    assert_eq!(received[1], parts[1]);
    assert_eq!(completed_messages, [0, 1]);
    for prefix in 0..=frames.len() {
        let mut state =
            NativeStream::new(binding(), Mode::NativeFragments, expected.clone()).unwrap();
        for data in &frames[..prefix] {
            state.accept(data).unwrap();
        }
        assert!(state.finish(false).is_err());
        if prefix < frames.len() {
            let mut state =
                NativeStream::new(binding(), Mode::NativeFragments, expected.clone()).unwrap();
            for data in &frames[..prefix] {
                state.accept(data).unwrap();
            }
            assert!(state.finish(true).is_err());
        }
    }
    let mut state = NativeStream::new(binding(), Mode::NativeFragments, expected).unwrap();
    for data in &frames {
        state.accept(data).unwrap();
    }
    assert!(state.accept(&frames[3]).is_err());
    assert!(state.finish(true).is_err());
}

#[test]
fn changed_fragment_coordinates_hashes_and_completion_poison_the_attempt() {
    let (expected, frames, _) = fixture();
    let mutations: [fn(&mut Frame); 8] = [
        |f| f.sequence += 1,
        |f| f.binding.as_mut().unwrap().attempt_id = Bytes::from(vec![8; 16]),
        |f| {
            let Some(Body::Native(n)) = &mut f.body else {
                panic!()
            };
            n.message_index += 1;
        },
        |f| {
            let Some(Body::Native(n)) = &mut f.body else {
                panic!()
            };
            n.message_offset += 1;
        },
        |f| {
            let Some(Body::Native(n)) = &mut f.body else {
                panic!()
            };
            n.message_bytes += 1;
        },
        |f| {
            let Some(Body::Native(n)) = &mut f.body else {
                panic!()
            };
            n.segments.reverse();
        },
        |f| {
            let Some(Body::Native(n)) = &mut f.body else {
                panic!()
            };
            n.segments.push(Bytes::from_static(b"extra"));
        },
        |f| {
            f.body = Some(Body::Arrow(ArrowPosition {
                ack: None,
                complete: None,
            }));
        },
    ];
    for mutate in mutations {
        let mut state =
            NativeStream::new(binding(), Mode::NativeFragments, expected.clone()).unwrap();
        state.accept(&frames[0]).unwrap();
        assert!(state.accept(&change(&frames[1], mutate)).is_err());
        assert!(state.accept(&frames[1]).is_err());
        assert!(state.finish(true).is_err());
    }
    let changes: [fn(&mut Complete); 5] = [
        |c| c.frames += 1,
        |c| c.messages += 1,
        |c| c.rows += 1,
        |c| c.payload_bytes += 1,
        |c| c.ordered_payload_sha256 = hash(b"wrong"),
    ];
    for mutate in changes {
        let mut state =
            NativeStream::new(binding(), Mode::NativeFragments, expected.clone()).unwrap();
        for data in &frames[..3] {
            state.accept(data).unwrap();
        }
        let bad = change(&frames[3], |frame| {
            let Some(Body::Complete(c)) = &mut frame.body else {
                panic!()
            };
            mutate(c);
        });
        assert!(state.accept(&bad).is_err());
        assert!(state.finish(true).is_err());
    }
}

#[test]
fn frame_prescan_bounds_repeated_segment_allocation_and_control_size() {
    let (_, frames, _) = fixture();
    let fragment = |segments| {
        frame(
            1,
            Body::Native(NativeFragment {
                message_index: 0,
                message_offset: 0,
                message_bytes: MAX_FRAGMENT as u64,
                segments,
            }),
        )
    };
    decode_frame(&fragment(vec![Bytes::from_static(b"x"); MAX_SEGMENTS])).unwrap();
    decode_frame(&fragment(vec![Bytes::from(vec![1; MAX_FRAGMENT])])).unwrap();
    for segments in [
        vec![],
        vec![Bytes::new()],
        vec![Bytes::from_static(b"x"); MAX_SEGMENTS + 1],
        vec![Bytes::from(vec![1; MAX_FRAGMENT + 1])],
    ] {
        assert!(decode_frame(&fragment(segments)).is_err());
    }
    let oversized = change(&frames[0], |frame| {
        frame.binding.as_mut().unwrap().source_id = "x".repeat(MAX_CONTROL);
    });
    assert_eq!(
        decode_frame(&oversized).unwrap_err().code,
        "SLX_RESOURCE_LIMIT"
    );
    let oversized = FlightData {
        data_body: Bytes::from(vec![1; MAX_RPC]),
        ..frames[0].clone()
    };
    assert_eq!(
        decode_frame(&oversized).unwrap_err().code,
        "SLX_RESOURCE_LIMIT"
    );
    let mut double = frames[0].app_metadata.to_vec();
    double.extend_from_slice(&[0x32, 0]);
    assert!(
        decode_frame(&FlightData {
            app_metadata: double.into(),
            ..Default::default()
        })
        .is_err()
    );
    let mut unknown = frames[0].app_metadata.to_vec();
    unknown.extend_from_slice(&[0x38, 1]);
    assert!(
        decode_frame(&FlightData {
            app_metadata: unknown.into(),
            ..Default::default()
        })
        .is_err()
    );
    for malformed in [
        vec![0x0a, 0xff],
        vec![
            0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2,
        ],
        vec![0x0d, 0, 0, 0, 0],
    ] {
        assert!(
            decode_frame(&FlightData {
                app_metadata: malformed.into(),
                ..Default::default()
            })
            .is_err()
        );
    }
}
