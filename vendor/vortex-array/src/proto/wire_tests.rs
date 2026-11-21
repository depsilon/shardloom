// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::fmt::Debug;
use std::process::Command;
use std::thread;

use prost::Message;
use prost::encoding::{self, WireType};

use super::dtype::{self as dt, d_type::DtypeType as Type};
use super::scalar::{self as sv, scalar_value::Kind};
use super::structural_tests::{self as fixtures, reference};

use super::wire_reference_fixtures as original;

fn wire_pair<A: Message + Clone, B: Message + Clone>(actual: &A, expected: &B) {
    let bytes = expected.encode_to_vec();
    assert_eq!(actual.encoded_len(), expected.encoded_len());
    assert_eq!(actual.encoded_len(), bytes.len());
    assert_eq!(actual.encode_to_vec(), bytes);
    let mut raw = Vec::new();
    actual.encode_raw(&mut raw);
    assert_eq!(raw, bytes);
    assert_eq!(
        actual.encode_length_delimited_to_vec(),
        expected.encode_length_delimited_to_vec()
    );
    for delimited in [false, true] {
        let needed = if delimited {
            expected.encode_length_delimited_to_vec().len()
        } else {
            bytes.len()
        };
        for capacity in [0, needed.saturating_sub(1), needed, needed + 1] {
            let mut left = vec![0xa5; capacity];
            let mut right = left.clone();
            let left_result = if delimited {
                actual.encode_length_delimited(&mut left.as_mut_slice())
            } else {
                actual.encode(&mut left.as_mut_slice())
            };
            let right_result = if delimited {
                expected.encode_length_delimited(&mut right.as_mut_slice())
            } else {
                expected.encode(&mut right.as_mut_slice())
            };
            assert_eq!(
                left_result.map_err(|e| e.to_string()),
                right_result.map_err(|e| e.to_string())
            );
            assert_eq!(left, right);
            if capacity < needed {
                assert!(left.iter().all(|byte| *byte == 0xa5));
            }
        }
    }
    let mut left = actual.clone();
    let mut right = expected.clone();
    left.clear();
    right.clear();
    assert_eq!(left.encode_to_vec(), right.encode_to_vec());
    assert!(left.encode_to_vec().is_empty());
}

#[test]
fn wire_bytes_lengths_presence_and_output_refusal_match_original() {
    let dtypes = fixtures::dtype_fixtures();
    let original_dtypes = original::dtype_fixtures();
    assert_eq!(dtypes.len(), original_dtypes.len());
    for (left, right) in dtypes.iter().zip(&original_dtypes) {
        wire_pair(left, right);
    }
    let values = fixtures::value_fixtures();
    let original_values = original::value_fixtures();
    assert_eq!(values.len(), original_values.len());
    for (left, right) in values.iter().zip(&original_values) {
        wire_pair(left, right);
    }
    for kind in 0..4 {
        wire_pair(
            &fixtures::value_chain(kind, 5, true),
            &original::value_chain(kind, 5, true),
        );
    }
    // Interleave complete branches and nonempty container tails within one plan.
    let fields = dt::DType {
        dtype_type: Some(Type::Struct(dt::Struct {
            names: (0..dtypes.len()).map(|i| format!("field_{i}")).collect(),
            dtypes,
            nullable: true,
        })),
    };
    let original_fields = reference::dtype::DType {
        dtype_type: Some(reference::dtype::d_type::DtypeType::Struct(
            reference::dtype::Struct {
                names: (0..original_dtypes.len())
                    .map(|i| format!("field_{i}"))
                    .collect(),
                dtypes: original_dtypes,
                nullable: true,
            },
        )),
    };
    let combined = sv::ScalarValue {
        kind: Some(Kind::VariantValue(Box::new(sv::Scalar {
            dtype: Some(fields),
            value: Some(Box::new(sv::ScalarValue {
                kind: Some(Kind::ListValue(sv::ListValue { values })),
            })),
        }))),
    };
    let original_combined = reference::scalar::ScalarValue {
        kind: Some(reference::scalar::scalar_value::Kind::VariantValue(
            Box::new(reference::scalar::Scalar {
                dtype: Some(original_fields),
                value: Some(Box::new(reference::scalar::ScalarValue {
                    kind: Some(reference::scalar::scalar_value::Kind::ListValue(
                        reference::scalar::ListValue {
                            values: original_values,
                        },
                    )),
                })),
            }),
        )),
    };
    wire_pair(&combined, &original_combined);
    for len in [0, 1, 126, 127, 128, 129, 16382, 16383, 16384, 16385] {
        let payload = vec![0xff; len];
        wire_pair(
            &sv::ScalarValue {
                kind: Some(Kind::BytesValue(payload.clone())),
            },
            &reference::scalar::ScalarValue {
                kind: Some(reference::scalar::scalar_value::Kind::BytesValue(
                    payload.clone(),
                )),
            },
        );
        let dtype = dt::DType {
            dtype_type: Some(Type::Extension(Box::new(dt::Extension {
                id: "id".into(),
                storage_dtype: Some(Box::new(fixtures::dtype_chain(7, 4, false))),
                metadata: Some(payload.clone()),
            }))),
        };
        let original = reference::dtype::DType {
            dtype_type: Some(reference::dtype::d_type::DtypeType::Extension(Box::new(
                reference::dtype::Extension {
                    id: "id".into(),
                    storage_dtype: Some(Box::new(original::dtype_chain(7, 4, false))),
                    metadata: Some(payload),
                },
            ))),
        };
        wire_pair(&dtype, &original);
    }
    for number in [0, 1, 127, 128, 16383, 16384, u32::MAX.into(), u64::MAX] {
        wire_pair(
            &sv::ScalarValue {
                kind: Some(Kind::Uint64Value(number)),
            },
            &reference::scalar::ScalarValue {
                kind: Some(reference::scalar::scalar_value::Kind::Uint64Value(number)),
            },
        );
    }
}

fn decode_result<M: Message + Default>(
    bytes: &[u8],
    delimited: bool,
) -> (Result<Vec<u8>, String>, usize) {
    let mut source = bytes;
    let result = if delimited {
        M::decode_length_delimited(&mut source)
    } else {
        M::decode(&mut source)
    };
    (
        result
            .map(|value| value.encode_to_vec())
            .map_err(|error| error.to_string()),
        source.len(),
    )
}

fn decode_pair(bytes: &[u8]) {
    for delimited in [false, true] {
        assert_eq!(
            decode_result::<dt::DType>(bytes, delimited),
            decode_result::<reference::dtype::DType>(bytes, delimited)
        );
        assert_eq!(
            decode_result::<sv::ScalarValue>(bytes, delimited),
            decode_result::<reference::scalar::ScalarValue>(bytes, delimited)
        );
    }
}

#[test]
fn default_decode_errors_unknown_fields_and_depth_limit_are_unchanged() {
    let mut bytes = original::dtype_fixtures()
        .iter()
        .map(Message::encode_to_vec)
        .collect::<Vec<_>>();
    bytes.extend(
        original::value_fixtures()
            .iter()
            .map(Message::encode_to_vec),
    );
    for wire in bytes.iter().filter(|wire| wire.len() <= 96) {
        decode_pair(wire);
        for end in 0..wire.len() {
            decode_pair(&wire[..end]);
        }
        for index in 0..wire.len() {
            for mask in [1, 0x80, 0xff] {
                let mut mutation = wire.clone();
                mutation[index] ^= mask;
                decode_pair(&mutation);
            }
        }
    }
    for depth in [0, 1, 48, 49, 50, 51, 52, 100, 101] {
        // Construct the deep raw bytes with the new writer; the original decoder
        // independently supplies the exact original admission/error boundary.
        decode_pair(&fixtures::dtype_chain(0, depth, false).encode_to_vec());
        decode_pair(&fixtures::value_chain(0, depth, false).encode_to_vec());
        let mut groups = Vec::new();
        for _ in 0..depth {
            encoding::encode_key(100, WireType::StartGroup, &mut groups);
        }
        for _ in 0..depth {
            encoding::encode_key(100, WireType::EndGroup, &mut groups);
        }
        decode_pair(&groups);
        groups.push(0xff);
        decode_pair(&groups);
    }
    assert!(
        dt::DType::decode(
            fixtures::dtype_chain(0, 10, false)
                .encode_to_vec()
                .as_slice()
        )
        .is_ok()
    );
    assert!(
        dt::DType::decode(
            fixtures::dtype_chain(0, 100, false)
                .encode_to_vec()
                .as_slice()
        )
        .is_err()
    );
    for wire_type in [
        WireType::Varint,
        WireType::SixtyFourBit,
        WireType::LengthDelimited,
        WireType::StartGroup,
        WireType::EndGroup,
        WireType::ThirtyTwoBit,
    ] {
        let mut unknown = Vec::new();
        encoding::encode_key(100, wire_type, &mut unknown);
        unknown.extend_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
        decode_pair(&unknown);
    }
}

fn merge_pair<A: Message + Debug, B: Message + Debug>(left: &mut A, right: &mut B, bytes: &[u8]) {
    let mut a = bytes;
    let mut b = bytes;
    assert_eq!(
        left.merge(&mut a).map_err(|error| error.to_string()),
        right.merge(&mut b).map_err(|error| error.to_string())
    );
    assert_eq!(a, b);
    assert_eq!(left.encode_to_vec(), right.encode_to_vec());
    assert_eq!(format!("{left:?}"), format!("{right:?}"));
}

#[test]
fn merging_retains_same_variant_partial_updates_and_other_variant_commit_policy() {
    let schema_failure = [0x3a, 8, 0x0a, 3, b'n', b'e', b'w', 0x12, 1, 0xff];
    let value_failure = [0x4a, 7, 0x0a, 2, 0x10, 1, 0x0a, 1, 0xff];
    for (mut left, mut right) in fixtures::dtype_fixtures()
        .into_iter()
        .zip(original::dtype_fixtures())
    {
        merge_pair(&mut left, &mut right, &schema_failure);
        merge_pair(&mut left, &mut right, &schema_failure);
    }
    for (mut left, mut right) in fixtures::value_fixtures()
        .into_iter()
        .zip(original::value_fixtures())
    {
        merge_pair(&mut left, &mut right, &value_failure);
        merge_pair(&mut left, &mut right, &value_failure);
    }
    let mut same = dt::DType {
        dtype_type: Some(Type::Struct(dt::Struct::default())),
    };
    assert!(same.merge(schema_failure.as_slice()).is_err());
    assert!(
        matches!(same.dtype_type.as_ref(), Some(Type::Struct(fields)) if fields.names == ["new"])
    );
    let mut other = dt::DType {
        dtype_type: Some(Type::Bool(dt::Bool { nullable: true })),
    };
    let unchanged = other.clone();
    assert!(other.merge(schema_failure.as_slice()).is_err());
    assert_eq!(other, unchanged);
    let mut same = sv::ScalarValue {
        kind: Some(Kind::ListValue(sv::ListValue::default())),
    };
    assert!(same.merge(value_failure.as_slice()).is_err());
    assert!(matches!(same.kind.as_ref(), Some(Kind::ListValue(list)) if list.values.len() == 1));
    let mut other = sv::ScalarValue {
        kind: Some(Kind::BoolValue(false)),
    };
    let unchanged = other.clone();
    assert!(other.merge(value_failure.as_slice()).is_err());
    assert_eq!(other, unchanged);
}

fn take_field<'a>(source: &mut &'a [u8], tag: u32) -> &'a [u8] {
    assert_eq!(
        encoding::decode_key(source).unwrap(),
        (tag, WireType::LengthDelimited)
    );
    let len = usize::try_from(encoding::decode_varint(source).unwrap()).unwrap();
    let (value, rest) = source.split_at(len);
    *source = rest;
    value
}

// Independently inspect the complete known fixture grammar. This does not use
// the new writer's plan or a deep decoder and does not claim decoder admission.
fn inspect_dtype(mut wire: &[u8], kind: usize, depth: usize, nullable: bool) {
    for level in (0..depth).rev() {
        let shape = if kind == 7 { level % 7 } else { kind };
        let tag = [8, 10, 7, 12, 13, 13, 9][shape];
        let mut body = take_field(&mut wire, tag);
        assert!(wire.is_empty());
        wire = match shape {
            0 | 1 => take_field(&mut body, 1),
            2 | 3 => {
                assert_eq!(
                    take_field(&mut body, 1),
                    if shape == 2 {
                        "λ\0🙂".as_bytes()
                    } else {
                        b"union"
                    }
                );
                take_field(&mut body, 2)
            }
            4 => {
                let child = take_field(&mut body, 1);
                assert!(take_field(&mut body, 2).is_empty());
                child
            }
            5 => {
                assert!(take_field(&mut body, 1).is_empty());
                take_field(&mut body, 2)
            }
            6 => {
                assert_eq!(take_field(&mut body, 1), b"test.extension");
                let child = take_field(&mut body, 2);
                assert_eq!(take_field(&mut body, 3), [0, 255, 17]);
                child
            }
            _ => unreachable!(),
        };
        let tail: &[u8] = match shape {
            0 => &[0x10, 1],
            1 => &[0x10, 7],
            2 | 4 => &[0x18, 1],
            3 => &[0x1a, 2, 255, 1],
            5 => &[0x20, 1],
            6 => &[],
            _ => unreachable!(),
        };
        assert_eq!(body, tail);
    }
    assert_eq!(
        wire,
        if nullable {
            &[0x12, 2, 8, 1][..]
        } else {
            &[0x12, 0][..]
        }
    );
}

fn inspect_value(mut wire: &[u8], kind: usize, depth: usize) {
    for level in (0..depth).rev() {
        let shape = if kind == 3 { level % 3 } else { kind };
        let mut body = take_field(&mut wire, [9, 12, 11][shape]);
        assert!(wire.is_empty());
        wire = match shape {
            0 => take_field(&mut body, 1),
            1 => {
                assert_eq!(
                    encoding::decode_key(&mut body).unwrap(),
                    (1, WireType::Varint)
                );
                assert_eq!(
                    encoding::decode_varint(&mut body).unwrap(),
                    u64::from(u32::MAX)
                );
                take_field(&mut body, 2)
            }
            2 => {
                inspect_dtype(take_field(&mut body, 1), 6, 2, true);
                take_field(&mut body, 2)
            }
            _ => unreachable!(),
        };
        assert!(body.is_empty());
    }
    assert_eq!(wire, [0x10, 0]);
}

fn deep_case(kind: usize) {
    let mut dtype = fixtures::dtype_chain(kind, 8192, false);
    let bytes = dtype.encode_to_vec();
    assert_eq!(dtype.encoded_len(), bytes.len());
    inspect_dtype(&bytes, kind, 8192, false);
    let mut standalone = Vec::new();
    dtype.dtype_type.as_ref().unwrap().encode(&mut standalone);
    assert_eq!(standalone, bytes);
    let mut short = [0xa5; 32];
    assert!(dtype.encode(&mut short.as_mut_slice()).is_err());
    assert_eq!(short, [0xa5; 32]);
    let scalar_kind = kind % 4;
    let mut value = fixtures::value_chain(scalar_kind, 8192, false);
    let bytes = value.encode_to_vec();
    assert_eq!(value.encoded_len(), bytes.len());
    inspect_value(&bytes, scalar_kind, 8192);
    let mut standalone = Vec::new();
    value.kind.as_ref().unwrap().encode(&mut standalone);
    assert_eq!(standalone, bytes);
    let scalar = sv::Scalar {
        dtype: Some(dtype.clone()),
        value: Some(Box::new(value.clone())),
    };
    let encoded = scalar.encode_to_vec();
    assert_eq!(scalar.encoded_len(), encoded.len());
    let mut bytes = encoded.as_slice();
    inspect_dtype(take_field(&mut bytes, 1), kind, 8192, false);
    inspect_value(take_field(&mut bytes, 2), scalar_kind, 8192);
    assert!(bytes.is_empty());
    let variant = sv::ScalarValue {
        kind: Some(Kind::VariantValue(Box::new(scalar))),
    };
    let encoded = variant.encode_to_vec();
    assert_eq!(variant.encoded_len(), encoded.len());
    let mut bytes = encoded.as_slice();
    let mut body = take_field(&mut bytes, 11);
    assert!(bytes.is_empty());
    inspect_dtype(take_field(&mut body, 1), kind, 8192, false);
    inspect_value(take_field(&mut body, 2), scalar_kind, 8192);
    assert!(body.is_empty());
    dtype.clear();
    value.clear();
    assert!(dtype.dtype_type.is_none());
    assert!(value.kind.is_none());
    drop(variant);
}

#[test]
fn deep_protobuf_writing_measurement_and_cleanup_use_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_PROTOBUF_WIRE";
    const COMPLETE: &str = "deep protobuf wire writing completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let index = kind.parse().unwrap();
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || deep_case(index))
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETE} {kind}");
        return;
    }
    for kind in 0..8 {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "proto::wire_tests::deep_protobuf_writing_measurement_and_cleanup_use_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETE} {kind}"))
        );
    }
}
