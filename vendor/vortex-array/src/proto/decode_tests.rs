// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::thread;

use prost::bytes::Buf;
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};

use super::dtype::{self as dt, d_type::DtypeType as Type};
use super::scalar::{self as sv, scalar_value::Kind};
use super::structural_tests::{self as fixtures, reference};

use super::wire_reference_fixtures as original;

// Exact Message::merge top-level semantics, with an explicitly supplied bound.
// The production default remains unchanged; this helper is test-only.
fn merge_bounded<M: Message>(
    value: &mut M,
    buf: &mut impl Buf,
    limit: u32,
) -> Result<(), DecodeError> {
    let context = DecodeContext::with_recursion_limit(limit);
    while buf.has_remaining() {
        let (tag, wire) = encoding::decode_key(buf)?;
        value.merge_field(tag, wire, buf, context.clone())?;
    }
    Ok(())
}

fn compare<A: Message + Clone, B: Message + Clone>(
    initial: &A,
    original: &B,
    bytes: &[u8],
    limit: u32,
) {
    let (mut actual, mut expected) = (initial.clone(), original.clone());
    let (mut a, mut b) = (bytes, bytes);
    assert_eq!(
        merge_bounded(&mut actual, &mut a, limit),
        merge_bounded(&mut expected, &mut b, limit),
        "limit={limit} bytes={bytes:?}",
    );
    assert_eq!(a, b);
    // Compare complete partial states, including NaN payload bits and presence.
    assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
}

fn message(tag: u32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    encoding::encode_key(tag, WireType::LengthDelimited, &mut bytes);
    encoding::encode_varint(body.len() as u64, &mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn bounded_merges_match_original_values_errors_and_partial_states() {
    fn check<A: Message + Clone, B: Message + Clone>(actual: A, expected: B) {
        let wire = expected.encode_to_vec();
        for limit in [0, 1, 2, 4, 8, 100] {
            compare(&actual, &expected, &wire, limit);
        }
        if wire.len() <= 96 {
            for limit in [4, 100] {
                for end in 0..wire.len() {
                    compare(&actual, &expected, &wire[..end], limit);
                }
                for index in 0..wire.len() {
                    for mask in [1, 0x80, 0xff] {
                        let mut changed = wire.clone();
                        changed[index] ^= mask;
                        compare(&actual, &expected, &changed, limit);
                    }
                }
            }
        }
        // These deliberately malformed, mixed-field bodies cover missing
        // optional initialization, completed siblings, packed fields, unknown
        // groups and post-field overrun across every initial oneof variant.
        let bodies: &[&[u8]] = &[
            &[],
            &[0x08, 1],
            &[0x0a, 0],
            &[0x0a, 1, 0xff],
            &[0x12, 0, 0x12, 1, 0xff],
            &[0x0a, 1, b'n', 0x12, 1, 0xff],
            &[0x0a, 0, 0x12, 1, 0xff],
            &[0x12, 2, 0x10, 1, 0xff],
            &[0x1a, 1, 0x80],
            &[0x2b, 0x33, 0x34, 0x2c],
        ];
        for tag in 1..=13 {
            for body in bodies {
                let wire = message(tag, body);
                for limit in [0, 1, 2, 4, 100] {
                    compare(&actual, &expected, &wire, limit);
                }
            }
        }
    }
    for (actual, expected) in fixtures::dtype_fixtures()
        .into_iter()
        .zip(original::dtype_fixtures())
    {
        check(actual, expected);
    }
    for (actual, expected) in fixtures::value_fixtures()
        .into_iter()
        .zip(original::value_fixtures())
    {
        check(actual, expected);
    }
    for depth in 0..10 {
        for kind in 0..4 {
            let actual = fixtures::value_chain(kind, depth, true);
            let expected = original::value_chain(kind, depth, true);
            for limit in 0..=28 {
                compare(&actual, &expected, &expected.encode_to_vec(), limit);
            }
        }
    }
}

fn dtype_child(dtype: &dt::DType, tag: u32) -> Option<&dt::DType> {
    match (dtype.dtype_type.as_ref(), tag) {
        (Some(Type::List(list)), 1) => list.element_type.as_deref(),
        (Some(Type::FixedSizeList(list)), 1) => list.element_type.as_deref(),
        (Some(Type::Extension(ext)), 2) => ext.storage_dtype.as_deref(),
        (Some(Type::Map(map)), 1) => map.key_type.as_deref(),
        (Some(Type::Map(map)), 2) => map.value_type.as_deref(),
        _ => unreachable!(),
    }
}

#[test]
fn optional_children_initialize_before_checks_and_keep_existing_boxes() {
    let child = dt::DType {
        dtype_type: Some(Type::Bool(dt::Bool { nullable: false })),
    };
    for (outer, tag) in [(8, 1), (10, 1), (9, 2), (13, 1), (13, 2)] {
        let initial_wire = message(outer, &message(tag, &child.encode_to_vec()));
        let mut value = dt::DType::decode(initial_wire.as_slice()).unwrap();
        let address = dtype_child(&value, tag).unwrap() as *const dt::DType;
        let update = message(outer, &message(tag, &[0x12, 2, 0x08, 1]));
        value.merge(update.as_slice()).unwrap();
        assert_eq!(
            dtype_child(&value, tag).unwrap() as *const dt::DType,
            address
        );
        assert!(
            matches!(dtype_child(&value, tag).unwrap().dtype_type.as_ref(),
            Some(Type::Bool(value)) if value.nullable)
        );
        let mut body = Vec::new();
        encoding::encode_key(tag, WireType::Varint, &mut body);
        body.push(0);
        let invalid = message(outer, &body);
        assert!(value.merge(invalid.as_slice()).is_err());
        assert_eq!(
            dtype_child(&value, tag).unwrap() as *const dt::DType,
            address
        );

        let mut missing = dt::DType::decode(message(outer, &[]).as_slice()).unwrap();
        assert!(dtype_child(&missing, tag).is_none());
        assert!(missing.merge(invalid.as_slice()).is_err());
        assert_eq!(dtype_child(&missing, tag), Some(&dt::DType::default()));
    }
    for outer in [11, 12] {
        let mut value =
            sv::ScalarValue::decode(message(outer, &message(2, &[0x10, 0])).as_slice()).unwrap();
        let child = |value: &sv::ScalarValue| match value.kind.as_ref().unwrap() {
            Kind::VariantValue(scalar) => {
                scalar.value.as_deref().unwrap() as *const sv::ScalarValue
            }
            Kind::UnionValue(union) => union.value.as_deref().unwrap() as *const sv::ScalarValue,
            _ => unreachable!(),
        };
        let address = child(&value);
        value
            .merge(message(outer, &message(2, &[0x10, 1])).as_slice())
            .unwrap();
        assert_eq!(child(&value), address);
        assert!(value.merge(message(outer, &[0x10, 1]).as_slice()).is_err());
        assert_eq!(child(&value), address);
        let mut missing = sv::ScalarValue::decode(message(outer, &[]).as_slice()).unwrap();
        assert!(
            missing
                .merge(message(outer, &[0x10, 1]).as_slice())
                .is_err()
        );
        match missing.kind.as_ref().unwrap() {
            Kind::VariantValue(scalar) => {
                assert_eq!(scalar.value.as_deref(), Some(&sv::ScalarValue::default()))
            }
            Kind::UnionValue(union) => {
                assert_eq!(union.value.as_deref(), Some(&sv::ScalarValue::default()))
            }
            _ => unreachable!(),
        }
    }
}

struct Interrupted<'a> {
    bytes: &'a [u8],
    offset: usize,
    stop: usize,
}

impl Buf for Interrupted<'_> {
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn chunk(&self) -> &[u8] {
        assert!(
            self.offset < self.stop,
            "injected protobuf buffer interruption"
        );
        &self.bytes[self.offset..(self.offset + 1).min(self.bytes.len())]
    }
    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining());
        self.offset += cnt;
    }
}

fn compare_interruption<A: Message + Clone, B: Message + Clone>(
    initial: A,
    reference: B,
    wire: &[u8],
) {
    for stop in 0..wire.len() {
        let (mut actual, mut expected) = (initial.clone(), reference.clone());
        let mut left = Interrupted {
            bytes: wire,
            offset: 0,
            stop,
        };
        let mut right = Interrupted {
            bytes: wire,
            offset: 0,
            stop,
        };
        let actual_result = catch_unwind(AssertUnwindSafe(|| {
            merge_bounded(&mut actual, &mut left, 100)
        }));
        let expected_result = catch_unwind(AssertUnwindSafe(|| {
            merge_bounded(&mut expected, &mut right, 100)
        }));
        assert_eq!(actual_result.is_err(), expected_result.is_err());
        if let (Ok(actual), Ok(expected)) = (actual_result, expected_result) {
            assert_eq!(actual, expected);
        }
        assert_eq!(left.offset, right.offset);
        assert_eq!(
            actual.encode_to_vec(),
            expected.encode_to_vec(),
            "stop={stop}"
        );
    }
}

#[test]
fn interrupted_inputs_restore_original_partial_states_without_replaying_reads() {
    for kind in 0..8 {
        let actual = fixtures::dtype_chain(kind, 3, true);
        let expected = original::dtype_chain(kind, 3, true);
        let wire = original::dtype_chain(kind, 4, false).encode_to_vec();
        compare_interruption(actual, expected, &wire);
        compare_interruption(
            dt::DType::default(),
            reference::dtype::DType::default(),
            &wire,
        );
    }
    for kind in 0..4 {
        let actual = fixtures::value_chain(kind, 3, true);
        let expected = original::value_chain(kind, 3, true);
        let wire = original::value_chain(kind, 4, false).encode_to_vec();
        compare_interruption(actual, expected, &wire);
        compare_interruption(
            sv::ScalarValue::default(),
            reference::scalar::ScalarValue::default(),
            &wire,
        );
    }
}

fn bounded<M: Message + Default>(wire: &[u8], limit: u32) -> Result<M, DecodeError> {
    let mut value = M::default();
    let mut source = wire;
    merge_bounded(&mut value, &mut source, limit)?;
    Ok(value)
}

fn take_message<'a>(source: &mut &'a [u8], expected_tag: u32) -> &'a [u8] {
    assert_eq!(
        encoding::decode_key(source).unwrap(),
        (expected_tag, WireType::LengthDelimited)
    );
    let len = usize::try_from(encoding::decode_varint(source).unwrap()).unwrap();
    let (body, rest) = source.split_at(len);
    *source = rest;
    body
}

fn deep_case(kind: usize) {
    const DEPTH: usize = 8192;
    let depth_limit = u32::try_from(DEPTH).unwrap();
    let bound = depth_limit * 2 + 1;
    let dtype = fixtures::dtype_chain(kind, DEPTH, true);
    let wire = dtype.encode_to_vec();
    let decoded: dt::DType = bounded(&wire, bound).unwrap();
    assert_eq!(decoded, dtype);
    assert_eq!(decoded.encode_to_vec(), wire);
    assert!(bounded::<dt::DType>(&wire, bound - 1).is_err());
    assert!(dt::DType::decode(wire.as_slice()).is_err());
    let mut rest = wire.as_slice();
    let (tag, wt) = encoding::decode_key(&mut rest).unwrap();
    let mut standalone = None;
    Type::merge(
        &mut standalone,
        tag,
        wt,
        &mut rest,
        DecodeContext::with_recursion_limit(bound),
    )
    .unwrap();
    assert!(rest.is_empty());
    assert_eq!(standalone.as_ref(), dtype.dtype_type.as_ref());

    let scalar_kind = kind % 4;
    let value = fixtures::value_chain(scalar_kind, DEPTH, true);
    let bytes = value.encode_to_vec();
    let limit = bound + 8;
    let decoded: sv::ScalarValue = bounded(&bytes, limit).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(decoded.encode_to_vec(), bytes);
    assert!(bounded::<sv::ScalarValue>(&bytes, depth_limit * 2 - 1).is_err());
    if scalar_kind < 2 {
        assert_eq!(
            bounded::<sv::ScalarValue>(&bytes, depth_limit * 2).unwrap(),
            value
        );
    }
    let scalar = sv::Scalar {
        dtype: Some(dtype),
        value: Some(Box::new(value)),
    };
    let bytes = scalar.encode_to_vec();
    let reopened: sv::Scalar = bounded(&bytes, limit + 1).unwrap();
    assert_eq!(reopened, scalar);

    // A completed first sibling survives failure of a later deep repeated child.
    let deep = fixtures::dtype_chain(0, DEPTH, true);
    let mut damaged = deep.encode_to_vec();
    let mut cursor = damaged.as_slice();
    for _ in 0..DEPTH {
        let mut list = take_message(&mut cursor, 8);
        cursor = take_message(&mut list, 1);
    }
    let leaf_offset = cursor.as_ptr() as usize - damaged.as_ptr() as usize;
    assert_eq!(cursor, [0x12, 2, 8, 1]);
    damaged[leaf_offset + 3] = 0x80;
    let mut state = dt::DType {
        dtype_type: Some(Type::Struct(dt::Struct {
            names: vec!["existing".into()],
            dtypes: vec![deep.clone()],
            nullable: false,
        })),
    };
    let mut body = message(2, &[]);
    body.extend(message(2, &damaged));
    let result = merge_bounded(&mut state, &mut message(7, &body).as_slice(), bound + 4);
    assert!(result.is_err());
    let Some(Type::Struct(fields)) = state.dtype_type.as_ref() else {
        unreachable!()
    };
    assert_eq!(fields.dtypes.len(), 2);
    assert_eq!(fields.dtypes[0], deep);
    assert_eq!(fields.dtypes[1], dt::DType::default());

    // Every active same-variant optional owner is restored during unwinding.
    let valid = deep.encode_to_vec();
    let mut same = deep.clone();
    let mut input = Interrupted {
        bytes: &valid,
        offset: 0,
        stop: leaf_offset / 2,
    };
    assert!(
        catch_unwind(AssertUnwindSafe(|| merge_bounded(
            &mut same, &mut input, bound
        )))
        .is_err()
    );
    assert_eq!(same, deep);
    let mut replacement = dt::DType::default();
    let mut input = Interrupted {
        bytes: &valid,
        offset: 0,
        stop: leaf_offset / 2,
    };
    assert!(
        catch_unwind(AssertUnwindSafe(|| merge_bounded(
            &mut replacement,
            &mut input,
            bound
        )))
        .is_err()
    );
    assert_eq!(replacement, dt::DType::default());

    // Generated leaf messages must also skip deep unknown groups iteratively.
    let mut groups = vec![0x2b; DEPTH];
    groups.extend(std::iter::repeat_n(0x2c, DEPTH));
    let root = message(2, &groups);
    let parsed: dt::DType = bounded(&root, depth_limit + 1).unwrap();
    assert_eq!(
        parsed,
        dt::DType {
            dtype_type: Some(Type::Bool(dt::Bool { nullable: false }))
        }
    );
    assert!(bounded::<dt::DType>(&root, depth_limit).is_err());
}

#[test]
fn deep_protobuf_decoding_bounds_errors_and_cleanup_use_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_PROTOBUF_DECODE";
    const COMPLETE: &str = "deep protobuf model decoding completed";
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
        let output = Command::new(env::current_exe().unwrap()).args([
            "--exact", "proto::decode_tests::deep_protobuf_decoding_bounds_errors_and_cleanup_use_small_stack",
            "--test-threads=1", "--nocapture",
        ]).env(SELECTOR, kind.to_string()).output().unwrap();
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
