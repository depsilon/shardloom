// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::cell::RefCell;
use std::env;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::thread;

use prost::Message;
use vortex_buffer::{BufferString, ByteBuffer};
use vortex_error::{VortexResult, vortex_err};
use vortex_session::{VortexSession, registry::CachedId};

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable};
use crate::dtype::half::f16;
use crate::dtype::session::DTypeSessionExt;
use crate::dtype::{DType, DecimalDType, Nullability, PType, StructFields, i256};
use crate::flatbuffers::{FlatBuffer, WriteFlatBufferExt};
use crate::proto::{dtype as dtype_pb, scalar as pb};
use crate::scalar::operation_test_support::{KINDS, lazy_struct, wrap};
use crate::scalar::{DecimalValue, PValue, Scalar, ScalarValue};

use crate::dtype::serde::proto_reference as dtype_reference;
#[path = "proto_reference.rs"]
mod reference;

use Nullability::{NonNullable, Nullable};
use pb::scalar_value::Kind;

fn message(kind: Kind) -> pb::ScalarValue {
    pb::ScalarValue { kind: Some(kind) }
}

fn outcome<T>(operation: impl FnOnce() -> VortexResult<T>) -> Result<T, String> {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(value) => value.map_err(|error| format!("error: {error}")),
        Err(panic) => Err(format!(
            "panic: {}",
            panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic")
        )),
    }
}

fn leaves() -> Vec<Scalar> {
    let mut values = vec![
        Scalar::null(DType::Null),
        Scalar::bool(true, NonNullable),
        Scalar::utf8(BufferString::from("λ\0naïve🙂"), NonNullable),
        Scalar::binary(ByteBuffer::from(vec![0, 255, 7]), NonNullable),
    ];
    for (ptype, value) in [
        (PType::I8, PValue::I8(i8::MIN)),
        (PType::I16, PValue::I16(i16::MIN)),
        (PType::I32, PValue::I32(i32::MIN)),
        (PType::I64, PValue::I64(i64::MIN)),
        (PType::U8, PValue::U8(u8::MAX)),
        (PType::U16, PValue::U16(u16::MAX)),
        (PType::U32, PValue::U32(u32::MAX)),
        (PType::U64, PValue::U64(u64::MAX)),
        (PType::F16, PValue::F16(f16::from_bits(0x7e01))),
        (PType::F32, PValue::F32(f32::from_bits(0x7fc0_0001))),
        (
            PType::F64,
            PValue::F64(f64::from_bits(0x7ff8_0000_0000_0001)),
        ),
        (PType::F64, PValue::F64(f64::NEG_INFINITY)),
        (PType::F64, PValue::F64(f64::INFINITY)),
        (PType::F64, PValue::F64(-0.0)),
    ] {
        values.push(Scalar {
            dtype: DType::Primitive(ptype, NonNullable),
            value: Some(ScalarValue::Primitive(value)),
        });
    }
    for (precision, value) in [
        (2, DecimalValue::I8(-9)),
        (4, DecimalValue::I16(-999)),
        (8, DecimalValue::I32(-999_999)),
        (18, DecimalValue::I64(-999_999_999)),
        (38, DecimalValue::I128(-999_999_999_999)),
        (76, DecimalValue::I256(i256::from_i128(-999_999_999_999))),
    ] {
        values.push(Scalar {
            dtype: DType::Decimal(DecimalDType::new(precision, -2), NonNullable),
            value: Some(ScalarValue::Decimal(value)),
        });
    }
    values
}

#[test]
fn scalar_proto_matches_original_all_widths_shapes_nulls_and_shallow_bytes() {
    let session = VortexSession::empty();
    session.allow_unknown();
    let mut fixtures = vec![
        Scalar {
            dtype: DType::Struct(StructFields::empty(), NonNullable),
            value: Some(ScalarValue::Tuple(vec![])),
        },
        Scalar {
            dtype: DType::List(Arc::new(DType::Bool(Nullable)), NonNullable),
            value: Some(ScalarValue::Tuple(vec![
                None,
                Some(ScalarValue::Bool(false)),
                None,
            ])),
        },
    ];
    for leaf in leaves() {
        let mut children = vec![leaf.clone(), Scalar::null(leaf.dtype().as_nullable())];
        if leaf.dtype() != &DType::Null {
            children.push(Scalar {
                dtype: leaf.dtype().as_nullable(),
                value: leaf.value().cloned(),
            });
        }
        for child in children {
            fixtures.push(child.clone());
            for kind in KINDS {
                if kind == "map-key" && child.dtype().is_nullable() {
                    continue;
                }
                fixtures.push(wrap(kind, child.clone()));
                fixtures.push(wrap("variant", wrap("struct", wrap(kind, child.clone()))));
            }
        }
    }
    fixtures.push(Scalar {
        dtype: DType::List(Arc::new(DType::Bool(NonNullable)), NonNullable),
        value: Some(ScalarValue::Tuple(vec![
            Some(ScalarValue::Bool(true));
            4096
        ])),
    });
    for scalar in fixtures {
        let expected = reference::scalar_write(&scalar);
        let actual = pb::Scalar::from(&scalar);
        // Bit-exact wire equality also covers NaNs, signed zero and decimal widths.
        assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
        assert_eq!(
            ScalarValue::to_proto(scalar.value()).encode_to_vec(),
            reference::optional_write(scalar.value()).encode_to_vec()
        );
        let expected_value = outcome(|| reference::scalar_read(&expected, &session));
        let actual_value = outcome(|| Scalar::from_proto(&actual, &session));
        assert_eq!(actual_value, expected_value, "{}", scalar.dtype());
        let lazy = DType::from_flatbuffer(
            FlatBuffer::from(scalar.dtype().write_flatbuffer_bytes().unwrap()),
            &session,
        )
        .unwrap();
        for dtype in [scalar.dtype(), &lazy] {
            assert_eq!(
                outcome(|| ScalarValue::from_proto(
                    actual.value.as_deref().unwrap(),
                    dtype,
                    &session
                )),
                outcome(|| reference::read(expected.value.as_deref().unwrap(), dtype, &session))
            );
        }
    }
}

fn malformed_messages() -> Vec<pb::ScalarValue> {
    let mut values = vec![
        pb::ScalarValue::default(),
        message(Kind::NullValue(-7)),
        message(Kind::BoolValue(true)),
        message(Kind::Int64Value(-1)),
        message(Kind::Int64Value(i64::MAX)),
        message(Kind::Uint64Value(u64::MAX)),
        message(Kind::Uint64Value(0x7e01)),
        message(Kind::F16Value(u64::MAX)),
        message(Kind::F32Value(f32::NAN)),
        message(Kind::F64Value(f64::INFINITY)),
        message(Kind::StringValue("é\0".into())),
        message(Kind::BytesValue(vec![255])),
        message(Kind::BytesValue(vec![0; 3])),
        message(Kind::ListValue(pb::ListValue::default())),
        message(Kind::ListValue(pb::ListValue {
            values: vec![message(Kind::BoolValue(true))],
        })),
        message(Kind::ListValue(pb::ListValue {
            values: vec![message(Kind::NullValue(0)), pb::ScalarValue::default()],
        })),
        message(Kind::VariantValue(Box::default())),
        message(Kind::VariantValue(Box::new(pb::Scalar {
            dtype: Some(dtype_pb::DType::default()),
            value: None,
        }))),
        message(Kind::VariantValue(Box::new(pb::Scalar {
            dtype: Some(dtype_pb::DType::try_from(&DType::Bool(NonNullable)).unwrap()),
            value: None,
        }))),
    ];
    for type_id in [0, 7, 255, 256, u32::MAX] {
        for value in [
            None,
            Some(Box::new(message(Kind::NullValue(0)))),
            Some(Box::new(pb::ScalarValue::default())),
        ] {
            values.push(message(Kind::UnionValue(Box::new(pb::UnionValue {
                type_id,
                value,
            }))));
        }
    }
    for length in [0, 2, 4, 8, 16, 32, 33] {
        values.push(message(Kind::BytesValue(vec![0; length])));
    }
    values
}

#[test]
fn scalar_proto_keeps_original_malformed_policy_and_one_extension_unwrap() {
    let session = VortexSession::empty();
    session.allow_unknown();
    let mut types: Vec<_> = leaves().into_iter().map(|v| v.into_parts().0).collect();
    for kind in KINDS {
        types.push(wrap(kind, Scalar::bool(false, NonNullable)).into_parts().0);
    }
    types.push(
        wrap(
            "extension",
            wrap("extension", Scalar::bool(false, NonNullable)),
        )
        .into_parts()
        .0,
    );
    for dtype in types {
        for nullable in [NonNullable, Nullable] {
            if dtype == DType::Null && nullable == NonNullable {
                continue;
            }
            let dtype = dtype.with_nullability(nullable);
            for value in malformed_messages() {
                let actual = outcome(|| ScalarValue::from_proto(&value, &dtype, &session));
                let expected = outcome(|| reference::read(&value, &dtype, &session));
                assert_eq!(actual, expected, "{dtype}: {value:?}");
                assert_eq!(
                    outcome(|| Scalar::from_proto_value(&value, &dtype, &session)),
                    outcome(|| Scalar::try_new(
                        dtype.clone(),
                        reference::read(&value, &dtype, &session)?
                    ))
                );
            }
        }
    }
    let double = wrap(
        "extension",
        wrap("extension", Scalar::bool(false, NonNullable)),
    );
    assert!(
        ScalarValue::from_proto(&message(Kind::BoolValue(true)), double.dtype(), &session).is_err()
    );
    assert_eq!(
        ScalarValue::from_proto(&message(Kind::NullValue(0)), double.dtype(), &session).unwrap(),
        None
    );
    let legacy = message(Kind::Uint64Value(0x7e01));
    assert_eq!(
        ScalarValue::from_proto(
            &legacy,
            &DType::Primitive(PType::F16, NonNullable),
            &session
        )
        .unwrap(),
        Some(ScalarValue::Primitive(PValue::F16(f16::from_bits(0x7e01))))
    );
}

thread_local! { static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) }; }
fn events() -> Vec<String> {
    EVENTS.with(|v| std::mem::take(&mut *v.borrow_mut()))
}
fn event(value: String) {
    EVENTS.with(|v| v.borrow_mut().push(value));
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;
impl ExtVTable for Probe {
    type Metadata = u8;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.scalar_proto.order");
        *ID
    }
    fn serialize_metadata(&self, id: &u8) -> VortexResult<Vec<u8>> {
        event(format!("write{id}"));
        if *id == 255 {
            return Err(vortex_err!("metadata write failure"));
        }
        Ok(vec![*id])
    }
    fn deserialize_metadata(&self, bytes: &[u8]) -> VortexResult<u8> {
        let id = *bytes
            .first()
            .ok_or_else(|| vortex_err!("missing metadata"))?;
        event(format!("read{id}"));
        if id == 255 {
            return Err(vortex_err!("metadata read failure"));
        }
        Ok(id)
    }
    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        event(format!("dtype{}", dtype.metadata()));
        Ok(())
    }
    fn validate_scalar_value(dtype: &ExtDType<Self>, _: &ScalarValue) -> VortexResult<()> {
        event(format!("value{}", dtype.metadata()));
        if *dtype.metadata() == 254 {
            return Err(vortex_err!("scalar validation failure"));
        }
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(value)
    }
}
fn probe(id: u8, child: Scalar) -> Scalar {
    let (dtype, value) = child.into_parts();
    Scalar {
        dtype: DType::Extension(ExtDType::<Probe>::try_new(id, dtype).unwrap().erased()),
        value,
    }
}

#[test]
fn scalar_proto_preserves_callback_counts_order_and_lazy_failures() {
    let session = VortexSession::empty();
    session.dtypes().register(Probe);
    for id in [2, 254, 255] {
        let scalar = wrap(
            "struct",
            wrap(
                "variant",
                wrap("union", probe(id, Scalar::bool(true, NonNullable))),
            ),
        );
        let mut writes = Vec::new();
        for original in [true, false] {
            events();
            let value = outcome(|| {
                Ok(if original {
                    reference::scalar_write(&scalar)
                } else {
                    pb::Scalar::from(&scalar)
                })
            });
            writes.push((value.map(|v| v.encode_to_vec()), events()));
        }
        assert_eq!(writes[0], writes[1]);
        if id == 255 {
            continue;
        }
        let wire = pb::Scalar::from(&scalar);
        let mut reads = Vec::new();
        for original in [true, false] {
            events();
            let value = outcome(|| {
                if original {
                    reference::scalar_read(&wire, &session)
                } else {
                    Scalar::from_proto(&wire, &session)
                }
            });
            reads.push((value, events()));
        }
        assert_eq!(reads[0], reads[1]);
        assert!(reads[0].1.iter().filter(|v| v.starts_with("value")).count() >= 1);
        // A parsed Variant dtype must run its callbacks before missing-value refusal.
        let value = message(Kind::VariantValue(Box::new(pb::Scalar {
            dtype: Some(
                dtype_pb::DType::try_from(probe(id, Scalar::bool(true, NonNullable)).dtype())
                    .unwrap(),
            ),
            value: None,
        })));
        let mut reads = Vec::new();
        for original in [true, false] {
            events();
            let result = outcome(|| {
                if original {
                    reference::read(&value, &DType::Variant(NonNullable), &session)
                } else {
                    ScalarValue::from_proto(&value, &DType::Variant(NonNullable), &session)
                }
            });
            reads.push((result, events()));
        }
        assert_eq!(reads[0], reads[1]);
        assert!(
            reads[0]
                .0
                .as_ref()
                .unwrap_err()
                .contains("Scalar missing value")
        );
        assert_eq!(reads[0].1, [format!("read{id}"), format!("dtype{id}")]);
    }
    for first_bad in [false, true] {
        let values = message(Kind::ListValue(pb::ListValue {
            values: vec![
                if first_bad {
                    pb::ScalarValue::default()
                } else {
                    message(Kind::BoolValue(true))
                },
                message(Kind::BoolValue(true)),
            ],
        }));
        let mut outcomes = Vec::new();
        for original in [true, false] {
            let dtype = lazy_struct(&DType::Bool(NonNullable));
            outcomes.push(outcome(|| {
                if original {
                    reference::read(&values, &dtype, &session)
                } else {
                    ScalarValue::from_proto(&values, &dtype, &session)
                }
            }));
        }
        assert_eq!(outcomes[0], outcomes[1]);
        assert!(outcomes[0].as_ref().unwrap_err().starts_with(if first_bad {
            "error:"
        } else {
            "panic:"
        }));
    }
    // Map entries resolve both lazy fields before converting even the first key.
    let unknown = wrap("extension", Scalar::bool(false, NonNullable))
        .into_parts()
        .0;
    let dtype = DType::map(
        probe(3, Scalar::bool(false, NonNullable)).into_parts().0,
        unknown,
        false,
        NonNullable,
    )
    .unwrap();
    let bytes = dtype.write_flatbuffer_bytes().unwrap();
    for values in [vec![], vec![pb::ScalarValue::default()]] {
        let value = message(Kind::ListValue(pb::ListValue { values }));
        let mut reads = Vec::new();
        for original in [true, false] {
            let lazy = DType::from_flatbuffer(FlatBuffer::from(bytes.clone()), &session).unwrap();
            events();
            let result = outcome(|| {
                if original {
                    reference::read(&value, &lazy, &session)
                } else {
                    ScalarValue::from_proto(&value, &lazy, &session)
                }
            });
            reads.push((result, events()));
        }
        assert_eq!(reads[0], reads[1]);
        assert!(reads[0].0.is_err());
    }
}

fn chain(kind: &str, depth: usize) -> Scalar {
    (0..depth).fold(Scalar::bool(true, NonNullable), |child, index| {
        wrap(
            if kind == "mixed" {
                KINDS[index % KINDS.len()]
            } else {
                kind
            },
            child,
        )
    })
}

fn invalidate_deep_leaf(mut value: &mut pb::ScalarValue) {
    loop {
        let compound = matches!(
            value.kind,
            Some(Kind::ListValue(_) | Kind::UnionValue(_) | Kind::VariantValue(_))
        );
        if !compound {
            value.kind = None;
            return;
        }
        value = match value.kind.as_mut().unwrap() {
            Kind::ListValue(list) => {
                let index = list
                    .values
                    .iter()
                    .position(|child| {
                        matches!(
                            child.kind,
                            Some(Kind::ListValue(_) | Kind::UnionValue(_) | Kind::VariantValue(_))
                        )
                    })
                    .unwrap_or(0);
                &mut list.values[index]
            }
            Kind::UnionValue(union) => union.value.as_deref_mut().unwrap(),
            Kind::VariantValue(scalar) => scalar.value.as_deref_mut().unwrap(),
            _ => unreachable!(),
        };
    }
}

#[test]
fn deep_scalar_proto_conversion_failure_and_temporary_cleanup_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DEEP_SCALAR_PROTO";
    const COMPLETE: &str = "deep scalar proto completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let session = VortexSession::empty();
                session.allow_unknown();
                let scalar = chain(&kind, 8192);
                let mut wire = pb::Scalar::from(&scalar);
                if kind == "extension" {
                    // Existing one-layer semantics deliberately refuse nested extensions.
                    assert!(Scalar::from_proto(&wire, &session).is_err());
                } else {
                    let restored = Scalar::from_proto(&wire, &session).unwrap();
                    assert_eq!(scalar, restored);
                    drop(restored);
                    let dtype = DType::Struct(
                        StructFields::from_iter([
                            ("built", scalar.dtype().clone()),
                            ("bad", DType::Bool(NonNullable)),
                        ]),
                        NonNullable,
                    );
                    let partial = message(Kind::ListValue(pb::ListValue {
                        values: vec![
                            ScalarValue::to_proto(scalar.value()),
                            pb::ScalarValue::default(),
                        ],
                    }));
                    assert!(ScalarValue::from_proto(&partial, &dtype, &session).is_err());
                    drop((partial, dtype));
                    // Unwind all open conversion frames, not just a completed
                    // earlier sibling, when the innermost value is malformed.
                    invalidate_deep_leaf(wire.value.as_deref_mut().unwrap());
                    assert!(Scalar::from_proto(&wire, &session).is_err());
                }
                drop(wire);
                // A later Variant schema failure unwinds an already constructed deep
                // protobuf sibling. No recursive generated trait is used here.
                let good = ScalarValue::to_proto(scalar.value());
                drop(Box::new(good));
                let partial = ScalarValue::Tuple(vec![
                    scalar.value().cloned(),
                    Some(ScalarValue::Variant(Box::new(probe(
                        255,
                        Scalar::bool(false, NonNullable),
                    )))),
                ]);
                assert!(
                    catch_unwind(AssertUnwindSafe(|| ScalarValue::to_proto(Some(&partial))))
                        .is_err()
                );
                drop(partial);
                drop(scalar);
                let unwind = catch_unwind(AssertUnwindSafe(|| {
                    let owned = pb::Scalar::from(&chain(&kind, 8192));
                    std::hint::black_box(&owned);
                    panic!("test protobuf owner unwind");
                }));
                assert!(unwind.is_err());
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed"]) {
        let output = Command::new(env::current_exe().unwrap()).args([
            "--exact", "scalar::proto::iterative_tests::deep_scalar_proto_conversion_failure_and_temporary_cleanup_on_small_stack", "--test-threads=1", "--nocapture",
        ]).env(SELECTOR, kind).output().unwrap();
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
