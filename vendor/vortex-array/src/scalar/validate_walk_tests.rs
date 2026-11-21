// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::{env, fmt, thread};

use flatbuffers::FlatBufferBuilder;
use parking_lot::Mutex;
use vortex_error::{VortexResult, vortex_ensure};
use vortex_session::{VortexSession, registry::CachedId};

use crate::dtype::extension::{ExtDType, ExtId, ExtVTable};
use crate::dtype::half::f16;
use crate::dtype::{
    DType, DecimalDType, MapDType, Nullability, PType, StructFields, UnionVariants, i256,
};
use crate::flatbuffers::FlatBuffer;
use crate::scalar::{DecimalValue, PValue, Scalar, ScalarValue, UnionValue};

#[path = "validate_reference.rs"]
mod reference;
use Nullability::{NonNullable, Nullable};
use reference::Original;

fn compare(dtype: &DType, value: Option<&ScalarValue>) {
    assert_eq!(
        Scalar::is_compatible(dtype, value),
        Original::is_compatible(dtype, value)
    );
    assert_eq!(
        Scalar::validate(dtype, value).map_err(|error| error.to_string()),
        Original::validate(dtype, value).map_err(|error| error.to_string()),
        "dtype {dtype}, value {value:?}"
    );
}

fn tuple(values: Vec<Option<ScalarValue>>) -> Option<ScalarValue> {
    Some(ScalarValue::Tuple(values))
}
fn boolean(value: bool) -> Option<ScalarValue> {
    Some(ScalarValue::Bool(value))
}

#[test]
fn shallow_validation_matches_original_types_nulls_lengths_and_tags() {
    let mut dtypes = vec![
        DType::Null,
        DType::Bool(NonNullable),
        DType::Utf8(NonNullable),
        DType::Binary(NonNullable),
        DType::Decimal(DecimalDType::new(3, 1), NonNullable),
        DType::Variant(NonNullable),
    ];
    let mut values = vec![
        None,
        boolean(true),
        Some(ScalarValue::Utf8("λ🙂".into())),
        Some(ScalarValue::Binary(vec![0_u8, 255].into())),
        tuple(vec![]),
        tuple(vec![None]),
        tuple(vec![boolean(true)]),
        tuple(vec![boolean(true), None]),
        tuple(vec![tuple(vec![boolean(true), None])]),
        tuple(vec![tuple(vec![None, boolean(true)])]),
        tuple(vec![tuple(vec![boolean(true)])]),
        Some(ScalarValue::Variant(Box::new(Scalar::null(DType::Null)))),
        Some(ScalarValue::Variant(Box::new(Scalar::null(DType::Bool(
            Nullable,
        ))))),
        Some(ScalarValue::Variant(Box::new(Scalar::from(true)))),
    ];
    for primitive in [
        PValue::I8(-1),
        PValue::I16(-2),
        PValue::I32(-3),
        PValue::I64(-4),
        PValue::U8(1),
        PValue::U16(2),
        PValue::U32(3),
        PValue::U64(4),
        PValue::F16(f16::from_bits(0x7e01)),
        PValue::F32(f32::NAN),
        PValue::F64(f64::INFINITY),
    ] {
        dtypes.push(DType::Primitive(primitive.ptype(), NonNullable));
        values.push(Some(ScalarValue::Primitive(primitive)));
    }
    for decimal in [
        DecimalValue::I8(9),
        DecimalValue::I16(999),
        DecimalValue::I32(1000),
        DecimalValue::I64(i64::MIN),
        DecimalValue::I128(i128::MIN),
        DecimalValue::I256(i256::MIN),
    ] {
        values.push(Some(ScalarValue::Decimal(decimal)));
    }
    for dtype in [DType::Bool(NonNullable), DType::Bool(Nullable)] {
        dtypes.extend([
            DType::List(Arc::new(dtype.clone()), NonNullable),
            DType::FixedSizeList(Arc::new(dtype.clone()), 0, NonNullable),
            DType::FixedSizeList(Arc::new(dtype.clone()), 2, NonNullable),
            DType::Struct(
                StructFields::new(["a", "b"].into(), vec![dtype.clone(), dtype.clone()]),
                NonNullable,
            ),
            DType::Map(
                MapDType::try_new(DType::Bool(NonNullable), dtype.clone(), false).unwrap(),
                NonNullable,
            ),
            DType::Union(
                UnionVariants::try_new(
                    ["first", "second"].into(),
                    vec![DType::Utf8(Nullable), dtype],
                    vec![7, 253],
                )
                .unwrap(),
                NonNullable,
            ),
        ]);
    }
    for tag in [7, 253, 255] {
        for value in [None, boolean(true), Some(ScalarValue::Utf8("text".into()))] {
            values.push(Some(ScalarValue::Union(UnionValue::new(tag, value))));
        }
    }
    for dtype in dtypes {
        for dtype in [dtype.clone(), dtype.as_nullable()] {
            for value in &values {
                compare(&dtype, value.as_ref());
            }
        }
    }
    // Repeat errors under compound ancestors to compare exact context order.
    for kind in KINDS.into_iter().chain(["mixed"]) {
        for valid in [true, false] {
            let scalar = chain(kind, 6, valid, &Arc::default());
            compare(scalar.dtype(), scalar.value());
        }
    }
}

#[test]
fn constructor_retains_top_level_only_legacy_f16_conversion() {
    let bits = 0x7e01;
    let dtype = DType::Primitive(PType::F16, NonNullable);
    let legacy = Some(ScalarValue::Primitive(PValue::U64(bits)));
    assert!(!Scalar::is_compatible(&dtype, legacy.as_ref()));
    let scalar = Scalar::try_new(dtype.clone(), legacy.clone()).unwrap();
    let Some(ScalarValue::Primitive(PValue::F16(value))) = scalar.value() else {
        panic!("expected F16")
    };
    assert_eq!(value.to_bits(), u16::try_from(bits).unwrap());
    assert_eq!(
        Scalar::try_new(
            dtype.clone(),
            Some(ScalarValue::Primitive(PValue::U64(65536)))
        )
        .unwrap_err()
        .to_string(),
        "f16 bit representation must fit in 16 bits, got 65536"
    );
    for nested in [
        DType::List(Arc::new(dtype.clone()), NonNullable),
        DType::FixedSizeList(Arc::new(dtype), 1, NonNullable),
    ] {
        let value = tuple(vec![legacy.clone()]);
        compare(&nested, value.as_ref());
        assert!(Scalar::try_new(nested, value).is_err());
    }
}

#[derive(Clone)]
struct Metadata {
    id: u8,
    fail: bool,
    events: Arc<Mutex<Vec<u8>>>,
}
impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}
impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        (self.id, self.fail) == (other.id, other.fail)
    }
}
impl Eq for Metadata {}
impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.id, self.fail).hash(state);
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;
impl ExtVTable for Probe {
    type Metadata = Metadata;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.scalar_validation");
        *ID
    }
    fn serialize_metadata(&self, _: &Metadata) -> VortexResult<Vec<u8>> {
        panic!("no metadata serialization")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Metadata> {
        panic!("no metadata deserialization")
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn unpack_native<'a>(
        dtype: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        let metadata = dtype.metadata();
        metadata.events.lock().push(metadata.id);
        vortex_ensure!(!metadata.fail, "extension {} rejected value", metadata.id);
        Ok(value)
    }
}
fn extension(dtype: DType, id: u8, fail: bool, events: &Arc<Mutex<Vec<u8>>>) -> DType {
    DType::Extension(
        ExtDType::<Probe>::try_new(
            Metadata {
                id,
                fail,
                events: Arc::clone(events),
            },
            dtype,
        )
        .unwrap()
        .erased(),
    )
}

#[test]
fn extension_callbacks_and_first_failure_match_original() {
    let events = Arc::new(Mutex::new(Vec::new()));
    for failure in [None, Some(1), Some(2), Some(3)] {
        let dtype = DType::List(
            Arc::new(DType::Struct(
                StructFields::new(
                    ["a", "b"].into(),
                    vec![
                        extension(
                            extension(DType::Bool(NonNullable), 1, failure == Some(1), &events),
                            2,
                            failure == Some(2),
                            &events,
                        ),
                        extension(DType::Bool(NonNullable), 3, failure == Some(3), &events),
                    ],
                ),
                NonNullable,
            )),
            NonNullable,
        );
        for value in [
            tuple(vec![tuple(vec![boolean(true), boolean(false)])]),
            tuple(vec![tuple(vec![None, boolean(false)])]),
        ] {
            events.lock().clear();
            let expected =
                Original::validate(&dtype, value.as_ref()).map_err(|error| error.to_string());
            let expected_events = std::mem::take(&mut *events.lock());
            let actual =
                Scalar::validate(&dtype, value.as_ref()).map_err(|error| error.to_string());
            assert_eq!(actual, expected);
            assert_eq!(*events.lock(), expected_events);
            if value.as_ref().unwrap()
                == &ScalarValue::Tuple(vec![tuple(vec![None, boolean(false)])])
            {
                assert!(expected_events.is_empty());
            } else {
                let expected_order = match failure {
                    None => vec![1, 2, 3],
                    Some(1) => vec![1, 1],
                    Some(2) => vec![1, 2, 1, 2],
                    Some(3) => vec![1, 2, 3, 1, 2, 3],
                    _ => unreachable!(),
                };
                assert_eq!(expected_events, expected_order);
            }
        }
    }
}

fn lazy_fixture(kind: &str) -> DType {
    use crate::dtype::flatbuffers as fb;
    let mut builder = FlatBufferBuilder::new();
    let bad = fb::DType::create(&mut builder, &fb::DTypeArgs::default());
    let boolean = fb::Bool::create(&mut builder, &fb::BoolArgs { nullable: false });
    let good = fb::DType::create(
        &mut builder,
        &fb::DTypeArgs {
            type_type: fb::Type::Bool,
            type_: Some(boolean.as_union_value()),
        },
    );
    let (type_type, type_) = match kind {
        "struct" => {
            let first = builder.create_string("first");
            let second = builder.create_string("bad");
            let names = builder.create_vector(&[first, second]);
            let dtypes = builder.create_vector(&[good, bad]);
            let fields = fb::Struct_::create(
                &mut builder,
                &fb::Struct_Args {
                    names: Some(names),
                    dtypes: Some(dtypes),
                    nullable: false,
                },
            );
            (fb::Type::Struct_, fields.as_union_value())
        }
        "union" => {
            let first = builder.create_string("before");
            let second = builder.create_string("selected");
            let third = builder.create_string("after");
            let names = builder.create_vector(&[first, second, third]);
            let dtypes = builder.create_vector(&[bad, good, bad]);
            let tags = builder.create_vector(&[1_i8, 7, 9]);
            let variants = fb::Union::create(
                &mut builder,
                &fb::UnionArgs {
                    names: Some(names),
                    dtypes: Some(dtypes),
                    type_ids: Some(tags),
                    nullable: false,
                },
            );
            (fb::Type::Union, variants.as_union_value())
        }
        "map" => {
            let map = fb::Map::create(
                &mut builder,
                &fb::MapArgs {
                    key_type: Some(good),
                    value_type: Some(bad),
                    keys_sorted: false,
                    nullable: false,
                },
            );
            (fb::Type::Map, map.as_union_value())
        }
        _ => panic!("unknown lazy fixture"),
    };
    let root = fb::DType::create(
        &mut builder,
        &fb::DTypeArgs {
            type_type,
            type_: Some(type_),
        },
    );
    builder.finish_minimal(root);
    DType::from_flatbuffer(
        FlatBuffer::align_from(builder.finished_data().to_vec()),
        &VortexSession::empty(),
    )
    .unwrap()
}

#[test]
fn lazy_field_order_and_selected_union_do_not_inspect_unvisited_children() {
    let dtype = lazy_fixture("struct");
    compare(&dtype, tuple(vec![None, None]).as_ref());
    compare(&dtype, tuple(vec![]).as_ref());
    assert!(
        catch_unwind(AssertUnwindSafe(|| Scalar::is_compatible(
            &dtype,
            tuple(vec![boolean(true), None]).as_ref()
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| Original::is_compatible(
            &dtype,
            tuple(vec![boolean(true), None]).as_ref()
        )))
        .is_err()
    );

    let dtype = lazy_fixture("union");
    for tag in [7, 255] {
        for value in [None, boolean(true)] {
            compare(
                &dtype,
                Some(&ScalarValue::Union(UnionValue::new(tag, value))),
            );
        }
    }

    let dtype = lazy_fixture("map");
    // Resolving the malformed value dtype must panic before rejecting a null entry.
    let value = tuple(vec![None]);
    assert!(
        catch_unwind(AssertUnwindSafe(|| Scalar::validate(
            &dtype,
            value.as_ref()
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| Original::validate(
            &dtype,
            value.as_ref()
        )))
        .is_err()
    );
}

const KINDS: [&str; 8] = [
    "list",
    "fixed",
    "struct",
    "map-key",
    "map-value",
    "union",
    "variant",
    "extension",
];
fn chain(kind: &str, depth: usize, valid: bool, events: &Arc<Mutex<Vec<u8>>>) -> Scalar {
    let leaf = Scalar {
        dtype: DType::Bool(NonNullable),
        value: if valid {
            boolean(true)
        } else {
            Some(ScalarValue::Utf8("bad".into()))
        },
    };
    (0..depth).fold(leaf, |child, index| {
        let kind = if kind == "mixed" {
            KINDS[index % KINDS.len()]
        } else {
            kind
        };
        if kind == "variant" {
            return Scalar {
                dtype: DType::Variant(NonNullable),
                value: Some(ScalarValue::Variant(Box::new(child))),
            };
        }
        let (dtype, value) = child.into_parts();
        let (dtype, value) = match kind {
            "list" => (
                DType::List(Arc::new(dtype), NonNullable),
                tuple(vec![value]),
            ),
            "fixed" => (
                DType::FixedSizeList(Arc::new(dtype), 1, NonNullable),
                tuple(vec![value]),
            ),
            "struct" => (
                DType::Struct(
                    StructFields::new(["child"].into(), vec![dtype]),
                    NonNullable,
                ),
                tuple(vec![value]),
            ),
            "map-key" => (
                DType::Map(
                    MapDType::try_new(dtype, DType::Bool(NonNullable), false).unwrap(),
                    NonNullable,
                ),
                tuple(vec![tuple(vec![value, boolean(true)])]),
            ),
            "map-value" => (
                DType::Map(
                    MapDType::try_new(DType::Bool(NonNullable), dtype, false).unwrap(),
                    NonNullable,
                ),
                tuple(vec![tuple(vec![boolean(true), value])]),
            ),
            "union" => (
                DType::Union(
                    UnionVariants::try_new(["child"].into(), vec![dtype], vec![7]).unwrap(),
                    NonNullable,
                ),
                Some(ScalarValue::Union(UnionValue::new(7, value))),
            ),
            "extension" => (extension(dtype, 0, false, events), value),
            _ => panic!("unknown validation shape"),
        };
        Scalar { value, dtype }
    })
}

#[test]
fn deep_validation_constructor_errors_and_cleanup_use_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_SCALAR_VALIDATION";
    const COMPLETE: &str = "deep scalar validation completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let events = Arc::default();
                let scalar = chain(&kind, 8192, true, &events);
                let (dtype, value) = scalar.into_parts();
                let accepted = Scalar::try_new(dtype, value).unwrap();
                assert!(Scalar::validate(accepted.dtype(), accepted.value()).is_ok());
                drop(accepted);
                let (dtype, value) = chain(&kind, 8192, false, &events).into_parts();
                let error = Scalar::try_new(dtype, value).unwrap_err().to_string();
                assert!(error.contains("bool dtype expected Bool value, got \"bad\""));
                assert!(
                    error.len() < 100_000,
                    "deep error repeated an unbounded schema"
                );
                if ["list", "fixed", "map-key", "map-value", "union", "mixed"]
                    .contains(&kind.as_str())
                {
                    assert!(error.contains("outer scalar validation contexts omitted"));
                }
                // A malformed root must not recursively display its raw nested value.
                let (_, value) = chain("list", 8192, true, &events).into_parts();
                assert_eq!(
                    Scalar::try_new(DType::Bool(NonNullable), value)
                        .unwrap_err()
                        .to_string(),
                    "bool dtype expected Bool value, got Tuple(..)"
                );
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed"]) {
        let output = Command::new(env::current_exe().unwrap()).args([
            "--exact", "scalar::validate::traversal_tests::deep_validation_constructor_errors_and_cleanup_use_small_stack", "--test-threads=1", "--nocapture"
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

#[test]
fn validation_context_boundary_is_exact_and_reports_omitted_ancestors() {
    for depth in [0, 1, 31, 32, 33, 40] {
        let scalar = chain("list", depth, false, &Arc::default());
        let error = Scalar::validate(scalar.dtype(), scalar.value())
            .unwrap_err()
            .to_string();
        assert_eq!(
            error.matches("list element at index 0").count(),
            depth.min(32)
        );
        if depth <= 32 {
            compare(scalar.dtype(), scalar.value());
            assert!(!error.contains("omitted"));
        } else {
            assert!(error.contains(&format!(
                "{} outer scalar validation contexts omitted",
                depth - 32
            )));
        }
    }
}
