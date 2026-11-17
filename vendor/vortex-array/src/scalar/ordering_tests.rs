// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::cmp::Ordering;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::{env, thread};

use flatbuffers::FlatBufferBuilder;
use vortex_session::{VortexSession, registry::CachedId};

use crate::dtype::extension::ForeignExtDType;
use crate::dtype::half::f16;
use crate::dtype::{
    DType, DecimalDType, MapDType, Nullability, PType, StructFields, UnionVariants, i256,
};
use crate::flatbuffers::FlatBuffer;
use crate::scalar::{DecimalValue, PValue, Scalar, ScalarValue, UnionValue};

#[path = "ordering_reference.rs"]
mod reference;
use Nullability::{NonNullable, Nullable};

fn original(left: &Scalar, right: &Scalar) -> Option<Ordering> {
    if !left.dtype().eq_ignore_nullability(right.dtype()) {
        return None;
    }
    reference::partial_cmp_scalar_values(left.dtype(), left.value(), right.value())
}

fn compare(left: &Scalar, right: &Scalar) {
    assert_eq!(
        left.partial_cmp(right),
        original(left, right),
        "{left:?}, {right:?}"
    );
}

fn foreign(storage: DType) -> DType {
    static ID: CachedId = CachedId::new("test.scalar_ordering");
    DType::Extension(ForeignExtDType::from_parts(*ID, vec![2, 3], storage).unwrap())
}

fn wrap(kind: &str, child: Scalar) -> Scalar {
    if kind == "variant" {
        return Scalar {
            value: Some(ScalarValue::Variant(Box::new(child))),
            dtype: DType::Variant(NonNullable),
        };
    }
    let (dtype, value) = child.into_parts();
    let (dtype, value) = match kind {
        "list" => (
            DType::List(Arc::new(dtype), NonNullable),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "fixed" => (
            DType::FixedSizeList(Arc::new(dtype), 1, NonNullable),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "struct" => (
            DType::Struct(
                StructFields::new(["child"].into(), vec![dtype]),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        "map-key" => (
            DType::Map(
                MapDType::try_new(dtype, DType::Bool(Nullable), false).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![Some(ScalarValue::Tuple(vec![
                value, None,
            ]))])),
        ),
        "map-value" => (
            DType::Map(
                MapDType::try_new(DType::Bool(NonNullable), dtype, true).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Tuple(vec![Some(ScalarValue::Tuple(vec![
                Some(ScalarValue::Bool(true)),
                value,
            ]))])),
        ),
        "union" => (
            DType::Union(
                UnionVariants::try_new(["selected"].into(), vec![dtype], vec![7]).unwrap(),
                NonNullable,
            ),
            Some(ScalarValue::Union(UnionValue::new(7, value))),
        ),
        "extension" => (foreign(dtype), value),
        "extension-list" => (
            foreign(DType::List(Arc::new(dtype), NonNullable)),
            Some(ScalarValue::Tuple(vec![value])),
        ),
        _ => panic!("unknown scalar ordering shape"),
    };
    Scalar { value, dtype }
}

#[test]
fn scalar_ordering_matches_original_shallow_types_and_nullability() {
    let mut leaves = vec![
        Scalar::from(false),
        Scalar::from(true),
        Scalar::from(""),
        Scalar::from("λ🙂"),
        Scalar::binary(vec![0_u8, 255], NonNullable),
        Scalar::binary(vec![0_u8, 254], NonNullable),
    ];
    for primitive in [
        PValue::I8(-1),
        PValue::I8(1),
        PValue::I16(-2),
        PValue::I32(-3),
        PValue::I64(-4),
        PValue::U8(1),
        PValue::U16(2),
        PValue::U32(3),
        PValue::U64(4),
        PValue::F16(f16::from_bits(0x7e01)),
        PValue::F16(f16::from_bits(0x3c00)),
        PValue::F32(f32::NAN),
        PValue::F32(-0.0),
        PValue::F32(0.0),
        PValue::F64(f64::INFINITY),
        PValue::F64(f64::NEG_INFINITY),
    ] {
        leaves.push(
            Scalar::try_new(
                DType::Primitive(primitive.ptype(), NonNullable),
                Some(ScalarValue::Primitive(primitive)),
            )
            .unwrap(),
        );
    }
    for decimal in [
        DecimalValue::I8(-9),
        DecimalValue::I16(99),
        DecimalValue::I32(-1000),
        DecimalValue::I64(1000),
        DecimalValue::I128(0),
        DecimalValue::I256(i256::from(1_i64)),
    ] {
        leaves.push(
            Scalar::try_new(
                DType::Decimal(DecimalDType::new(8, 2), NonNullable),
                Some(ScalarValue::Decimal(decimal)),
            )
            .unwrap(),
        );
    }
    let mut scalars = vec![Scalar::null(DType::Null)];
    for value in leaves {
        scalars.push(Scalar::null(value.dtype().as_nullable()));
        scalars.push(Scalar::try_new(value.dtype().as_nullable(), value.value().cloned()).unwrap());
        for kind in KINDS.into_iter().chain(["variant", "extension"]) {
            let nested = wrap(kind, value.clone());
            scalars.push(Scalar::null(nested.dtype().as_nullable()));
            scalars.push(nested);
        }
        scalars.push(value);
    }
    // An extension around a union remains unordered under the original contract.
    scalars.push(wrap("extension", wrap("union", Scalar::from(true))));
    for left in &scalars {
        for right in &scalars {
            compare(left, right);
        }
    }
}

fn boolean(value: bool) -> Option<ScalarValue> {
    Some(ScalarValue::Bool(value))
}
fn tuple(values: Vec<Option<ScalarValue>>) -> Option<ScalarValue> {
    Some(ScalarValue::Tuple(values))
}

#[test]
fn lexicographic_lengths_nulls_struct_shapes_and_union_tags_are_unchanged() {
    let bools = DType::Bool(Nullable);
    let dtype = DType::List(Arc::new(bools.clone()), Nullable);
    let lists = [
        None,
        tuple(vec![]),
        tuple(vec![None]),
        tuple(vec![boolean(false)]),
        tuple(vec![boolean(false), None]),
        tuple(vec![boolean(false), boolean(true)]),
        tuple(vec![boolean(true)]),
    ];
    for left in &lists {
        for right in &lists {
            compare(
                &Scalar {
                    value: left.clone(),
                    dtype: dtype.clone(),
                },
                &Scalar {
                    value: right.clone(),
                    dtype: dtype.clone(),
                },
            );
        }
    }
    let struct_dtype = DType::Struct(
        StructFields::new(["a", "b"].into(), vec![bools.clone(), bools.clone()]),
        NonNullable,
    );
    for left in &lists {
        for right in &lists {
            // Intentionally malformed raw tuples preserve the existing shape refusal.
            compare(
                &Scalar {
                    value: left.clone(),
                    dtype: struct_dtype.clone(),
                },
                &Scalar {
                    value: right.clone(),
                    dtype: struct_dtype.clone(),
                },
            );
        }
    }
    let map = DType::Map(
        MapDType::try_new(DType::Bool(NonNullable), bools.clone(), false).unwrap(),
        NonNullable,
    );
    let entries = [
        tuple(vec![]),
        tuple(vec![None]),
        tuple(vec![tuple(vec![boolean(false), None])]),
        tuple(vec![tuple(vec![boolean(false), boolean(true)])]),
        tuple(vec![
            tuple(vec![boolean(false), None]),
            tuple(vec![boolean(true), boolean(false)]),
        ]),
        tuple(vec![tuple(vec![boolean(true), None])]),
        tuple(vec![tuple(vec![boolean(false)])]),
    ];
    for left in &entries {
        for right in &entries {
            compare(
                &Scalar {
                    value: left.clone(),
                    dtype: map.clone(),
                },
                &Scalar {
                    value: right.clone(),
                    dtype: map.clone(),
                },
            );
        }
    }
    let union = DType::Union(
        UnionVariants::try_new(
            ["first", "second"].into(),
            vec![bools.clone(), bools],
            vec![7, 253],
        )
        .unwrap(),
        NonNullable,
    );
    for left_tag in [7, 253, 255] {
        for right_tag in [7, 253, 255] {
            for left in [None, boolean(false), boolean(true)] {
                for right in [None, boolean(false), boolean(true)] {
                    compare(
                        &Scalar {
                            value: Some(ScalarValue::Union(UnionValue::new(
                                left_tag,
                                left.clone(),
                            ))),
                            dtype: union.clone(),
                        },
                        &Scalar {
                            value: Some(ScalarValue::Union(UnionValue::new(right_tag, right))),
                            dtype: union.clone(),
                        },
                    );
                }
            }
        }
    }
    let mut wide = vec![boolean(false); 4096];
    let left = Scalar {
        value: tuple(wide.clone()),
        dtype: dtype.clone(),
    };
    wide[4095] = boolean(true);
    let right = Scalar {
        value: tuple(wide),
        dtype,
    };
    assert_eq!(left.partial_cmp(&right), Some(Ordering::Less));
    compare(&left, &right);
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
            let a = builder.create_string("first");
            let b = builder.create_string("bad");
            let names = builder.create_vector(&[a, b]);
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
            let a = builder.create_string("bad");
            let b = builder.create_string("selected");
            let names = builder.create_vector(&[a, b]);
            let dtypes = builder.create_vector(&[bad, good]);
            let type_ids = builder.create_vector(&[1_i8, 7]);
            let variants = fb::Union::create(
                &mut builder,
                &fb::UnionArgs {
                    names: Some(names),
                    dtypes: Some(dtypes),
                    type_ids: Some(type_ids),
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
        _ => panic!("unknown lazy ordering fixture"),
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
fn ordering_preserves_lazy_short_circuits_and_map_preresolution() {
    let dtype = lazy_fixture("struct");
    let left = tuple(vec![boolean(false), None]);
    let right = tuple(vec![boolean(true), None]);
    assert_eq!(
        super::compare(&dtype, left.as_ref(), right.as_ref()),
        Some(Ordering::Less)
    );
    assert_eq!(
        super::compare(&dtype, left.as_ref(), right.as_ref()),
        reference::partial_cmp_scalar_values(&dtype, left.as_ref(), right.as_ref())
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| super::compare(
            &dtype,
            left.as_ref(),
            left.as_ref()
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| reference::partial_cmp_scalar_values(
            &dtype,
            left.as_ref(),
            left.as_ref()
        )))
        .is_err()
    );
    let dtype = lazy_fixture("union");
    let left = Some(ScalarValue::Union(UnionValue::new(7, boolean(false))));
    let right = Some(ScalarValue::Union(UnionValue::new(7, boolean(true))));
    assert_eq!(
        super::compare(&dtype, left.as_ref(), right.as_ref()),
        Some(Ordering::Less)
    );
    assert_eq!(
        super::compare(&dtype, left.as_ref(), right.as_ref()),
        reference::partial_cmp_scalar_values(&dtype, left.as_ref(), right.as_ref())
    );
    let dtype = lazy_fixture("map");
    let empty = tuple(vec![]);
    assert!(
        catch_unwind(AssertUnwindSafe(|| super::compare(
            &dtype,
            empty.as_ref(),
            empty.as_ref()
        )))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| reference::partial_cmp_scalar_values(
            &dtype,
            empty.as_ref(),
            empty.as_ref()
        )))
        .is_err()
    );
}

const KINDS: [&str; 7] = [
    "list",
    "fixed",
    "struct",
    "map-key",
    "map-value",
    "union",
    "extension-list",
];
fn chain(kind: &str, depth: usize, leaf: bool) -> Scalar {
    (0..depth).fold(Scalar::from(leaf), |child, index| {
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

#[test]
fn deep_scalar_ordering_and_typed_views_use_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_SCALAR_ORDERING";
    const COMPLETE: &str = "deep scalar ordering completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let left = chain(&kind, 8192, false);
                let same = chain(&kind, 8192, false);
                let right = chain(&kind, 8192, true);
                if kind == "variant" {
                    assert_eq!(left.partial_cmp(&same), None);
                    assert_eq!(left.partial_cmp(&right), None);
                } else {
                    assert_eq!(left.partial_cmp(&same), Some(Ordering::Equal));
                    assert_eq!(left.partial_cmp(&right), Some(Ordering::Less));
                    assert_eq!(right.partial_cmp(&left), Some(Ordering::Greater));
                    match kind.as_str() {
                        "list" | "fixed" => assert_eq!(
                            left.as_list().partial_cmp(&right.as_list()),
                            Some(Ordering::Less)
                        ),
                        "struct" => assert_eq!(
                            left.as_struct().partial_cmp(&right.as_struct()),
                            Some(Ordering::Less)
                        ),
                        "extension" | "extension-list" => assert_eq!(
                            left.as_extension().partial_cmp(&right.as_extension()),
                            Some(Ordering::Less)
                        ),
                        _ => {}
                    }
                }
                // A dtype mismatch must remain unordered, including outer nulls.
                assert_eq!(
                    left.partial_cmp(&Scalar::null(DType::Primitive(PType::I32, Nullable))),
                    None
                );
                drop(left);
                drop(same);
                drop(right);
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["extension", "variant", "mixed"]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "scalar::ordering::tests::deep_scalar_ordering_and_typed_views_use_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
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
