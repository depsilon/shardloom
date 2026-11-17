// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::{env, thread};

use crate::dtype::half::f16;
use crate::dtype::{
    DType, DecimalDType, MapDType, Nullability, PType, StructFields, UnionVariants, i256,
};
use crate::scalar::operation_test_support::{Events, KINDS, lazy_struct, probe, wrap};
use crate::scalar::{DecimalValue, PValue, Scalar, ScalarValue, UnionValue};

#[path = "inspection_reference.rs"]
mod reference;

use Nullability::{NonNullable, Nullable};

fn compare(scalar: &Scalar) {
    assert_eq!(
        catch_unwind(AssertUnwindSafe(|| scalar.is_zero())).map_err(|_| ()),
        catch_unwind(AssertUnwindSafe(|| reference::is_zero(scalar))).map_err(|_| ())
    );
    assert_eq!(
        catch_unwind(AssertUnwindSafe(|| scalar.approx_nbytes())).map_err(|_| ()),
        catch_unwind(AssertUnwindSafe(|| reference::approx_nbytes(scalar))).map_err(|_| ())
    );
}

#[test]
fn inspection_matches_original_leaf_widths_nulls_and_nested_shapes() {
    let mut leaves = vec![
        Scalar::from(false),
        Scalar::from(true),
        Scalar::from(""),
        Scalar::from("λ🙂abc"),
        Scalar::binary(Vec::<u8>::new(), NonNullable),
        Scalar::binary(vec![0_u8, 255, 1], NonNullable),
    ];
    let primitives = [
        PValue::U8(1),
        PValue::U16(2),
        PValue::U32(3),
        PValue::U64(4),
        PValue::I8(-1),
        PValue::I16(-2),
        PValue::I32(-3),
        PValue::I64(-4),
        PValue::F16(f16::from_bits(0x7e01)),
        PValue::F32(f32::NAN),
        PValue::F64(f64::INFINITY),
    ];
    for primitive in primitives {
        for value in [primitive, PValue::zero(&primitive.ptype())] {
            leaves.push(
                Scalar::try_new(
                    DType::Primitive(value.ptype(), NonNullable),
                    Some(ScalarValue::Primitive(value)),
                )
                .unwrap(),
            );
        }
    }
    for value in [
        PValue::F16(f16::from_bits(0x8000)),
        PValue::F32(-0.0),
        PValue::F64(-0.0),
    ] {
        leaves.push(
            Scalar::try_new(
                DType::Primitive(value.ptype(), NonNullable),
                Some(ScalarValue::Primitive(value)),
            )
            .unwrap(),
        );
    }
    for precision in [2, 38, 76] {
        for value in [
            DecimalValue::I8(0),
            DecimalValue::I16(1),
            DecimalValue::I32(-1),
            DecimalValue::I64(0),
            DecimalValue::I128(1),
            DecimalValue::I256(i256::from(0_i64)),
        ] {
            leaves.push(
                Scalar::try_new(
                    DType::Decimal(DecimalDType::new(precision, 1), NonNullable),
                    Some(ScalarValue::Decimal(value)),
                )
                .unwrap(),
            );
        }
    }
    compare(&Scalar::null(DType::Null));
    compare(&wrap("variant", Scalar::null(DType::Null)));
    for leaf in leaves {
        compare(&leaf);
        for nullable in [false, true] {
            let child = if nullable {
                Scalar::null(leaf.dtype().as_nullable())
            } else {
                leaf.clone()
            };
            compare(&child);
            for kind in KINDS {
                if nullable && matches!(kind, "map-key" | "variant") {
                    continue;
                }
                let scalar = wrap(kind, child.clone());
                compare(&Scalar::null(scalar.dtype().as_nullable()));
                compare(&scalar);
                compare(&wrap("fixed", wrap("struct", scalar)));
            }
        }
    }
}

#[test]
fn inspection_keeps_empty_lengths_tags_and_wide_estimates() {
    let boolean = DType::Bool(Nullable);
    for size in [0, 1, 2] {
        for values in [
            vec![],
            vec![None],
            vec![
                Some(ScalarValue::Bool(false)),
                Some(ScalarValue::Bool(true)),
            ],
        ] {
            let scalar = Scalar {
                value: Some(ScalarValue::Tuple(values)),
                dtype: DType::FixedSizeList(Arc::new(boolean.clone()), size, NonNullable),
            };
            compare(&scalar);
        }
    }
    let union = DType::Union(
        UnionVariants::try_new(
            ["a", "b"].into(),
            vec![boolean.clone(), boolean.clone()],
            vec![7, 253],
        )
        .unwrap(),
        NonNullable,
    );
    for tag in [7, 253, 255] {
        for value in [
            None,
            Some(ScalarValue::Bool(false)),
            Some(ScalarValue::Bool(true)),
        ] {
            compare(&Scalar {
                value: Some(ScalarValue::Union(UnionValue::new(tag, value))),
                dtype: union.clone(),
            });
        }
    }
    let structure = DType::Struct(
        StructFields::new(["a", "b"].into(), vec![boolean.clone(), boolean.clone()]),
        NonNullable,
    );
    for values in [vec![], vec![None], vec![None, None, None]] {
        compare(&Scalar {
            value: Some(ScalarValue::Tuple(values)),
            dtype: structure.clone(),
        });
    }
    for dtype in [
        DType::List(Arc::new(boolean.clone()), NonNullable),
        DType::Map(
            MapDType::try_new(boolean.as_nonnullable(), boolean.clone(), false).unwrap(),
            NonNullable,
        ),
        DType::Struct(StructFields::new(Default::default(), vec![]), NonNullable),
    ] {
        let scalar = Scalar {
            value: Some(ScalarValue::Tuple(vec![])),
            dtype,
        };
        assert_eq!(scalar.is_zero(), Some(true));
        assert_eq!(scalar.approx_nbytes(), 0);
        compare(&scalar);
    }
    let wide = Scalar {
        value: Some(ScalarValue::Tuple(vec![None; 8192])),
        dtype: DType::FixedSizeList(
            Arc::new(DType::Primitive(PType::U64, Nullable)),
            8192,
            NonNullable,
        ),
    };
    assert_eq!(wide.approx_nbytes(), 8192 * 8);
    compare(&wide);
    let entry = Some(ScalarValue::Tuple(vec![
        Some(ScalarValue::Bool(false)),
        None,
    ]));
    let map = Scalar {
        value: Some(ScalarValue::Tuple(vec![entry; 128])),
        dtype: DType::Map(
            MapDType::try_new(boolean.as_nonnullable(), boolean, false).unwrap(),
            NonNullable,
        ),
    };
    assert_eq!(map.approx_nbytes(), 256);
    assert_eq!(map.is_zero(), Some(false));
    compare(&map);
}

#[test]
fn inspection_keeps_debug_validation_eager_lists_and_lazy_short_circuits() {
    let events = Events::default();
    for reject in [false, true] {
        for nonzero in [false, true] {
            let (first_dtype, first_value) =
                wrap("fixed", probe(nonzero, 1, false, &events)).into_parts();
            let (second_dtype, second_value) =
                wrap("fixed", probe(false, 2, reject, &events)).into_parts();
            let child = Scalar {
                value: Some(ScalarValue::Tuple(vec![first_value, second_value])),
                dtype: DType::Struct(
                    StructFields::new(["a", "b"].into(), vec![first_dtype, second_dtype]),
                    NonNullable,
                ),
            };
            for kind in KINDS {
                let scalar = wrap(kind, child.clone());
                let mut cases = vec![scalar];
                if kind == "list" {
                    let scalar = &cases[0];
                    let values = scalar.value().unwrap().as_list();
                    cases.push(Scalar {
                        value: Some(ScalarValue::Tuple(vec![
                            values[0].clone(),
                            values[0].clone(),
                        ])),
                        dtype: scalar.dtype().clone(),
                    });
                }
                for scalar in cases {
                    for bytes in [false, true] {
                        events.lock().clear();
                        let expected = catch_unwind(AssertUnwindSafe(|| {
                            if bytes {
                                Some(reference::approx_nbytes(&scalar))
                            } else {
                                reference::is_zero(&scalar).map(usize::from)
                            }
                        }))
                        .map_err(|_| ());
                        let expected_events = std::mem::take(&mut *events.lock());
                        let actual = catch_unwind(AssertUnwindSafe(|| {
                            if bytes {
                                Some(scalar.approx_nbytes())
                            } else {
                                scalar.is_zero().map(usize::from)
                            }
                        }))
                        .map_err(|_| ());
                        assert_eq!(
                            actual, expected,
                            "{kind}, bytes={bytes}, reject={reject}, nonzero={nonzero}"
                        );
                        assert_eq!(
                            *events.lock(),
                            expected_events,
                            "{kind}, bytes={bytes}, reject={reject}, nonzero={nonzero}"
                        );
                    }
                }
            }
        }
    }
    // A nonzero first field and a truncated raw tuple must both avoid parsing
    // the malformed later dtype; an all-zero complete tuple reaches it.
    for values in [
        vec![Some(ScalarValue::Bool(true)), None],
        vec![Some(ScalarValue::Bool(false))],
        vec![Some(ScalarValue::Bool(false)), None],
    ] {
        let scalar = Scalar {
            value: Some(ScalarValue::Tuple(values)),
            dtype: lazy_struct(&DType::Bool(NonNullable)),
        };
        compare(&scalar);
    }
}

#[test]
fn deep_zero_and_byte_inspection_use_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_SCALAR_INSPECTION";
    const COMPLETE: &str = "deep scalar inspection completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let mut scalar = Scalar::from(false);
                let mut bytes = 1;
                let mut zero = true;
                for index in 0..8192 {
                    let shape = if kind == "mixed" {
                        KINDS[index % KINDS.len()]
                    } else {
                        kind.as_str()
                    };
                    bytes += usize::from(matches!(shape, "union" | "map-key" | "map-value"));
                    zero &= !matches!(shape, "list" | "map-key" | "map-value");
                    scalar = wrap(shape, scalar);
                }
                assert_eq!(scalar.is_zero(), Some(zero));
                assert_eq!(scalar.approx_nbytes(), bytes);
                let variant = wrap("variant", scalar);
                assert_eq!(variant.as_variant().is_zero(), Some(zero));
                assert_eq!(variant.approx_nbytes(), bytes);
                drop(variant);
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed"]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "scalar::inspection::tests::deep_zero_and_byte_inspection_use_small_stack",
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
