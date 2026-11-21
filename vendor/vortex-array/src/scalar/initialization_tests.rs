// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;
use std::sync::Arc;
use std::{env, thread};

use crate::dtype::{DType, DecimalDType, Nullability, PType, StructFields, UnionVariants};
use crate::scalar::operation_test_support::{Events, KINDS, lazy_struct, probe_dtype, wrap};
use crate::scalar::{Scalar, ScalarValue};

#[path = "initialization_reference.rs"]
mod reference;

use Nullability::{NonNullable, Nullable};

fn types() -> Vec<DType> {
    let mut leaves = vec![
        DType::Null,
        DType::Bool(NonNullable),
        DType::Utf8(NonNullable),
        DType::Binary(NonNullable),
        DType::Variant(NonNullable),
    ];
    for primitive in [
        PType::U8,
        PType::U16,
        PType::U32,
        PType::U64,
        PType::I8,
        PType::I16,
        PType::I32,
        PType::I64,
        PType::F16,
        PType::F32,
        PType::F64,
    ] {
        leaves.push(DType::Primitive(primitive, NonNullable));
    }
    for precision in [2, 4, 8, 18, 38, 76] {
        leaves.push(DType::Decimal(DecimalDType::new(precision, 2), NonNullable));
    }
    let mut types = Vec::new();
    for leaf in leaves {
        for nullable in [NonNullable, Nullable] {
            if leaf == DType::Null && nullable == NonNullable {
                continue;
            }
            let child = leaf.with_nullability(nullable);
            types.push(child.clone());
            for kind in KINDS {
                if kind == "map-key" && child.is_nullable() {
                    continue;
                }
                let dtype = wrap(
                    kind,
                    Scalar {
                        value: None,
                        dtype: child.clone(),
                    },
                )
                .into_parts()
                .0;
                types.push(dtype.as_nullable());
                types.push(dtype.clone());
                types.push(
                    wrap("fixed", wrap("struct", Scalar { value: None, dtype }))
                        .into_parts()
                        .0,
                );
            }
            for size in [0, 3] {
                types.push(DType::FixedSizeList(
                    Arc::new(child.clone()),
                    size,
                    NonNullable,
                ));
            }
        }
    }
    types.push(DType::Struct(
        StructFields::new(Default::default(), vec![]),
        NonNullable,
    ));
    let names: Vec<_> = (0..512).map(|i| format!("f{i}")).collect();
    types.push(DType::Struct(
        StructFields::new(
            names.iter().map(String::as_str).collect(),
            vec![DType::Bool(Nullable); names.len()],
        ),
        NonNullable,
    ));
    types
}

fn reference_default(dtype: &DType) -> Option<Scalar> {
    Scalar::try_new(dtype.clone(), reference::default(dtype)?).ok()
}
fn reference_zero(dtype: &DType) -> Scalar {
    Scalar::try_new(
        dtype.clone(),
        Some(reference::zero(dtype).expect("no zero")),
    )
    .unwrap()
}

#[test]
fn defaults_and_zeros_match_original_all_types_nulls_and_shapes() {
    for dtype in types() {
        assert_eq!(
            ScalarValue::try_default_value(&dtype),
            reference::default(&dtype),
            "{dtype}"
        );
        assert_eq!(
            ScalarValue::try_zero_value(&dtype),
            reference::zero(&dtype),
            "{dtype}"
        );
        assert_eq!(
            Scalar::try_default_value(&dtype),
            reference_default(&dtype),
            "{dtype}"
        );
        assert_eq!(
            catch_unwind(AssertUnwindSafe(|| Scalar::zero_value(&dtype))).map_err(|_| ()),
            catch_unwind(AssertUnwindSafe(|| reference_zero(&dtype))).map_err(|_| ()),
            "{dtype}"
        );
    }
    for first in [DType::Null, DType::Bool(Nullable), DType::Bool(NonNullable)] {
        let dtype = DType::Union(
            UnionVariants::try_new(
                ["first", "second"].into(),
                vec![first, DType::Bool(NonNullable)],
                vec![91, 2],
            )
            .unwrap(),
            NonNullable,
        );
        assert_eq!(
            ScalarValue::try_default_value(&dtype),
            reference::default(&dtype)
        );
        assert_eq!(ScalarValue::try_zero_value(&dtype), reference::zero(&dtype));
    }
}

#[test]
fn initializer_callbacks_and_early_lazy_failures_keep_original_order() {
    let events = Events::default();
    for reject in [false, true] {
        let first = probe_dtype(DType::Bool(NonNullable), 1, false, &events);
        let second = probe_dtype(DType::Bool(NonNullable), 2, reject, &events);
        let child = DType::Struct(
            StructFields::new(["a", "b"].into(), vec![first, second]),
            NonNullable,
        );
        for kind in KINDS {
            let dtype = wrap(
                kind,
                Scalar {
                    value: None,
                    dtype: child.clone(),
                },
            )
            .into_parts()
            .0;
            for default in [false, true] {
                events.lock().clear();
                let expected = catch_unwind(AssertUnwindSafe(|| {
                    if default {
                        reference_default(&dtype)
                    } else {
                        Some(reference_zero(&dtype))
                    }
                }))
                .map_err(|_| ());
                let expected_events = std::mem::take(&mut *events.lock());
                let actual = catch_unwind(AssertUnwindSafe(|| {
                    if default {
                        Scalar::try_default_value(&dtype)
                    } else {
                        Some(Scalar::zero_value(&dtype))
                    }
                }))
                .map_err(|_| ());
                assert_eq!(
                    actual, expected,
                    "{kind}, default={default}, reject={reject}"
                );
                assert_eq!(
                    *events.lock(),
                    expected_events,
                    "{kind}, default={default}, reject={reject}"
                );
            }
        }
    }
    // Zero cannot construct the first Null field, so the malformed later field
    // must remain lazy. A default can pass the first field and must fail there.
    let dtype = lazy_struct(&DType::Null);
    assert_eq!(ScalarValue::try_zero_value(&dtype), None);
    assert_eq!(reference::zero(&dtype), None);
    assert!(catch_unwind(AssertUnwindSafe(|| ScalarValue::try_default_value(&dtype))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| reference::default(&dtype))).is_err());
    for kind in ["list", "map-value"] {
        let dtype = wrap(
            kind,
            Scalar {
                value: None,
                dtype: lazy_struct(&DType::Null),
            },
        )
        .into_parts()
        .0;
        assert!(Scalar::try_default_value(&dtype).is_some());
        assert_eq!(ScalarValue::try_zero_value(&dtype), reference::zero(&dtype));
    }
    let dtype = DType::FixedSizeList(Arc::new(lazy_struct(&DType::Null)), 0, NonNullable);
    assert!(Scalar::try_default_value(&dtype).is_some());
    assert_eq!(ScalarValue::try_zero_value(&dtype), reference::zero(&dtype));
}

fn chain(kind: &str, leaf: DType) -> DType {
    (0..8192).fold(leaf, |dtype, index| {
        let kind = if kind == "mixed" {
            ["fixed", "struct", "union", "extension"][index % 4]
        } else {
            kind
        };
        wrap(kind, Scalar { value: None, dtype }).into_parts().0
    })
}

#[test]
fn deep_initialization_and_failed_partial_cleanup_use_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_SCALAR_INITIALIZATION";
    const COMPLETE: &str = "deep scalar initialization completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let dtype = chain(&kind, DType::Bool(NonNullable));
                let zero = Scalar::zero_value(&dtype);
                let default = Scalar::default_value(&dtype);
                assert_eq!(zero, default);
                Scalar::validate(&dtype, zero.value()).unwrap();
                drop(zero);
                drop(default);
                drop(dtype);
                if matches!(
                    kind.as_str(),
                    "fixed" | "struct" | "union" | "extension" | "mixed"
                ) {
                    let fail = chain(&kind, DType::Null);
                    let good = chain(&kind, DType::Bool(NonNullable));
                    let dtype = DType::Struct(
                        StructFields::new(["good", "fail"].into(), vec![good, fail]),
                        NonNullable,
                    );
                    assert!(ScalarValue::try_zero_value(&dtype).is_none());
                    assert!(catch_unwind(AssertUnwindSafe(|| Scalar::zero_value(&dtype))).is_err());
                    drop(dtype);
                }
                let events = Events::default();
                let reject = probe_dtype(DType::Bool(NonNullable), 7, true, &events);
                let dtype = chain("mixed", reject);
                assert!(Scalar::try_default_value(&dtype).is_none());
                assert_eq!(*events.lock(), [7, 7]);
                drop(dtype);
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in [
        "fixed",
        "struct",
        "union",
        "extension",
        "list",
        "map-value",
        "mixed",
    ] {
        let output = Command::new(env::current_exe().unwrap()).args([
            "--exact", "scalar::initialization::tests::deep_initialization_and_failed_partial_cleanup_use_small_stack", "--test-threads=1", "--nocapture",
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
