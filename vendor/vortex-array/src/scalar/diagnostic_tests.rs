// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::process::Command;
use std::sync::Arc;
use std::{env, fmt, thread};

use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use super::ValueDisplay;
use crate::dtype::extension::{ExtDType, ExtId, ExtVTable};
use crate::dtype::{DType, Nullability, UnionVariants};
use crate::scalar::{Scalar, ScalarValue, UnionValue};

const KINDS: [&str; 3] = ["tuple", "union", "variant"];

fn chain(kind: &str, depth: usize, leaf: Scalar) -> Scalar {
    (0..depth).fold(leaf, |child, index| {
        match if kind == "mixed" {
            KINDS[index % KINDS.len()]
        } else {
            kind
        } {
            "tuple" => {
                let (dtype, value) = child.into_parts();
                Scalar {
                    dtype: DType::List(Arc::new(dtype), Nullability::NonNullable),
                    value: Some(ScalarValue::Tuple(vec![value])),
                }
            }
            "union" => {
                let (dtype, value) = child.into_parts();
                Scalar {
                    dtype: DType::Union(
                        UnionVariants::try_new(["child"].into(), vec![dtype], vec![7]).unwrap(),
                        Nullability::NonNullable,
                    ),
                    value: Some(ScalarValue::Union(UnionValue::new(7, value))),
                }
            }
            "variant" => Scalar {
                dtype: DType::Variant(Nullability::NonNullable),
                value: Some(ScalarValue::Variant(Box::new(child))),
            },
            _ => panic!("unknown diagnostic shape"),
        }
    })
}

#[test]
fn raw_value_diagnostics_bound_depth_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_SCALAR_DIAGNOSTIC";
    const COMPLETE: &str = "deep scalar diagnostics completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETE} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                for depth in [0, 1, 31, 32, 33, 8192] {
                    let scalar = chain(&kind, depth, Scalar::from(true));
                    let value = scalar.value().unwrap();
                    for text in [
                        format!("{value:?}"),
                        format!("{value:#?}"),
                        format!("{value:#x?}"),
                    ] {
                        if depth <= 32 {
                            assert!(text.contains("Bool"));
                            assert!(!text.contains(".."));
                        } else {
                            assert!(text.contains(".."));
                            assert!(text.len() < 32);
                        }
                    }
                    let shown = ValueDisplay(value).to_string();
                    if depth <= 32 {
                        assert_eq!(shown, value.to_string());
                        assert!(shown.contains("true"));
                    } else {
                        assert!(shown.contains(".."));
                        assert!(shown.len() < 32);
                    }
                }
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
                "scalar::diagnostic::tests::raw_value_diagnostics_bound_depth_on_small_stack",
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

#[derive(Clone, PartialEq, Eq, Hash)]
struct Unformatted;
impl fmt::Debug for Unformatted {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("depth inspection must not format metadata")
    }
}
impl fmt::Display for Unformatted {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("depth inspection must not format metadata")
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct Probe;
impl ExtVTable for Probe {
    type Metadata = Unformatted;
    type NativeValue<'a> = &'a ScalarValue;
    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("test.scalar_diagnostic");
        *ID
    }
    fn serialize_metadata(&self, _: &Unformatted) -> VortexResult<Vec<u8>> {
        panic!("no metadata serialization")
    }
    fn deserialize_metadata(&self, _: &[u8]) -> VortexResult<Unformatted> {
        panic!("no metadata deserialization")
    }
    fn validate_dtype(_: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }
    fn unpack_native<'a>(
        _: &'a ExtDType<Self>,
        _: &'a ScalarValue,
    ) -> VortexResult<&'a ScalarValue> {
        panic!("depth inspection must not validate values")
    }
}

#[test]
fn diagnostic_probe_never_formats_or_validates_hidden_values() {
    let leaf = Scalar {
        dtype: DType::Extension(
            ExtDType::<Probe>::try_new(Unformatted, DType::Bool(Nullability::NonNullable))
                .unwrap()
                .erased(),
        ),
        value: Some(ScalarValue::Bool(true)),
    };
    let scalar = chain("variant", 33, leaf);
    let value = scalar.value().unwrap();
    assert_eq!(format!("{value:?}"), "Variant(..)");
    assert_eq!(ValueDisplay(value).to_string(), "Variant(..)");
    assert!(!Scalar::is_compatible(
        &DType::Bool(Nullability::NonNullable),
        Some(value)
    ));
    assert_eq!(
        Scalar::validate(&DType::Bool(Nullability::NonNullable), Some(value))
            .unwrap_err()
            .to_string(),
        "bool dtype expected Bool value, got Variant(..)"
    );
}
