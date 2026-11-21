//! Frozen-reference semantic parity and private deep native validation proof.

use super::*;
use crate::resident_session::ResidentVortexSession;
use parquet_variant::VariantBuilder;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray,
        arrays::{BoolArray, ConstantArray, ListArray, PrimitiveArray, VarBinArray},
        dtype::{FieldNames, Nullability, PType},
        scalar::Scalar,
        validity::Validity,
    },
    session::VortexSession,
};

fn record(names: &[&str], children: Vec<ArrayRef>, validity: Validity) -> ArrayRef {
    StructArray::new(
        FieldNames::from_iter(names.iter().copied()),
        children,
        1,
        validity,
    )
    .into_array()
}

fn shell(typed: ArrayRef) -> ArrayRef {
    record(&["typed_value"], vec![typed], Validity::NonNullable)
}

fn binary(bytes: Vec<u8>) -> ArrayRef {
    VarBinArray::from_vec(vec![bytes], DType::Binary(Nullability::NonNullable)).into_array()
}

fn metadata(names: &[&str]) -> Vec<u8> {
    VariantBuilder::new()
        .with_field_names(names.iter().copied())
        .with_value(Value::Null)
        .finish()
        .0
}

fn nested(mut child: ArrayRef, shredded: bool, shape: usize, depth: usize) -> ArrayRef {
    let session = VortexSession::default();
    let mut execution = session.create_execution_ctx();
    for level in 0..depth {
        if shredded {
            child = shell(child);
        }
        child = if shape == 0 || (shape == 2 && level % 2 == 0) {
            record(&["x"], vec![child], Validity::NonNullable)
        } else {
            ListArray::try_new(
                child,
                PrimitiveArray::from_iter([0_u64, 1]).into_array(),
                Validity::NonNullable,
                &mut execution,
            )
            .unwrap()
            .into_array()
        };
    }
    child
}

struct Input<'a> {
    metadata: &'a [u8],
    raw: Option<&'a ArrayRef>,
    typed: &'a ArrayRef,
    shredded: bool,
}

fn inspect(
    input: &Input<'_>,
    old: bool,
    max_depth: usize,
    budget: u64,
    cancelled: bool,
) -> std::result::Result<bool, String> {
    let session = ResidentVortexSession::new(budget, 1).unwrap();
    let token = CancellationToken::default();
    let result = session
        .with_native_execution_context(&token, |context| {
            let before = context.memory().snapshot().reserved_bytes;
            if cancelled {
                token.cancel();
            }
            let mut execution = context.native_session().create_execution_ctx();
            let result = if input.shredded {
                if old {
                    validation::reference::parts(
                        input.metadata,
                        input.raw,
                        Some(input.typed),
                        0,
                        &mut execution,
                        context,
                        0,
                    )
                } else {
                    validation::walk::parts(
                        input.metadata,
                        input.raw,
                        Some(input.typed),
                        0,
                        &mut execution,
                        context,
                        0,
                        max_depth,
                    )
                }
            } else {
                let raw = input
                    .raw
                    .map(|array| validation::binary(array, 0, &mut execution))
                    .transpose()?
                    .flatten();
                let parsed = raw
                    .as_ref()
                    .map(|bytes| {
                        crate::native_variant::value(input.metadata, bytes.as_slice(), &|| {
                            context.check_cancelled()
                        })
                    })
                    .transpose()?;
                if old {
                    reference::logical(
                        input.metadata,
                        parsed.as_ref(),
                        input.typed,
                        0,
                        &mut execution,
                        context,
                        0,
                    )
                } else {
                    walk::logical(
                        input.metadata,
                        parsed.as_ref(),
                        input.typed,
                        0,
                        &mut execution,
                        context,
                        0,
                        max_depth,
                    )
                }
                .map(|()| true)
            };
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            result
        })
        .map_err(|error| error.to_string());
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    result
}

fn parity(input: &Input<'_>) {
    let original = inspect(input, true, 24, 128 << 20, false);
    assert_eq!(inspect(input, false, 24, 128 << 20, false), original);
}

#[test]
fn variant_validation_continuations_match_original_results_and_error_precedence() {
    let metadata = metadata(&["x", "tail"]);
    let absent_metadata = self::metadata(&[]);
    let null = binary(vec![0]);
    let malformed = binary(vec![3]);
    let mut builder = VariantBuilder::new().with_field_names(["x", "tail"]);
    let mut object = builder.new_object();
    object.insert("x", Value::Int32(9));
    object.finish();
    let (object_metadata, object_bytes) = builder.finish();
    let object = binary(object_bytes);
    let leaves = [
        PrimitiveArray::from_iter([7_i32]).into_array(),
        BoolArray::from_iter([true]).into_array(),
        binary(vec![1, 2, 3]),
        VarBinArray::from_iter([Some("text")], DType::Utf8(Nullability::NonNullable)).into_array(),
        PrimitiveArray::from_iter([u64::MAX]).into_array(),
        ConstantArray::new(
            Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable)),
            1,
        )
        .into_array(),
    ];
    for shredded in [false, true] {
        for leaf in &leaves {
            for shape in 0..3 {
                for depth in [0, 1, 3, 24, 25] {
                    let typed = nested(leaf.clone(), shredded, shape, depth);
                    for (metadata, raw) in [
                        (metadata.as_slice(), None),
                        (metadata.as_slice(), Some(&null)),
                        (metadata.as_slice(), Some(&malformed)),
                        (object_metadata.as_slice(), Some(&object)),
                        (absent_metadata.as_slice(), None),
                    ] {
                        parity(&Input {
                            metadata,
                            raw,
                            typed: &typed,
                            shredded,
                        });
                    }
                }
            }
        }
    }
    competing_errors(&object_metadata, &object);
}

fn competing_errors(metadata: &[u8], raw: &ArrayRef) {
    let bad_leaf = PrimitiveArray::from_iter([u64::MAX]).into_array();
    for shredded in [false, true] {
        let child = if shredded {
            shell(bad_leaf.clone())
        } else {
            bad_leaf.clone()
        };
        let typed = record(
            &["x", "absent"],
            vec![child.clone(), child],
            Validity::NonNullable,
        );
        parity(&Input {
            metadata,
            raw: Some(raw),
            typed: &typed,
            shredded,
        });
        let null =
            ConstantArray::new(Scalar::null(typed.dtype().clone().as_nullable()), 1).into_array();
        parity(&Input {
            metadata,
            raw: Some(raw),
            typed: &null,
            shredded,
        });
    }
    for bad_shell in [
        bad_leaf.clone(),
        record(&["bad"], vec![bad_leaf], Validity::NonNullable),
    ] {
        let typed = record(&["x"], vec![bad_shell], Validity::NonNullable);
        parity(&Input {
            metadata,
            raw: Some(raw),
            typed: &typed,
            shredded: true,
        });
    }
}

fn deep_child() {
    let metadata = metadata(&["x", "tail"]);
    for shredded in [false, true] {
        for shape in 0..3 {
            let leaf = PrimitiveArray::from_iter([7_i32]).into_array();
            let typed = nested(leaf, shredded, shape, 8192);
            let input = Input {
                metadata: &metadata,
                raw: None,
                typed: &typed,
                shredded,
            };
            assert_eq!(
                inspect(&input, false, usize::MAX, 128 << 20, false),
                Ok(true)
            );
            assert!(
                inspect(&input, false, 24, 128 << 20, false)
                    .unwrap_err()
                    .contains("depth")
            );
            assert!(
                inspect(&input, false, usize::MAX, 32 << 10, false)
                    .unwrap_err()
                    .contains("reservation denied")
            );
            assert!(
                inspect(&input, false, usize::MAX, 128 << 20, true)
                    .unwrap_err()
                    .contains("cancelled")
            );
            let bad = PrimitiveArray::from_iter([u64::MAX]).into_array();
            let invalid = nested(bad.clone(), shredded, shape, 8192);
            let input = Input {
                typed: &invalid,
                ..input
            };
            assert!(
                inspect(&input, false, usize::MAX, 128 << 20, false)
                    .unwrap_err()
                    .contains("unsupported physical dtype")
            );
            let (first, last) = if shredded {
                (shell(typed.clone()), shell(bad))
            } else {
                (typed.clone(), bad)
            };
            let late = record(&["x", "tail"], vec![first, last], Validity::NonNullable);
            let input = Input {
                typed: &late,
                ..input
            };
            assert!(
                inspect(&input, false, usize::MAX, 128 << 20, false)
                    .unwrap_err()
                    .contains("unsupported physical dtype")
            );
        }
    }
    deep_unwind(&metadata);
}

fn deep_unwind(metadata: &[u8]) {
    // Preserve the pinned decoder's inherited out-of-range date panic. Here it
    // is a fault injection after thousands of parents/credits were suspended.
    let mut bytes = vec![11 << 2];
    bytes.extend_from_slice(&i32::MAX.to_le_bytes());
    let inner = record(&["value"], vec![binary(bytes)], Validity::NonNullable);
    let first = record(&["x"], vec![inner], Validity::NonNullable);
    let typed = nested(first, true, 2, 8191);
    let session = ResidentVortexSession::new(128 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut execution = context.native_session().create_execution_ctx();
                validation::walk::parts(
                    metadata,
                    None,
                    Some(&typed),
                    0,
                    &mut execution,
                    context,
                    0,
                    usize::MAX,
                )
            }));
            assert!(
                result.is_err(),
                "pinned date decoder must trigger the unwind fixture"
            );
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn variant_validation_continuations_deep_native_trees_and_cleanup_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_DEEP_VARIANT_VALIDATION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            concat!(
                module_path!(),
                "::variant_validation_continuations_deep_native_trees_and_cleanup_on_small_stack"
            )
            .strip_prefix("shardloom_vortex::")
            .unwrap(),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
