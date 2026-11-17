use super::*;
use crate::resident_session::ResidentVortexSession;
use parquet_variant::{Variant, VariantBuilder};
use shardloom_exec::compute_pool::CancellationToken;
use std::panic::{AssertUnwindSafe, catch_unwind};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray,
        arrays::{BoolArray, ListArray, PrimitiveArray, SliceArray, VarBinArray},
        dtype::{FieldNames, Nullability},
        validity::Validity,
    },
    session::VortexSession,
};

fn record(names: &[&str], fields: Vec<ArrayRef>, validity: Validity) -> ArrayRef {
    StructArray::new(
        FieldNames::from_iter(names.iter().copied()),
        fields,
        1,
        validity,
    )
    .into_array()
}

fn binary(bytes: Vec<u8>) -> ArrayRef {
    VarBinArray::from_vec(vec![bytes], DType::Binary(Nullability::NonNullable)).into_array()
}

fn nested(mut array: ArrayRef, depth: usize, shape: usize) -> ArrayRef {
    let mut execution = VortexSession::default().create_execution_ctx();
    for level in 0..depth {
        let child = record(&["typed_value"], vec![array], Validity::NonNullable);
        array = if shape == 0 || (shape == 2 && level % 2 == 0) {
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
    array
}

fn wrappers(mut array: ArrayRef, depth: usize) -> ArrayRef {
    for _ in 0..depth {
        array = SliceArray::try_new(array, 0..1).unwrap().into_array();
    }
    array
}

enum Input<'a, 'm, 'v> {
    Physical(&'a ArrayRef),
    Typed(&'a ArrayRef, &'a [u8]),
    Expanded(&'a Variant<'m, 'v>),
}

fn observe(
    input: &Input<'_, '_, '_>,
    old: bool,
    depth: usize,
    maximum: usize,
    budget: u64,
) -> (std::result::Result<(), String>, u64) {
    let session = ResidentVortexSession::new(budget, 1).unwrap();
    let observed = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let mut credit = context.memory().reserve(4096)?;
            let mut execution = context.native_session().create_execution_ctx();
            let result = match input {
                Input::Physical(array) => {
                    if old {
                        reference::physical(array, &mut credit, context, depth)
                    } else {
                        walk::physical(array, &mut credit, context, depth, maximum)
                    }
                }
                Input::Typed(array, metadata) => {
                    if old {
                        reference::typed_value(
                            array,
                            0,
                            metadata,
                            &mut credit,
                            context,
                            &mut execution,
                            depth,
                        )
                    } else {
                        walk::typed_value(
                            array,
                            0,
                            metadata,
                            &mut credit,
                            context,
                            &mut execution,
                            depth,
                            maximum,
                        )
                    }
                }
                Input::Expanded(value) => {
                    if old {
                        reference::expanded(value, &mut credit, context)
                    } else {
                        walk::expanded(value, &mut credit, context)
                    }
                }
            }
            .map_err(|error| error.to_string());
            let bytes = credit.bytes();
            drop(credit);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok((result, bytes))
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    observed
}

fn raw_parts(metadata: Vec<u8>, bytes: Vec<u8>, typed: Option<ArrayRef>) -> ArrayRef {
    ParquetVariant::try_new(
        Validity::AllValid,
        binary(metadata),
        Some(binary(bytes)),
        typed,
    )
    .unwrap()
    .into_array()
}

#[test]
fn variant_workspace_continuations_match_original_costs_partial_credit_and_errors() {
    let metadata = vec![1, 1, 0, 1, b'x'];
    let leaf = PrimitiveArray::from_iter([7_i32]).into_array();
    for depth in [0, 1, 24, 25, 128, 129] {
        let physical = wrappers(leaf.clone(), depth);
        let input = Input::Physical(&physical);
        assert_eq!(
            observe(&input, false, 0, 128, 128 << 20),
            observe(&input, true, 0, 128, 128 << 20)
        );
    }
    for shape in 0..3 {
        for depth in [0, 1, 3, 24, 25] {
            let typed = nested(leaf.clone(), depth, shape);
            let input = Input::Typed(&typed, &metadata);
            assert_eq!(
                observe(&input, false, 0, 24, 128 << 20),
                observe(&input, true, 0, 24, 128 << 20)
            );
        }
    }
    for value in [
        Variant::Null,
        Variant::from("short"),
        Variant::from("a longer encoded text value"),
        Variant::Binary(&[0, 255]),
        Variant::Int64(i64::MIN),
    ] {
        let (m, bytes) = VariantBuilder::new().with_value(value).finish();
        let parsed = Variant::try_new(&m, &bytes).unwrap();
        let input = Input::Expanded(&parsed);
        assert_eq!(
            observe(&input, false, 0, 24, 128 << 20),
            observe(&input, true, 0, 24, 128 << 20)
        );
        let physical = raw_parts(m, bytes, None);
        let input = Input::Physical(&physical);
        assert_eq!(
            observe(&input, false, 0, 128, 128 << 20),
            observe(&input, true, 0, 128, 128 << 20)
        );
    }
    error_precedence(&metadata, &leaf);
    reserve_parity(&leaf);
}

fn reserve_parity(source: &ArrayRef) {
    let session = ResidentVortexSession::new(128 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let original = reference::reserve(source, source.dtype(), None, context)?;
            let expected = original.bytes();
            drop(original);
            let current = reserve(source, source.dtype(), None, context)?;
            assert_eq!(current.bytes(), expected);
            drop(current);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn error_precedence(metadata: &[u8], leaf: &ArrayRef) {
    for bytes in [vec![], vec![3], vec![0], vec![252]] {
        let good = record(
            &["typed_value"],
            vec![nested(leaf.clone(), 3, 2)],
            Validity::NonNullable,
        );
        let bad = record(
            &["value"],
            vec![binary(bytes.clone())],
            Validity::NonNullable,
        );
        let typed = record(&["x", "late"], vec![good, bad], Validity::NonNullable);
        let input = Input::Typed(&typed, metadata);
        assert_eq!(
            observe(&input, false, 0, 24, 128 << 20),
            observe(&input, true, 0, 24, 128 << 20)
        );
        let physical = raw_parts(metadata.to_vec(), bytes, Some(typed));
        let input = Input::Physical(&physical);
        assert_eq!(
            observe(&input, false, 0, 128, 128 << 20),
            observe(&input, true, 0, 128, 128 << 20)
        );
    }
    for valid in [Validity::NonNullable, Validity::AllInvalid] {
        let bad = record(
            &["unexpected"],
            vec![BoolArray::from_iter([true]).into_array()],
            valid,
        );
        let typed = record(&["x"], vec![bad], Validity::NonNullable);
        let input = Input::Typed(&typed, metadata);
        assert_eq!(
            observe(&input, false, 0, 24, 128 << 20),
            observe(&input, true, 0, 24, 128 << 20)
        );
    }
}

fn raw_chain(depth: usize, object: bool) -> Vec<u8> {
    let prefix = if object { 11 } else { 10 };
    let mut bytes = Vec::with_capacity(depth * prefix + 1);
    for level in 0..depth {
        bytes.extend_from_slice(&[if object { 14 } else { 15 }, 1]);
        if object {
            bytes.push(0);
        }
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from((depth - level - 1) * prefix + 1)
                .unwrap()
                .to_le_bytes(),
        );
    }
    bytes.push(0);
    bytes
}

fn deep_child() {
    let metadata = [1, 1, 0, 1, b'x'];
    let leaf = PrimitiveArray::from_iter([7_i32]).into_array();
    let base = observe(&Input::Physical(&leaf), true, 0, 128, 128 << 20).1;
    let one = wrappers(leaf.clone(), 1);
    let one = observe(&Input::Physical(&one), true, 0, 128, 128 << 20).1;
    let deep = wrappers(leaf.clone(), 8192);
    let input = Input::Physical(&deep);
    assert_eq!(
        observe(&input, false, 0, usize::MAX, 256 << 20),
        (Ok(()), base + (one - base) * 8192)
    );
    assert!(
        observe(&input, false, 0, 128, 256 << 20)
            .0
            .unwrap_err()
            .contains("depth")
    );
    assert!(
        observe(&input, false, 0, usize::MAX, 32 << 10)
            .0
            .unwrap_err()
            .contains("reservation denied")
    );
    for shape in 0..3 {
        let typed = nested(leaf.clone(), 8192, shape);
        let input = Input::Typed(&typed, &metadata);
        assert_eq!(
            observe(&input, false, 0, usize::MAX, 128 << 20),
            (Ok(()), 4096)
        );
        assert!(
            observe(&input, false, 0, 24, 128 << 20)
                .0
                .unwrap_err()
                .contains("depth")
        );
        assert!(
            observe(&input, false, 0, usize::MAX, 32 << 10)
                .0
                .unwrap_err()
                .contains("reservation denied")
        );
    }
    for object in [false, true] {
        let bytes = raw_chain(8192, object);
        let parsed = Variant::try_new(&metadata, &bytes).unwrap();
        let input = Input::Expanded(&parsed);
        assert_eq!(
            observe(&input, false, 0, usize::MAX, 128 << 20),
            (
                Ok(()),
                4096 + 8193 * 8192 + if object { 8192 * 128 } else { 0 }
            )
        );
        assert!(
            observe(&input, false, 0, usize::MAX, 32 << 10)
                .0
                .unwrap_err()
                .contains("reservation denied")
        );
    }
    deep_unwind(&metadata);
    cancelled(&deep, &nested(leaf, 8192, 2), &metadata);
}

fn cancelled(physical: &ArrayRef, typed: &ArrayRef, metadata: &[u8]) {
    let session = ResidentVortexSession::new(256 << 20, 1).unwrap();
    let token = CancellationToken::default();
    let result = session.with_native_execution_context(&token, |context| {
        let before = context.memory().snapshot().reserved_bytes;
        let mut credit = context.memory().reserve(4096)?;
        let mut execution = context.native_session().create_execution_ctx();
        token.cancel();
        for error in [
            walk::physical(physical, &mut credit, context, 0, usize::MAX).unwrap_err(),
            walk::typed_value(
                typed,
                0,
                metadata,
                &mut credit,
                context,
                &mut execution,
                0,
                usize::MAX,
            )
            .unwrap_err(),
            walk::expanded(&Variant::Null, &mut credit, context).unwrap_err(),
        ] {
            assert!(error.to_string().contains("cancelled"));
        }
        assert_eq!(credit.bytes(), 4096);
        drop(credit);
        assert_eq!(context.memory().snapshot().reserved_bytes, before);
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn deep_unwind(metadata: &[u8]) {
    let mut bytes = vec![11 << 2];
    bytes.extend_from_slice(&i32::MAX.to_le_bytes());
    let physical = wrappers(raw_parts(metadata.to_vec(), bytes.clone(), None), 8192);
    let raw = record(&["value"], vec![binary(bytes)], Validity::NonNullable);
    let typed = nested(record(&["x"], vec![raw], Validity::NonNullable), 8191, 2);
    for input in [Input::Physical(&physical), Input::Typed(&typed, metadata)] {
        let session = ResidentVortexSession::new(256 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let before = context.memory().snapshot().reserved_bytes;
                let mut credit = context.memory().reserve(4096)?;
                let mut execution = context.native_session().create_execution_ctx();
                let panic = catch_unwind(AssertUnwindSafe(|| match input {
                    Input::Physical(array) => {
                        walk::physical(array, &mut credit, context, 0, usize::MAX)
                    }
                    Input::Typed(array, metadata) => walk::typed_value(
                        array,
                        0,
                        metadata,
                        &mut credit,
                        context,
                        &mut execution,
                        0,
                        usize::MAX,
                    ),
                    Input::Expanded(_) => unreachable!(),
                }));
                assert!(panic.is_err());
                assert_eq!(
                    context.memory().snapshot().reserved_bytes,
                    before + credit.bytes()
                );
                drop(credit);
                assert_eq!(context.memory().snapshot().reserved_bytes, before);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
}

#[test]
fn variant_workspace_continuations_deep_costs_denial_and_unwind_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_DEEP_VARIANT_WORKSPACE_CHILD";
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
                "::variant_workspace_continuations_deep_costs_denial_and_unwind_on_small_stack"
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
