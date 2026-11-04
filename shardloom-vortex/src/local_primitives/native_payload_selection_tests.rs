//! Shared batch coordinates retain the original field and selected-value errors.

use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{BoolArray, VarBinViewArray},
    dtype::FieldNames,
};

fn compare(source: &ArrayRef, positions: &[Option<u64>], dtype: &DType) {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let indices = PrimitiveArray::from_option_iter(positions.iter().copied()).into_array();
            let expected = validate_selection(source, &indices, dtype, context)
                .map_err(|error| error.to_string());
            assert_eq!(
                expected,
                take(source, &indices, dtype, context)
                    .map(drop)
                    .map_err(|error| error.to_string())
            );
            let selection = Selection::new(&indices, source.len(), context)?;
            let actual = validate_prepared_selection(source, &selection, dtype, context)
                .map_err(|error| error.to_string());
            assert_eq!(actual, expected);
            assert_eq!(
                take_prepared_selection(
                    source,
                    &selection,
                    dtype,
                    CopyPolicy::ValidateValues,
                    context
                )
                .map(drop)
                .map_err(|error| error.to_string()),
                expected
            );
            drop(selection);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_selection_preserves_coordinates_types_nulls_and_first_errors() {
    let columns = [
        PrimitiveArray::from_iter([0_i64, 7, -2]).into_array(),
        PrimitiveArray::from_option_iter([Some(0_u32), None, Some(2)]).into_array(),
        PrimitiveArray::from_iter([0.0_f64, f64::NAN, 2.0]).into_array(),
        BoolArray::from_iter([true, false, true]).into_array(),
        VarBinViewArray::from_iter_str(["a", "", "longer text"]).into_array(),
        VarBinViewArray::from_iter(
            [Some("a"), None, Some("b")],
            DType::Utf8(Nullability::Nullable),
        )
        .into_array(),
    ];
    for source in &columns {
        for rows in [
            vec![],
            vec![Some(0)],
            vec![Some(2), Some(0), Some(2)],
            vec![None, Some(0)],
            vec![Some(0), Some(1)],
            vec![None, Some(8)],
            vec![Some(8), None],
            vec![Some(u64::MAX)],
        ] {
            compare(source, &rows, source.dtype());
            compare(source, &rows, &source.dtype().as_nullable());
            compare(source, &rows, &source.dtype().as_nonnullable());
        }
        compare(
            source,
            &[None, Some(8)],
            &DType::Binary(Nullability::NonNullable),
        );
    }
}

#[test]
fn prepared_selection_keeps_recursive_null_parent_masks() {
    let source = StructArray::try_new(
        FieldNames::from(["hidden", "value"]),
        vec![
            PrimitiveArray::from_iter([f64::NAN, 1.0, 2.0]).into_array(),
            PrimitiveArray::from_iter([11_i64, 12, 13]).into_array(),
        ],
        3,
        Validity::Array(BoolArray::from_iter([false, true, true]).into_array()),
    )
    .unwrap()
    .into_array();
    for rows in [
        vec![Some(0), Some(1)],
        vec![None, Some(0), Some(2)],
        vec![Some(2), Some(0), Some(2)],
        vec![None, None],
    ] {
        compare(&source, &rows, source.dtype());
    }
}

#[test]
fn prepared_selection_preserves_text_sizing_errors_before_observed_null_errors() {
    use vortex::array::{
        ArrayParts,
        arrays::{MaskedArray, VarBin, VarBinArray},
    };

    let binary = VarBinArray::from(vec![&b"hidden"[..], &b"\xff"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let text = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let source = MaskedArray::try_new(
        text,
        Validity::from_iter([false, true]),
        &mut vortex::array::array_session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    compare(
        &source,
        &[Some(0), Some(1)],
        &DType::Utf8(Nullability::NonNullable),
    );
}

#[test]
fn prepared_selection_reuses_one_native_domain_across_hidden_fields() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let before = context.memory().snapshot().reserved_bytes;
            let integer = PrimitiveArray::from_iter([1_i64, 2, 3]).into_array();
            let text = VarBinViewArray::from_iter_str(["first", "second", "third"]).into_array();
            let indices = PrimitiveArray::from_iter([2_u64, 0, 2, 1]).into_array();
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            let selection = Selection::new(&indices, integer.len(), context)?;
            for source in [&integer, &text] {
                validate_prepared_selection(source, &selection, source.dtype(), context)?;
            }
            let mut proof = String::new();
            context.append_payload_work_certificate(&mut proof)?;
            for counter in [
                "payload_prepared_selections=1",
                "payload_prepared_coordinates=4",
                "payload_scalar_coordinates=0",
                "payload_rebuilt_selections=0",
                "payload_typed_validation_columns=2",
                "payload_typed_validation_rows=8",
            ] {
                assert!(
                    proof.split_whitespace().any(|field| field == counter),
                    "{proof}"
                );
            }
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.payload_selection_domains, 1);
                assert_eq!(counts.payload_selection_coordinates, 4);
                assert_eq!(counts.payload_selection_scalar_coordinates, 0);
                assert_eq!(counts.payload_selection_arrays, 0);
            }
            drop(selection);
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn omitted_missing_columns_validate_declarations_without_output_buffers() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let nested = DType::Struct(
                vortex::array::dtype::StructFields::new(
                    FieldNames::from(["value"]),
                    vec![DType::Primitive(PType::F64, Nullability::NonNullable)],
                ),
                Nullability::Nullable,
            );
            for dtype in [
                DType::Primitive(PType::I64, Nullability::Nullable),
                DType::Primitive(PType::I64, Nullability::NonNullable),
                DType::Utf8(Nullability::Nullable),
                DType::Bool(Nullability::Nullable),
                nested,
            ] {
                for rows in [0, 3] {
                    let expected = missing_column(&dtype, rows, true, context)
                        .map(drop)
                        .map_err(|error| error.to_string());
                    let before = context.memory().snapshot().reserved_bytes;
                    let actual = missing_column(&dtype, rows, false, context)
                        .map(drop)
                        .map_err(|error| error.to_string());
                    assert_eq!(actual, expected);
                    assert_eq!(context.memory().snapshot().reserved_bytes, before);
                }
            }
            let before = context.memory().snapshot().reserved_bytes;
            let held = context.memory().reserve((4 << 20) - before)?;
            missing_column(
                &DType::Primitive(PType::F64, Nullability::Nullable),
                1_000_000,
                false,
                context,
            )?;
            assert_eq!(context.memory().snapshot().reserved_bytes, 4 << 20);
            drop(held);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
