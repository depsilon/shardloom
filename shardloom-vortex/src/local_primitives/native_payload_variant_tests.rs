use super::*;
use crate::{
    local_primitives::native_relational_batch::index_array, resident_session::ResidentVortexSession,
};
use parquet_variant::{Variant as PqValue, VariantBuilder};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        arrays::{ConstantArray, DictArray, VarBinArray},
        scalar::Scalar,
    },
    encodings::parquet_variant::{ParquetVariant, ParquetVariantArraySlotsExt as _},
};

fn raw(values: &[PqValue<'_, '_>], validity: Validity) -> ArrayRef {
    let (metadata, values): (Vec<_>, Vec<_>) = values
        .iter()
        .map(|value| VariantBuilder::new().with_value(value.clone()).finish())
        .unzip();
    ParquetVariant::try_new(
        validity,
        VarBinArray::from_vec(metadata, DType::Binary(Nullability::NonNullable)).into_array(),
        Some(VarBinArray::from_vec(values, DType::Binary(Nullability::NonNullable)).into_array()),
        None,
    )
    .unwrap()
    .into_array()
}

#[test]
fn native_variant_selection_preserves_bytes_nulls_and_last_owner_credit() {
    let source = raw(
        &[
            PqValue::Int64(i64::MIN),
            PqValue::Null,
            PqValue::from("last"),
        ],
        Validity::AllValid,
    );
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let retained = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(4, true, context, |row| {
                Ok([Some(2), None, Some(1), Some(2)][row])
            })?;
            let copied = take(&source, &indices, source.dtype(), context)?;
            assert_eq!(copied.dtype(), source.dtype());
            let storage = copied.as_::<ParquetVariant>();
            let original = source.as_::<ParquetVariant>();
            let mut execution = context.native_session().create_execution_ctx();
            assert!(copied.is_invalid(1, &mut execution).unwrap());
            assert!(copied.is_valid(2, &mut execution).unwrap());
            assert_eq!(
                copied
                    .execute_scalar(2, &mut execution)
                    .unwrap()
                    .as_variant()
                    .is_variant_null(),
                Some(true)
            );
            for row in [0, 3] {
                assert_eq!(
                    storage
                        .metadata()
                        .execute_scalar(row, &mut execution)
                        .unwrap(),
                    original
                        .metadata()
                        .execute_scalar(2, &mut execution)
                        .unwrap()
                );
                assert_eq!(
                    storage
                        .value()
                        .unwrap()
                        .execute_scalar(row, &mut execution)
                        .unwrap(),
                    original
                        .value()
                        .unwrap()
                        .execute_scalar(2, &mut execution)
                        .unwrap()
                );
            }
            let child = storage.value().unwrap().clone();
            drop(copied);
            Ok(child)
        })
        .unwrap();
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(retained);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_variant_canonical_conflicts_denial_and_cancellation_release_partial_owners() {
    use vortex::array::{arrays::VariantArray, dtype::FieldNames};
    let mut builder = VariantBuilder::new();
    let mut object = builder.new_object();
    object.insert("score", PqValue::Int64(7));
    object.finish();
    let (metadata, value) = builder.finish();
    let core = ParquetVariant::try_new(
        Validity::AllValid,
        VarBinArray::from_vec(vec![metadata], DType::Binary(Nullability::NonNullable)).into_array(),
        Some(
            VarBinArray::from_vec(vec![value], DType::Binary(Nullability::NonNullable))
                .into_array(),
        ),
        None,
    )
    .unwrap()
    .into_array();
    let typed = StructArray::new(
        FieldNames::from(["score"]),
        vec![PrimitiveArray::from_iter([1_i64]).into_array()],
        1,
        Validity::NonNullable,
    )
    .into_array();
    let overlapping = VariantArray::try_new(core, Some(typed))
        .unwrap()
        .into_array();
    let conflicting = VariantArray::try_new(
        raw(&[PqValue::Int64(1)], Validity::AllValid),
        Some(PrimitiveArray::from_iter([2_i64]).into_array()),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let cancellation = CancellationToken::default();
    let cancelled = session
        .with_native_execution_context(&cancellation, |context| {
            let indices = index_array(1, false, context, |_| Ok(Some(0)))?;
            for (source, reason) in [(&overlapping, "overlap"), (&conflicting, "conflicting")] {
                let before = context.memory().snapshot().reserved_bytes;
                for policy in [CopyPolicy::ValidateValues, CopyPolicy::PreserveUnobserved] {
                    let failure =
                        take_with_policy(source, &indices, source.dtype(), policy, context)
                            .unwrap_err();
                    assert!(failure.to_string().contains(reason), "{failure}");
                    assert_eq!(context.memory().snapshot().reserved_bytes, before);
                }
            }
            let source = raw(&[PqValue::from("retained")], Validity::AllValid);
            let retained = take(&source, &indices, source.dtype(), context)?;
            let snapshot = context.memory().snapshot();
            let blocker = context
                .memory()
                .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)?;
            let before = context.memory().snapshot().reserved_bytes;
            let error = take(&source, &indices, source.dtype(), context).unwrap_err();
            assert!(error.to_string().contains("reservation denied"), "{error}");
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            drop(blocker);
            let before = context.memory().snapshot().reserved_bytes;
            cancellation.cancel();
            assert!(take(&source, &indices, source.dtype(), context).is_err());
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            drop(retained);
            Ok(())
        })
        .unwrap_err();
    assert!(cancelled.to_string().contains("cancelled"));
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_variant_dictionary_chunks_and_constant_null_keep_selected_coordinates() {
    let source = raw(
        &[PqValue::from("first"), PqValue::from("last")],
        Validity::AllValid,
    );
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter([Some(1_u32), None, Some(0)]).into_array(),
        source.clone(),
    )
    .unwrap()
    .into_array();
    let chunks = ChunkedArray::try_new(
        vec![source, dictionary],
        DType::Variant(Nullability::Nullable),
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = index_array(4, true, context, |row| {
                Ok([Some(4), Some(0), Some(3), None][row])
            })?;
            let output = take(&chunks, &indices, chunks.dtype(), context)?;
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..2 {
                assert_eq!(
                    output
                        .execute_scalar(row, &mut execution)
                        .unwrap()
                        .as_variant()
                        .value()
                        .unwrap()
                        .as_utf8()
                        .value()
                        .unwrap()
                        .as_str(),
                    "first"
                );
            }
            assert!(output.is_invalid(2, &mut execution).unwrap());
            assert!(output.is_invalid(3, &mut execution).unwrap());
            let constant =
                ConstantArray::new(Scalar::variant(Scalar::null(DType::Null)), 100).into_array();
            let indices = index_array(2, true, context, |row| Ok([Some(99), None][row]))?;
            let output = take(
                &constant,
                &indices,
                &DType::Variant(Nullability::Nullable),
                context,
            )?;
            assert_eq!(
                output
                    .execute_scalar(0, &mut execution)
                    .unwrap()
                    .as_variant()
                    .is_variant_null(),
                Some(true)
            );
            assert!(output.is_invalid(1, &mut execution).unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn native_variant_malformed_selected_bytes_fail_without_observing_unselected_values() {
    let source = ParquetVariant::try_new(
        Validity::NonNullable,
        VarBinArray::from_bytes(vec![&[1, 0, 0], &[1, 0, 0]]).into_array(),
        Some(VarBinArray::from_bytes(vec![&[0], &[255]]).into_array()),
        None,
    )
    .unwrap()
    .into_array();
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let good = index_array(1, false, context, |_| Ok(Some(0)))?;
            drop(take(&source, &good, source.dtype(), context)?);
            let bad = index_array(1, false, context, |_| Ok(Some(1)))?;
            let before = context.memory().snapshot().reserved_bytes;
            assert!(take(&source, &bad, source.dtype(), context).is_err());
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            assert!(
                take_with_policy(
                    &source,
                    &bad,
                    source.dtype(),
                    CopyPolicy::PreserveUnobserved,
                    context
                )
                .is_err()
            );
            assert_eq!(context.memory().snapshot().reserved_bytes, before);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
#[cfg(feature = "universal-format-io")]
fn native_variant_arrow_export_terminates_for_chunked_and_canonical_cores() {
    use crate::native_arrow_conversion::{ConversionLimits, canonical};
    use arrow_array::{Array as _, cast::AsArray as _};
    use vortex::{array::arrays::VariantArray, arrow::ArrowSessionExt as _};
    let first = raw(
        &[PqValue::Int64(i64::MIN), PqValue::Null],
        Validity::AllValid,
    );
    let second = raw(&[PqValue::from("last")], Validity::AllValid);
    let chunks = ChunkedArray::try_new(vec![first, second], DType::Variant(Nullability::Nullable))
        .unwrap()
        .into_array();
    let wrapped = VariantArray::try_new(chunks.clone(), None)
        .unwrap()
        .into_array();
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let limits = ConversionLimits {
                string_bytes: usize::MAX,
                arrow_batch_bytes: 32 << 20,
                check: &|| Ok(()),
            };
            for source in [chunks, wrapped] {
                let mut expanded = 0;
                let prepared = canonical(&source, &mut execution, &limits, &mut expanded)?;
                assert!(expanded > 0);
                let target = crate::native_variant::arrow::field("payload", true);
                let output = context
                    .native_session()
                    .arrow()
                    .execute_arrow(prepared, Some(&target), &mut execution)
                    .map_err(vortex_error)?;
                assert_eq!(output.len(), 3);
                let payload = output.as_struct();
                for (row, expected) in [
                    PqValue::Int64(i64::MIN),
                    PqValue::Null,
                    PqValue::from("last"),
                ]
                .iter()
                .enumerate()
                {
                    let metadata = payload.column(0).as_binary::<i32>().value(row);
                    let value = payload.column(1).as_binary::<i32>().value(row);
                    assert_eq!(PqValue::try_new(metadata, value).unwrap(), *expected);
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
#[cfg(feature = "universal-format-io")]
#[allow(clippy::too_many_lines)]
fn native_variant_canonical_shredding_keeps_native_selection_and_export_values() {
    use arrow_array::{Array as _, cast::AsArray as _};
    use vortex::{
        array::{Canonical, arrays::ListArray, dtype::FieldNames},
        arrow::ArrowSessionExt as _,
    };
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let shell = StructArray::new(
                FieldNames::from(["typed_value"]),
                vec![PrimitiveArray::from_iter([1_i64, -7]).into_array()],
                2,
                Validity::NonNullable,
            )
            .into_array();
            let object = StructArray::new(
                FieldNames::from(["score"]),
                vec![shell.clone()],
                2,
                Validity::NonNullable,
            )
            .into_array();
            let list = ListArray::try_new(
                shell,
                PrimitiveArray::from_iter([0_u64, 2, 2]).into_array(),
                Validity::NonNullable,
                &mut execution,
            )
            .unwrap()
            .into_array();
            let (metadata, _) = VariantBuilder::new()
                .with_field_names(["score"])
                .with_value(PqValue::Null)
                .finish();
            for (typed, is_list) in [(object, false), (list, true)] {
                let source = ParquetVariant::try_new(
                    Validity::AllValid,
                    VarBinArray::from_vec(
                        vec![metadata.clone(); 2],
                        DType::Binary(Nullability::NonNullable),
                    )
                    .into_array(),
                    None,
                    Some(typed),
                )
                .unwrap()
                .into_array();
                let source = source
                    .execute::<Canonical>(&mut execution)
                    .unwrap()
                    .into_array();
                let indices = index_array(4, true, context, |row| {
                    Ok([Some(1), Some(0), None, Some(0)][row])
                })?;
                let copied = take(&source, &indices, source.dtype(), context)?;
                let mut expanded = 0;
                let prepared = crate::native_arrow_conversion::canonical(
                    &copied,
                    &mut execution,
                    &crate::native_arrow_conversion::ConversionLimits {
                        string_bytes: usize::MAX,
                        arrow_batch_bytes: 32 << 20,
                        check: &|| Ok(()),
                    },
                    &mut expanded,
                )?;
                let output = context
                    .native_session()
                    .arrow()
                    .execute_arrow(
                        prepared,
                        Some(&crate::native_variant::arrow::field("payload", true)),
                        &mut execution,
                    )
                    .map_err(vortex_error)?;
                let payload = output.as_struct();
                assert!(payload.is_null(2));
                for (row, original) in [(0, 1), (1, 0), (3, 0)] {
                    let value = PqValue::try_new(
                        payload.column(0).as_binary::<i32>().value(row),
                        payload.column(1).as_binary::<i32>().value(row),
                    )
                    .unwrap();
                    if is_list {
                        assert_eq!(
                            value.as_list().unwrap().iter().collect::<Vec<_>>(),
                            if original == 0 {
                                vec![PqValue::Int64(1), PqValue::Int64(-7)]
                            } else {
                                vec![]
                            }
                        );
                    } else {
                        assert_eq!(
                            value.as_object().unwrap().get("score"),
                            Some(PqValue::Int64(if original == 0 { 1 } else { -7 }))
                        );
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}
