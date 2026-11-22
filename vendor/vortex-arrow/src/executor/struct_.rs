// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::StructArray as ArrowStructArray;
use arrow_buffer::NullBuffer;
use arrow_schema::Field;
use arrow_schema::Fields;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::Chunked;
use vortex_array::arrays::ScalarFn;
use vortex_array::arrays::Struct;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::scalar_fn::ScalarFnArrayExt;
use vortex_array::arrays::struct_::StructDataParts;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldNames;
use vortex_array::matcher::Matcher;
use vortex_array::scalar_fn::fns::pack::Pack;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;

use crate::executor::ExportStep;
use crate::executor::ExportTask;
use crate::executor::infer_nearest_arrow_field;
use crate::executor::validity::to_arrow_null_buffer;
use crate::session::ArrowSessionExt;

/// Matches the encodings [`start_arrow_struct`] requires for export.
struct ArrowStructExportable;

impl Matcher for ArrowStructExportable {
    type Match<'a> = &'a ArrayRef;

    fn try_match(array: &ArrayRef) -> Option<Self::Match<'_>> {
        (array.is::<Struct>()
            || array.is::<Chunked>()
            || array
                .as_opt::<ScalarFn>()
                .is_some_and(|scalar_fn| scalar_fn.scalar_fn().as_opt::<Pack>().is_some()))
        .then_some(array)
    }
}

pub(super) enum StructExportFrame {
    Target {
        fields: Fields,
        vortex_fields: Vec<ArrayRef>,
        next_idx: usize,
        built: Vec<ArrowArrayRef>,
        null_buffer: Option<NullBuffer>,
        len: usize,
    },
    Inferred {
        names: FieldNames,
        vortex_fields: Vec<ArrayRef>,
        next_idx: usize,
        current_inferred: Field,
        built_fields: Vec<arrow_schema::FieldRef>,
        built_arrays: Vec<ArrowArrayRef>,
        null_buffer: Option<NullBuffer>,
        len: usize,
    },
}

pub(super) fn start_arrow_struct(
    array: ArrayRef,
    target_fields: Option<&Fields>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    let len = array.len();

    let array = array.execute_until::<ArrowStructExportable>(ctx)?;

    // If the array is chunked, then we invert the chunk-of-struct to struct-of-chunk.
    let array = match array.try_downcast::<Chunked>() {
        Ok(array) => {
            // NOTE(ngates): this currently uses the old into_canonical code path, but we should
            //  just call directly into the swizzle-chunks function.
            array.into_array().execute::<StructArray>(ctx)?.into_array()
        }
        Err(array) => array,
    };

    // Attempt to short-circuit if the array is already a Struct:
    let array = match array.try_downcast::<Struct>() {
        Ok(array) => {
            let StructDataParts {
                validity,
                fields,
                struct_fields,
                ..
            } = array.into_data_parts();
            let validity = to_arrow_null_buffer(validity, len, ctx)?;
            return start_from_fields(
                target_fields.ok_or_else(|| struct_fields.names().clone()),
                fields,
                validity,
                len,
                ctx,
            );
        }
        Err(array) => array,
    };

    // We can also short-circuit if the array is a `pack` scalar function:
    if let Some(array) = array.as_opt::<ScalarFn>()
        && let Some(_pack_options) = array.scalar_fn().as_opt::<Pack>()
    {
        let DType::Struct(struct_fields, _) = array.dtype() else {
            unreachable!("Pack must have Struct dtype");
        };
        return start_from_fields(
            target_fields.ok_or_else(|| struct_fields.names().clone()),
            array.children().to_vec(),
            None, // Pack is never null,
            len,
            ctx,
        );
    }

    // Otherwise, we fall back to executing to a StructArray.
    let array = if let Some(fields) = target_fields {
        let vx_fields = ctx.session().arrow().from_arrow_fields(fields)?;
        // We apply a cast to ensure we push down casting where possible into the struct fields.
        let dtype = DType::Struct(vx_fields, array.dtype().nullability());
        array.cast(dtype)?
    } else {
        array
    };

    let struct_array = array.execute::<StructArray>(ctx)?;
    let StructDataParts {
        validity,
        fields,
        struct_fields,
        ..
    } = struct_array.into_data_parts();

    let validity = to_arrow_null_buffer(validity, len, ctx)?;
    start_from_fields(
        target_fields.ok_or_else(|| struct_fields.names().clone()),
        fields,
        validity,
        len,
        ctx,
    )
}

fn start_from_fields(
    fields: Result<&Fields, FieldNames>,
    vortex_fields: Vec<ArrayRef>,
    null_buffer: Option<NullBuffer>,
    len: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    match fields {
        Ok(fields) => {
            vortex_ensure_eq!(
                vortex_fields.len(),
                fields.len(),
                "StructArray field count must match the target Arrow type"
            );
            let Some((first_field, first_vx)) = fields.first().zip(vortex_fields.first()) else {
                return Ok(ExportStep::Done(Arc::new(unsafe {
                    ArrowStructArray::new_unchecked_with_length(
                        fields.clone(),
                        Vec::new(),
                        null_buffer,
                        len,
                    )
                })));
            };
            let first_target = first_field.as_ref().clone();
            let first_array = first_vx.clone();
            let built = Vec::with_capacity(vortex_fields.len());
            Ok(ExportStep::Descend {
                frame: StructExportFrame::Target {
                    fields: fields.clone(),
                    vortex_fields,
                    next_idx: 0,
                    built,
                    null_buffer,
                    len,
                }
                .into(),
                task: ExportTask::WithField {
                    array: first_array,
                    target: Some(first_target),
                },
            })
        }
        Err(names) => {
            let Some((first_name, first_vx)) = names.get(0).zip(vortex_fields.first()) else {
                return Ok(ExportStep::Done(Arc::new(unsafe {
                    ArrowStructArray::new_unchecked_with_length(
                        Fields::empty(),
                        Vec::new(),
                        null_buffer,
                        len,
                    )
                })));
            };
            let current_inferred =
                infer_nearest_arrow_field(first_vx, first_name.as_ref(), ctx)?;
            let target = current_inferred
                .extension_type_name()
                .is_some()
                .then(|| current_inferred.clone());
            let first_array = first_vx.clone();
            let count = vortex_fields.len();
            Ok(ExportStep::Descend {
                frame: StructExportFrame::Inferred {
                    names,
                    vortex_fields,
                    next_idx: 0,
                    current_inferred,
                    built_fields: Vec::with_capacity(count),
                    built_arrays: Vec::with_capacity(count),
                    null_buffer,
                    len,
                }
                .into(),
                task: ExportTask::WithField {
                    array: first_array,
                    target,
                },
            })
        }
    }
}

pub(super) fn step_arrow_struct(
    frame: StructExportFrame,
    completed: ArrowArrayRef,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    match frame {
        StructExportFrame::Target {
            fields,
            vortex_fields,
            next_idx,
            mut built,
            null_buffer,
            len,
        } => {
            let field = &fields[next_idx];
            vortex_ensure!(
                field.is_nullable() || completed.null_count() == 0,
                "Cannot convert field '{}' to non-nullable Arrow field because it contains nulls",
                field.name()
            );
            built.push(completed);
            let next = next_idx + 1;
            if let (Some(next_field), Some(next_vx)) = (fields.get(next), vortex_fields.get(next))
            {
                let next_target = next_field.as_ref().clone();
                let next_array = next_vx.clone();
                return Ok(ExportStep::Descend {
                    frame: StructExportFrame::Target {
                        fields,
                        vortex_fields,
                        next_idx: next,
                        built,
                        null_buffer,
                        len,
                    }
                    .into(),
                    task: ExportTask::WithField {
                        array: next_array,
                        target: Some(next_target),
                    },
                });
            }
            Ok(ExportStep::Done(Arc::new(unsafe {
                ArrowStructArray::new_unchecked_with_length(fields, built, null_buffer, len)
            })))
        }
        StructExportFrame::Inferred {
            names,
            vortex_fields,
            next_idx,
            current_inferred,
            mut built_fields,
            mut built_arrays,
            null_buffer,
            len,
        } => {
            built_fields.push(Arc::new(
                Field::new(
                    names[next_idx].as_ref(),
                    completed.data_type().clone(),
                    vortex_fields[next_idx].dtype().is_nullable(),
                )
                .with_metadata(current_inferred.metadata().clone()),
            ));
            built_arrays.push(completed);
            let next = next_idx + 1;
            if let (Some(next_name), Some(next_vx)) = (names.get(next), vortex_fields.get(next)) {
                let next_inferred =
                    infer_nearest_arrow_field(next_vx, next_name.as_ref(), ctx)?;
                let target = next_inferred
                    .extension_type_name()
                    .is_some()
                    .then(|| next_inferred.clone());
                let next_array = next_vx.clone();
                return Ok(ExportStep::Descend {
                    frame: StructExportFrame::Inferred {
                        names,
                        vortex_fields,
                        next_idx: next,
                        current_inferred: next_inferred,
                        built_fields,
                        built_arrays,
                        null_buffer,
                        len,
                    }
                    .into(),
                    task: ExportTask::WithField {
                        array: next_array,
                        target,
                    },
                });
            }
            let arrow_fields = Fields::from(built_fields);
            Ok(ExportStep::Done(Arc::new(unsafe {
                ArrowStructArray::new_unchecked_with_length(
                    arrow_fields,
                    built_arrays,
                    null_buffer,
                    len,
                )
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::LazyLock;

    use arrays::varbinview::VarBinViewArray;
    use arrow_array::Array;
    use arrow_array::ArrayRef;
    use arrow_array::Int64Array;
    use arrow_array::PrimitiveArray as ArrowPrimitiveArray;
    use arrow_array::StringViewArray;
    use arrow_array::StructArray as ArrowStructArray;
    use arrow_array::types::Int32Type;
    use arrow_buffer::NullBuffer;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays;
    use vortex_array::arrays::BoolArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::StructArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::dtype::FieldNames;
    use vortex_array::scalar_fn::fns::mask::Mask;
    use vortex_array::validity::Validity;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::ArrowArrayExecutor;
    use crate::convert::from_arrow_dyn;
    use crate::dtype::to_data_type_naive;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    #[test]
    fn struct_nullable_non_null_to_arrow() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let xs = PrimitiveArray::new(buffer![0i64, 1, 2, 3, 4], Validity::AllValid);

        let struct_a = StructArray::try_new(
            FieldNames::from(["xs"]),
            vec![xs.into_array()],
            5,
            Validity::AllValid,
        )?;

        let fields = vec![Field::new("xs", DataType::Int64, false)];
        let arrow_dt = DataType::Struct(fields.into());

        let actual = struct_a
            .into_array()
            .execute_arrow(Some(&arrow_dt), &mut ctx)?;
        let expected = ArrowStructArray::new(
            vec![Field::new("xs", DataType::Int64, false)].into(),
            vec![Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4]))],
            None,
        );
        assert_eq!(actual.to_data(), expected.to_data());
        Ok(())
    }

    #[test]
    fn struct_nullable_with_nulls_to_arrow() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let xs =
            PrimitiveArray::from_option_iter(vec![Some(0_i64), Some(1), Some(2), None, Some(3)]);

        let struct_a = StructArray::try_new(
            FieldNames::from(["xs"]),
            vec![xs.into_array()],
            5,
            Validity::AllValid,
        )?;

        let fields = vec![Field::new("xs", DataType::Int64, false)];
        let arrow_dt = DataType::Struct(fields.into());

        let error = struct_a
            .into_array()
            .execute_arrow(Some(&arrow_dt), &mut ctx)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("non-nullable Arrow field because it contains nulls")
        );
        Ok(())
    }

    #[test]
    fn struct_to_arrow_with_schema_mismatch() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let xs = PrimitiveArray::new(buffer![0i64, 1, 2, 3, 4], Validity::AllValid);

        let struct_a = StructArray::try_new(
            FieldNames::from(["xs"]),
            vec![xs.into_array()],
            5,
            Validity::AllValid,
        )?;

        let fields = vec![
            Field::new("xs", DataType::Int8, false),
            Field::new("ys", DataType::Int64, false),
        ];
        let arrow_dt = DataType::Struct(fields.into());

        let err = struct_a
            .into_array()
            .execute_arrow(Some(&arrow_dt), &mut ctx)
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("StructArray field count must match the target Arrow type")
        );
        Ok(())
    }

    #[test]
    fn test_to_arrow() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = StructArray::from_fields(
            vec![
                (
                    "a",
                    PrimitiveArray::from_option_iter(vec![Some(1), None, Some(2)]).into_array(),
                ),
                (
                    "b",
                    VarBinViewArray::from_iter_str(vec!["a", "b", "c"]).into_array(),
                ),
            ]
            .as_slice(),
        )?;

        let arrow_array: ArrayRef = Arc::new(ArrowStructArray::try_from(vec![
            (
                "a",
                Arc::new(
                    ArrowPrimitiveArray::<Int32Type>::from_iter_values_with_nulls(
                        vec![1, 0, 2],
                        Some(NullBuffer::from(vec![true, false, true])),
                    ),
                ) as ArrayRef,
            ),
            (
                "b",
                Arc::new(StringViewArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ),
        ])?);

        let arrow_dtype = to_data_type_naive(array.dtype())?;
        assert_eq!(
            &array
                .into_array()
                .execute_arrow(Some(&arrow_dtype), &mut ctx)?,
            &arrow_array
        );
        Ok(())
    }

    #[test]
    fn mask_wrapped_struct_exports_via_struct_fast_path() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        // A struct behind a lazy `mask` scalar-fn nulling out row 1 — the shape a scan
        // produces when a row mask is applied to a top-level struct batch.
        let xs = PrimitiveArray::new(buffer![1i64, 2, 3], Validity::NonNullable);
        let struct_array = StructArray::try_new(
            FieldNames::from(["xs"]),
            vec![xs.into_array()],
            3,
            Validity::NonNullable,
        )?;
        let mask = BoolArray::from_iter([true, false, true]);
        let masked = Mask::try_new(struct_array.into_array(), mask.into_array())?.into_array();

        let arrow = masked.execute_arrow(None, &mut ctx)?;

        let arrow_struct = arrow
            .as_any()
            .downcast_ref::<ArrowStructArray>()
            .expect("struct array");
        assert_eq!(arrow_struct.len(), 3);
        assert!(!arrow_struct.is_null(0));
        assert!(arrow_struct.is_null(1));
        assert!(!arrow_struct.is_null(2));
        let xs_col = arrow_struct
            .column(0)
            .as_any()
            .downcast_ref::<ArrowPrimitiveArray<arrow_array::types::Int64Type>>()
            .expect("int64 column");
        assert_eq!(xs_col.values(), &[1, 2, 3]);
        Ok(())
    }

    #[test]
    fn to_arrow_with_non_nullable_fields() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let array = StructArray::from_fields(
            vec![
                (
                    "a",
                    PrimitiveArray::from_option_iter(vec![Some(1), None, Some(2)]).into_array(),
                ),
                (
                    "b",
                    VarBinViewArray::from_iter_str(vec!["a", "b", "c"]).into_array(),
                ),
            ]
            .as_slice(),
        )?;
        let original = array.clone();
        let orig_dtype = array.dtype().clone();
        let arrow_array = array.into_array().execute_arrow(None, &mut ctx)?;
        let from_arrow = from_arrow_dyn(arrow_array.as_ref(), false)?;
        assert_eq!(&orig_dtype, from_arrow.dtype());
        assert_arrays_eq!(from_arrow, original, &mut ctx);
        Ok(())
    }
}
