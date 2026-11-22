// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::RunArray;
use arrow_array::cast::AsArray;
use arrow_array::new_null_array;
use arrow_array::types::*;
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use arrow_schema::Field;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::Constant;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::RunEnd;
use vortex_array::arrays::runend::RunEndArrayExt;
use vortex_array::arrays::runend::RunEndArraySlotsExt;
use vortex_array::matcher::Matcher;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

use crate::executor::ExportStep;
use crate::executor::ExportTask;

/// Matches the encodings [`start_arrow_run_end`] requires for export.
struct ArrowRunEndExportable;

impl Matcher for ArrowRunEndExportable {
    type Match<'a> = &'a ArrayRef;

    fn try_match(array: &ArrayRef) -> Option<Self::Match<'_>> {
        (array.is::<RunEnd>() || array.is::<Constant>()).then_some(array)
    }
}

pub(super) enum RunEndExportFrame {
    HaveEnds {
        values: ArrayRef,
        ends_type: DataType,
        values_type: Field,
        offset: usize,
        length: usize,
    },
    HaveValues {
        ends_type: DataType,
        arrow_ends: ArrowArrayRef,
        offset: usize,
        length: usize,
    },
    ConstantValues {
        ends_type: DataType,
        len: usize,
    },
    CastValues {
        ends_type: DataType,
        values_type: Field,
    },
}

pub(super) fn start_arrow_run_end(
    array: ArrayRef,
    ends_type: &DataType,
    values_type: &Field,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ExportStep> {
    let array = array.execute_until::<ArrowRunEndExportable>(ctx)?;

    let array = match array.try_downcast::<Constant>() {
        Ok(constant) => {
            let len = constant.len();
            let scalar = constant.scalar();
            if scalar.is_null() || len == 0 {
                let ree_type = DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", ends_type.clone(), false)),
                    Arc::new(values_type.clone()),
                );
                return Ok(ExportStep::Done(new_null_array(&ree_type, len)));
            }
            let values_const = ConstantArray::new(scalar.clone(), 1).into_array();
            return Ok(ExportStep::Descend {
                frame: RunEndExportFrame::ConstantValues {
                    ends_type: ends_type.clone(),
                    len,
                }
                .into(),
                task: ExportTask::WithField {
                    array: values_const,
                    target: Some(values_type.clone()),
                },
            });
        }
        Err(array) => array,
    };

    let array = match array.try_downcast::<RunEnd>() {
        Ok(run_end) => {
            let length = run_end.len();
            let offset = run_end.offset();
            let ends = run_end.ends().clone();
            let values = run_end.values().clone();
            return Ok(ExportStep::Descend {
                frame: RunEndExportFrame::HaveEnds {
                    values,
                    ends_type: ends_type.clone(),
                    values_type: values_type.clone(),
                    offset,
                    length,
                }
                .into(),
                task: ExportTask::Naive {
                    array: ends,
                    data_type: Some(ends_type.clone()),
                },
            });
        }
        Err(array) => array,
    };

    Ok(ExportStep::Descend {
        frame: RunEndExportFrame::CastValues {
            ends_type: ends_type.clone(),
            values_type: values_type.clone(),
        }
        .into(),
        task: ExportTask::WithField {
            array,
            target: Some(values_type.clone()),
        },
    })
}

pub(super) fn step_arrow_run_end(
    frame: RunEndExportFrame,
    completed: ArrowArrayRef,
) -> VortexResult<ExportStep> {
    match frame {
        RunEndExportFrame::HaveEnds {
            values,
            ends_type,
            values_type,
            offset,
            length,
        } => Ok(ExportStep::Descend {
            frame: RunEndExportFrame::HaveValues {
                ends_type,
                arrow_ends: completed,
                offset,
                length,
            }
            .into(),
            task: ExportTask::WithField {
                array: values,
                target: Some(values_type),
            },
        }),
        RunEndExportFrame::HaveValues {
            ends_type,
            arrow_ends,
            offset,
            length,
        } => match ends_type {
            DataType::Int16 => {
                build_run_array::<Int16Type>(&arrow_ends, &completed, offset, length)
            }
            DataType::Int32 => {
                build_run_array::<Int32Type>(&arrow_ends, &completed, offset, length)
            }
            DataType::Int64 => {
                build_run_array::<Int64Type>(&arrow_ends, &completed, offset, length)
            }
            _ => vortex_bail!("Unsupported run-end index type: {:?}", ends_type),
        }
        .map(ExportStep::Done),
        RunEndExportFrame::ConstantValues { ends_type, len } => match ends_type {
            DataType::Int16 => build_constant_run_array::<Int16Type>(len, &completed),
            DataType::Int32 => build_constant_run_array::<Int32Type>(len, &completed),
            DataType::Int64 => build_constant_run_array::<Int64Type>(len, &completed),
            _ => vortex_bail!("Unsupported run-end index type: {:?}", ends_type),
        }
        .map(ExportStep::Done),
        RunEndExportFrame::CastValues {
            ends_type,
            values_type,
        } => {
            let ree_type = DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", ends_type, false)),
                Arc::new(values_type),
            );
            arrow_cast::cast(&completed, &ree_type)
                .map(ExportStep::Done)
                .map_err(VortexError::from)
        }
    }
}

fn build_run_array<R: RunEndIndexType>(
    ends: &ArrowArrayRef,
    values: &ArrowArrayRef,
    offset: usize,
    length: usize,
) -> VortexResult<ArrowArrayRef>
where
    R::Native: std::ops::Sub<Output = R::Native> + Ord,
{
    let offset_native = R::Native::from_usize(offset)
        .ok_or_else(|| vortex_err!("Offset {offset} exceeds run-end index capacity"))?;
    let length_native = R::Native::from_usize(length)
        .ok_or_else(|| vortex_err!("Length {length} exceeds run-end index capacity"))?;

    let ends_prim = ends.as_primitive::<R>();
    if offset == 0 && ends_prim.values().last() == Some(&length_native) {
        // Fast path: no trimming or adjustment needed.
        return Ok(Arc::new(RunArray::<R>::try_new(ends_prim, values)?) as ArrowArrayRef);
    }

    // Trim to only include runs covering the [offset, offset+length) range.
    // Runs beyond this would produce duplicate adjusted ends, violating
    // Arrow's strict-ordering requirement for RunArray.
    // Run ends are strictly increasing, so we can binary search.
    let num_runs = (ends_prim
        .values()
        .partition_point(|&e| e - offset_native < length_native)
        + 1)
    .min(ends_prim.len());

    let trimmed_ends = ends.slice(0, num_runs);
    let trimmed_values = values.slice(0, num_runs);

    let adjusted = trimmed_ends
        .as_primitive::<R>()
        .unary(|end| (end - offset_native).min(length_native));

    Ok(Arc::new(RunArray::<R>::try_new(&adjusted, &trimmed_values)?) as ArrowArrayRef)
}

fn build_constant_run_array<R: RunEndIndexType>(
    len: usize,
    values: &ArrowArrayRef,
) -> VortexResult<ArrowArrayRef> {
    let end = R::Native::from_usize(len)
        .ok_or_else(|| vortex_err!("Array length {len} exceeds run-end index capacity"))?;
    let run_ends = arrow_array::PrimitiveArray::<R>::from_value(end, 1);
    Ok(Arc::new(RunArray::<R>::try_new(&run_ends, values)?) as ArrowArrayRef)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::LazyLock;

    use arrow_array::Int16Array;
    use arrow_array::Int32Array;
    use arrow_array::Int64Array;
    use arrow_array::RunArray;
    use arrow_array::types::Int16Type;
    use arrow_array::types::Int32Type;
    use arrow_array::types::Int64Type;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::SliceArray;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability::Nullable;
    use vortex_array::dtype::PType;
    use vortex_array::scalar::Scalar;
    use vortex_array::validity::Validity;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_session::VortexSession;

    use crate::ArrowArrayExecutor;
    use crate::executor::run_end::ConstantArray;
    use crate::executor::run_end::RunEnd;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    fn ree_type(ends: DataType, values_dtype: DataType) -> DataType {
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", ends, false)),
            Arc::new(Field::new("values", values_dtype, true)),
        )
    }

    fn execute(
        array: vortex_array::ArrayRef,
        dt: &DataType,
    ) -> VortexResult<arrow_array::ArrayRef> {
        array.execute_arrow(Some(dt), &mut SESSION.create_execution_ctx())
    }

    fn constant_i32_with_i16_ends() -> arrow_array::ArrayRef {
        Arc::new(
            RunArray::<Int16Type>::try_new(
                &Int16Array::from(vec![5i16]),
                &Int32Array::from(vec![42]),
            )
            .expect("valid run-end test array"),
        ) as arrow_array::ArrayRef
    }

    fn constant_f64_with_i64_ends() -> arrow_array::ArrayRef {
        Arc::new(
            RunArray::<Int64Type>::try_new(
                &Int64Array::from(vec![7i64]),
                &arrow_array::Float64Array::from(vec![1.5]),
            )
            .expect("valid run-end test array"),
        ) as arrow_array::ArrayRef
    }

    #[rstest]
    #[case::i32_with_i16_ends(
        ConstantArray::new(Scalar::from(42i32), 5).into_array(),
        ree_type(DataType::Int16, DataType::Int32),
        constant_i32_with_i16_ends(),
    )]
    #[case::f64_with_i64_ends(
        ConstantArray::new(Scalar::from(1.5f64), 7).into_array(),
        ree_type(DataType::Int64, DataType::Float64),
        constant_f64_with_i64_ends(),
    )]
    #[case::null(
        ConstantArray::new(Scalar::null(DType::Primitive(PType::I32, Nullable)), 4).into_array(),
        ree_type(DataType::Int32, DataType::Int32),
        arrow_array::new_null_array(
            &ree_type(DataType::Int32, DataType::Int32),
            4,
        ),
    )]
    #[case::empty(
        ConstantArray::new(Scalar::from(42i32), 0).into_array(),
        ree_type(DataType::Int32, DataType::Int32),
        arrow_array::new_null_array(
            &ree_type(DataType::Int32, DataType::Int32),
            0,
        ),
    )]
    fn constant_to_ree(
        #[case] input: vortex_array::ArrayRef,
        #[case] target_type: DataType,
        #[case] expected: arrow_array::ArrayRef,
    ) -> VortexResult<()> {
        let result = execute(input, &target_type)?;
        assert_eq!(result.as_ref(), expected.as_ref());
        Ok(())
    }

    #[test]
    fn nested_wrappers_reveal_run_end_fast_path() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        // Runs [7, 7, 8, 8] behind two nested lazy Slice wrappers. A single execution step
        // only merges the slices, so the exporter must keep stepping to reveal the
        // RunEndArray underneath.
        let ends = PrimitiveArray::new(buffer![2u32, 4], Validity::NonNullable);
        let values = PrimitiveArray::new(buffer![7i64, 8], Validity::NonNullable);
        let values_ptr = values.as_slice::<i64>().as_ptr();
        let ree = RunEnd::try_new(ends.into_array(), values.into_array(), &mut ctx)?;
        let wrapped = SliceArray::new(SliceArray::new(ree.into_array(), 0..4).into_array(), 0..4)
            .into_array();

        let result = execute(wrapped, &ree_type(DataType::Int32, DataType::Int64))?;

        let ree_arrow = result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .ok_or_else(|| vortex_err!("expected Int32 run-end array"))?;
        assert_eq!(ree_arrow.run_ends().values(), &[2, 4]);

        // The RunEnd fast path exports the values buffer zero-copy; the flat-then-recode
        // fallback would materialize new buffers.
        let arrow_values = ree_arrow
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| vortex_err!("expected Int64 values"))?;
        assert_eq!(arrow_values.values().as_ptr(), values_ptr);
        let expected = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 4]),
            &Int64Array::from(vec![7, 8]),
        )?;
        assert_eq!(ree_arrow, &expected);
        Ok(())
    }

    #[test]
    fn primitive_to_ree() -> VortexResult<()> {
        let array = PrimitiveArray::from_iter(vec![10i32, 10, 20, 20, 20]).into_array();
        let target = ree_type(DataType::Int32, DataType::Int32);
        let result = execute(array, &target)?;

        let expected = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 5]),
            &Int32Array::from(vec![10, 20]),
        )?;
        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    /// Regression: build_run_array must trim excess trailing runs and
    /// respect the `length` parameter. This happens when a vortex
    /// RunEndArray is sliced to fewer rows than the physical run_ends cover.
    #[rstest]
    #[case::offset_zero(0, 5, &[3, 5], &[100, 200])]
    #[case::nonzero_offset(2, 3, &[1, 3], &[100, 200])]
    #[case::all_runs_needed_but_last_exceeds(0, 8, &[3, 5, 8], &[100, 200, 300])]
    fn build_run_array_trims_excess_runs(
        #[case] offset: usize,
        #[case] length: usize,
        #[case] expected_ends: &[i32],
        #[case] expected_values: &[i64],
    ) -> VortexResult<()> {
        // 3 runs covering 10 rows: [0..3), [3..5), [5..10)
        let ends: arrow_array::ArrayRef = Arc::new(Int32Array::from(vec![3i32, 5, 10]));
        let values: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![100i64, 200, 300]));

        let result = super::build_run_array::<Int32Type>(&ends, &values, offset, length)?;
        assert_eq!(result.len(), length);

        let ree = result
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .ok_or_else(|| vortex_err!("expected Int32 run-end array"))?;
        assert_eq!(ree.run_ends().values(), expected_ends);
        let values = ree
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| vortex_err!("expected Int64 values"))?;
        assert_eq!(values.values(), expected_values);
        Ok(())
    }
}
