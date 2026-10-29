//! Typed construction after the existing ordered numeric preparation. Vortex
//! binary arithmetic has different strict-value and diagnostic semantics;
//! these loops share `ShardLoom`'s existing arithmetic and native output owners.

use super::{
    ArrayRef, BinaryOp, DType, Expression, KeyColumn, Kind, NativeExecutionContext, PType, Result,
    floating_binary, integer_binary, keys, result_batch, signed_result, unsigned_result,
};
use crate::local_primitives::{
    native_numeric_owner::NativeNumericOwner, native_relational_keys::validate_finite_scalar,
};
use vortex::array::{
    match_each_integer_ptype,
    memory::HostMemorySessionExt as _,
    scalar::{PValue, ScalarValue},
};

#[cfg(test)]
#[path = "native_relational_expression_numeric_tests.rs"]
mod tests;

pub(super) fn binary(
    left: &KeyColumn,
    op: BinaryOp,
    right: &KeyColumn,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ArrayRef>> {
    let (KeyColumn::Numeric(left), KeyColumn::Numeric(right), DType::Primitive(ptype, _)) =
        (left, right, dtype)
    else {
        return Ok(None);
    };
    if !matches!(ptype, PType::I64 | PType::U64 | PType::F64)
        || matches!(op, BinaryOp::And | BinaryOp::Or)
        || left.len() != right.len()
    {
        return Ok(None);
    }
    if left.ptype() != *ptype || right.ptype() != *ptype {
        return if ptype.is_int() && left.is_integer() && right.is_integer() {
            mixed_integer_binary(left, op, right, dtype, context).map(Some)
        } else {
            Ok(None)
        };
    }
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.expression_numeric_binary_batches += 1;
    });
    let allocator = context.native_session().host_allocator();
    macro_rules! build {
        ($native:ty, $calculate:expr, $validate:expr) => {{
            let left_values = left.primitive().as_slice::<$native>();
            let right_values = right.primitive().as_slice::<$native>();
            let left_valid = left.validity();
            let right_valid = right.validity();
            let all_valid = left.all_valid() && right.all_valid();
            result_batch::build_numeric_column(dtype, left.len(), &allocator, |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                #[cfg(all(test, debug_assertions))]
                super::super::native_relational_observation::record(|counts| {
                    counts.expression_numeric_binary_rows += 1;
                });
                let (left_present, right_present) = if all_valid {
                    (true, true)
                } else {
                    (left_valid.value(row), right_valid.value(row))
                };
                // raw_cell observes each present operand before binary applies
                // NULL propagation, even when the other operand is NULL.
                if left_present {
                    ($validate)(left_values[row])?;
                }
                if right_present {
                    ($validate)(right_values[row])?;
                }
                if left_present && right_present {
                    ($calculate)(left_values[row], right_values[row]).map(Some)
                } else {
                    Ok(None)
                }
            })
        }};
    }
    let output = match ptype {
        PType::I64 => build!(
            i64,
            |left, right| signed_result(integer_binary(i128::from(left), op, i128::from(right))?),
            |_| Ok::<(), shardloom_core::ShardLoomError>(())
        ),
        PType::U64 => build!(
            u64,
            |left, right| unsigned_result(integer_binary(i128::from(left), op, i128::from(right))?),
            |_| Ok::<(), shardloom_core::ShardLoomError>(())
        ),
        PType::F64 => build!(
            f64,
            |left, right| floating_binary(left, op, right),
            validate_finite_scalar
        ),
        _ => unreachable!("physical type admitted above"),
    }?;
    context.check_cancelled()?;
    Ok(Some(output))
}

/// Dispatch borrowed original-width integer slices once per batch. The existing
/// scalar arithmetic widens to i128 before the bound result conversion; widening
/// only in registers preserves that contract without per-row Cell/Value owners
/// or an additional cast buffer. Integer payloads have no nonfinite validation,
/// so absent operands may be skipped after the same ordered input preparation.
fn mixed_integer_binary(
    left: &NativeNumericOwner,
    op: BinaryOp,
    right: &NativeNumericOwner,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.expression_numeric_binary_batches += 1;
    });
    let allocator = context.native_session().host_allocator();
    let all_valid = left.all_valid() && right.all_valid();
    let output = match_each_integer_ptype!(left.ptype(), |L| {
        let left_values = left.primitive().as_slice::<L>();
        match_each_integer_ptype!(right.ptype(), |R| {
            let right_values = right.primitive().as_slice::<R>();
            let value = |row: usize| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                #[cfg(all(test, debug_assertions))]
                super::super::native_relational_observation::record(|counts| {
                    counts.expression_numeric_binary_rows += 1;
                });
                if all_valid || (left.validity().value(row) && right.validity().value(row)) {
                    integer_binary(
                        i128::from(left_values[row]),
                        op,
                        i128::from(right_values[row]),
                    )
                    .map(Some)
                } else {
                    Ok(None)
                }
            };
            if matches!(dtype, DType::Primitive(PType::U64, _)) {
                result_batch::build_numeric_column(dtype, left.len(), &allocator, |row| {
                    value(row)?.map(unsigned_result).transpose()
                })
            } else {
                result_batch::build_numeric_column(dtype, left.len(), &allocator, |row| {
                    value(row)?.map(signed_result).transpose()
                })
            }
        })
    })?;
    context.check_cancelled()?;
    Ok(output)
}

pub(super) fn coalesce(
    expression: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    children: &[Expression],
) -> Result<Option<ArrayRef>> {
    let [column, literal] = children else {
        return Ok(None);
    };
    let (Kind::Column(_), Kind::Literal(scalar), DType::Primitive(ptype, _)) =
        (&column.kind, &literal.kind, &expression.dtype)
    else {
        return Ok(None);
    };
    if input.is_empty()
        || !matches!(ptype, PType::I64 | PType::U64 | PType::F64)
        || !expression.dtype.eq_ignore_nullability(&column.dtype)
        || !expression.dtype.eq_ignore_nullability(&literal.dtype)
        || scalar.dtype() != &literal.dtype
    {
        return Ok(None);
    }
    let Some(ScalarValue::Primitive(value)) = scalar.value() else {
        return Ok(None);
    };
    if !matches!(
        (ptype, value),
        (PType::I64, PValue::I64(_)) | (PType::U64, PValue::U64(_))
    ) && !matches!((ptype, value), (PType::F64, PValue::F64(value)) if value.is_finite())
    {
        return Ok(None);
    }
    // Only the admitted column is evaluated. The fallback literal is finite,
    // non-null and has no evaluation effects; all other lazy forms stay in the
    // general owner and still run solely on their selected rows.
    let array = column.evaluate(input, context)?;
    let KeyColumn::Numeric(owner) = keys(&array, context)? else {
        return Err(super::failed("bound numeric coalesce changed its storage"));
    };
    if owner.ptype() != *ptype {
        return Err(super::failed(
            "bound numeric coalesce changed its physical type",
        ));
    }
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.expression_numeric_coalesce_batches += 1;
    });
    let allocator = context.native_session().host_allocator();
    macro_rules! build {
        ($native:ty, $literal:expr, $validate:expr) => {{
            let values = owner.primitive().as_slice::<$native>();
            let valid = owner.validity();
            result_batch::build_numeric_column(&expression.dtype, owner.len(), &allocator, |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                #[cfg(all(test, debug_assertions))]
                super::super::native_relational_observation::record(|counts| {
                    counts.expression_numeric_coalesce_rows += 1;
                });
                let value = if valid.value(row) {
                    values[row]
                } else {
                    $literal
                };
                ($validate)(value)?;
                Ok(Some(value))
            })
        }};
    }
    let output = match value {
        PValue::I64(value) => build!(
            i64,
            *value,
            |_| Ok::<(), shardloom_core::ShardLoomError>(())
        ),
        PValue::U64(value) => build!(
            u64,
            *value,
            |_| Ok::<(), shardloom_core::ShardLoomError>(())
        ),
        PValue::F64(value) => build!(f64, *value, result_batch::validate_finite_numeric_output),
        _ => unreachable!("literal physical type admitted above"),
    }?;
    context.check_cancelled()?;
    Ok(Some(output))
}
