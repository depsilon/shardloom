//! Preserve a proven finite literal as a scalar, before array materialization.

use super::{
    ArrayRef, BinaryOp, DType, Expression, KeyColumn, Kind, NativeExecutionContext,
    NativeNumericOwner, PType, PValue, Result, ScalarValue, floating_binary, integer_binary, keys,
    result_batch, signed_result, unsigned_result, validate_float_scalar,
};
use vortex::array::{match_each_integer_ptype, memory::HostMemorySessionExt as _};

enum Operand {
    Integer(i128),
    Float(f64),
}

pub(in crate::local_primitives) fn evaluate(
    expression: &Expression,
    input: &ArrayRef,
    context: &NativeExecutionContext<'_>,
    completed: Option<&[ArrayRef]>,
    left: &Expression,
    op: BinaryOp,
    right: &Expression,
) -> Result<Option<ArrayRef>> {
    let Some((literal, array, literal_left)) = operands(left, right) else {
        return Ok(None);
    };
    let Some(value) = admit(literal, array, &expression.dtype, op) else {
        return Ok(None);
    };
    // The finite nonnull literal cannot fail value validation. Preparing only
    // the other child preserves its evaluation/reuse domain and first errors.
    // All decline decisions precede evaluation, so no child can run twice.
    let source = array.evaluate_in_scope(input, context, completed)?;
    let KeyColumn::Numeric(owner) = keys(&source, context)? else {
        return Err(super::super::failed(
            "bound numeric scalar operand changed its storage",
        ));
    };
    if source.dtype() != &array.dtype || owner.len() != input.len() {
        return Err(super::super::failed(
            "bound numeric scalar operand changed its domain",
        ));
    }
    #[cfg(all(test, debug_assertions))]
    super::super::super::native_relational_observation::record(|counts| {
        counts.expression_numeric_scalar_batches += 1;
    });
    let output = match value {
        Operand::Integer(value) => {
            integer(&owner, value, literal_left, op, &expression.dtype, context)
        }
        Operand::Float(value) => {
            floating(&owner, value, literal_left, op, &expression.dtype, context)
        }
    }?;
    context.check_cancelled()?;
    Ok(Some(output))
}

fn operands<'a>(
    left: &'a Expression,
    right: &'a Expression,
) -> Option<(&'a Expression, &'a Expression, bool)> {
    match (&left.kind, &right.kind) {
        (Kind::Literal(_), Kind::Literal(_)) => None,
        (Kind::Literal(_), _) => Some((left, right, true)),
        (_, Kind::Literal(_)) => Some((right, left, false)),
        _ => None,
    }
}

fn admit(literal: &Expression, array: &Expression, dtype: &DType, op: BinaryOp) -> Option<Operand> {
    let (
        Kind::Literal(scalar),
        DType::Primitive(literal_type, _),
        DType::Primitive(array_type, _),
        DType::Primitive(output_type, _),
    ) = (&literal.kind, &literal.dtype, &array.dtype, dtype)
    else {
        return None;
    };
    if literal.reuse_output.is_some()
        || scalar.dtype() != &literal.dtype
        || matches!(op, BinaryOp::And | BinaryOp::Or)
    {
        return None;
    }
    let ScalarValue::Primitive(value) = scalar.value()? else {
        return None;
    };
    if *output_type == PType::F64 && *array_type == PType::F64 {
        return match (literal_type, value) {
            (PType::F64, PValue::F64(value)) if value.is_finite() => Some(Operand::Float(*value)),
            _ => None,
        };
    }
    if !matches!(output_type, PType::I64 | PType::U64) || !array_type.is_int() {
        return None;
    }
    let value = match (literal_type, value) {
        (PType::I8, PValue::I8(value)) => i128::from(*value),
        (PType::I16, PValue::I16(value)) => i128::from(*value),
        (PType::I32, PValue::I32(value)) => i128::from(*value),
        (PType::I64, PValue::I64(value)) => i128::from(*value),
        (PType::U8, PValue::U8(value)) => i128::from(*value),
        (PType::U16, PValue::U16(value)) => i128::from(*value),
        (PType::U32, PValue::U32(value)) => i128::from(*value),
        (PType::U64, PValue::U64(value)) => i128::from(*value),
        _ => return None,
    };
    Some(Operand::Integer(value))
}

fn observe_row(row: usize, context: &NativeExecutionContext<'_>) -> Result<()> {
    if row.is_multiple_of(1024) {
        context.check_cancelled()?;
    }
    #[cfg(all(test, debug_assertions))]
    super::super::super::native_relational_observation::record(|counts| {
        counts.expression_numeric_scalar_rows += 1;
    });
    Ok(())
}

fn integer(
    owner: &NativeNumericOwner,
    literal: i128,
    literal_left: bool,
    op: BinaryOp,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let allocator = context.native_session().host_allocator();
    let all_valid = owner.all_valid();
    match_each_integer_ptype!(owner.ptype(), |T| {
        let values = owner.primitive().as_slice::<T>();
        let value = |row: usize| {
            observe_row(row, context)?;
            if !all_valid && !owner.validity().value(row) {
                return Ok(None);
            }
            let value = i128::from(values[row]);
            let (left, right) = if literal_left {
                (literal, value)
            } else {
                (value, literal)
            };
            integer_binary(left, op, right).map(Some)
        };
        if matches!(dtype, DType::Primitive(PType::U64, _)) {
            result_batch::build_numeric_column(dtype, owner.len(), &allocator, |row| {
                value(row)?.map(unsigned_result).transpose()
            })
        } else {
            result_batch::build_numeric_column(dtype, owner.len(), &allocator, |row| {
                value(row)?.map(signed_result).transpose()
            })
        }
    })
}

fn floating(
    owner: &NativeNumericOwner,
    literal: f64,
    literal_left: bool,
    op: BinaryOp,
    dtype: &DType,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let allocator = context.native_session().host_allocator();
    let values = owner.primitive().as_slice::<f64>();
    let all_valid = owner.all_valid();
    let policy = context.float_policy();
    result_batch::build_numeric_column(dtype, owner.len(), &allocator, |row| {
        observe_row(row, context)?;
        if !all_valid && !owner.validity().value(row) {
            return Ok(None);
        }
        let value = values[row];
        validate_float_scalar(value, policy)?;
        let (left, right) = if literal_left {
            (literal, value)
        } else {
            (value, literal)
        };
        floating_binary(left, op, right, policy).map(Some)
    })
}
