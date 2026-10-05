//! Schema-bound literal conversion and arithmetic share the native expression
//! kernels. Primitive rewrites retain their established coercion contract.

use super::{
    DType, Result, StatValue, failed,
    values::{OwnedScalar, OwnedStat, from_stat},
};
use shardloom_core::{BinaryOp, ScalarValue};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::{DecimalDType, PType};

pub(super) use super::super::prepared_relational::{
    scalar_arithmetic_dtype as arithmetic_dtype, scalar_literal_dtype as literal_dtype,
};

pub(super) fn operator(value: &str) -> Result<BinaryOp> {
    match value.trim() {
        "+" => Ok(BinaryOp::Add),
        "-" => Ok(BinaryOp::Subtract),
        "*" => Ok(BinaryOp::Multiply),
        "/" => Ok(BinaryOp::Divide),
        "%" => Ok(BinaryOp::Remainder),
        _ => Err(failed("arithmetic rewrite requires +, -, * or /")),
    }
}

pub(super) fn coerce(
    target: &DType,
    value: &ScalarValue,
    memory: &LiveMemoryPool,
) -> Result<OwnedScalar> {
    let source = literal_dtype(value)?;
    if source == DType::Null {
        return OwnedScalar::copy(value, memory);
    }
    if matches!(
        target,
        DType::Bool(_) | DType::Utf8(_) | DType::Primitive(..)
    ) {
        let witness = match target {
            DType::Bool(_) => StatValue::Boolean(false),
            DType::Utf8(_) => StatValue::Utf8(String::new()),
            DType::Primitive(p, _) if p.is_signed_int() => StatValue::Int64(0),
            DType::Primitive(p, _) if p.is_unsigned_int() => StatValue::UInt64(0),
            DType::Primitive(PType::F32 | PType::F64, _) => StatValue::Float64(0.0),
            _ => return Err(failed("rewrite target requires an admitted scalar dtype")),
        };
        let literal = OwnedStat::from_scalar(value, memory)?;
        let bytes = if let StatValue::Utf8(text) = literal.value() {
            text.len()
        } else {
            0
        };
        return OwnedScalar::produce(memory, bytes, || {
            super::super::coerce_rewrite_value(&witness, literal.value()).map(from_stat)
        });
    }
    if matches!(target, DType::Decimal(..)) {
        if !matches!(
            source,
            DType::Decimal(..) | DType::Primitive(PType::I64 | PType::U64, _)
        ) {
            return Err(failed(
                "decimal rewrite literals require exact decimal or integer values",
            ));
        }
        let mut scratch = memory.reserve(0)?;
        let converted = super::super::native_relational_expression::scalar::cast(
            numeric_cell(value)?,
            target,
            false,
            &mut scratch,
        )?;
        return OwnedScalar::from_native(converted, target, memory);
    }
    if source.as_nonnullable() == target.as_nonnullable()
        && crate::native_payload_schema::admitted_scalar(target)
    {
        return OwnedScalar::copy(value, memory);
    }
    Err(failed(
        "rewrite literal type is incompatible with the declared target",
    ))
}

pub(super) fn numeric_cell(
    value: &ScalarValue,
) -> Result<super::super::native_relational_keys::Cell> {
    use super::super::native_relational_keys::Cell;
    Ok(match value {
        ScalarValue::Null => Cell::Null,
        ScalarValue::Int64(value) if *value < 0 => Cell::NegativeInteger(*value),
        ScalarValue::Int64(value) => Cell::NonnegativeInteger(value.unsigned_abs()),
        ScalarValue::UInt64(value) => Cell::NonnegativeInteger(*value),
        ScalarValue::Float64(value) => Cell::Float(value.to_bits()),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            shardloom_core::expression::Decimal128Operand::decimal(*value, *precision, *scale)?;
            Cell::Decimal(
                *value,
                DecimalDType::new(*precision, i8::try_from(*scale).expect("validated scale")),
            )
        }
        _ => return Err(failed("arithmetic requires an admitted numeric scalar")),
    })
}
