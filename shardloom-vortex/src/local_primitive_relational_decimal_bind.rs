//! Literal decimal result admission in the shared native expression binder.

use super::{
    BinaryOp, DType, DecimalDType, Expression, Function, Kind, Result, ScalarDefinition,
    decimal_operand, failed, function_nullability, literal_text,
};
use shardloom_core::decimal_output::{DecimalOutput, DecimalRounding};

pub(super) fn bind(definition: &ScalarDefinition, mut args: Vec<Expression>) -> Result<Expression> {
    let operation = match definition.name {
        "decimal_rescale" => None,
        "decimal_add" => Some(BinaryOp::Add),
        "decimal_subtract" => Some(BinaryOp::Subtract),
        "decimal_multiply" => Some(BinaryOp::Multiply),
        "decimal_divide" => Some(BinaryOp::Divide),
        _ => {
            return Err(failed(
                "decimal output requires an admitted scalar operation",
            ));
        }
    };
    let count = if operation.is_some() { 2 } else { 1 };
    let output = DecimalOutput::new(
        control(&args[count])?,
        control(&args[count + 1])?,
        DecimalRounding::parse(
            literal_text(&args[count + 2], "decimal rounding")?
                .ok_or_else(|| failed("decimal rounding requires a nonnull UTF8 literal"))?,
        )?,
    )?;
    args.truncate(count);
    for argument in &args {
        decimal_operand(0, &argument.dtype)?;
    }
    let nullable = function_nullability(definition.null_rule, &args);
    Ok(Expression::new(
        DType::Decimal(
            DecimalDType::new(
                output.precision(),
                i8::try_from(output.scale()).expect("validated scale"),
            ),
            nullable,
        ),
        Kind::Function {
            function: Function::Decimal { output, operation },
            args,
        },
    ))
}

fn control(argument: &Expression) -> Result<u8> {
    use vortex::array::scalar::{PValue, ScalarValue};
    let Kind::Literal(value) = &argument.kind else {
        return Err(failed(
            "decimal precision and scale require integer literals",
        ));
    };
    let Some(ScalarValue::Primitive(value)) = value.value() else {
        return Err(failed(
            "decimal precision and scale require nonnull integer literals",
        ));
    };
    let value = match value {
        PValue::I8(value) => i128::from(*value),
        PValue::I16(value) => i128::from(*value),
        PValue::I32(value) => i128::from(*value),
        PValue::I64(value) => i128::from(*value),
        PValue::U8(value) => i128::from(*value),
        PValue::U16(value) => i128::from(*value),
        PValue::U32(value) => i128::from(*value),
        PValue::U64(value) => i128::from(*value),
        _ => {
            return Err(failed(
                "decimal precision and scale require integer literals",
            ));
        }
    };
    u8::try_from(value)
        .map_err(|_| failed("decimal precision and scale require integer literals in 0..38"))
}
