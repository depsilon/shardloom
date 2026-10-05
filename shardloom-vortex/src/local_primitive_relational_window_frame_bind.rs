//! Bind frame shape, native value types and literal offsets before reading rows.

use super::{DType, Result, failed, field, key, validate_key, validate_name};
use crate::{
    local_primitives::{
        SimpleAggregateFunction as Aggregate, native_relational_window as window,
        native_relational_window_frame as frame,
    },
    relational_query::{
        VortexRelationalFrameBound as Bound, VortexRelationalFrameFunction as Function,
        VortexRelationalFrameOffset as Offset, VortexRelationalFrameUnit as Unit,
        VortexRelationalWindowExpression, VortexRelationalWindowFrame,
    },
};
use shardloom_core::ScalarValue;
use vortex::array::dtype::PType;

pub(super) fn bind(
    declaration: &Function,
    expression: &VortexRelationalWindowExpression,
    fields: &[(String, DType)],
    window: &mut window::Spec,
) -> Result<(frame::Spec, DType)> {
    let column = declaration.column().map(shardloom_core::ColumnRef::as_str);
    let source = column
        .map(|name| {
            validate_name(name)?;
            field(fields, name)
        })
        .transpose()?;
    let function = match declaration {
        Function::CountAll | Function::Count(_) => frame::Function::Aggregate(Aggregate::Count),
        Function::CountDistinct(_) => frame::Function::Aggregate(Aggregate::CountDistinct),
        Function::Sum(_) => frame::Function::Aggregate(Aggregate::Sum),
        Function::Avg(_) => frame::Function::Aggregate(Aggregate::Avg),
        Function::Min(_) => frame::Function::Aggregate(Aggregate::Min),
        Function::Max(_) => frame::Function::Aggregate(Aggregate::Max),
        Function::FirstValue(_) => frame::Function::First,
        Function::LastValue(_) => frame::Function::Last,
        Function::NthValue { index, .. } => {
            if *index == 0 {
                return Err(failed("NTH_VALUE position must be positive"));
            }
            frame::Function::Nth(
                usize::try_from(*index)
                    .map_err(|_| failed("NTH_VALUE position exceeds addressable rows"))?,
            )
        }
    };
    let dtype = match function {
        frame::Function::Aggregate(function) => {
            if let Some(source) = source {
                validate_key(source)?;
            }
            super::super::aggregate::reduction_dtype(function, source)?
        }
        _ => source
            .ok_or_else(|| failed("window value function requires an argument"))?
            .as_nullable(),
    };
    let measure_key = if matches!(function, frame::Function::Aggregate(_)) {
        column
            .map(|column| key(window, fields, column))
            .transpose()?
    } else {
        None
    };
    let decimal = match (function, source) {
        (
            frame::Function::Aggregate(Aggregate::Sum | Aggregate::Avg),
            Some(DType::Decimal(dtype, _)),
        ) => Some(*dtype),
        _ => None,
    };
    Ok((
        frame::Spec {
            function,
            column: column.map(str::to_owned),
            key: measure_key,
            decimal,
            frame: policy(
                expression
                    .frame
                    .as_ref()
                    .unwrap_or(&VortexRelationalWindowFrame::default()),
                expression,
                fields,
                window,
            )?,
        },
        dtype,
    ))
}

pub(super) fn policy(
    requested: &VortexRelationalWindowFrame,
    expression: &VortexRelationalWindowExpression,
    fields: &[(String, DType)],
    window: &mut window::Spec,
) -> Result<frame::Frame> {
    if matches!(requested.start, Bound::UnboundedFollowing)
        || matches!(requested.end, Bound::UnboundedPreceding)
        || bound_rank(&requested.start) > bound_rank(&requested.end)
    {
        return Err(failed(
            "window frame end category precedes its start, or uses an invalid unbounded endpoint",
        ));
    }
    if requested.unit == Unit::Groups && expression.order_by.is_empty() {
        return Err(failed("GROUPS frames require ORDER BY"));
    }
    let bounded_range = requested.unit == Unit::Range
        && [&requested.start, &requested.end]
            .iter()
            .any(|bound| matches!(bound, Bound::Preceding(_) | Bound::Following(_)));
    if bounded_range && expression.order_by.len() != 1 {
        return Err(failed(
            "bounded RANGE frames require exactly one ORDER BY key",
        ));
    }
    let order = if bounded_range {
        let requested = &expression.order_by[0];
        Some(window::OrderKey {
            key: key(window, fields, requested.column.as_str())?,
            descending: requested.descending,
            nulls: requested.nulls,
        })
    } else {
        None
    };
    let order_dtype = if bounded_range {
        Some(field(fields, expression.order_by[0].column.as_str())?)
    } else {
        None
    };
    Ok(frame::Frame {
        unit: requested.unit,
        start: bind_bound(&requested.start, requested.unit, order_dtype)?,
        end: bind_bound(&requested.end, requested.unit, order_dtype)?,
        exclusion: requested.exclusion,
        order,
    })
}

fn bound_rank(bound: &Bound) -> u8 {
    match bound {
        Bound::UnboundedPreceding => 0,
        Bound::Preceding(_) => 1,
        Bound::CurrentRow => 2,
        Bound::Following(_) => 3,
        Bound::UnboundedFollowing => 4,
    }
}

fn bind_bound(bound: &Bound, unit: Unit, order: Option<&DType>) -> Result<frame::Bound> {
    Ok(match bound {
        Bound::UnboundedPreceding => frame::Bound::UnboundedPreceding,
        Bound::UnboundedFollowing => frame::Bound::UnboundedFollowing,
        Bound::CurrentRow => frame::Bound::CurrentRow,
        Bound::Preceding(offset) => frame::Bound::Preceding(bind_offset(offset, unit, order)?),
        Bound::Following(offset) => frame::Bound::Following(bind_offset(offset, unit, order)?),
    })
}

fn nonnegative_integer(value: &ScalarValue) -> Result<u64> {
    match value {
        ScalarValue::UInt64(value) => Ok(*value),
        ScalarValue::Int64(value) if *value >= 0 => Ok(value.unsigned_abs()),
        _ => Err(failed(
            "window frame offset requires a nonnegative integer literal",
        )),
    }
}

#[allow(clippy::cast_precision_loss)] // The existing primitive-to-F64 numeric policy.
fn bind_offset(offset: &Offset, unit: Unit, order: Option<&DType>) -> Result<frame::Offset> {
    if unit != Unit::Range {
        let Offset::Number(number) = offset else {
            return Err(failed(
                "ROWS and GROUPS frame offsets count rows or peer groups, not durations",
            ));
        };
        return Ok(frame::Offset::Count(
            usize::try_from(nonnegative_integer(number)?)
                .map_err(|_| failed("window frame offset exceeds addressable rows"))?,
        ));
    }
    let order = order.ok_or_else(|| failed("bounded RANGE has no bound ordering domain"))?;
    let offset = match (order, offset) {
        (DType::Primitive(ptype, _), Offset::Number(value)) if ptype.is_int() => {
            frame::RangeOffset::Integer(nonnegative_integer(value)?)
        }
        (DType::Primitive(PType::F32 | PType::F64, _), Offset::Number(value)) => {
            let value = match value {
                ScalarValue::Float64(value) => *value,
                value => nonnegative_integer(value)? as f64,
            };
            if !value.is_finite() || value < 0.0 {
                return Err(failed(
                    "RANGE floating offset must be finite and nonnegative",
                ));
            }
            frame::RangeOffset::Float(value)
        }
        (DType::Decimal(_, _), Offset::Number(value)) => {
            let (value, scale) = match value {
                ScalarValue::Decimal128 {
                    value,
                    precision,
                    scale,
                } => {
                    shardloom_core::expression::Decimal128Operand::decimal(
                        *value, *precision, *scale,
                    )?;
                    (*value, *scale)
                }
                value => (i128::from(nonnegative_integer(value)?), 0),
            };
            if value < 0 {
                return Err(failed("RANGE decimal offset must be nonnegative"));
            }
            frame::RangeOffset::Decimal { value, scale }
        }
        (DType::Extension(_), Offset::DurationMicros(micros)) => {
            match crate::native_payload_schema::temporal_storage(order) {
                Some(PType::I32) if micros.is_multiple_of(86_400_000_000) => {
                    frame::RangeOffset::Date(micros / 86_400_000_000)
                }
                Some(PType::I64) => frame::RangeOffset::Timestamp(*micros),
                _ => {
                    return Err(failed(
                        "temporal RANGE offset requires whole days for Date32 or fixed microseconds for TimestampMicros",
                    ));
                }
            }
        }
        _ => {
            return Err(failed(
                "RANGE offset does not match its numeric or temporal ordering key",
            ));
        }
    };
    Ok(frame::Offset::Range(offset))
}
