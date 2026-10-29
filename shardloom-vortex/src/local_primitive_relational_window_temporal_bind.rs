//! Exact elapsed and calendar offsets use the existing temporal domain contract.

use super::{Binder, DType, Offset, Result, failed, frame};
use shardloom_core::temporal::{
    TemporalKind, TemporalUnit,
    civil::NANOS_PER_DAY,
    operations::{Kernel, Operation},
};

pub(super) fn bind(
    offset: &Offset,
    order: &DType,
    binder: &mut Binder<'_>,
) -> Result<frame::RangeOffset> {
    let dtype = crate::native_temporal::temporal_dtype(order)
        .ok_or_else(|| failed("temporal RANGE requires a declared temporal ordering domain"))?;
    let duration = match offset {
        Offset::DurationMicros(value) => Some((*value, TemporalUnit::Microseconds)),
        Offset::Duration { value, unit } => Some((*value, *unit)),
        _ => None,
    };
    if let Some((value, unit)) = duration {
        // All admitted integer widths, their differences and u64 elapsed
        // offsets fit exact i128 nanoseconds without rounding source ticks.
        let nanoseconds = i128::from(value) * unit.nanoseconds();
        let tick_nanoseconds = match dtype.kind() {
            TemporalKind::Date32 | TemporalKind::Date64 => {
                if nanoseconds % NANOS_PER_DAY != 0 {
                    return Err(failed("date RANGE offsets require whole elapsed days"));
                }
                if dtype.kind() == &TemporalKind::Date32 {
                    NANOS_PER_DAY
                } else {
                    1_000_000
                }
            }
            TemporalKind::Time(unit)
            | TemporalKind::Duration(unit)
            | TemporalKind::Timestamp { unit, .. } => unit.nanoseconds(),
        };
        return Ok(frame::RangeOffset::Temporal {
            nanoseconds,
            tick_nanoseconds,
        });
    }
    let (amount, operation) = match offset {
        Offset::CalendarDays { days, ambiguity } => (
            *days,
            Operation::CalendarDays {
                ambiguity: *ambiguity,
            },
        ),
        Offset::CalendarMonths {
            months,
            month_end,
            ambiguity,
        } => (
            *months,
            Operation::CalendarMonths {
                month_end: *month_end,
                ambiguity: *ambiguity,
            },
        ),
        _ => {
            return Err(failed(
                "temporal RANGE requires an elapsed or calendar offset",
            ));
        }
    };
    // Policy/type admission and accounting precede parsing timezone rules.
    operation.output_dtype(&dtype, None)?;
    binder.charge(operation.admission_bytes(&dtype, None)?)?;
    Ok(frame::RangeOffset::Calendar {
        amount,
        kernel: Box::new(Kernel::bind(operation, dtype, None)?),
    })
}
