//! Temporal function admission in the existing native expression binder.

use super::{
    Binder, DType, Expression, Function, Kind, PType, Result, ScalarDefinition, ScalarTypeRule,
    bound_cast, failed, function_nullability, integer, literal_text,
};
use crate::local_primitives::native_relational_expression::scalar::TemporalArgument;
use shardloom_core::temporal::{
    TemporalDType, TemporalKind,
    civil::MonthEnd,
    operations::{Kernel, Operation, Part},
    timezone::Ambiguity,
};

#[allow(clippy::too_many_lines)] // One registry dispatch validates and binds the temporal policies.
pub(super) fn bind(
    binder: &mut Binder<'_>,
    definition: &ScalarDefinition,
    mut args: Vec<Expression>,
) -> Result<Expression> {
    use ScalarTypeRule as T;
    let legacy_date = matches!(
        definition.type_rule,
        T::DateField | T::DateOffset | T::DateDifference
    );
    let legacy_timestamp = matches!(
        definition.type_rule,
        T::TimestampField | T::TimestampOffset | T::TimestampDifference
    );
    let difference = matches!(
        definition.type_rule,
        T::DateDifference | T::TimestampDifference
    ) || definition.name == "timestamp_difference";
    if legacy_date || legacy_timestamp {
        let count = if difference { 2 } else { 1 };
        for arg in args.iter_mut().take(count) {
            if matches!(arg.dtype, DType::Utf8(_)) {
                let dtype = crate::native_temporal::vortex_dtype(
                    &TemporalDType::parse(if legacy_date {
                        "date32"
                    } else {
                        "timestamp_micros"
                    })
                    .expect("canonical alias"),
                    arg.dtype.nullability(),
                )?;
                let original = std::mem::replace(
                    arg,
                    Expression::new(
                        DType::Null,
                        Kind::Literal(vortex::array::scalar::Scalar::null(DType::Null)),
                    ),
                );
                *arg = bound_cast(binder, original, dtype)?;
            }
        }
    }
    let mut source = crate::native_temporal::temporal_dtype(&args[0].dtype);
    if source.is_none() && args[0].dtype == DType::Null {
        if difference {
            source = crate::native_temporal::temporal_dtype(&args[1].dtype);
        }
        if source.is_none() && (legacy_date || legacy_timestamp) {
            source = TemporalDType::parse(if legacy_date {
                "date32"
            } else {
                "timestamp_micros"
            });
        }
    }
    let source = source.ok_or_else(|| {
        failed("temporal function requires an explicitly typed date, time, timestamp or duration")
    })?;
    if legacy_date && !matches!(source.kind(), TemporalKind::Date32 | TemporalKind::Date64)
        || legacy_timestamp && !matches!(source.kind(), TemporalKind::Timestamp { .. })
    {
        return Err(failed(
            "date functions require dates; timestamp functions require timestamps",
        ));
    }
    let mut second = TemporalArgument::None;
    let operation = match definition.name {
        "temporal_extract" => Operation::Extract(part(&args[1])?),
        "temporal_trunc" => Operation::Truncate {
            part: part(&args[1])?,
            ambiguity: ambiguity(&args, 2)?,
        },
        "calendar_add_days" => {
            second = TemporalArgument::Integer;
            Operation::CalendarDays {
                ambiguity: ambiguity(&args, 2)?,
            }
        }
        "calendar_add_months" => {
            second = TemporalArgument::Integer;
            Operation::CalendarMonths {
                month_end: month_end(&args, 2)?,
                ambiguity: ambiguity(&args, 3)?,
            }
        }
        "temporal_add_duration" | "temporal_sub_duration" => {
            second = TemporalArgument::Temporal;
            Operation::AddDuration {
                subtract: definition.name == "temporal_sub_duration",
            }
        }
        "timestamp_difference" | "timestamp_diff_seconds" => {
            second = TemporalArgument::Temporal;
            Operation::TimestampDifference {
                seconds: definition.name == "timestamp_diff_seconds",
            }
        }
        "date_diff_days" => {
            second = TemporalArgument::Temporal;
            Operation::DateDifference
        }
        "date_add_days" | "date_sub_days" => {
            second = TemporalArgument::Integer;
            Operation::DateOffset {
                subtract: definition.name == "date_sub_days",
            }
        }
        "timestamp_add_seconds" | "timestamp_sub_seconds" => {
            second = TemporalArgument::Integer;
            Operation::TimestampOffset {
                subtract: definition.name == "timestamp_sub_seconds",
            }
        }
        "timestamp_assign_timezone" => Operation::AssignTimezone {
            timezone: control(&args[1], "timezone")?.to_owned(),
            ambiguity: ambiguity(&args, 2)?,
        },
        "timestamp_convert_timezone" => Operation::ConvertTimezone {
            timezone: control(&args[1], "timezone")?.to_owned(),
        },
        "timestamp_remove_timezone" => Operation::RemoveTimezone,
        name if matches!(definition.type_rule, T::DateField | T::TimestampField) => {
            let (_, field) = name
                .split_once('_')
                .ok_or_else(|| failed("temporal field name absent"))?;
            Operation::Extract(
                Part::parse(field).ok_or_else(|| failed("temporal field is not admitted"))?,
            )
        }
        _ => return Err(failed("temporal function has no bound native operation")),
    };
    let other = match second {
        TemporalArgument::None => None,
        TemporalArgument::Integer => {
            if args[1].dtype != DType::Null
                && !matches!(args[1].dtype, DType::Primitive(ptype, _) if integer(ptype).is_some())
            {
                return Err(failed("calendar and second offsets require an integer"));
            }
            None
        }
        TemporalArgument::Temporal => {
            let dtype = if args[1].dtype == DType::Null {
                if matches!(operation, Operation::AddDuration { .. }) {
                    TemporalDType::new(TemporalKind::Duration(source.unit().ok_or_else(|| {
                        failed("fixed duration requires a timestamp, time or duration")
                    })?))?
                } else {
                    source.clone()
                }
            } else {
                crate::native_temporal::temporal_dtype(&args[1].dtype).ok_or_else(|| {
                    failed("second temporal operand requires explicit logical metadata")
                })?
            };
            Some(dtype)
        }
    };
    // Bind every literal policy before retaining only the runtime operands.
    binder.charge(operation.admission_bytes(&source, other.as_ref())?)?;
    let kernel = Kernel::bind(operation, source, other)?;
    args.truncate(if second == TemporalArgument::None {
        1
    } else {
        2
    });
    let nullable = function_nullability(definition.null_rule, &args);
    let dtype = kernel
        .output_dtype()
        .map_or(Ok(DType::Primitive(PType::I64, nullable)), |dtype| {
            crate::native_temporal::vortex_dtype(dtype, nullable)
        })?;
    Ok(Expression::new(
        dtype,
        Kind::Function {
            function: Function::Temporal {
                kernel: Box::new(kernel),
                second,
            },
            args,
        },
    ))
}

fn control<'a>(arg: &'a Expression, name: &str) -> Result<&'a str> {
    literal_text(arg, name)?.ok_or_else(|| failed("temporal policy literals must be non-null"))
}

fn part(arg: &Expression) -> Result<Part> {
    Part::parse(&control(arg, "temporal part")?.to_ascii_lowercase())
        .ok_or_else(|| failed("temporal part is not admitted"))
}

fn ambiguity(args: &[Expression], index: usize) -> Result<Ambiguity> {
    args.get(index).map_or(Ok(Ambiguity::Reject), |arg| {
        Ambiguity::parse(&control(arg, "ambiguity policy")?.to_ascii_lowercase())
            .ok_or_else(|| failed("ambiguity policy requires reject, earlier or later"))
    })
}

fn month_end(args: &[Expression], index: usize) -> Result<MonthEnd> {
    args.get(index).map_or(Ok(MonthEnd::Reject), |arg| {
        MonthEnd::parse(&control(arg, "month-end policy")?.to_ascii_lowercase())
            .ok_or_else(|| failed("month-end policy requires reject or clamp"))
    })
}
