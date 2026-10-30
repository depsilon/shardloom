//! Metadata-bound integer/calendar kernels for the shared expression engine.
//! These kernels do not evaluate expressions, allocate rows or select a runtime.

use super::{
    TemporalDType, TemporalGridStep, TemporalKind as Kind, TemporalUnit as Unit,
    civil::{self, CivilDateTime, MonthEnd, NANOS_PER_DAY, NANOS_PER_SECOND},
    failed,
    timezone::{Ambiguity, BoundTimeZone, validate_name},
};
use crate::Result;

#[path = "temporal_grid.rs"]
mod grid;

#[cfg(test)]
#[path = "temporal_extract_tests.rs"]
mod extract_tests;

/// Calendar component or truncation boundary, with Monday-based ISO weekdays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    Year,
    Month,
    Week,
    Day,
    DayOfWeek,
    DayOfYear,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Part {
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "year" => Self::Year,
            "month" => Self::Month,
            "week" => Self::Week,
            "day" => Self::Day,
            "day_of_week" => Self::DayOfWeek,
            "day_of_year" => Self::DayOfYear,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "millisecond" => Self::Millisecond,
            "microsecond" => Self::Microsecond,
            "nanosecond" => Self::Nanosecond,
            _ => return None,
        })
    }

    fn date_part(self) -> bool {
        matches!(
            self,
            Self::Year | Self::Month | Self::Week | Self::Day | Self::DayOfWeek | Self::DayOfYear
        )
    }
}

/// An explicit operation and its already-parsed literal policies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    Extract(Part),
    Truncate {
        part: Part,
        ambiguity: Ambiguity,
    },
    Bucket {
        step: TemporalGridStep,
        origin: Option<i64>,
    },
    DateOffset {
        subtract: bool,
    },
    TimestampOffset {
        subtract: bool,
    },
    DateDifference,
    TimestampDifference {
        seconds: bool,
    },
    AddDuration {
        subtract: bool,
    },
    CalendarDays {
        ambiguity: Ambiguity,
    },
    CalendarMonths {
        month_end: MonthEnd,
        ambiguity: Ambiguity,
    },
    AssignTimezone {
        timezone: String,
        ambiguity: Ambiguity,
    },
    ConvertTimezone {
        timezone: String,
    },
    RemoveTimezone,
}

impl Operation {
    /// Determine the exact output domain. `None` means an Int64 result.
    ///
    /// # Errors
    /// Rejects incompatible operands, policies or temporal component requests.
    pub fn output_dtype(
        &self,
        source: &TemporalDType,
        other: Option<&TemporalDType>,
    ) -> Result<Option<TemporalDType>> {
        let kind = source.kind();
        let is_date = matches!(kind, Kind::Date32 | Kind::Date64);
        let is_timestamp = matches!(kind, Kind::Timestamp { .. });
        let same = || Ok(Some(source.clone()));
        match self {
            Self::Extract(part) => {
                validate_part(kind, *part, false)?;
                Ok(None)
            }
            Self::Truncate { part, .. } => {
                validate_part(kind, *part, true)?;
                same()
            }
            Self::Bucket { step, origin } => {
                grid::validate(source, *step, *origin)?;
                same()
            }
            Self::DateOffset { .. } if is_date => same(),
            Self::TimestampOffset { .. } if is_timestamp => same(),
            Self::CalendarDays { .. } | Self::CalendarMonths { .. } if is_date || is_timestamp => {
                same()
            }
            Self::DateDifference if is_date && other == Some(source) => Ok(None),
            Self::TimestampDifference { seconds } if is_timestamp && other == Some(source) => {
                if *seconds {
                    Ok(None)
                } else {
                    TemporalDType::new(Kind::Duration(unit(source))).map(Some)
                }
            }
            Self::AddDuration { .. }
                if matches!(
                    kind,
                    Kind::Time(_) | Kind::Timestamp { .. } | Kind::Duration(_)
                ) && other.is_some_and(|dtype| matches!(dtype.kind(), Kind::Duration(_))) =>
            {
                same()
            }
            Self::AssignTimezone { timezone, .. } | Self::ConvertTimezone { timezone } => {
                let Kind::Timestamp {
                    unit,
                    timezone: prior,
                } = kind
                else {
                    return Err(failed("timezone operations require a timestamp"));
                };
                if prior.is_some() == matches!(self, Self::AssignTimezone { .. }) {
                    return Err(failed(
                        "assign_timezone requires a naive timestamp; convert_timezone requires a zoned instant",
                    ));
                }
                validate_name(timezone)?;
                TemporalDType::new(Kind::Timestamp {
                    unit: *unit,
                    timezone: Some(timezone.clone()),
                })
                .map(Some)
            }
            Self::RemoveTimezone if source.timezone().is_some() => {
                TemporalDType::new(Kind::Timestamp {
                    unit: unit(source),
                    timezone: None,
                })
                .map(Some)
            }
            _ => Err(failed(
                "temporal operation requires compatible declared logical domains",
            )),
        }
    }

    fn zone<'a>(&'a self, source: &'a TemporalDType) -> Option<&'a str> {
        match self {
            Self::AssignTimezone { timezone, .. } => Some(timezone),
            Self::Extract(_)
            | Self::Truncate { .. }
            | Self::CalendarDays { .. }
            | Self::CalendarMonths { .. }
            | Self::RemoveTimezone => source.timezone(),
            Self::Bucket { step, .. } if !matches!(step, TemporalGridStep::Duration { .. }) => {
                source.timezone()
            }
            _ => None,
        }
    }

    /// Conservative admission before cloning metadata or parsing timezone rules.
    ///
    /// # Errors
    /// Rejects unknown timezone names and workspace accounting overflow.
    pub fn admission_bytes(
        &self,
        source: &TemporalDType,
        other: Option<&TemporalDType>,
    ) -> Result<usize> {
        let metadata = source
            .heap_bytes()
            .checked_add(other.map_or(0, TemporalDType::heap_bytes))
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| failed("temporal operation metadata admission overflow"))?;
        metadata
            .checked_add(
                self.zone(source)
                    .map_or(Ok(0), BoundTimeZone::admission_bytes)?,
            )
            .ok_or_else(|| failed("temporal operation workspace admission overflow"))
    }
}

/// One schema-bound kernel. The caller owns its memory lease and cancellation.
#[derive(Debug)]
pub struct Kernel {
    operation: Operation,
    source: TemporalDType,
    other: Option<TemporalDType>,
    output: Option<TemporalDType>,
    zone: Option<BoundTimeZone>,
}

impl Kernel {
    /// Exact bound semantic identity within this pinned timezone-provider build.
    /// Parsed rules are immutable derivatives of the operation and input domains.
    #[must_use]
    pub fn same_semantics(&self, other: &Self) -> bool {
        self.operation == other.operation
            && self.source == other.source
            && self.other == other.other
            && self.output == other.output
    }

    /// Construct only after admitting `operation.admission_bytes`.
    ///
    /// # Errors
    /// Rejects unsupported metadata, incompatible domains and invalid zones.
    pub fn bind(
        operation: Operation,
        source: TemporalDType,
        other: Option<TemporalDType>,
    ) -> Result<Self> {
        let output = operation.output_dtype(&source, other.as_ref())?;
        let zone = operation
            .zone(&source)
            .map(BoundTimeZone::new)
            .transpose()?;
        let mut kernel = Self {
            operation,
            source,
            other,
            output,
            zone,
        };
        if matches!(kernel.operation, Operation::Bucket { .. }) {
            let resolved = kernel.grid_origin()?;
            if let Operation::Bucket { origin, .. } = &mut kernel.operation {
                *origin = Some(resolved);
            }
        }
        Ok(kernel)
    }

    #[must_use]
    pub fn output_dtype(&self) -> Option<&TemporalDType> {
        self.output.as_ref()
    }

    /// Apply to exact ticks. The second value is either typed temporal ticks or
    /// a widened integer offset, determined by the bound operation.
    ///
    /// # Errors
    /// Rejects invalid restricted values, overflow, precision loss, calendar
    /// range failures and unresolved daylight-saving gaps/folds.
    pub fn apply(&self, value: i64, right: Option<i128>) -> Result<i64> {
        self.source.validate_value(value)?;
        let argument =
            || right.ok_or_else(|| failed("temporal operation is missing its second operand"));
        if let Some(other) = &self.other {
            other.validate_value(narrow(argument()?)?)?;
        }
        let result = match self.operation {
            Operation::Extract(part) => self.extract(value, part)?,
            Operation::Truncate { part, ambiguity } => {
                self.civil_ticks(truncate(self.civil(value)?, part)?, ambiguity)?
            }
            Operation::Bucket { .. } => self.grid_point(self.bucket_index(value)?)?,
            Operation::DateOffset { subtract } => {
                let unit = if matches!(self.source.kind(), Kind::Date32) {
                    1
                } else {
                    86_400_000
                };
                add(value, signed(argument()?, subtract)?, unit)?
            }
            Operation::TimestampOffset { subtract } => add(
                value,
                signed(argument()?, subtract)?,
                NANOS_PER_SECOND / unit(&self.source).nanoseconds(),
            )?,
            Operation::DateDifference => {
                let divisor = if matches!(self.source.kind(), Kind::Date32) {
                    1
                } else {
                    86_400_000
                };
                narrow((i128::from(value) - argument()?) / divisor)?
            }
            Operation::TimestampDifference { seconds } => {
                let difference = i128::from(value) - argument()?;
                if seconds {
                    narrow(difference / (NANOS_PER_SECOND / unit(&self.source).nanoseconds()))?
                } else {
                    narrow(difference)?
                }
            }
            Operation::AddDuration { subtract } => {
                let other = self
                    .other
                    .as_ref()
                    .ok_or_else(|| failed("duration metadata absent"))?;
                let delta = signed(argument()?, subtract)?
                    .checked_mul(unit(other).nanoseconds())
                    .ok_or_else(|| failed("duration arithmetic overflow"))?;
                civil::nanos_to_ticks(
                    i128::from(value) * unit(&self.source).nanoseconds() + delta,
                    unit(&self.source),
                )?
            }
            Operation::CalendarDays { ambiguity } => {
                if argument()? == 0 {
                    value
                } else {
                    self.civil_ticks(
                        self.civil(value)?.add_days(narrow(argument()?)?)?,
                        ambiguity,
                    )?
                }
            }
            Operation::CalendarMonths {
                month_end,
                ambiguity,
            } => {
                if argument()? == 0 {
                    value
                } else {
                    self.civil_ticks(
                        self.civil(value)?
                            .add_months(narrow(argument()?)?, month_end)?,
                        ambiguity,
                    )?
                }
            }
            Operation::AssignTimezone { ambiguity, .. } => self
                .zone
                .as_ref()
                .ok_or_else(|| failed("timezone assignment rules absent"))?
                .timestamp(
                    CivilDateTime::from_ticks(value, unit(&self.source)),
                    unit(&self.source),
                    ambiguity,
                )?,
            Operation::ConvertTimezone { .. } => value,
            Operation::RemoveTimezone => self.civil(value)?.ticks(unit(&self.source))?,
        };
        if let Some(output) = &self.output {
            output.validate_value(result)?;
        }
        Ok(result)
    }

    fn extract(&self, value: i64, part: Part) -> Result<i64> {
        // The timezone provider owns range and DST behavior. Source/second-value
        // validation still precedes this helper in apply, including Date/Time
        // restrictions. Naive domains need only the requested exact component.
        let nanos = match self.source.kind() {
            Kind::Timestamp {
                timezone: Some(_), ..
            } => {
                return Ok(extract(self.civil(value)?, part));
            }
            Kind::Date32 => i128::from(value) * NANOS_PER_DAY,
            Kind::Date64 => i128::from(value) * Unit::Milliseconds.nanoseconds(),
            Kind::Timestamp { unit, .. } | Kind::Time(unit) => {
                i128::from(value) * unit.nanoseconds()
            }
            Kind::Duration(_) => return Err(failed("fixed durations have no calendar fields")),
        };
        let days = nanos.div_euclid(NANOS_PER_DAY);
        let result = match part {
            Part::Hour => nanos.div_euclid(3_600 * NANOS_PER_SECOND).rem_euclid(24),
            Part::Minute => nanos.div_euclid(60 * NANOS_PER_SECOND).rem_euclid(60),
            Part::Second => nanos.div_euclid(NANOS_PER_SECOND).rem_euclid(60),
            Part::Millisecond => nanos.rem_euclid(NANOS_PER_SECOND) / 1_000_000,
            Part::Microsecond => nanos.rem_euclid(NANOS_PER_SECOND) / 1_000,
            Part::Nanosecond => nanos.rem_euclid(NANOS_PER_SECOND),
            Part::DayOfWeek => (days + 3).rem_euclid(7) + 1,
            Part::Year | Part::Month | Part::Day | Part::DayOfYear => {
                let (year, month, day) = civil::civil_from_days(days)?;
                match part {
                    Part::Year => i128::from(year),
                    Part::Month => i128::from(month),
                    Part::Day => i128::from(day),
                    Part::DayOfYear => days - civil::days_from_civil(year, 1, 1) + 1,
                    _ => unreachable!("calendar components admitted above"),
                }
            }
            Part::Week => unreachable!("week extraction rejected at bind"),
        };
        narrow(result)
    }

    fn civil(&self, value: i64) -> Result<CivilDateTime> {
        match self.source.kind() {
            Kind::Date32 => CivilDateTime::from_nanoseconds(i128::from(value) * NANOS_PER_DAY),
            Kind::Date64 => Ok(CivilDateTime::from_ticks(value, Unit::Milliseconds)),
            Kind::Timestamp {
                timezone: Some(_),
                unit,
            } => self
                .zone
                .as_ref()
                .ok_or_else(|| failed("calendar timezone rules absent"))?
                .civil(value, *unit),
            Kind::Timestamp { unit, .. } | Kind::Time(unit) => {
                Ok(CivilDateTime::from_ticks(value, *unit))
            }
            Kind::Duration(_) => Err(failed("fixed durations have no calendar fields")),
        }
    }

    fn civil_ticks(&self, civil: CivilDateTime, ambiguity: Ambiguity) -> Result<i64> {
        match self.source.kind() {
            Kind::Date32 => narrow(civil::days_from_civil(civil.year, civil.month, civil.day)),
            Kind::Date64 => civil.ticks(Unit::Milliseconds),
            Kind::Timestamp {
                timezone: Some(_),
                unit,
            } => self
                .zone
                .as_ref()
                .ok_or_else(|| failed("calendar timezone rules absent"))?
                .timestamp(civil, *unit, ambiguity),
            Kind::Timestamp { unit, .. } | Kind::Time(unit) => civil.ticks(*unit),
            Kind::Duration(_) => Err(failed("fixed durations have no calendar fields")),
        }
    }
}

fn unit(dtype: &TemporalDType) -> Unit {
    dtype
        .unit()
        .expect("bound time, timestamp or duration domain")
}

fn narrow(value: i128) -> Result<i64> {
    i64::try_from(value).map_err(|_| failed("temporal arithmetic exceeds signed 64-bit result"))
}

fn signed(value: i128, subtract: bool) -> Result<i128> {
    if subtract {
        value
            .checked_neg()
            .ok_or_else(|| failed("temporal offset sign overflow"))
    } else {
        Ok(value)
    }
}

fn add(value: i64, offset: i128, multiplier: i128) -> Result<i64> {
    let result = offset
        .checked_mul(multiplier)
        .and_then(|offset| i128::from(value).checked_add(offset))
        .ok_or_else(|| failed("temporal arithmetic overflow"))?;
    narrow(result)
}

fn validate_part(kind: &Kind, part: Part, truncation: bool) -> Result<()> {
    let valid = match kind {
        Kind::Date32 | Kind::Date64 => part.date_part(),
        Kind::Time(_) => !part.date_part(),
        Kind::Timestamp { .. } => true,
        Kind::Duration(_) => false,
    };
    if !valid
        || (truncation && matches!(part, Part::DayOfWeek | Part::DayOfYear))
        || (!truncation && part == Part::Week)
    {
        Err(failed(
            "temporal field or truncation unit is incompatible with its logical domain",
        ))
    } else {
        Ok(())
    }
}

fn extract(civil: CivilDateTime, part: Part) -> i64 {
    match part {
        Part::Year => civil.year,
        Part::Month => i64::from(civil.month),
        Part::Day => i64::from(civil.day),
        Part::Hour => i64::from(civil.hour),
        Part::Minute => i64::from(civil.minute),
        Part::Second => i64::from(civil.second),
        Part::Millisecond => i64::from(civil.nanosecond / 1_000_000),
        Part::Microsecond => i64::from(civil.nanosecond / 1000),
        Part::Nanosecond => i64::from(civil.nanosecond),
        Part::DayOfWeek => i64::try_from(
            (civil::days_from_civil(civil.year, civil.month, civil.day) + 3).rem_euclid(7) + 1,
        )
        .expect("ISO weekday in 1..=7"),
        Part::DayOfYear => i64::try_from(
            civil::days_from_civil(civil.year, civil.month, civil.day)
                - civil::days_from_civil(civil.year, 1, 1)
                + 1,
        )
        .expect("ordinal day in 1..=366"),
        Part::Week => unreachable!("week extraction rejected at bind"),
    }
}

fn truncate(mut civil: CivilDateTime, part: Part) -> Result<CivilDateTime> {
    match part {
        Part::Year => {
            civil.month = 1;
            civil.day = 1;
        }
        Part::Month => civil.day = 1,
        Part::Week => {
            let days = civil::days_from_civil(civil.year, civil.month, civil.day);
            (civil.year, civil.month, civil.day) =
                civil::civil_from_days(days - (days + 3).rem_euclid(7))?;
        }
        _ => {}
    }
    if part.date_part() {
        civil.hour = 0;
    }
    if part.date_part() || part == Part::Hour {
        civil.minute = 0;
    }
    if part.date_part() || matches!(part, Part::Hour | Part::Minute) {
        civil.second = 0;
    }
    civil.nanosecond = match part {
        Part::Nanosecond => civil.nanosecond,
        Part::Microsecond => civil.nanosecond / 1000 * 1000,
        Part::Millisecond => civil.nanosecond / 1_000_000 * 1_000_000,
        _ => 0,
    };
    Ok(civil)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dtype(name: &str) -> TemporalDType {
        TemporalDType::parse(name).unwrap()
    }
    fn kernel(name: &str, operation: Operation, other: Option<&str>) -> Kernel {
        let source = dtype(name);
        let other = other.map(dtype);
        assert!(operation.admission_bytes(&source, other.as_ref()).unwrap() >= 8192);
        Kernel::bind(operation, source, other).unwrap()
    }
    fn utc(text: &str) -> i64 {
        civil::parse_timestamp(text)
            .unwrap()
            .0
            .ticks(Unit::Seconds)
            .unwrap()
    }

    #[test]
    fn temporal_kernel_identity_covers_every_bound_domain_and_policy() {
        let days = |ambiguity| Operation::CalendarDays { ambiguity };
        let base = kernel(
            "timestamp(us,America/New_York)",
            days(Ambiguity::Reject),
            None,
        );
        assert!(base.same_semantics(&kernel(
            "timestamp(us,America/New_York)",
            days(Ambiguity::Reject),
            None
        )));
        for other in [
            kernel(
                "timestamp(ns,America/New_York)",
                days(Ambiguity::Reject),
                None,
            ),
            kernel(
                "timestamp(us,America/Chicago)",
                days(Ambiguity::Reject),
                None,
            ),
            kernel(
                "timestamp(us,America/New_York)",
                days(Ambiguity::Earlier),
                None,
            ),
            kernel(
                "timestamp(us,America/New_York)",
                Operation::Truncate {
                    part: Part::Day,
                    ambiguity: Ambiguity::Reject,
                },
                None,
            ),
        ] {
            assert!(!base.same_semantics(&other));
        }
        assert!(
            !kernel(
                "timestamp(us)",
                Operation::AddDuration { subtract: false },
                Some("duration(us)")
            )
            .same_semantics(&kernel(
                "timestamp(us)",
                Operation::AddDuration { subtract: false },
                Some("duration(ns)")
            ))
        );
        assert!(
            !kernel(
                "date32",
                Operation::CalendarMonths {
                    month_end: MonthEnd::Reject,
                    ambiguity: Ambiguity::Reject
                },
                None
            )
            .same_semantics(&kernel(
                "date32",
                Operation::CalendarMonths {
                    month_end: MonthEnd::Clamp,
                    ambiguity: Ambiguity::Reject
                },
                None
            ))
        );
    }

    #[test]
    fn temporal_fields_preserve_precision_and_apply_pinned_presentation_zone() {
        for (part, expected) in [
            (Part::Year, 1969),
            (Part::Month, 12),
            (Part::Day, 31),
            (Part::Hour, 23),
            (Part::Minute, 59),
            (Part::Second, 59),
            (Part::Millisecond, 999),
            (Part::Microsecond, 999_999),
            (Part::Nanosecond, 999_999_999),
            (Part::DayOfWeek, 3),
            (Part::DayOfYear, 365),
        ] {
            assert_eq!(
                kernel("timestamp(ns)", Operation::Extract(part), None)
                    .apply(-1, None)
                    .unwrap(),
                expected
            );
        }
        assert_eq!(
            kernel(
                "timestamp(s,America/New_York)",
                Operation::Extract(Part::Hour),
                None
            )
            .apply(0, None)
            .unwrap(),
            19
        );
        assert_eq!(
            kernel(
                "timestamp(s,Asia/Kathmandu)",
                Operation::Extract(Part::Minute),
                None
            )
            .apply(utc("2024-01-01T00:00:00"), None)
            .unwrap(),
            45
        );
        assert_eq!(
            kernel("date64", Operation::Extract(Part::Year), None)
                .apply(-86_400_000, None)
                .unwrap(),
            1969
        );
        assert_eq!(
            kernel("time(ns)", Operation::Extract(Part::Nanosecond), None)
                .apply(1, None)
                .unwrap(),
            1
        );
        assert!(Kernel::bind(Operation::Extract(Part::Year), dtype("duration(s)"), None).is_err());
        assert!(Kernel::bind(Operation::Extract(Part::Day), dtype("time(s)"), None).is_err());
        assert!(Kernel::bind(Operation::Extract(Part::Hour), dtype("date32"), None).is_err());
    }

    #[test]
    fn temporal_fixed_and_calendar_days_have_different_dst_results() {
        let source = "timestamp(s,America/New_York)";
        let calendar = kernel(
            source,
            Operation::CalendarDays {
                ambiguity: Ambiguity::Reject,
            },
            None,
        );
        let fixed = kernel(
            source,
            Operation::AddDuration { subtract: false },
            Some("duration(s)"),
        );
        for (start, next, hours) in [
            ("2024-03-10T05:00:00", "2024-03-11T04:00:00", 23),
            ("2024-11-03T04:00:00", "2024-11-04T05:00:00", 25),
        ] {
            let (start, next) = (utc(start), utc(next));
            assert_eq!(calendar.apply(start, Some(1)).unwrap(), next);
            assert_eq!(next - start, hours * 3600);
            assert_eq!(fixed.apply(start, Some(86_400)).unwrap(), start + 86_400);
        }
        let half_hour = kernel(
            "timestamp(s,Australia/Lord_Howe)",
            Operation::CalendarDays {
                ambiguity: Ambiguity::Reject,
            },
            None,
        );
        let start = utc("2024-10-05T13:30:00");
        assert_eq!(half_hour.apply(start, Some(1)).unwrap() - start, 84_600);
    }

    #[test]
    fn temporal_month_end_gap_fold_and_truncation_policies_are_explicit() {
        let january = utc("2024-01-31T12:00:00");
        let reject = Operation::CalendarMonths {
            month_end: MonthEnd::Reject,
            ambiguity: Ambiguity::Reject,
        };
        assert!(
            kernel("timestamp(s)", reject, None)
                .apply(january, Some(1))
                .is_err()
        );
        let clamp = Operation::CalendarMonths {
            month_end: MonthEnd::Clamp,
            ambiguity: Ambiguity::Reject,
        };
        assert_eq!(
            kernel("timestamp(s)", clamp, None)
                .apply(january, Some(1))
                .unwrap(),
            utc("2024-02-29T12:00:00")
        );
        let source = "timestamp(s,America/New_York)";
        let before_gap = utc("2024-03-09T07:30:00");
        assert!(
            kernel(
                source,
                Operation::CalendarDays {
                    ambiguity: Ambiguity::Reject
                },
                None
            )
            .apply(before_gap, Some(1))
            .is_err()
        );
        assert_eq!(
            kernel(
                source,
                Operation::CalendarDays {
                    ambiguity: Ambiguity::Later
                },
                None
            )
            .apply(before_gap, Some(1))
            .unwrap(),
            utc("2024-03-10T07:30:00")
        );
        let fold = utc("2024-11-03T06:30:00");
        assert!(
            kernel(
                source,
                Operation::Truncate {
                    part: Part::Hour,
                    ambiguity: Ambiguity::Reject
                },
                None
            )
            .apply(fold, None)
            .is_err()
        );
        assert_eq!(
            kernel(
                source,
                Operation::Truncate {
                    part: Part::Hour,
                    ambiguity: Ambiguity::Later
                },
                None
            )
            .apply(fold, None)
            .unwrap(),
            utc("2024-11-03T06:00:00")
        );
        assert_eq!(
            kernel(
                "timestamp(ns)",
                Operation::Truncate {
                    part: Part::Second,
                    ambiguity: Ambiguity::Reject
                },
                None
            )
            .apply(-1, None)
            .unwrap(),
            -1_000_000_000
        );
        assert_eq!(
            kernel(
                "date64",
                Operation::Truncate {
                    part: Part::Week,
                    ambiguity: Ambiguity::Reject
                },
                None
            )
            .apply(0, None)
            .unwrap(),
            -3 * 86_400_000
        );
    }

    #[test]
    fn temporal_timezone_assignment_conversion_and_removal_are_distinct() {
        let zone = "America/New_York";
        let assign = |ambiguity| Operation::AssignTimezone {
            timezone: zone.into(),
            ambiguity,
        };
        let civil = utc("2024-11-03T01:30:00");
        assert!(
            kernel("timestamp(s)", assign(Ambiguity::Reject), None)
                .apply(civil, None)
                .is_err()
        );
        assert_eq!(
            kernel("timestamp(s)", assign(Ambiguity::Earlier), None)
                .apply(civil, None)
                .unwrap(),
            utc("2024-11-03T05:30:00")
        );
        assert_eq!(
            kernel("timestamp(s)", assign(Ambiguity::Later), None)
                .apply(civil, None)
                .unwrap(),
            utc("2024-11-03T06:30:00")
        );
        let converted = kernel(
            "timestamp(s,America/New_York)",
            Operation::ConvertTimezone {
                timezone: "Asia/Tokyo".into(),
            },
            None,
        );
        assert_eq!(
            converted.output_dtype().unwrap().as_str(),
            "timestamp(s,Asia/Tokyo)"
        );
        for ticks in [i64::MIN, 0, i64::MAX] {
            assert_eq!(converted.apply(ticks, None).unwrap(), ticks);
        }
        let remove = kernel(
            "timestamp(s,America/New_York)",
            Operation::RemoveTimezone,
            None,
        );
        assert_eq!(remove.output_dtype().unwrap().as_str(), "timestamp(s)");
        assert_eq!(remove.apply(0, None).unwrap(), -18_000);
        assert!(remove.apply(i64::MAX, None).is_err());
        assert!(Kernel::bind(assign(Ambiguity::Reject), dtype("timestamp(s,UTC)"), None).is_err());
        assert!(
            Kernel::bind(
                Operation::ConvertTimezone {
                    timezone: "UTC".into()
                },
                dtype("timestamp(s)"),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn temporal_arithmetic_checks_full_domains_precision_and_metadata() {
        let nanos = kernel(
            "timestamp(ns)",
            Operation::AddDuration { subtract: false },
            Some("duration(us)"),
        );
        assert_eq!(nanos.apply(-1001, Some(1)).unwrap(), -1);
        assert!(nanos.apply(i64::MAX, Some(1)).is_err());
        let micros = kernel(
            "timestamp_micros",
            Operation::AddDuration { subtract: false },
            Some("duration(ns)"),
        );
        assert!(micros.apply(0, Some(1)).is_err());
        let clock = kernel(
            "time(s)",
            Operation::AddDuration { subtract: false },
            Some("duration(s)"),
        );
        assert!(clock.apply(86_399, Some(1)).is_err());
        let difference = kernel(
            "timestamp(s)",
            Operation::TimestampDifference { seconds: false },
            Some("timestamp(s)"),
        );
        assert_eq!(difference.output_dtype().unwrap().as_str(), "duration(s)");
        assert!(
            difference
                .apply(i64::MAX, Some(i128::from(i64::MIN)))
                .is_err()
        );
        let seconds = kernel(
            "timestamp(ns)",
            Operation::TimestampDifference { seconds: true },
            Some("timestamp(ns)"),
        );
        assert_eq!(seconds.apply(-1, Some(0)).unwrap(), 0);
        assert_eq!(seconds.apply(-1_000_000_001, Some(0)).unwrap(), -1);
        assert!(
            Kernel::bind(
                Operation::TimestampDifference { seconds: true },
                dtype("timestamp(ns)"),
                Some(dtype("timestamp_micros"))
            )
            .is_err()
        );
        let add = kernel(
            "timestamp(s)",
            Operation::TimestampOffset { subtract: false },
            None,
        );
        assert_eq!(
            add.apply(i64::MIN, Some(i128::from(u64::MAX))).unwrap(),
            i64::MAX
        );
        let date = kernel("date32", Operation::DateOffset { subtract: false }, None);
        assert!(date.apply(i64::from(i32::MAX), Some(1)).is_err());
    }
}
