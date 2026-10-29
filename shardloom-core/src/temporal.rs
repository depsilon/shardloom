//! Provider-neutral temporal metadata and exact integer-domain conversions.
//!
//! Logical domains are independent of physical integer width. Calendar and
//! timezone operations must be explicitly admitted by the executing provider.

use crate::{LogicalDType, Result, ShardLoomError};

#[path = "temporal_civil.rs"]
pub mod civil;

#[path = "temporal_text.rs"]
pub mod text;

#[cfg(feature = "temporal-calendar")]
#[path = "temporal_timezone.rs"]
pub mod timezone;

#[cfg(feature = "temporal-calendar")]
#[path = "temporal_operations.rs"]
pub mod operations;

/// Resolution for a civil time repeated by a fold or skipped by a gap.
/// The declaration is available without enabling calendar execution.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Ambiguity {
    #[default]
    Reject,
    Earlier,
    Later,
}

impl Ambiguity {
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "reject" => Some(Self::Reject),
            "earlier" => Some(Self::Earlier),
            "later" => Some(Self::Later),
            _ => None,
        }
    }
}

/// Fixed units shared by timestamps, time-of-day and elapsed durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemporalUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl std::fmt::Display for TemporalUnit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TemporalUnit {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seconds => "s",
            Self::Milliseconds => "ms",
            Self::Microseconds => "us",
            Self::Nanoseconds => "ns",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "s" => Self::Seconds,
            "ms" => Self::Milliseconds,
            "us" => Self::Microseconds,
            "ns" => Self::Nanoseconds,
            _ => return None,
        })
    }

    #[must_use]
    pub const fn nanoseconds(self) -> i128 {
        match self {
            Self::Seconds => 1_000_000_000,
            Self::Milliseconds => 1_000_000,
            Self::Microseconds => 1_000,
            Self::Nanoseconds => 1,
        }
    }

    #[must_use]
    pub const fn ticks_per_second(self) -> i64 {
        match self {
            Self::Seconds => 1,
            Self::Milliseconds => 1_000,
            Self::Microseconds => 1_000_000,
            Self::Nanoseconds => 1_000_000_000,
        }
    }
}

/// Positive spacing between anchored temporal grid points. The declaration is
/// available without enabling calendar execution; binding validates its domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalGridStep {
    Duration {
        value: u64,
        unit: TemporalUnit,
    },
    CalendarDays {
        value: u64,
        ambiguity: Ambiguity,
    },
    CalendarMonths {
        value: u64,
        month_end: civil::MonthEnd,
        ambiguity: Ambiguity,
    },
}

impl TemporalGridStep {
    /// Parse inert grid controls shared by scalar and relational declarations.
    /// Domain precision and origin validity are checked by the bound kernel.
    ///
    /// # Errors
    /// Rejects zero/unsupported spacing, unknown units and irrelevant policies.
    pub fn from_parts(
        value: u64,
        unit: &str,
        month_end: civil::MonthEnd,
        ambiguity: Ambiguity,
    ) -> Result<Self> {
        if value == 0 {
            return Err(failed("temporal grid spacing must be positive"));
        }
        let months = unit.eq_ignore_ascii_case("calendar_months");
        let days = unit.eq_ignore_ascii_case("calendar_days");
        if month_end != civil::MonthEnd::Reject && !months {
            return Err(failed(
                "month-end policy applies only to calendar-month grids",
            ));
        }
        if months || days {
            if i64::try_from(value).is_err() {
                return Err(failed(
                    "calendar grid spacing requires a positive signed-64-bit count",
                ));
            }
            return Ok(if months {
                Self::CalendarMonths {
                    value,
                    month_end,
                    ambiguity,
                }
            } else {
                Self::CalendarDays { value, ambiguity }
            });
        }
        if ambiguity != Ambiguity::Reject {
            return Err(failed("ambiguity policy applies only to calendar grids"));
        }
        let unit = [
            TemporalUnit::Seconds,
            TemporalUnit::Milliseconds,
            TemporalUnit::Microseconds,
            TemporalUnit::Nanoseconds,
        ]
        .into_iter()
        .find(|candidate| candidate.as_str().eq_ignore_ascii_case(unit))
        .ok_or_else(|| {
            failed("grid unit requires s, ms, us, ns, calendar_days or calendar_months")
        })?;
        Ok(Self::Duration { value, unit })
    }
}

/// A temporal domain. A timezone-bearing timestamp stores epoch ticks; a
/// timezone-free timestamp stores civil ticks without an inferred host zone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TemporalKind {
    Date32,
    Date64,
    Time(TemporalUnit),
    Timestamp {
        unit: TemporalUnit,
        timezone: Option<String>,
    },
    Duration(TemporalUnit),
}

/// Validated temporal metadata with a stable canonical public name.
///
/// Construction validates the shape of a zone identifier. The native calendar
/// provider separately validates its presence in the pinned timezone database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TemporalDType {
    kind: TemporalKind,
    name: String,
}

impl TemporalDType {
    /// Construct metadata without consulting a filesystem, environment or clock.
    ///
    /// # Errors
    /// Returns an error for an empty, oversized or malformed timezone identifier.
    pub fn new(kind: TemporalKind) -> Result<Self> {
        let name = match &kind {
            TemporalKind::Date32 => "date32".to_owned(),
            TemporalKind::Date64 => "date64".to_owned(),
            TemporalKind::Time(unit) => format!("time({})", unit.as_str()),
            TemporalKind::Duration(unit) => format!("duration({})", unit.as_str()),
            TemporalKind::Timestamp { unit, timezone } => {
                if let Some(zone) = timezone {
                    validate_zone_name(zone)?;
                    format!("timestamp({},{zone})", unit.as_str())
                } else if *unit == TemporalUnit::Microseconds {
                    "timestamp_micros".to_owned()
                } else {
                    format!("timestamp({})", unit.as_str())
                }
            }
        };
        Ok(Self { kind, name })
    }

    /// Parse a canonical name or the compatible `timestamp(us)` alias.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        let kind = match name {
            "date32" => TemporalKind::Date32,
            "date64" => TemporalKind::Date64,
            "timestamp_micros" => TemporalKind::Timestamp {
                unit: TemporalUnit::Microseconds,
                timezone: None,
            },
            _ => {
                let (family, arguments) = name.strip_suffix(')')?.split_once('(')?;
                let (unit, zone) = arguments
                    .split_once(',')
                    .map_or((arguments, None), |(unit, zone)| (unit, Some(zone)));
                let unit = TemporalUnit::parse(unit)?;
                match (family, zone) {
                    ("time", None) => TemporalKind::Time(unit),
                    ("duration", None) => TemporalKind::Duration(unit),
                    ("timestamp", zone) => TemporalKind::Timestamp {
                        unit,
                        timezone: zone.map(str::to_owned),
                    },
                    _ => return None,
                }
            }
        };
        Self::new(kind).ok()
    }

    /// Parse a public type declaration, ignoring type/unit capitalization and
    /// surrounding whitespace while retaining the exact timezone spelling.
    /// A zone may be a single-quoted SQL type argument.
    #[must_use]
    pub fn parse_declaration(name: &str) -> Option<Self> {
        let name = name.trim();
        if let Some((family, arguments)) =
            name.strip_suffix(')').and_then(|name| name.split_once('('))
        {
            let (unit, zone) = arguments
                .split_once(',')
                .map_or((arguments, None), |(unit, zone)| (unit, Some(zone.trim())));
            let family = family.trim().to_ascii_lowercase();
            let unit = unit.trim().to_ascii_lowercase();
            let normalized = if let Some(zone) = zone {
                let zone = zone
                    .strip_prefix('\'')
                    .and_then(|zone| zone.strip_suffix('\''))
                    .unwrap_or(zone);
                format!("{family}({unit},{zone})")
            } else {
                format!("{family}({unit})")
            };
            Self::parse(&normalized)
        } else {
            Self::parse(&name.to_ascii_lowercase())
        }
    }

    #[must_use]
    pub fn kind(&self) -> &TemporalKind {
        &self.kind
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.name
    }

    /// Heap capacity retained by this owned metadata, for execution admission.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        self.name.capacity()
            + match &self.kind {
                TemporalKind::Timestamp {
                    timezone: Some(zone),
                    ..
                } => zone.capacity(),
                _ => 0,
            }
    }

    #[must_use]
    pub const fn unit(&self) -> Option<TemporalUnit> {
        match self.kind {
            TemporalKind::Time(unit)
            | TemporalKind::Timestamp { unit, .. }
            | TemporalKind::Duration(unit) => Some(unit),
            TemporalKind::Date32 | TemporalKind::Date64 => None,
        }
    }

    #[must_use]
    pub fn timezone(&self) -> Option<&str> {
        match &self.kind {
            TemporalKind::Timestamp { timezone, .. } => timezone.as_deref(),
            _ => None,
        }
    }

    /// Preserve the two original logical type variants as compatibility aliases.
    #[must_use]
    pub fn logical(&self) -> LogicalDType {
        match &self.kind {
            TemporalKind::Date32 => LogicalDType::Date32,
            TemporalKind::Timestamp {
                unit: TemporalUnit::Microseconds,
                timezone: None,
            } => LogicalDType::TimestampMicros,
            _ => LogicalDType::Temporal(self.clone()),
        }
    }

    #[must_use]
    pub fn from_logical(dtype: &LogicalDType) -> Option<Self> {
        match dtype {
            LogicalDType::Date32 => Self::parse("date32"),
            LogicalDType::TimestampMicros => Self::parse("timestamp_micros"),
            LogicalDType::Temporal(dtype) => Some(dtype.clone()),
            _ => None,
        }
    }

    /// Validate physical ticks without restricting timestamps to calendar range.
    ///
    /// # Errors
    /// Rejects date32 overflow, date64 values not at midnight, and time-of-day
    /// values outside `[00:00, 24:00)` in their declared unit.
    pub fn validate_value(&self, value: i64) -> Result<()> {
        match self.kind {
            TemporalKind::Date32 if i32::try_from(value).is_err() => {
                Err(failed("date32 ticks exceed signed 32-bit days"))
            }
            TemporalKind::Date64 if value % 86_400_000 != 0 => {
                Err(failed("date64 requires a whole epoch day in milliseconds"))
            }
            TemporalKind::Time(unit) if !(0..86_400 * unit.ticks_per_second()).contains(&value) => {
                Err(failed(
                    "time-of-day ticks must be in [midnight, next midnight)",
                ))
            }
            _ => Ok(()),
        }
    }
}

fn validate_zone_name(zone: &str) -> Result<()> {
    // The maximum includes UTC offsets and all ordinary IANA names. No syntax
    // delimiters, controls, paths from the host or unbounded metadata are accepted.
    if zone.is_empty()
        || zone.len() > 255
        || !zone
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"/_+-.:".contains(&byte))
        || zone.starts_with('/')
        || zone
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(failed(
            "timezone requires a bounded IANA name or explicit UTC offset",
        ));
    }
    Ok(())
}

/// Exact physical temporal value. The constructor validates the declared domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalValue {
    value: i64,
    dtype: TemporalDType,
}

impl TemporalValue {
    /// # Errors
    /// Returns an error when ticks violate the declared temporal domain.
    pub fn new(value: i64, dtype: TemporalDType) -> Result<Self> {
        dtype.validate_value(value)?;
        Ok(Self { value, dtype })
    }

    #[must_use]
    pub const fn value(&self) -> i64 {
        self.value
    }

    #[must_use]
    pub fn dtype(&self) -> &TemporalDType {
        &self.dtype
    }

    /// Preserve existing scalar aliases when crossing the public scalar API.
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // Opaque values are validated by the constructor.
    pub fn into_scalar(self) -> crate::ScalarValue {
        match self.dtype.logical() {
            LogicalDType::Date32 => crate::ScalarValue::Date32(
                i32::try_from(self.value).expect("validated date32 ticks"),
            ),
            LogicalDType::TimestampMicros => crate::ScalarValue::TimestampMicros(self.value),
            _ => crate::ScalarValue::Temporal(self),
        }
    }
}

/// Convert fixed-unit ticks exactly, rejecting lost precision and overflow.
///
/// # Errors
/// Returns an error when the destination cannot represent the exact value.
pub fn rescale_ticks(value: i64, source: TemporalUnit, target: TemporalUnit) -> Result<i64> {
    let nanos = i128::from(value) * source.nanoseconds();
    if nanos % target.nanoseconds() != 0 {
        return Err(failed(
            "temporal unit conversion would discard fractional precision",
        ));
    }
    i64::try_from(nanos / target.nanoseconds())
        .map_err(|_| failed("temporal unit conversion exceeds signed 64-bit ticks"))
}

pub(crate) fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_names_are_unambiguous_and_preserve_legacy_aliases() {
        for name in [
            "date32",
            "date64",
            "time(s)",
            "time(ms)",
            "time(us)",
            "time(ns)",
            "timestamp(s)",
            "timestamp(ms)",
            "timestamp_micros",
            "timestamp(ns)",
            "timestamp(ns,America/New_York)",
            "timestamp(us,+05:30)",
            "duration(s)",
            "duration(ms)",
            "duration(us)",
            "duration(ns)",
        ] {
            let dtype = TemporalDType::parse(name).unwrap();
            assert_eq!(dtype.as_str(), name);
            assert_eq!(TemporalDType::from_logical(&dtype.logical()), Some(dtype));
        }
        assert_eq!(
            TemporalDType::parse("timestamp(us)").unwrap().logical(),
            LogicalDType::TimestampMicros
        );
        for name in [
            "timestamp(days)",
            "timestamp(ns,)",
            "timestamp(ns,UTC,UTC)",
            "timestamp(ns,/etc/localtime)",
            "timestamp(ns,../UTC)",
            "timestamp(ns,America//New_York)",
            "timestamp(ns,America/New York)",
            "time(s,UTC)",
            "duration(day)",
            "duration(ns)ignored",
            "date64()",
        ] {
            assert!(TemporalDType::parse(name).is_none(), "{name}");
        }
        assert!(TemporalDType::parse(&format!("timestamp(ns,{})", "A".repeat(256))).is_none());
        for (declared, canonical) in [
            (" DATE32 ", "date32"),
            ("Date64", "date64"),
            ("TIMESTAMP( US )", "timestamp_micros"),
            (
                "TIMESTAMP( NS , 'America/New_York' )",
                "timestamp(ns,America/New_York)",
            ),
        ] {
            assert_eq!(
                TemporalDType::parse_declaration(declared).unwrap().as_str(),
                canonical
            );
        }
    }

    #[test]
    fn unit_conversion_is_exact_for_negative_values_and_storage_endpoints() {
        use TemporalUnit::{
            Microseconds as Us, Milliseconds as Ms, Nanoseconds as Ns, Seconds as S,
        };
        for unit in [S, Ms, Us, Ns] {
            for value in [i64::MIN, -1, 0, 1, i64::MAX] {
                assert_eq!(rescale_ticks(value, unit, unit).unwrap(), value);
            }
        }
        assert_eq!(rescale_ticks(-2, S, Ns).unwrap(), -2_000_000_000);
        assert_eq!(rescale_ticks(-1_000, Ns, Us).unwrap(), -1);
        assert!(rescale_ticks(-1, Ns, Us).is_err());
        assert!(rescale_ticks(1_001, Ns, Us).is_err());
        assert!(rescale_ticks(i64::MAX, S, Ns).is_err());
        assert!(rescale_ticks(i64::MIN, Us, Ns).is_err());
        assert_eq!(
            rescale_ticks(i64::MIN / 1_000, Us, Ns).unwrap(),
            i64::MIN / 1_000 * 1_000
        );
    }

    #[test]
    fn value_domains_are_separate_from_integer_width_and_calendar_range() {
        let date = TemporalDType::parse("date64").unwrap();
        assert!(date.validate_value(-86_400_000).is_ok());
        assert!(date.validate_value(1).is_err());
        for unit in [
            TemporalUnit::Seconds,
            TemporalUnit::Milliseconds,
            TemporalUnit::Microseconds,
            TemporalUnit::Nanoseconds,
        ] {
            let time = TemporalDType::new(TemporalKind::Time(unit)).unwrap();
            let end = 86_400 * unit.ticks_per_second();
            assert!(time.validate_value(0).is_ok());
            assert!(time.validate_value(end - 1).is_ok());
            assert!(time.validate_value(end).is_err());
            assert!(time.validate_value(-1).is_err());
            for kind in [
                TemporalKind::Duration(unit),
                TemporalKind::Timestamp {
                    unit,
                    timezone: Some("UTC".to_owned()),
                },
            ] {
                let dtype = TemporalDType::new(kind).unwrap();
                assert!(TemporalValue::new(i64::MIN, dtype.clone()).is_ok());
                assert!(TemporalValue::new(i64::MAX, dtype).is_ok());
            }
        }
    }
}
