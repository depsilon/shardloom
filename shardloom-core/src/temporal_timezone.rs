//! Explicit calendar conversion using pinned, embedded IANA rules.
//! No system timezone/database lookup or global mutable cache is used.

use super::{
    TemporalUnit,
    civil::{CivilDateTime, nanos_to_ticks, parse_offset},
    failed,
};
use crate::Result;
use jiff::tz::{Offset, TimeZone};

pub use super::Ambiguity;

#[must_use]
pub fn database_version() -> &'static str {
    jiff_tzdb::VERSION.unwrap_or("unknown")
}

/// Validate a metadata name against the pinned database without parsing its
/// rules, consulting the host or allocating a timezone object.
///
/// # Errors
/// Rejects unknown names and malformed fixed offsets.
pub fn validate_name(name: &str) -> Result<()> {
    if name.starts_with(['+', '-']) {
        parse_offset(name).map(|_| ())
    } else if jiff_tzdb::get(name).is_some_and(|(canonical, _)| canonical == name) {
        Ok(())
    } else {
        Err(failed(
            "timezone is absent from the pinned IANA database or has noncanonical capitalization",
        ))
    }
}

/// Immutable parsed rules, owned by a bound operation under its memory grant.
#[derive(Debug, Clone)]
pub struct BoundTimeZone {
    zone: TimeZone,
}

impl BoundTimeZone {
    /// Conservative admission before parsing a zone's immutable `TZif` data.
    ///
    /// # Errors
    /// Rejects unknown names, malformed offsets and arithmetic overflow.
    pub fn admission_bytes(name: &str) -> Result<usize> {
        validate_name(name)?;
        let bytes = jiff_tzdb::get(name).map_or(0, |(_, bytes)| bytes.len());
        bytes
            .checked_mul(64)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| failed("timezone workspace admission overflow"))
    }

    /// Bind the rules once, after the caller admits `admission_bytes`.
    ///
    /// # Errors
    /// Rejects unsupported names, provider parsing errors and oversized state.
    pub fn new(name: &str) -> Result<Self> {
        let admitted = Self::admission_bytes(name)?;
        let zone = if name.starts_with(['+', '-']) {
            TimeZone::fixed(
                Offset::from_seconds(parse_offset(name)?)
                    .map_err(|_| failed("UTC offset exceeds calendar provider range"))?,
            )
        } else {
            let (canonical, bytes) = jiff_tzdb::get(name)
                .ok_or_else(|| failed("timezone disappeared from the pinned database"))?;
            TimeZone::tzif(canonical, bytes)
                .map_err(|_| failed("pinned timezone rules could not be parsed"))?
        };
        if zone
            .memory_usage()
            .saturating_add(std::mem::size_of::<Self>())
            > admitted
        {
            return Err(failed(
                "timezone provider state exceeded its admitted workspace",
            ));
        }
        Ok(Self { zone })
    }

    /// # Errors
    /// Rejects ticks outside the explicitly bounded calendar provider range.
    pub fn civil(&self, ticks: i64, unit: TemporalUnit) -> Result<CivilDateTime> {
        let instant = instant(ticks, unit)?;
        let civil = self.zone.to_datetime(instant);
        Ok(CivilDateTime {
            year: i64::from(civil.year()),
            month: u8::try_from(civil.month()).map_err(|_| range_error())?,
            day: u8::try_from(civil.day()).map_err(|_| range_error())?,
            hour: u8::try_from(civil.hour()).map_err(|_| range_error())?,
            minute: u8::try_from(civil.minute()).map_err(|_| range_error())?,
            second: u8::try_from(civil.second()).map_err(|_| range_error())?,
            nanosecond: u32::try_from(civil.subsec_nanosecond()).map_err(|_| range_error())?,
        })
    }

    /// # Errors
    /// Rejects invalid/out-of-range civil fields, ambiguity under `Reject`,
    /// precision loss and output tick overflow.
    pub fn timestamp(
        &self,
        civil: CivilDateTime,
        unit: TemporalUnit,
        ambiguity: Ambiguity,
    ) -> Result<i64> {
        civil.validate()?;
        let year = i16::try_from(civil.year).map_err(|_| range_error())?;
        let datetime = jiff::civil::DateTime::new(
            year,
            i8::try_from(civil.month).map_err(|_| range_error())?,
            i8::try_from(civil.day).map_err(|_| range_error())?,
            i8::try_from(civil.hour).map_err(|_| range_error())?,
            i8::try_from(civil.minute).map_err(|_| range_error())?,
            i8::try_from(civil.second).map_err(|_| range_error())?,
            i32::try_from(civil.nanosecond).map_err(|_| range_error())?,
        )
        .map_err(|_| range_error())?;
        let ambiguous = self.zone.to_ambiguous_timestamp(datetime);
        let timestamp = match ambiguity {
            Ambiguity::Reject => ambiguous.unambiguous(),
            Ambiguity::Earlier => ambiguous.earlier(),
            Ambiguity::Later => ambiguous.later(),
        }.map_err(|_| failed("civil time is ambiguous, nonexistent or outside calendar range; choose earlier/later explicitly for a gap or fold"))?;
        nanos_to_ticks(timestamp.as_nanosecond(), unit)
    }

    /// # Errors
    /// Rejects ticks outside the calendar provider's supported range.
    pub fn offset_seconds(&self, ticks: i64, unit: TemporalUnit) -> Result<i32> {
        Ok(self.zone.to_offset(instant(ticks, unit)?).seconds())
    }
}

fn instant(ticks: i64, unit: TemporalUnit) -> Result<jiff::Timestamp> {
    let nanos = i128::from(ticks) * unit.nanoseconds();
    // Jiff 0.2.35's from_nanosecond delegates to jiff-core 0.1.0, whose
    // constructor checks i64 storage but omits the calendar bounds before an
    // internal unchecked construction. Use the checked seconds/fraction API.
    let seconds = i64::try_from(nanos / 1_000_000_000).map_err(|_| range_error())?;
    let fraction = i32::try_from(nanos % 1_000_000_000).expect("fraction below one second");
    jiff::Timestamp::new(seconds, fraction).map_err(|_| range_error())
}

fn range_error() -> crate::ShardLoomError {
    failed(
        "timestamp is outside the timezone calendar range; exact native tick storage remains supported",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporal::civil::parse_timestamp;

    fn civil(text: &str) -> CivilDateTime {
        parse_timestamp(text).unwrap().0
    }
    fn utc(text: &str) -> i64 {
        civil(text).ticks(TemporalUnit::Seconds).unwrap()
    }

    #[test]
    fn pinned_new_york_gap_fold_and_daily_duration() {
        assert_eq!(database_version(), "2026a");
        let zone = BoundTimeZone::new("America/New_York").unwrap();
        let gap = civil("2024-03-10T02:30:00");
        let fold = civil("2024-11-03T01:30:00");
        let unit = TemporalUnit::Seconds;
        assert!(zone.timestamp(gap, unit, Ambiguity::Reject).is_err());
        assert_eq!(
            zone.timestamp(gap, unit, Ambiguity::Earlier).unwrap(),
            utc("2024-03-10T06:30:00")
        );
        assert_eq!(
            zone.timestamp(gap, unit, Ambiguity::Later).unwrap(),
            utc("2024-03-10T07:30:00")
        );
        assert!(zone.timestamp(fold, unit, Ambiguity::Reject).is_err());
        assert_eq!(
            zone.timestamp(fold, unit, Ambiguity::Earlier).unwrap(),
            utc("2024-11-03T05:30:00")
        );
        assert_eq!(
            zone.timestamp(fold, unit, Ambiguity::Later).unwrap(),
            utc("2024-11-03T06:30:00")
        );
        for (day, hours) in [("2024-03-10T00:00:00", 23), ("2024-11-03T00:00:00", 25)] {
            let today = civil(day);
            let start = zone.timestamp(today, unit, Ambiguity::Reject).unwrap();
            let end = zone
                .timestamp(today.add_days(1).unwrap(), unit, Ambiguity::Reject)
                .unwrap();
            assert_eq!(end - start, hours * 3_600);
        }
    }

    #[test]
    fn half_hour_transition_offsets_and_calendar_range_are_explicit() {
        let unit = TemporalUnit::Nanoseconds;
        let zone = BoundTimeZone::new("Australia/Lord_Howe").unwrap();
        let repeated = civil("2024-04-07T01:45:00.000000123");
        let earlier = zone.timestamp(repeated, unit, Ambiguity::Earlier).unwrap();
        let later = zone.timestamp(repeated, unit, Ambiguity::Later).unwrap();
        assert_eq!(later - earlier, 1_800_000_000_000);
        assert_eq!(zone.civil(earlier, unit).unwrap(), repeated);
        assert_eq!(zone.civil(later, unit).unwrap(), repeated);
        let fixed = BoundTimeZone::new("+05:30").unwrap();
        assert_eq!(fixed.offset_seconds(0, unit).unwrap(), 19_800);
        assert_eq!(
            fixed.civil(0, unit).unwrap().datetime_text(unit).unwrap(),
            "1970-01-01T05:30:00"
        );
        assert!(zone.civil(i64::MAX, TemporalUnit::Seconds).is_err());
        assert!(zone.civil(i64::MIN, TemporalUnit::Seconds).is_err());
        for edge in [
            jiff::Timestamp::MIN.as_second(),
            jiff::Timestamp::MAX.as_second(),
        ] {
            assert!(instant(edge, TemporalUnit::Seconds).is_ok());
        }
        assert!(instant(jiff::Timestamp::MIN.as_second() - 1, TemporalUnit::Seconds).is_err());
        assert!(instant(jiff::Timestamp::MAX.as_second() + 1, TemporalUnit::Seconds).is_err());
        assert!(
            zone.timestamp(civil("12000-01-01T00:00:00"), unit, Ambiguity::Reject)
                .is_err()
        );
        for name in [
            "Not/A/Zone",
            "america/new_york",
            "+24:00",
            "",
            "/etc/localtime",
        ] {
            assert!(validate_name(name).is_err(), "{name}");
        }
    }
}
