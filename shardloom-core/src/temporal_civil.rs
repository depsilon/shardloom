//! Checked proleptic Gregorian arithmetic for the full admitted tick domains.
//! No clock, timezone database, external provider or floating point is used.

use super::{TemporalUnit, failed};
use crate::Result;
use std::fmt::Write as _;

pub const NANOS_PER_SECOND: i128 = 1_000_000_000;
pub const NANOS_PER_DAY: i128 = 86_400 * NANOS_PER_SECOND;

/// Gregorian civil fields. Validation is performed by conversion operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CivilDateTime {
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nanosecond: u32,
}

/// Month-end behavior for explicit calendar arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonthEnd {
    Reject,
    Clamp,
}

impl MonthEnd {
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "reject" => Some(Self::Reject),
            "clamp" => Some(Self::Clamp),
            _ => None,
        }
    }
}

impl CivilDateTime {
    /// Interpret raw naive ticks as civil time, using Euclidean division before
    /// the epoch so fractional negative timestamps belong to the preceding day.
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // The i64 tick domain fits Gregorian i64 years.
    pub fn from_ticks(value: i64, unit: TemporalUnit) -> Self {
        Self::from_nanoseconds(i128::from(value) * unit.nanoseconds())
            .expect("every i64 temporal tick domain fits Gregorian i64 years")
    }

    /// # Errors
    /// Rejects a calendar year outside the signed-64-bit domain.
    pub fn from_nanoseconds(nanos: i128) -> Result<Self> {
        let days = nanos.div_euclid(NANOS_PER_DAY);
        let remainder = nanos.rem_euclid(NANOS_PER_DAY);
        let (year, month, day) = civil_from_days(days)?;
        Ok(Self {
            year,
            month,
            day,
            hour: u8::try_from(remainder / (3_600 * NANOS_PER_SECOND))
                .map_err(|_| failed("invalid civil hour"))?,
            minute: u8::try_from(remainder / (60 * NANOS_PER_SECOND) % 60)
                .map_err(|_| failed("invalid civil minute"))?,
            second: u8::try_from(remainder / NANOS_PER_SECOND % 60)
                .map_err(|_| failed("invalid civil second"))?,
            nanosecond: u32::try_from(remainder % NANOS_PER_SECOND)
                .map_err(|_| failed("invalid civil fraction"))?,
        })
    }

    /// # Errors
    /// Rejects invalid dates, leap-second notation and invalid clock fields.
    pub fn epoch_nanoseconds(self) -> Result<i128> {
        self.validate()?;
        Ok(
            days_from_civil(self.year, self.month, self.day) * NANOS_PER_DAY
                + (i128::from(self.hour) * 3_600
                    + i128::from(self.minute) * 60
                    + i128::from(self.second))
                    * NANOS_PER_SECOND
                + i128::from(self.nanosecond),
        )
    }

    /// # Errors
    /// Rejects invalid fields, precision loss and destination tick overflow.
    pub fn ticks(self, unit: TemporalUnit) -> Result<i64> {
        nanos_to_ticks(self.epoch_nanoseconds()?, unit)
    }

    /// # Errors
    /// Rejects a date not present in the Gregorian calendar or invalid time fields.
    pub fn validate(self) -> Result<()> {
        if !(1..=12).contains(&self.month)
            || self.day == 0
            || self.day > days_in_month(self.year, self.month)
            || self.hour > 23
            || self.minute > 59
            || self.second > 59
            || self.nanosecond >= 1_000_000_000
        {
            return Err(failed(
                "invalid Gregorian date/time; leap seconds are not admitted",
            ));
        }
        Ok(())
    }

    /// # Errors
    /// Rejects calendar overflow or a missing month-end under `Reject` policy.
    pub fn add_months(mut self, months: i64, policy: MonthEnd) -> Result<Self> {
        self.validate()?;
        let month = i128::from(self.year) * 12 + i128::from(self.month) - 1 + i128::from(months);
        self.year = i64::try_from(month.div_euclid(12))
            .map_err(|_| failed("calendar month arithmetic exceeds year range"))?;
        self.month =
            u8::try_from(month.rem_euclid(12) + 1).map_err(|_| failed("invalid calendar month"))?;
        let last = days_in_month(self.year, self.month);
        if self.day > last {
            if policy == MonthEnd::Reject {
                return Err(failed(
                    "calendar month arithmetic reaches a missing day; choose clamp explicitly",
                ));
            }
            self.day = last;
        }
        Ok(self)
    }

    /// # Errors
    /// Rejects calendar overflow or invalid input fields.
    pub fn add_days(self, days: i64) -> Result<Self> {
        Self::from_nanoseconds(self.epoch_nanoseconds()? + i128::from(days) * NANOS_PER_DAY)
    }

    #[must_use]
    pub fn date_text(self) -> String {
        let year = if self.year < 0 {
            format!("-{:04}", self.year.unsigned_abs())
        } else {
            format!("{:04}", self.year)
        };
        format!("{year}-{:02}-{:02}", self.month, self.day)
    }

    /// Format exact civil fields with the fractional width of the declared unit.
    ///
    /// # Errors
    /// Rejects invalid fields or precision incompatible with the declared unit.
    pub fn time_text(self, unit: TemporalUnit) -> Result<String> {
        self.validate()?;
        if i128::from(self.nanosecond) % unit.nanoseconds() != 0 {
            return Err(failed(
                "civil fractional precision exceeds its temporal unit",
            ));
        }
        let mut text = format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second);
        if self.nanosecond != 0 {
            let width = match unit {
                TemporalUnit::Seconds => 0,
                TemporalUnit::Milliseconds => 3,
                TemporalUnit::Microseconds => 6,
                TemporalUnit::Nanoseconds => 9,
            };
            let fraction = i128::from(self.nanosecond) / unit.nanoseconds();
            write!(text, ".{fraction:0width$}")
                .map_err(|_| failed("civil fraction formatting failed"))?;
        }
        Ok(text)
    }

    /// # Errors
    /// Rejects invalid fields or precision incompatible with the declared unit.
    pub fn datetime_text(self, unit: TemporalUnit) -> Result<String> {
        Ok(format!("{}T{}", self.date_text(), self.time_text(unit)?))
    }
}

/// # Errors
/// Rejects loss of fractional precision and destination tick overflow.
pub fn nanos_to_ticks(nanos: i128, unit: TemporalUnit) -> Result<i64> {
    if nanos % unit.nanoseconds() != 0 {
        return Err(failed(
            "temporal unit conversion would discard fractional precision",
        ));
    }
    i64::try_from(nanos / unit.nanoseconds())
        .map_err(|_| failed("temporal value exceeds signed 64-bit ticks"))
}

#[must_use]
pub const fn days_in_month(year: i64, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 400 == 0 || (year % 4 == 0 && year % 100 != 0) => 29,
        2 => 28,
        _ => 0,
    }
}

#[must_use]
pub fn days_from_civil(year: i64, month: u8, day: u8) -> i128 {
    let year = i128::from(year) - i128::from(month <= 2);
    let era = year.div_euclid(400);
    let year = year.rem_euclid(400);
    let month = i128::from(month) + if month > 2 { -3 } else { 9 };
    let ordinal = (153 * month + 2) / 5 + i128::from(day) - 1;
    era * 146_097 + year * 365 + year / 4 - year / 100 + ordinal - 719_468
}

/// # Errors
/// Rejects epoch-day values whose calendar year cannot fit signed 64 bits.
pub fn civil_from_days(days: i128) -> Result<(i64, u8, u8)> {
    let days = days
        .checked_add(719_468)
        .ok_or_else(|| failed("Gregorian day range overflow"))?;
    let era = days.div_euclid(146_097);
    let day = days.rem_euclid(146_097);
    let year = (day - day / 1460 + day / 36_524 - day / 146_096) / 365;
    let ordinal = day - (365 * year + year / 4 - year / 100);
    let month = (5 * ordinal + 2) / 153;
    let day = ordinal - (153 * month + 2) / 5 + 1;
    let month = month + if month < 10 { 3 } else { -9 };
    let year = year + era * 400 + i128::from(month <= 2);
    Ok((
        i64::try_from(year).map_err(|_| failed("Gregorian year exceeds signed 64-bit range"))?,
        u8::try_from(month).map_err(|_| failed("invalid Gregorian month"))?,
        u8::try_from(day).map_err(|_| failed("invalid Gregorian day"))?,
    ))
}

/// Parse ISO civil date and time plus an optional explicit `Z`/`+HH:MM` offset.
/// No timezone is inferred when an offset is absent.
///
/// # Errors
/// Rejects malformed fields, leap seconds and more than nine fractional digits.
pub fn parse_timestamp(text: &str) -> Result<(CivilDateTime, Option<i32>)> {
    if !text.is_ascii() || text.len() > 128 {
        return Err(failed("timestamp text requires bounded ASCII ISO syntax"));
    }
    let (date, time) = text
        .split_once('T')
        .ok_or_else(|| failed("timestamp text requires an ISO date and T-separated time"))?;
    let (year, month, day) = parse_date(date)?;
    let (time, offset) = if let Some(time) = time.strip_suffix('Z') {
        (time, Some(0))
    } else if let Some(index) = time.find(['+', '-']) {
        (&time[..index], Some(parse_offset(&time[index..])?))
    } else {
        (time, None)
    };
    let (hour, minute, second, nanosecond) = parse_time(time)?;
    let result = CivilDateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
    };
    result.validate()?;
    Ok((result, offset))
}

/// # Errors
/// Rejects malformed or invalid ISO dates. Signed extended years are admitted.
pub fn parse_date(text: &str) -> Result<(i64, u8, u8)> {
    if !text.is_ascii() || text.len() > 32 {
        return Err(failed("date text requires bounded ASCII ISO syntax"));
    }
    let (prefix, day) = text
        .rsplit_once('-')
        .ok_or_else(|| failed("date requires YYYY-MM-DD"))?;
    let (year, month) = prefix
        .rsplit_once('-')
        .ok_or_else(|| failed("date requires YYYY-MM-DD"))?;
    let digits = year.strip_prefix(['-', '+']).unwrap_or(year);
    if digits.len() < 4 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(failed("date year requires at least four decimal digits"));
    }
    let year = year
        .parse::<i64>()
        .map_err(|_| failed("date year exceeds signed 64-bit range"))?;
    let month = two_digits(month)?;
    let day = two_digits(day)?;
    if day == 0 || day > days_in_month(year, month) {
        return Err(failed("date is not present in the Gregorian calendar"));
    }
    Ok((year, month, day))
}

/// # Errors
/// Rejects malformed ISO clock fields, leap seconds or excess precision.
pub fn parse_time(text: &str) -> Result<(u8, u8, u8, u32)> {
    let (clock, fraction) = text
        .split_once('.')
        .map_or((text, None), |(a, b)| (a, Some(b)));
    let mut parts = clock.split(':');
    let hour = two_digits(parts.next().unwrap_or(""))?;
    let minute = two_digits(parts.next().unwrap_or(""))?;
    let second = two_digits(parts.next().unwrap_or(""))?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return Err(failed(
            "time requires HH:MM:SS; leap seconds are not admitted",
        ));
    }
    let nanosecond = if let Some(fraction) = fraction {
        if fraction.is_empty()
            || fraction.len() > 9
            || !fraction.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(failed("time fraction requires one to nine decimal digits"));
        }
        fraction
            .parse::<u32>()
            .map_err(|_| failed("invalid time fraction"))?
            * 10_u32.pow(
                9 - u32::try_from(fraction.len()).map_err(|_| failed("invalid fraction width"))?,
            )
    } else {
        0
    };
    Ok((hour, minute, second, nanosecond))
}

/// # Errors
/// Rejects offsets other than explicit signed `HH:MM[:SS]` with hours below 24.
pub fn parse_offset(text: &str) -> Result<i32> {
    if !text.is_ascii()
        || !matches!(text.len(), 6 | 9)
        || !text.starts_with(['+', '-'])
        || text.as_bytes()[3] != b':'
    {
        return Err(failed("UTC offset requires signed HH:MM or HH:MM:SS"));
    }
    let hour = two_digits(&text[1..3])?;
    let minute = two_digits(&text[4..6])?;
    let second = if text.len() == 9 {
        if text.as_bytes()[6] != b':' {
            return Err(failed("UTC offset seconds require a colon separator"));
        }
        two_digits(&text[7..9])?
    } else {
        0
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(failed("UTC offset fields exceed their ranges"));
    }
    let seconds = i32::from(hour) * 3_600 + i32::from(minute) * 60 + i32::from(second);
    Ok(if text.starts_with('-') {
        -seconds
    } else {
        seconds
    })
}

fn two_digits(text: &str) -> Result<u8> {
    if text.len() != 2 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(failed("ISO date/time fields require two decimal digits"));
    }
    text.parse()
        .map_err(|_| failed("invalid ISO date/time field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_tick_domains_keep_pre_epoch_fraction_and_endpoints() {
        for unit in [
            TemporalUnit::Seconds,
            TemporalUnit::Milliseconds,
            TemporalUnit::Microseconds,
            TemporalUnit::Nanoseconds,
        ] {
            for value in [
                i64::MIN,
                i64::MIN + 1,
                -86_400_000_001,
                -1,
                0,
                1,
                i64::MAX - 1,
                i64::MAX,
            ] {
                let civil = CivilDateTime::from_ticks(value, unit);
                assert_eq!(civil.ticks(unit).unwrap(), value);
                let (parsed, offset) =
                    parse_timestamp(&civil.datetime_text(unit).unwrap()).unwrap();
                assert_eq!(parsed, civil);
                assert_eq!(offset, None);
            }
        }
        let before = CivilDateTime::from_ticks(-1, TemporalUnit::Nanoseconds);
        assert_eq!(
            before.datetime_text(TemporalUnit::Nanoseconds).unwrap(),
            "1969-12-31T23:59:59.999999999"
        );
        assert_eq!(
            CivilDateTime::from_ticks(i64::MAX, TemporalUnit::Seconds).year,
            292_277_026_596
        );
        assert_eq!(
            CivilDateTime::from_ticks(i64::MIN, TemporalUnit::Seconds).year,
            -292_277_022_657
        );
    }

    #[test]
    fn gregorian_boundaries_and_month_end_policy_are_independent() {
        for (text, days) in [
            ("1970-01-01", 0),
            ("1969-12-31", -1),
            ("2000-02-29", 11_016),
            ("2024-02-29", 19_782),
            ("1900-03-01", -25_508),
        ] {
            let (y, m, d) = parse_date(text).unwrap();
            assert_eq!(days_from_civil(y, m, d), days);
            assert_eq!(civil_from_days(days).unwrap(), (y, m, d));
        }
        let (january, _) = parse_timestamp("2024-01-31T12:30:01.000000123").unwrap();
        assert!(january.add_months(1, MonthEnd::Reject).is_err());
        assert_eq!(
            january
                .add_months(1, MonthEnd::Clamp)
                .unwrap()
                .datetime_text(TemporalUnit::Nanoseconds)
                .unwrap(),
            "2024-02-29T12:30:01.000000123"
        );
        assert_eq!(
            january
                .add_months(-13, MonthEnd::Reject)
                .unwrap()
                .date_text(),
            "2022-12-31"
        );
        assert_eq!(january.add_days(30).unwrap().date_text(), "2024-03-01");
        for text in ["1900-02-29", "2024-00-00", "2023-02-29", "2024-01-32"] {
            assert!(parse_date(text).is_err());
        }
        for text in [
            "2024-01-01T23:59:60Z",
            "2024-01-01T24:00:00Z",
            "2024-01-01T00:00:00.1234567890Z",
            "2024-01-01T00:00:00+24:00",
        ] {
            assert!(parse_timestamp(text).is_err());
        }
    }
}
