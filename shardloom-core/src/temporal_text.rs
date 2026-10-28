//! Shared exact temporal text conversion for input and scalar execution.
//! Calendar rules are bound once by the caller's schema/expression owner.

#[cfg(feature = "temporal-calendar")]
use super::timezone::{Ambiguity, BoundTimeZone};
use super::{
    TemporalDType, TemporalKind as Kind, TemporalUnit as Unit, TemporalValue,
    civil::{self, CivilDateTime, NANOS_PER_DAY, NANOS_PER_SECOND},
    failed,
};
use crate::{Result, ScalarValue};

/// Immutable text conversion bound to one exact temporal type.
#[derive(Debug, Clone)]
pub struct TemporalTextCodec {
    dtype: TemporalDType,
    #[cfg(feature = "temporal-calendar")]
    zone: Option<BoundTimeZone>,
}

impl TemporalTextCodec {
    /// Conservative workspace required before binding and retaining a codec.
    ///
    /// # Errors
    /// Rejects unavailable timezone support or an invalid pinned zone name.
    pub fn admission_bytes(dtype: &TemporalDType) -> Result<usize> {
        let base = 2048 + dtype.heap_bytes();
        if let Some(zone) = dtype.timezone() {
            #[cfg(feature = "temporal-calendar")]
            return BoundTimeZone::admission_bytes(zone)?
                .checked_add(base)
                .ok_or_else(|| failed("temporal text admission overflow"));
            #[cfg(not(feature = "temporal-calendar"))]
            {
                let _ = zone;
                return Err(failed(
                    "timezone text conversion requires temporal-calendar",
                ));
            }
        }
        Ok(base)
    }

    /// Bind after the owner admits `admission_bytes`; no host zone is inferred.
    ///
    /// # Errors
    /// Rejects unavailable timezone support or invalid pinned rule metadata.
    pub fn new(dtype: TemporalDType) -> Result<Self> {
        Self::admission_bytes(&dtype)?;
        #[cfg(feature = "temporal-calendar")]
        let zone = dtype.timezone().map(BoundTimeZone::new).transpose()?;
        Ok(Self {
            dtype,
            #[cfg(feature = "temporal-calendar")]
            zone,
        })
    }

    #[must_use]
    pub fn dtype(&self) -> &TemporalDType {
        &self.dtype
    }

    /// Parse ISO civil fields, exact signed duration ticks, or an explicitly
    /// offset timestamp. Zoned civil text rejects gaps and folds by default.
    ///
    /// # Errors
    /// Rejects malformed text, precision loss, overflow and calendar ambiguity.
    pub fn parse(&self, text: &str) -> Result<i64> {
        let ticks = match self.dtype.kind() {
            Kind::Date32 | Kind::Date64 => {
                let (year, month, day) = civil::parse_date(text)?;
                let days = civil::days_from_civil(year, month, day);
                i64::try_from(if matches!(self.dtype.kind(), Kind::Date32) { days } else { days * 86_400_000 })
                    .map_err(|_| failed("date conversion exceeds its storage domain"))?
            }
            Kind::Time(unit) => {
                let (hour, minute, second, fraction) = civil::parse_time(text)?;
                let nanos = (i128::from(hour) * 3600 + i128::from(minute) * 60 + i128::from(second)) * NANOS_PER_SECOND + i128::from(fraction);
                civil::nanos_to_ticks(nanos, *unit)?
            }
            Kind::Duration(unit) => text.strip_suffix(unit.as_str()).unwrap_or(text)
                .parse().map_err(|_| failed("duration text requires exact signed ticks and an optional matching unit suffix"))?,
            Kind::Timestamp { unit, .. } => {
                let (date, offset) = civil::parse_timestamp(text)?;
                if let Some(offset) = offset {
                    // Keep the original timestamp_micros offset normalization.
                    civil::nanos_to_ticks(date.epoch_nanoseconds()? - i128::from(offset) * NANOS_PER_SECOND, *unit)?
                } else {
                    self.local_ticks(date, *unit)?
                }
            }
        };
        self.dtype.validate_value(ticks)?;
        Ok(ticks)
    }

    /// Parse into the shared scalar representation, retaining legacy aliases.
    ///
    /// # Errors
    /// Returns the same validation errors as `parse`.
    pub fn parse_scalar(&self, text: &str) -> Result<ScalarValue> {
        Ok(TemporalValue::new(self.parse(text)?, self.dtype.clone())?.into_scalar())
    }

    fn local_ticks(&self, date: CivilDateTime, unit: Unit) -> Result<i64> {
        #[cfg(feature = "temporal-calendar")]
        if let Some(zone) = &self.zone {
            return zone.timestamp(date, unit, Ambiguity::Reject);
        }
        date.ticks(unit)
    }

    /// Format exact ticks using their declared precision and pinned zone.
    ///
    /// # Errors
    /// Rejects invalid restricted values or zoned values outside calendar range.
    /// Raw timestamp and duration storage remains valid outside that range.
    pub fn format(&self, value: i64) -> Result<String> {
        self.dtype.validate_value(value)?;
        match self.dtype.kind() {
            Kind::Date32 => Ok(crate::expression::format_iso_date32(
                i32::try_from(value)
                    .map_err(|_| failed("date32 ticks exceed signed 32-bit range"))?,
            )),
            Kind::Date64 => Ok(CivilDateTime::from_nanoseconds(
                i128::from(value / 86_400_000) * NANOS_PER_DAY,
            )?
            .date_text()),
            Kind::Time(unit) => CivilDateTime::from_ticks(value, *unit).time_text(*unit),
            Kind::Duration(unit) => Ok(format!("{value}{unit}")),
            Kind::Timestamp { unit, .. } => self.timestamp_text(value, *unit),
        }
    }

    fn timestamp_text(&self, value: i64, unit: Unit) -> Result<String> {
        #[cfg(feature = "temporal-calendar")]
        if let Some(zone) = &self.zone {
            use std::fmt::Write as _;
            let mut text = zone.civil(value, unit)?.datetime_text(unit)?;
            let offset = zone.offset_seconds(value, unit)?;
            let magnitude = offset.unsigned_abs();
            write!(
                text,
                "{}{:02}:{:02}",
                if offset < 0 { '-' } else { '+' },
                magnitude / 3600,
                magnitude / 60 % 60
            )
            .map_err(|_| failed("timezone offset formatting failed"))?;
            if magnitude % 60 != 0 {
                write!(text, ":{:02}", magnitude % 60)
                    .map_err(|_| failed("timezone offset formatting failed"))?;
            }
            return Ok(text);
        }
        if unit == Unit::Microseconds {
            Ok(crate::expression::format_iso_timestamp_micros(value))
        } else {
            CivilDateTime::from_ticks(value, unit).datetime_text(unit)
        }
    }
}
