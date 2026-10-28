// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom, 2026-10-27: preserve the full signed tick domain for
// every timestamp unit and format out-of-calendar values without panicking.
// Resolve named zones from the pinned embedded rules, never the host database.

//! Temporal extension data types.

use std::fmt;
use std::sync::Arc;

use jiff::fmt::temporal::DateTimeParser;
use jiff::tz::TimeZone;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_session::registry::CachedId;

use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::dtype::extension::ExtDType;
use crate::dtype::extension::ExtId;
use crate::dtype::extension::ExtVTable;
use crate::extension::datetime::TimeUnit;
use crate::scalar::ScalarValue;

/// Timestamp DType.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Timestamp;

impl Timestamp {
    /// Creates a new Timestamp extension =dtype with the given time unit and nullability.
    pub fn new(time_unit: TimeUnit, nullability: Nullability) -> ExtDType<Self> {
        Self::new_with_tz(time_unit, None, nullability)
    }

    /// Creates a new Timestamp extension dtype with the given time unit, timezone, and nullability.
    pub fn new_with_tz(
        time_unit: TimeUnit,
        timezone: Option<Arc<str>>,
        nullability: Nullability,
    ) -> ExtDType<Self> {
        ExtDType::try_new(
            TimestampOptions {
                unit: time_unit,
                tz: timezone,
            },
            DType::Primitive(PType::I64, nullability),
        )
        .vortex_expect("failed to create timestamp dtype")
    }

    /// Creates a new `Timestamp` extension dtype with the given options and nullability.
    pub fn new_with_options(options: TimestampOptions, nullability: Nullability) -> ExtDType<Self> {
        ExtDType::try_new(options, DType::Primitive(PType::I64, nullability))
            .vortex_expect("failed to create timestamp dtype")
    }
}

/// Options for the Timestamp DType.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimestampOptions {
    /// The time unit of the timestamp.
    pub unit: TimeUnit,
    /// The timezone of the timestamp, if any.
    pub tz: Option<Arc<str>>,
}

impl fmt::Display for TimestampOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.tz {
            Some(tz) => write!(f, "{}, tz={}", self.unit, tz),
            None => write!(f, "{}", self.unit),
        }
    }
}

/// Resolve an Arrow timestamp timezone, accepting IANA names and fixed offsets.
pub(crate) fn resolve_time_zone(name: Option<&str>) -> VortexResult<TimeZone> {
    Ok(match name {
        Some(name) if name.starts_with('+') || name.starts_with('-') => {
            DateTimeParser::new().parse_time_zone(name)?
        }
        Some(name) => {
            let (canonical, bytes) = jiff_tzdb::get(name)
                .filter(|(canonical, _)| *canonical == name)
                .ok_or_else(|| vortex_err!("Timezone is absent from the pinned IANA database"))?;
            TimeZone::tzif(canonical, bytes)?
        }
        None => TimeZone::UTC,
    })
}

/// Unpacked value of a [`Timestamp`] extension scalar.
///
/// Each variant carries the raw storage value and an optional timezone.
pub enum TimestampValue<'a> {
    /// Seconds since the Unix epoch.
    Seconds(i64, Option<&'a Arc<str>>),
    /// Milliseconds since the Unix epoch.
    Milliseconds(i64, Option<&'a Arc<str>>),
    /// Microseconds since the Unix epoch.
    Microseconds(i64, Option<&'a Arc<str>>),
    /// Nanoseconds since the Unix epoch.
    Nanoseconds(i64, Option<&'a Arc<str>>),
}

impl fmt::Display for TimestampValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (value, unit, timestamp, tz) = match self {
            TimestampValue::Seconds(v, tz) => (*v, "s", jiff::Timestamp::from_second(*v), *tz),
            TimestampValue::Milliseconds(v, tz) => (*v, "ms", jiff::Timestamp::from_millisecond(*v), *tz),
            TimestampValue::Microseconds(v, tz) => (*v, "us", jiff::Timestamp::from_microsecond(*v), *tz),
            TimestampValue::Nanoseconds(v, tz) => (*v, "ns", jiff::Timestamp::new(*v / 1_000_000_000, (*v % 1_000_000_000) as i32), *tz),
        };
        if let Ok(timestamp) = timestamp {
            match tz {
                None => return write!(f, "{timestamp}"),
                Some(tz) => {
                    if let Ok(zone) = resolve_time_zone(Some(tz)) {
                        return write!(f, "{}", timestamp.to_zoned(zone));
                    }
                }
            }
        }
        write!(f, "{value}{unit} since Unix epoch")?;
        if let Some(tz) = tz {
            write!(f, " [{tz}]")?;
        }
        Ok(())
    }
}

impl ExtVTable for Timestamp {
    type Metadata = TimestampOptions;

    type NativeValue<'a> = TimestampValue<'a>;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("vortex.timestamp");
        *ID
    }

    // NOTE(ngates): unfortunately we're stuck with this hand-rolled serialization format for
    //  backwards compatibility.
    fn serialize_metadata(&self, metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        let mut bytes = Vec::with_capacity(4);
        let unit_tag: u8 = metadata.unit.into();

        bytes.push(unit_tag);

        // Encode time_zone as u16 length followed by utf8 bytes.
        match &metadata.tz {
            None => bytes.extend_from_slice(0u16.to_le_bytes().as_slice()),
            Some(tz) => {
                let tz_bytes = tz.as_bytes();
                let tz_len = u16::try_from(tz_bytes.len())
                    .unwrap_or_else(|err| vortex_panic!("tz did not fit in u16: {}", err));
                bytes.extend_from_slice(tz_len.to_le_bytes().as_slice());
                bytes.extend_from_slice(tz_bytes);
            }
        }

        Ok(bytes)
    }

    fn deserialize_metadata(&self, data: &[u8]) -> VortexResult<Self::Metadata> {
        vortex_ensure!(
            data.len() >= 3,
            "Timestamp metadata must have at least 3 bytes, got {}",
            data.len()
        );

        let tag = data[0];
        let time_unit = TimeUnit::try_from(tag)?;
        let tz_len_bytes: [u8; 2] = data[1..3]
            .try_into()
            .ok()
            .vortex_expect("Verified to have two bytes");
        let tz_len = u16::from_le_bytes(tz_len_bytes) as usize;
        if tz_len == 0 {
            return Ok(TimestampOptions {
                unit: time_unit,
                tz: None,
            });
        }

        // Attempt to load from len-prefixed bytes
        vortex_ensure!(
            data.len() >= 3 + tz_len,
            "Timestamp metadata is truncated: declared timezone length {} but only {} bytes available",
            tz_len,
            data.len() - 3
        );
        let tz_bytes = &data[3..3 + tz_len];
        let tz: Arc<str> = str::from_utf8(tz_bytes)
            .map_err(|e| vortex_err!("timezone is not valid utf8 string: {e}"))?
            .to_string()
            .into();
        Ok(TimestampOptions {
            unit: time_unit,
            tz: Some(tz),
        })
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        vortex_ensure!(
            matches!(ext_dtype.storage_dtype(), DType::Primitive(PType::I64, _)),
            "Timestamp storage dtype must be i64"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        let metadata = ext_dtype.metadata();
        let ts_value = storage_value.as_primitive().cast::<i64>()?;
        let tz = metadata.tz.as_ref();

        // Calendar operations have a narrower domain than storage, constants
        // and statistics. Never construct a bounded Span to validate raw ticks.
        let value = match metadata.unit {
            TimeUnit::Nanoseconds => TimestampValue::Nanoseconds(ts_value, tz),
            TimeUnit::Microseconds => TimestampValue::Microseconds(ts_value, tz),
            TimeUnit::Milliseconds => TimestampValue::Milliseconds(ts_value, tz),
            TimeUnit::Seconds => TimestampValue::Seconds(ts_value, tz),
            TimeUnit::Days => vortex_bail!("Timestamp does not support Days time unit"),
        };

        if let Some(tz) = tz {
            // Scalar/statistics construction must not parse rule tables on
            // every row. Native admission owns the stricter metadata contract.
            if tz.starts_with('+') || tz.starts_with('-') {
                DateTimeParser::new().parse_time_zone(tz.as_ref())?;
            } else {
                vortex_ensure!(
                    jiff_tzdb::get(tz).is_some_and(|(canonical, _)| canonical == tz.as_ref()),
                    "Timezone is absent from the pinned IANA database"
                );
            }
        }

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use vortex_error::VortexResult;

    use crate::dtype::DType;
    use crate::dtype::Nullability::Nullable;
    use crate::extension::datetime::TimeUnit;
    use crate::extension::datetime::Timestamp;
    use crate::scalar::PValue;
    use crate::scalar::Scalar;
    use crate::scalar::ScalarValue;

    #[test]
    fn validate_timestamp_scalar() -> VortexResult<()> {
        let dtype = DType::Extension(Timestamp::new(TimeUnit::Seconds, Nullable).erased());
        Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(0))))?;

        Ok(())
    }

    #[test]
    fn timestamp_storage_endpoints_do_not_require_a_calendar_value() -> VortexResult<()> {
        for unit in [TimeUnit::Seconds, TimeUnit::Milliseconds, TimeUnit::Microseconds, TimeUnit::Nanoseconds] {
            for timezone in [None, Some(Arc::from("UTC")), Some(Arc::from("+05:30"))] {
                let dtype = DType::Extension(Timestamp::new_with_tz(unit, timezone, Nullable).erased());
                for value in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
                    let scalar = Scalar::try_new(dtype.clone(), Some(ScalarValue::Primitive(PValue::I64(value))))?;
                    assert_eq!(scalar.as_extension().to_storage_scalar().as_primitive().as_::<i64>(), Some(value));
                    assert!(!scalar.as_extension().to_string().is_empty());
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[rstest]
    #[case("+01:00", "1970-01-01T01:00:00+01:00[+01:00]")]
    #[case("-05:30", "1969-12-31T18:30:00-05:30[-05:30]")]
    fn display_fixed_offset_timestamp(
        #[case] timezone: &str,
        #[case] expected: &str,
    ) -> VortexResult<()> {
        let dtype = DType::Extension(
            Timestamp::new_with_tz(TimeUnit::Seconds, Some(timezone.into()), Nullable).erased(),
        );
        let scalar = Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(0))))?;
        assert_eq!(format!("{}", scalar.as_extension()), expected);
        Ok(())
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn reject_timestamp_with_invalid_timezone() {
        let dtype = DType::Extension(
            Timestamp::new_with_tz(
                TimeUnit::Seconds,
                Some(Arc::from("Not/A/Timezone")),
                Nullable,
            )
            .erased(),
        );
        let result = Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I64(0))));
        assert!(result.is_err());
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn display_timestamp_scalar() {
        // Local (no timezone) timestamp.
        let local_dtype = DType::Extension(Timestamp::new(TimeUnit::Seconds, Nullable).erased());
        let scalar = Scalar::new(local_dtype, Some(ScalarValue::Primitive(PValue::I64(0))));
        assert_eq!(format!("{}", scalar.as_extension()), "1970-01-01T00:00:00Z");

        // Zoned timestamp.
        let zoned_dtype = DType::Extension(
            Timestamp::new_with_tz(
                TimeUnit::Seconds,
                Some(Arc::from("America/New_York")),
                Nullable,
            )
            .erased(),
        );
        let scalar = Scalar::new(zoned_dtype, Some(ScalarValue::Primitive(PValue::I64(0))));
        assert_eq!(
            format!("{}", scalar.as_extension()),
            "1969-12-31T19:00:00-05:00[America/New_York]"
        );
    }

    #[test]
    fn deserialize_empty_metadata_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Timestamp;
        assert!(vtable.deserialize_metadata(&[]).is_err());
    }

    #[test]
    fn deserialize_too_short_metadata_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Timestamp;
        // Only 2 bytes - too short for the required 3-byte header.
        assert!(vtable.deserialize_metadata(&[0x00, 0x01]).is_err());
    }

    #[test]
    fn deserialize_truncated_timezone_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Timestamp;
        // Valid tag (0x00 = Nanoseconds), tz_len = 10 (little-endian: [0x0A, 0x00]),
        // but only 3 bytes of timezone data instead of the declared 10.
        let data = [0x00u8, 0x0A, 0x00, b'U', b'T', b'C'];
        assert!(vtable.deserialize_metadata(&data).is_err());
    }
}
