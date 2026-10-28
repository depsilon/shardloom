// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom, 2026-10-27: validate ticks before bounded calendar construction.

use std::fmt;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_session::registry::CachedId;

use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::dtype::extension::ExtDType;
use crate::dtype::extension::ExtId;
use crate::dtype::extension::ExtVTable;
use crate::extension::datetime::TimeUnit;
use crate::scalar::ScalarValue;

/// Time DType.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Time;

fn time_ptype(time_unit: &TimeUnit) -> Option<PType> {
    Some(match time_unit {
        TimeUnit::Nanoseconds | TimeUnit::Microseconds => PType::I64,
        TimeUnit::Milliseconds | TimeUnit::Seconds => PType::I32,
        TimeUnit::Days => return None,
    })
}

impl Time {
    /// Creates a new Time extension dtype with the given time unit and nullability.
    ///
    /// Note that Days units are not supported for Time.
    pub fn try_new(time_unit: TimeUnit, nullability: Nullability) -> VortexResult<ExtDType<Self>> {
        let ptype = time_ptype(&time_unit)
            .ok_or_else(|| vortex_err!("Time type does not support time unit {}", time_unit))?;
        ExtDType::try_new(time_unit, DType::Primitive(ptype, nullability))
    }

    /// Creates a new Time extension dtype with the given time unit and nullability.
    pub fn new(time_unit: TimeUnit, nullability: Nullability) -> ExtDType<Self> {
        Self::try_new(time_unit, nullability).vortex_expect("failed to create time dtype")
    }
}

/// Unpacked value of a [`Time`] extension scalar.
pub enum TimeValue {
    /// Seconds since midnight.
    Seconds(i32),
    /// Milliseconds since midnight.
    Milliseconds(i32),
    /// Microseconds since midnight.
    Microseconds(i64),
    /// Nanoseconds since midnight.
    Nanoseconds(i64),
}

impl fmt::Display for TimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (ticks, scale, unit) = match self {
            TimeValue::Seconds(v) => (i64::from(*v), 1, "s"),
            TimeValue::Milliseconds(v) => (i64::from(*v), 1_000, "ms"),
            TimeValue::Microseconds(v) => (*v, 1_000_000, "us"),
            TimeValue::Nanoseconds(v) => (*v, 1_000_000_000, "ns"),
        };
        if !(0..86_400 * scale).contains(&ticks) {
            return write!(f, "{ticks}{unit} since midnight (out of range)");
        }
        let seconds = ticks / scale;
        let time = jiff::civil::Time::new(
            i8::try_from(seconds / 3_600).map_err(|_| fmt::Error)?,
            i8::try_from(seconds / 60 % 60).map_err(|_| fmt::Error)?,
            i8::try_from(seconds % 60).map_err(|_| fmt::Error)?,
            i32::try_from((ticks % scale) * (1_000_000_000 / scale))
                .map_err(|_| fmt::Error)?,
        )
        .map_err(|_| fmt::Error)?;
        write!(f, "{time}")
    }
}

impl ExtVTable for Time {
    type Metadata = TimeUnit;

    type NativeValue<'a> = TimeValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("vortex.time");
        *ID
    }

    fn serialize_metadata(&self, metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(vec![u8::from(*metadata)])
    }

    fn deserialize_metadata(&self, data: &[u8]) -> VortexResult<Self::Metadata> {
        vortex_ensure!(!data.is_empty(), "Time metadata must not be empty");
        let tag = data[0];
        TimeUnit::try_from(tag)
    }

    fn validate_dtype(ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        let metadata = ext_dtype.metadata();
        let ptype = time_ptype(metadata)
            .ok_or_else(|| vortex_err!("Time type does not support time unit {}", metadata))?;

        vortex_ensure_eq!(
            ext_dtype.storage_dtype().as_ptype(),
            ptype,
            "Time storage dtype for {} has the wrong ptype",
            metadata
        );

        Ok(())
    }

    fn unpack_native<'a>(
        ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        let length_of_time = storage_value.as_primitive().cast::<i64>()?;

        let per_second = match *ext_dtype.metadata() {
            TimeUnit::Seconds => 1,
            TimeUnit::Milliseconds => 1_000,
            TimeUnit::Microseconds => 1_000_000,
            TimeUnit::Nanoseconds => 1_000_000_000,
            TimeUnit::Days => vortex_bail!("Time type does not support Days time unit"),
        };
        vortex_ensure!((0..86_400 * per_second).contains(&length_of_time), "Time scalar must be within [midnight, next midnight)");
        let value = match *ext_dtype.metadata() {
            TimeUnit::Seconds => {
                let v = i32::try_from(length_of_time)
                    .map_err(|e| vortex_err!("Time seconds value out of i32 range: {e}"))?;
                TimeValue::Seconds(v)
            }
            TimeUnit::Milliseconds => {
                let v = i32::try_from(length_of_time)
                    .map_err(|e| vortex_err!("Time milliseconds value out of i32 range: {e}"))?;
                TimeValue::Milliseconds(v)
            }
            TimeUnit::Microseconds => TimeValue::Microseconds(length_of_time),
            TimeUnit::Nanoseconds => TimeValue::Nanoseconds(length_of_time),
            d @ TimeUnit::Days => vortex_bail!("Time type does not support time unit {d}"),
        };

        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use vortex_error::VortexResult;

    use crate::dtype::DType;
    use crate::dtype::Nullability::Nullable;
    use crate::extension::datetime::Time;
    use crate::extension::datetime::TimeUnit;
    use crate::scalar::PValue;
    use crate::scalar::Scalar;
    use crate::scalar::ScalarValue;

    #[test]
    fn validate_time_scalar() -> VortexResult<()> {
        // 3661 seconds = 1 hour, 1 minute, 1 second.
        let dtype = DType::Extension(Time::new(TimeUnit::Seconds, Nullable).erased());
        Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I32(3661))))?;

        Ok(())
    }

    #[test]
    fn reject_time_out_of_range() {
        // 86400 seconds = exactly 24 hours, which exceeds the valid `jiff::civil::Time` range.
        let dtype = DType::Extension(Time::new(TimeUnit::Seconds, Nullable).erased());
        let result = Scalar::try_new(dtype, Some(ScalarValue::Primitive(PValue::I32(86400))));
        assert!(result.is_err());
    }

    #[test]
    fn display_time_scalar() {
        let dtype = DType::Extension(Time::new(TimeUnit::Seconds, Nullable).erased());

        let scalar = Scalar::new(
            dtype.clone(),
            Some(ScalarValue::Primitive(PValue::I32(3661))),
        );
        assert_eq!(format!("{}", scalar.as_extension()), "01:01:01");

        let scalar = Scalar::new(dtype, Some(ScalarValue::Primitive(PValue::I32(0))));
        assert_eq!(format!("{}", scalar.as_extension()), "00:00:00");
    }

    #[test]
    fn deserialize_empty_metadata_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Time;
        assert!(vtable.deserialize_metadata(&[]).is_err());
    }

    #[test]
    fn deserialize_invalid_tag_returns_error() {
        use crate::dtype::extension::ExtVTable;

        let vtable = Time;
        // 0xFF is not a valid TimeUnit tag.
        assert!(vtable.deserialize_metadata(&[0xFF]).is_err());
    }
}
