//! Versioned `ShardLoom` fixed-duration metadata over Vortex extension storage.
//! Calendar months/days are never represented as elapsed duration ticks.

use shardloom_core::temporal::TemporalUnit;
use std::fmt;
use vortex::{
    array::{
        dtype::{
            DType, Nullability, PType,
            extension::{ExtDType, ExtId, ExtVTable},
        },
        scalar::ScalarValue,
    },
    error::{VortexResult, vortex_ensure, vortex_err},
    session::registry::CachedId,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) struct Duration;

impl Duration {
    pub(crate) fn new(unit: TemporalUnit, nullable: Nullability) -> ExtDType<Self> {
        ExtDType::try_new(unit, DType::Primitive(PType::I64, nullable))
            .expect("duration has fixed signed-64-bit storage")
    }
}

pub(crate) struct DurationValue(i64, TemporalUnit);

impl fmt::Display for DurationValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.0, self.1.as_str())
    }
}

impl ExtVTable for Duration {
    type Metadata = TemporalUnit;
    type NativeValue<'a> = DurationValue;

    fn id(&self) -> ExtId {
        static ID: CachedId = CachedId::new("shardloom.duration.v1");
        *ID
    }

    fn serialize_metadata(&self, unit: &TemporalUnit) -> VortexResult<Vec<u8>> {
        Ok(vec![match unit {
            TemporalUnit::Seconds => 0,
            TemporalUnit::Milliseconds => 1,
            TemporalUnit::Microseconds => 2,
            TemporalUnit::Nanoseconds => 3,
        }])
    }

    fn deserialize_metadata(&self, metadata: &[u8]) -> VortexResult<TemporalUnit> {
        Ok(match metadata {
            [0] => TemporalUnit::Seconds,
            [1] => TemporalUnit::Milliseconds,
            [2] => TemporalUnit::Microseconds,
            [3] => TemporalUnit::Nanoseconds,
            _ => {
                return Err(vortex_err!(
                    "ShardLoom duration v1 requires one fixed-unit tag"
                ));
            }
        })
    }

    fn validate_dtype(dtype: &ExtDType<Self>) -> VortexResult<()> {
        vortex_ensure!(
            matches!(dtype.storage_dtype(), DType::Primitive(PType::I64, _)),
            "ShardLoom duration storage must be signed 64-bit ticks"
        );
        Ok(())
    }

    fn unpack_native<'a>(
        dtype: &'a ExtDType<Self>,
        value: &'a ScalarValue,
    ) -> VortexResult<DurationValue> {
        Ok(DurationValue(
            value.as_primitive().cast::<i64>()?,
            *dtype.metadata(),
        ))
    }
}
