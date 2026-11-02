// SPDX-License-Identifier: Apache-2.0
//! Attach payload owners before the first decoded record. Schema and decoder
//! structure have separate admission; this visitor does not claim those costs.

use super::{AvroError, Decoder, Memory, RecordDecoder, UnionReadPlan};

impl RecordDecoder {
    pub(crate) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        for field in &mut self.fields {
            field.set_memory(memory)?;
        }
        Ok(())
    }
}

impl Decoder {
    pub(super) fn set_memory(&mut self, memory: &Memory) -> Result<(), AvroError> {
        match self {
            Self::Null(_) => Ok(()),
            Self::Boolean(values) => values.set_memory(memory),
            Self::Int32(v) | Self::Date32(v) | Self::TimeMillis(v) => v.set_memory(memory),
            Self::Int64(v)
            | Self::TimeMicros(v)
            | Self::TimestampMillis(_, v)
            | Self::TimestampMicros(_, v)
            | Self::TimestampNanos(_, v)
            | Self::Int32ToInt64(v) => v.set_memory(memory),
            Self::Float32(v) | Self::Int32ToFloat32(v) | Self::Int64ToFloat32(v) => {
                v.set_memory(memory)
            }
            Self::Float64(v)
            | Self::Int32ToFloat64(v)
            | Self::Int64ToFloat64(v)
            | Self::Float32ToFloat64(v) => v.set_memory(memory),
            Self::BytesToString(offsets, values)
            | Self::StringToBytes(offsets, values)
            | Self::Binary(offsets, values)
            | Self::String(offsets, values)
            | Self::StringView(offsets, values) => {
                offsets.set_memory(memory)?;
                values.set_memory(memory)
            }
            Self::Array(_, offsets, values) => {
                offsets.set_memory(memory)?;
                values.set_memory(memory)
            }
            Self::Record(_, fields, _, _, _) => {
                for field in fields {
                    field.set_memory(memory)?;
                }
                Ok(())
            }
            Self::Map(_, key_offsets, map_offsets, keys, values) => {
                key_offsets.set_memory(memory)?;
                map_offsets.set_memory(memory)?;
                keys.set_memory(memory)?;
                values.set_memory(memory)
            }
            Self::Fixed(_, v) | Self::Uuid(v) => v.set_memory(memory),
            Self::Enum(v, _, _) => v.set_memory(memory),
            Self::Duration(v) => v.set_memory(memory),
            Self::Decimal128(_, _, _, v) => v.set_memory(memory),
            Self::Decimal256(_, _, _, v) => v.set_memory(memory),
            Self::Nullable(_, nulls, values) => {
                nulls.set_memory(memory)?;
                values.set_memory(memory)
            }
            Self::Union(union) => {
                union.branches.type_ids.set_memory(memory)?;
                union.branches.offsets.set_memory(memory)?;
                for branch in &mut union.branches.decoders {
                    branch.set_memory(memory)?;
                }
                if let UnionReadPlan::ToSingle { target, .. } = &mut union.plan {
                    target.set_memory(memory)?;
                }
                Ok(())
            }
            #[cfg(feature = "small_decimals")]
            Self::Decimal32(_, _, _, v) => v.set_memory(memory),
            #[cfg(feature = "small_decimals")]
            Self::Decimal64(_, _, _, v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::DurationSecond(v)
            | Self::DurationMillisecond(v)
            | Self::DurationMicrosecond(v)
            | Self::DurationNanosecond(v)
            | Self::Date64(v)
            | Self::TimeNanos(v)
            | Self::TimestampSecs(_, v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::Time32Secs(v) | Self::IntervalYearMonth(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::Int8(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::Int16(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::UInt8(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::UInt16(v) | Self::Float16(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::UInt32(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::UInt64(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::IntervalDayTime(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::IntervalMonthDayNano(v) => v.set_memory(memory),
            #[cfg(feature = "avro_custom_types")]
            Self::RunEndEncoded(_, _, inner) => {
                if memory.is_some() {
                    return Err(AvroError::NYI(
                        "Reserved Avro run-end gather is not supported".to_string(),
                    ));
                }
                inner.set_memory(memory)
            }
        }
    }
}
