// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use crate::{
    array_decoder::ArrowDataType,
    column::Column,
    encoding::{
        integer::{get_signed_int_decoder, get_unsigned_int_decoder},
        timestamp::{TimestampDecoder, TimestampNanosecondAsDecimalDecoder},
        PrimitiveValueDecoder,
    },
    error::{MismatchedSchemaSnafu, Result},
    proto::stream::Kind,
    stripe::Stripe,
};
use arrow::datatypes::{
    ArrowTimestampType, Decimal128Type, DecimalType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::{array::ArrayRef, buffer::NullBuffer};
use chrono::offset::TimeZone;
use chrono::Offset;
use chrono_tz::{Tz, UTC};

use super::{
    decimal::DecimalArrayDecoder, ArrayBatchDecoder, PresentDecoder, PrimitiveArrayDecoder,
};
use crate::error::UnsupportedTypeVariantSnafu;

const NANOSECONDS_IN_SECOND: i128 = 1_000_000_000;
const NANOSECOND_DIGITS: i8 = 9;

/// Seconds from ORC epoch of 1 January 2015, which serves as the 0
/// point for all timestamp values, to the UNIX epoch of 1 January 1970.
const ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH: i64 = 1_420_070_400;

fn get_inner_timestamp_decoder<T: ArrowTimestampType + Send>(
    column: &Column,
    stripe: &Stripe,
    seconds_since_unix_epoch: i64,
) -> PrimitiveArrayDecoder<T> {
    let data = stripe.stream_map().get(column, Kind::Data);
    let data = get_signed_int_decoder(data, column.rle_version());

    let secondary = stripe.stream_map().get(column, Kind::Secondary);
    let secondary = get_unsigned_int_decoder(secondary, column.rle_version());

    let present = PresentDecoder::from_stripe(stripe, column);

    let iter = Box::new(TimestampDecoder::<T>::new(
        seconds_since_unix_epoch,
        data,
        secondary,
    ));
    PrimitiveArrayDecoder::<T>::new(iter, present)
}

fn get_timestamp_decoder<T: ArrowTimestampType + Send>(
    column: &Column,
    stripe: &Stripe,
    seconds_since_unix_epoch: i64,
) -> Box<dyn ArrayBatchDecoder> {
    let inner = get_inner_timestamp_decoder::<T>(column, stripe, seconds_since_unix_epoch);
    match stripe.writer_tz() {
        Some(writer_tz) if writer_tz != UTC => {
            Box::new(TimestampOffsetArrayDecoder { inner, writer_tz })
        }
        // UTC has no offset conversion. Routing it through chrono would reduce
        // the valid original-width timestamp domain to chrono's calendar range.
        _ => Box::new(inner),
    }
}

fn get_timestamp_instant_decoder<T: ArrowTimestampType + Send>(
    column: &Column,
    stripe: &Stripe,
) -> Box<dyn ArrayBatchDecoder> {
    // TIMESTAMP_INSTANT is encoded as UTC so we don't check writer timezone in stripe
    let inner =
        get_inner_timestamp_decoder::<T>(column, stripe, ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH);
    Box::new(TimestampInstantArrayDecoder(inner))
}

fn decimal128_decoder(
    column: &Column,
    stripe: &Stripe,
    seconds_since_unix_epoch: i64,
    writer_tz: Option<Tz>,
) -> DecimalArrayDecoder {
    let data = stripe.stream_map().get(column, Kind::Data);
    let data = get_signed_int_decoder(data, column.rle_version());

    let secondary = stripe.stream_map().get(column, Kind::Secondary);
    let secondary = get_unsigned_int_decoder(secondary, column.rle_version());

    let present = PresentDecoder::from_stripe(stripe, column);

    let iter = TimestampNanosecondAsDecimalDecoder::new(seconds_since_unix_epoch, data, secondary);

    let iter: Box<dyn PrimitiveValueDecoder<i128> + Send> = match writer_tz {
        Some(UTC) | None => Box::new(iter),
        Some(writer_tz) => Box::new(TimestampNanosecondAsDecimalWithTzDecoder(iter, writer_tz)),
    };

    DecimalArrayDecoder::new(
        Decimal128Type::MAX_PRECISION,
        NANOSECOND_DIGITS,
        iter,
        present,
    )
}

/// Decodes a TIMESTAMP column stripe into batches of Timestamp{Nano,Micro,Milli,}secondArrays
/// with no timezone. Will convert timestamps from writer timezone to UTC if a writer timezone
/// is specified for the stripe.
pub fn new_timestamp_decoder(
    column: &Column,
    field_type: ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let seconds_since_unix_epoch = match stripe.writer_tz() {
        Some(writer_tz) => {
            // If writer timezone exists then we must take the ORC epoch according
            // to that timezone, and find seconds since UTC UNIX epoch for the base.
            writer_tz
                .with_ymd_and_hms(2015, 1, 1, 0, 0, 0)
                .unwrap()
                .timestamp()
        }
        None => {
            // No writer timezone, we can assume UTC, so we can use known fixed value
            // for the base offset.
            ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH
        }
    };

    match field_type {
        ArrowDataType::Timestamp(TimeUnit::Second, None) => {
            Ok(get_timestamp_decoder::<TimestampSecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            ))
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
            Ok(get_timestamp_decoder::<TimestampMillisecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            ))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
            Ok(get_timestamp_decoder::<TimestampMicrosecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            ))
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Ok(get_timestamp_decoder::<TimestampNanosecondType>(
                column,
                stripe,
                seconds_since_unix_epoch,
            ))
        }
        ArrowDataType::Decimal128(Decimal128Type::MAX_PRECISION, NANOSECOND_DIGITS) => {
            Ok(Box::new(decimal128_decoder(
                column,
                stripe,
                seconds_since_unix_epoch,
                stripe.writer_tz(),
            )))
        }
        _ => MismatchedSchemaSnafu {
            orc_type: column.data_type().clone(),
            arrow_type: field_type,
        }
        .fail(),
    }
}

/// Decodes a TIMESTAMP_INSTANT column stripe into batches of
/// Timestamp{Nano,Micro,Milli,}secondArrays with UTC timezone.
pub fn new_timestamp_instant_decoder(
    column: &Column,
    field_type: ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    match field_type {
        ArrowDataType::Timestamp(TimeUnit::Second, Some(tz)) if tz.as_ref() == "UTC" => Ok(
            get_timestamp_instant_decoder::<TimestampSecondType>(column, stripe),
        ),
        ArrowDataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => Ok(
            get_timestamp_instant_decoder::<TimestampMillisecondType>(column, stripe),
        ),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => Ok(
            get_timestamp_instant_decoder::<TimestampMicrosecondType>(column, stripe),
        ),
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "UTC" => Ok(
            get_timestamp_instant_decoder::<TimestampNanosecondType>(column, stripe),
        ),
        ArrowDataType::Timestamp(_, Some(_)) => UnsupportedTypeVariantSnafu {
            msg: "Non-UTC Arrow timestamps",
        }
        .fail(),
        ArrowDataType::Decimal128(Decimal128Type::MAX_PRECISION, NANOSECOND_DIGITS) => {
            Ok(Box::new(decimal128_decoder(
                column,
                stripe,
                ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
                None,
            )))
        }
        _ => MismatchedSchemaSnafu {
            orc_type: column.data_type().clone(),
            arrow_type: field_type,
        }
        .fail()?,
    }
}

/// Wrapper around PrimitiveArrayDecoder to decode timestamps which are encoded in
/// timezone of the writer to their UTC value.
struct TimestampOffsetArrayDecoder<T: ArrowTimestampType> {
    inner: PrimitiveArrayDecoder<T>,
    writer_tz: chrono_tz::Tz,
}

impl<T: ArrowTimestampType> ArrayBatchDecoder for TimestampOffsetArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self
            .inner
            .next_primitive_batch(batch_size, parent_present)?;

        let array = array.try_unary::<_, T, _>(|ts| {
            timestamp_with_writer_offset(ts, &T::UNIT, self.writer_tz).ok_or_else(|| {
                crate::error::OutOfSpecSnafu {
                    msg: "timestamp writer timezone conversion exceeds the exact supported range",
                }
                .build()
            })
        })?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        self.inner.skip_values(n, parent_present)
    }
}

fn timestamp_with_writer_offset(ts: i64, unit: &TimeUnit, writer_tz: Tz) -> Option<i64> {
    let units_per_second = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let seconds = ts.div_euclid(units_per_second);
    let remainder = ts.rem_euclid(units_per_second);
    let nanos = u32::try_from(remainder * (1_000_000_000 / units_per_second)).ok()?;
    let date = writer_tz.timestamp_opt(seconds, nanos).single()?;
    let offset = i128::from(date.offset().fix().local_minus_utc());
    ((i128::from(seconds) + offset) * i128::from(units_per_second) + i128::from(remainder))
        .try_into()
        .ok()
}

/// Wrapper around PrimitiveArrayDecoder to allow specifying the timezone of the output
/// timestamp array as UTC.
struct TimestampInstantArrayDecoder<T: ArrowTimestampType>(PrimitiveArrayDecoder<T>);

impl<T: ArrowTimestampType> ArrayBatchDecoder for TimestampInstantArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self
            .0
            .next_primitive_batch(batch_size, parent_present)?
            .with_timezone("UTC");
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        self.0.skip_values(n, parent_present)
    }
}

struct TimestampNanosecondAsDecimalWithTzDecoder(TimestampNanosecondAsDecimalDecoder, Tz);

impl TimestampNanosecondAsDecimalWithTzDecoder {
    fn next_inner(&self, ts: i128) -> Result<i128> {
        decimal_timestamp_with_writer_offset(ts, self.1).ok_or_else(|| {
            crate::error::OutOfSpecSnafu {
                msg: "decimal timestamp writer timezone conversion exceeds the exact supported range",
            }
            .build()
        })
    }
}

impl PrimitiveValueDecoder<i128> for TimestampNanosecondAsDecimalWithTzDecoder {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.0.skip(n)?;
        Ok(())
    }

    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        self.0.decode(out)?;
        for x in out.iter_mut() {
            *x = self.next_inner(*x)?;
        }
        Ok(())
    }
}

fn decimal_timestamp_with_writer_offset(ts: i128, writer_tz: Tz) -> Option<i128> {
    let seconds = i64::try_from(ts.div_euclid(NANOSECONDS_IN_SECOND)).ok()?;
    let nanoseconds = u32::try_from(ts.rem_euclid(NANOSECONDS_IN_SECOND)).ok()?;
    let date = writer_tz.timestamp_opt(seconds, nanoseconds).single()?;
    let offset = i128::from(date.offset().fix().local_minus_utc());
    ts.checked_add(offset * NANOSECONDS_IN_SECOND)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, TimestampMicrosecondArray};
    use chrono_tz::{America::New_York, Asia::Kathmandu};

    struct Values(std::vec::IntoIter<i64>);

    impl PrimitiveValueDecoder<i64> for Values {
        fn skip(&mut self, n: usize) -> Result<()> {
            for _ in 0..n {
                self.0.next().unwrap();
            }
            Ok(())
        }

        fn decode(&mut self, out: &mut [i64]) -> Result<()> {
            for value in out {
                *value = self.0.next().unwrap();
            }
            Ok(())
        }
    }

    #[test]
    fn writer_offsets_preserve_negative_fractions_and_seasonal_offsets() {
        for (seconds, zone, offset) in [
            (-1_i64, New_York, -18_000_i64),
            (1_704_067_200, New_York, -18_000),
            (1_719_792_000, New_York, -14_400),
            (1_704_067_200, Kathmandu, 20_700),
        ] {
            for (unit, factor) in [
                (TimeUnit::Second, 1_i64),
                (TimeUnit::Millisecond, 1_000),
                (TimeUnit::Microsecond, 1_000_000),
                (TimeUnit::Nanosecond, 1_000_000_000),
            ] {
                let fraction = factor - 1;
                assert_eq!(
                    timestamp_with_writer_offset(seconds * factor + fraction, &unit, zone),
                    Some((seconds + offset) * factor + fraction)
                );
            }
            let nanos = i128::from(seconds) * NANOSECONDS_IN_SECOND + 999_999_999;
            assert_eq!(
                decimal_timestamp_with_writer_offset(nanos, zone),
                Some(nanos + i128::from(offset) * NANOSECONDS_IN_SECOND)
            );
        }
    }

    #[test]
    fn writer_offset_range_errors_do_not_wrap_panic_or_create_nulls() {
        for value in [i64::MIN, i64::MAX] {
            for unit in [
                TimeUnit::Second,
                TimeUnit::Millisecond,
                TimeUnit::Microsecond,
            ] {
                assert_eq!(timestamp_with_writer_offset(value, &unit, New_York), None);
            }
        }
        assert_eq!(
            timestamp_with_writer_offset(i64::MAX, &TimeUnit::Nanosecond, Kathmandu),
            None
        );
        assert_eq!(
            timestamp_with_writer_offset(i64::MIN, &TimeUnit::Nanosecond, New_York),
            None
        );
        for value in [
            i128::MIN,
            i128::MAX,
            i128::from(i64::MAX) * NANOSECONDS_IN_SECOND,
        ] {
            assert_eq!(decimal_timestamp_with_writer_offset(value, Kathmandu), None);
        }
        let inner = PrimitiveArrayDecoder::<TimestampMicrosecondType>::new(
            Box::new(Values(vec![i64::MAX].into_iter())),
            None,
        );
        let mut decoder = TimestampOffsetArrayDecoder {
            inner,
            writer_tz: New_York,
        };
        let error = decoder.next_batch(1, None).unwrap_err().to_string();
        assert!(error.contains("timezone conversion exceeds"), "{error}");
    }

    #[test]
    fn writer_offset_conversion_preserves_parent_nulls() {
        let inner = PrimitiveArrayDecoder::<TimestampMicrosecondType>::new(
            Box::new(Values(vec![-1, 1_704_067_200_000_123].into_iter())),
            None,
        );
        let mut decoder = TimestampOffsetArrayDecoder {
            inner,
            writer_tz: New_York,
        };
        let present = NullBuffer::from(vec![false, true, false, true]);
        let array = decoder.next_batch(4, Some(&present)).unwrap();
        let values = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.nulls(), Some(&present));
        assert_eq!(values.value(1), -18_000_000_001);
        assert_eq!(values.value(3), 1_704_049_200_000_123);
    }
}
