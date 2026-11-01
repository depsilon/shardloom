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

use arrow::array::{ArrayRef, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef,
};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use snafu::{ensure, ResultExt};

use crate::column::Column;
use crate::encoding::float::FloatDecoder;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{
    self, MismatchedSchemaSnafu, Result, UnexpectedSnafu, UnsupportedTypeVariantSnafu,
};
use crate::proto::stream::Kind;
use crate::reader::memory::decoded::{bitmap, Memory, Values};
use crate::schema::DataType;
use crate::stripe::Stripe;
use crate::RowSelection;

use self::decimal::new_decimal_decoder;
use self::list::ListArrayDecoder;
use self::map::MapArrayDecoder;
use self::string::{new_binary_decoder, new_string_decoder};
use self::struct_decoder::StructArrayDecoder;
use self::timestamp::{new_timestamp_decoder, new_timestamp_instant_decoder};
use self::union::UnionArrayDecoder;

mod decimal;
mod decoder_memory;
mod list;
mod map;
mod string;
mod struct_decoder;
mod timestamp;
mod union;

#[path = "structural_memory.rs"]
mod structural_memory;

pub(crate) use structural_memory::metadata_per_node;

#[cfg(test)]
#[path = "ownership_tests.rs"]
mod ownership_tests;

pub trait ArrayBatchDecoder: Send {
    /// Used as base for decoding ORC columns into Arrow arrays. Provide an input `batch_size`
    /// which specifies the upper limit of the number of values returned in the output array.
    ///
    /// If parent nested type (e.g. Struct) indicates a null in it's PRESENT stream,
    /// then the child doesn't have a value (similar to other nullability). So we need
    /// to take care to insert these null values as Arrow requires the child to hold
    /// data in the null slot of the child.
    // TODO: encode nullability in generic -> for a given column in a stripe, we will always know
    //       upfront if we need to bother with nulls or not, so we don't need to keep checking this
    //       for every invocation of next_batch
    // NOTE: null parent may have non-null child, so would still have to account for this
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef>;

    /// Skip the next `n` values without decoding them, failing if it cannot skip the enough values.
    /// If parent nested type (e.g. Struct) indicates a null in it's PRESENT stream,
    /// then the child doesn't have a value (similar to other nullability). So we need
    /// to take care to insert these null values as Arrow requires the child to hold
    /// data in the null slot of the child.
    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()>;
}

struct PrimitiveArrayDecoder<T: ArrowPrimitiveType> {
    iter: Box<dyn PrimitiveValueDecoder<T::Native> + Send>,
    present: Option<PresentDecoder>,
    memory: Memory,
}

impl<T: ArrowPrimitiveType> PrimitiveArrayDecoder<T> {
    pub fn new(
        iter: Box<dyn PrimitiveValueDecoder<T::Native> + Send>,
        present: Option<PresentDecoder>,
        memory: Memory,
    ) -> Self {
        Self {
            iter,
            present,
            memory,
        }
    }

    fn next_primitive_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<PrimitiveArray<T>> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;
        let mut data = Values::<T::Native>::zeroed(batch_size, &self.memory)?;
        if let Some(present) = &present {
            self.iter.decode_spaced(&mut data, present)?;
        } else {
            self.iter.decode(&mut data)?;
        }
        Ok(PrimitiveArray::<T>::new(data.into_buffer().into(), present))
    }
}

impl<T: ArrowPrimitiveType> ArrayBatchDecoder for PrimitiveArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self.next_primitive_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;
        self.iter.skip(non_null_count)
    }
}

type Int64ArrayDecoder = PrimitiveArrayDecoder<Int64Type>;
type Int32ArrayDecoder = PrimitiveArrayDecoder<Int32Type>;
type Int16ArrayDecoder = PrimitiveArrayDecoder<Int16Type>;
type Int8ArrayDecoder = PrimitiveArrayDecoder<Int8Type>;
type Float32ArrayDecoder = PrimitiveArrayDecoder<Float32Type>;
type Float64ArrayDecoder = PrimitiveArrayDecoder<Float64Type>;
type DateArrayDecoder = PrimitiveArrayDecoder<Date32Type>; // TODO: does ORC encode as i64 or i32?

struct BooleanArrayDecoder {
    iter: Box<dyn PrimitiveValueDecoder<bool> + Send>,
    present: Option<PresentDecoder>,
    memory: Memory,
}

impl BooleanArrayDecoder {
    pub fn new(
        iter: Box<dyn PrimitiveValueDecoder<bool> + Send>,
        present: Option<PresentDecoder>,
        memory: Memory,
    ) -> Self {
        Self {
            iter,
            present,
            memory,
        }
    }
}

impl ArrayBatchDecoder for BooleanArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;
        let mut data = Values::<bool>::zeroed(batch_size, &self.memory)?;
        if let Some(present) = &present {
            self.iter.decode_spaced(&mut data, present)?;
        } else {
            self.iter.decode(&mut data)?;
        }
        let bits = bitmap(batch_size, &self.memory, |index| data[index])?;
        let array = BooleanArray::new(bits, present);
        Ok(Arc::new(array))
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;
        self.iter.skip(non_null_count)
    }
}

struct PresentDecoder {
    // TODO: ideally directly reference BooleanDecoder, doing this way to avoid
    //       the generic propagation that would be required (BooleanDecoder<R: Read>)
    inner: Box<dyn PrimitiveValueDecoder<bool> + Send>,
    memory: Memory,
}

impl PresentDecoder {
    fn count_non_null(&mut self, mut count: usize) -> Result<usize> {
        let mut values = [false; 1024];
        let mut non_null = 0;
        while count != 0 {
            let take = count.min(values.len());
            self.inner.decode(&mut values[..take])?;
            non_null += values[..take].iter().filter(|&&value| value).count();
            count -= take;
        }
        Ok(non_null)
    }

    fn from_stripe(stripe: &Stripe, column: &Column) -> Result<Option<Self>> {
        let Some(stream) = stripe.stream_map().get_opt_checked(column, Kind::Present)? else {
            return Ok(None);
        };
        let inner = decoder_memory::boolean(stream, stripe.stream_map().memory())?;
        Ok(Some(PresentDecoder {
            inner,
            memory: stripe.stream_map().memory(),
        }))
    }

    fn next_buffer(&mut self, size: usize) -> Result<NullBuffer> {
        let mut data = Values::<bool>::zeroed(size, &self.memory)?;
        self.inner.decode(&mut data)?;
        Ok(NullBuffer::new(bitmap(size, &self.memory, |index| {
            data[index]
        })?))
    }
}

fn merge_parent_present(
    parent_present: &NullBuffer,
    present: Result<NullBuffer>,
    memory: &Memory,
) -> Result<NullBuffer> {
    let present = present?;
    let non_null_count = parent_present.len() - parent_present.null_count();
    debug_assert!(present.len() == non_null_count);
    let mut values = present.iter();
    Ok(NullBuffer::new(bitmap(
        parent_present.len(),
        memory,
        |index| parent_present.is_valid(index) && values.next().unwrap_or(false),
    )?))
}

fn derive_present_vec(
    present: &mut Option<PresentDecoder>,
    parent_present: Option<&NullBuffer>,
    batch_size: usize,
) -> Option<Result<NullBuffer>> {
    let present = match (present, parent_present) {
        (Some(present), Some(parent_present)) => {
            let element_count = parent_present.len() - parent_present.null_count();
            let values = present.next_buffer(element_count);
            Some(merge_parent_present(
                parent_present,
                values,
                &present.memory,
            ))
        }
        (Some(present), None) => Some(present.next_buffer(batch_size)),
        (None, Some(parent_present)) => Some(Ok(parent_present.clone())),
        (None, None) => None,
    };

    // omit the null buffer if there are no nulls
    match present {
        Some(Ok(present)) if present.null_count() == 0 => None,
        other => other,
    }
}

/// Skip n values and return the non-null count for the data stream
fn skip_present_and_get_non_null_count(
    present: &mut Option<PresentDecoder>,
    parent_present: Option<&NullBuffer>,
    n: usize,
) -> Result<usize> {
    match (present, parent_present) {
        (Some(present), Some(parent_present)) => {
            // Parent has nulls, so we need to decode parent present to know how many
            // of our present values to skip
            let non_null_in_parent = parent_present.len() - parent_present.null_count();

            // Skip our present values for non-null parents and count non-nulls
            present.count_non_null(non_null_in_parent)
        }
        (Some(present), None) => {
            // No parent present, skip n values and count non-nulls
            present.count_non_null(n)
        }
        (None, Some(parent_present)) => {
            // No our present stream, all non-null parents have data
            Ok(parent_present.len() - parent_present.null_count())
        }
        (None, None) => {
            // No nulls at all, all n values have data
            Ok(n)
        }
    }
}

fn skip_lengths(
    decoder: &mut (dyn PrimitiveValueDecoder<i64> + Send),
    mut count: usize,
) -> Result<usize> {
    let mut values = [0; 512];
    let mut total = 0usize;
    while count != 0 {
        let take = count.min(values.len());
        decoder.decode(&mut values[..take])?;
        let part = crate::reader::memory::decoded::total_length(&values[..take], usize::MAX)?;
        total = total.checked_add(part).ok_or_else(|| {
            error::OutOfSpecSnafu {
                msg: "skipped lengths overflow",
            }
            .build()
        })?;
        count -= take;
    }
    Ok(total)
}

pub struct NaiveStripeDecoder {
    stripe: Stripe,
    schema_ref: SchemaRef,
    decoders: Vec<Box<dyn ArrayBatchDecoder>>,
    index: usize,
    batch_size: usize,
    number_of_rows: usize,
    row_selection: Option<RowSelection>,
    selection_index: usize,
    selection_consumed: usize,
}

impl NaiveStripeDecoder {
    /// Advance according to the configured row selection and return the next batch, if any.
    ///
    /// Behavior:
    /// - Iterates `RowSelection` segments (skip/select) starting at `selection_index`.
    /// - For skip segments: clamp to remaining rows in this stripe, advance decoders via
    ///   `skip_rows(actual_skip)`, and advance `index`. If the segment is fully consumed,
    ///   increment `selection_index`.
    /// - For select segments: decode up to `min(row_count, batch_size, remaining_in_stripe)`,
    ///   advance `index`, update `selection_index` if fully consumed, and return the batch.
    /// - If a segment requests rows beyond the end of the stripe, it is skipped (advancing
    ///   `selection_index`) without touching decoders.
    fn next_with_row_selection(&mut self) -> Option<Result<RecordBatch>> {
        // Process selectors until we produce a batch or exhaust selection
        loop {
            let (is_skip, row_count) = {
                let selectors = self.row_selection.as_ref().unwrap().selectors();
                if self.selection_index >= selectors.len() {
                    return None;
                }
                let selector = selectors[self.selection_index];
                (selector.skip, selector.row_count - self.selection_consumed)
            };

            if is_skip {
                let remaining = self.number_of_rows - self.index;
                let actual_skip = row_count.min(remaining);

                if actual_skip == 0 {
                    // Nothing to skip in this stripe; try next selector
                    self.selection_index += 1;
                    self.selection_consumed = 0;
                    continue;
                }

                // Keep decoders in sync by skipping values per column
                if let Err(e) = self.skip_rows(actual_skip) {
                    return Some(Err(e));
                }
                self.index += actual_skip;

                if actual_skip >= row_count {
                    self.selection_index += 1;
                    self.selection_consumed = 0;
                } else {
                    self.selection_consumed += actual_skip;
                }
            } else {
                let rows_to_read = row_count.min(self.batch_size);
                let remaining = self.number_of_rows - self.index;
                let actual_rows = rows_to_read.min(remaining);

                if actual_rows == 0 {
                    // Nothing to read from this selector in this stripe; advance selector
                    self.selection_index += 1;
                    self.selection_consumed = 0;
                    continue;
                }

                let record = self.decode_next_batch(actual_rows).transpose()?;
                self.index += actual_rows;

                if actual_rows >= row_count {
                    self.selection_index += 1;
                    self.selection_consumed = 0;
                } else {
                    self.selection_consumed += actual_rows;
                }
                return Some(record);
            }
        }
    }
}

impl Iterator for NaiveStripeDecoder {
    type Item = Result<RecordBatch>;

    // TODO: check if we can make this more efficient
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.number_of_rows {
            // Handle row selection if present
            if self.row_selection.is_some() {
                self.next_with_row_selection()
            } else {
                // No row selection - decode normally
                let record = self
                    .decode_next_batch(self.number_of_rows - self.index)
                    .transpose()?;
                self.index += self.batch_size.min(self.number_of_rows - self.index);
                Some(record)
            }
        } else {
            None
        }
    }
}

pub fn array_decoder_factory(
    column: &Column,
    hinted_arrow_type: &ArrowDataType,
    stripe: &Stripe,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let decoder: Box<dyn ArrayBatchDecoder> = match (column.data_type(), hinted_arrow_type) {
        // TODO: try make branches more generic, reduce duplication
        (DataType::Boolean { .. }, ArrowDataType::Boolean) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter = decoder_memory::boolean(iter, stripe.stream_map().memory())?;
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(BooleanArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Byte { .. }, ArrowDataType::Int8) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter = decoder_memory::byte(iter, stripe.stream_map().memory())?;
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(Int8ArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Short { .. }, ArrowDataType::Int16) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter =
                decoder_memory::signed(iter, column.rle_version(), stripe.stream_map().memory())?;
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(Int16ArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Int { .. }, ArrowDataType::Int32) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter =
                decoder_memory::signed(iter, column.rle_version(), stripe.stream_map().memory())?;
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(Int32ArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Long { .. }, ArrowDataType::Int64) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter =
                decoder_memory::signed(iter, column.rle_version(), stripe.stream_map().memory())?;
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(Int64ArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Float { .. }, ArrowDataType::Float32) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter = Box::new(FloatDecoder::new(iter));
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(Float32ArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Double { .. }, ArrowDataType::Float64) => {
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter = Box::new(FloatDecoder::new(iter));
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(Float64ArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::String { .. }, ArrowDataType::Utf8)
        | (DataType::Varchar { .. }, ArrowDataType::Utf8)
        | (DataType::Char { .. }, ArrowDataType::Utf8) => new_string_decoder(column, stripe)?,
        (DataType::Binary { .. }, ArrowDataType::Binary) => new_binary_decoder(column, stripe)?,
        (
            DataType::Decimal {
                precision, scale, ..
            },
            ArrowDataType::Decimal128(a_precision, a_scale),
        ) if *precision as u8 == *a_precision && *scale as i8 == *a_scale => {
            new_decimal_decoder(column, stripe, *precision, *scale)?
        }
        (DataType::Timestamp { .. }, field_type) => {
            new_timestamp_decoder(column, field_type.clone(), stripe)?
        }
        (DataType::TimestampWithLocalTimezone { .. }, field_type) => {
            new_timestamp_instant_decoder(column, field_type.clone(), stripe)?
        }
        (DataType::Date { .. }, ArrowDataType::Date32) => {
            // TODO: allow Date64
            let iter = stripe.stream_map().get_checked(column, Kind::Data)?;
            let iter =
                decoder_memory::signed(iter, column.rle_version(), stripe.stream_map().memory())?;
            let present = PresentDecoder::from_stripe(stripe, column)?;
            Box::new(DateArrayDecoder::new(
                iter,
                present,
                stripe.stream_map().memory(),
            ))
        }
        (DataType::Struct { .. }, ArrowDataType::Struct(fields)) => {
            Box::new(StructArrayDecoder::new(column, fields.clone(), stripe)?)
        }
        (DataType::List { .. }, ArrowDataType::List(field)) => {
            // TODO: add support for ArrowDataType::LargeList
            Box::new(ListArrayDecoder::new(column, field.clone(), stripe)?)
        }
        (DataType::Map { .. }, ArrowDataType::Map(entries, sorted)) => {
            ensure!(!sorted, UnsupportedTypeVariantSnafu { msg: "Sorted map" });
            let ArrowDataType::Struct(entries) = entries.data_type() else {
                UnexpectedSnafu {
                    msg: "arrow Map with non-Struct entry type".to_owned(),
                }
                .fail()?
            };
            ensure!(
                entries.len() == 2,
                UnexpectedSnafu {
                    msg: format!(
                        "arrow Map with {} columns per entry (expected 2)",
                        entries.len()
                    )
                }
            );
            let keys_field = entries[0].clone();
            let values_field = entries[1].clone();

            Box::new(MapArrayDecoder::new(
                column,
                keys_field,
                values_field,
                stripe,
            )?)
        }
        (DataType::Union { .. }, ArrowDataType::Union(fields, _)) => {
            Box::new(UnionArrayDecoder::new(column, fields.clone(), stripe)?)
        }
        (data_type, field_type) => {
            return MismatchedSchemaSnafu {
                orc_type: data_type.clone(),
                arrow_type: field_type.clone(),
            }
            .fail()
        }
    };

    Ok(decoder)
}

impl NaiveStripeDecoder {
    /// Change the next output extent without rebuilding the held stripe or
    /// column decoders. The owning reader validates that the size is positive.
    pub(crate) fn set_batch_size(&mut self, batch_size: usize) {
        self.batch_size = batch_size;
    }

    fn inner_decode_next_batch(&mut self, remaining: usize) -> Result<Vec<ArrayRef>> {
        let chunk = self.batch_size.min(remaining);

        let mut fields = Vec::with_capacity(self.stripe.columns().len());

        for decoder in &mut self.decoders {
            let array = decoder.next_batch(chunk, None)?;
            if array.is_empty() {
                break;
            } else {
                fields.push(array);
            }
        }

        Ok(fields)
    }

    fn decode_next_batch(&mut self, remaining: usize) -> Result<Option<RecordBatch>> {
        let fields = self.inner_decode_next_batch(remaining)?;

        if fields.is_empty() {
            if remaining == 0 {
                Ok(None)
            } else {
                // In case of empty projection, we need to create a RecordBatch with `row_count` only
                // to reflect the row number
                Ok(Some(
                    RecordBatch::try_new_with_options(
                        Arc::clone(&self.schema_ref),
                        fields,
                        &RecordBatchOptions::new()
                            .with_row_count(Some(self.batch_size.min(remaining))),
                    )
                    .context(error::ConvertRecordBatchSnafu)?,
                ))
            }
        } else {
            Ok(Some(
                RecordBatch::try_new(Arc::clone(&self.schema_ref), fields)
                    .context(error::ConvertRecordBatchSnafu)?,
            ))
        }
    }

    pub fn new(stripe: Stripe, schema_ref: SchemaRef, batch_size: usize) -> Result<Self> {
        Self::new_with_selection(stripe, schema_ref, batch_size, None)
    }

    pub fn new_with_selection(
        stripe: Stripe,
        schema_ref: SchemaRef,
        batch_size: usize,
        row_selection: Option<RowSelection>,
    ) -> Result<Self> {
        let number_of_rows = stripe.number_of_rows();
        let decoders = stripe
            .columns()
            .iter()
            .zip(schema_ref.fields.iter())
            .map(|(col, field)| array_decoder_factory(col, field.data_type(), &stripe))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            stripe,
            schema_ref,
            decoders,
            index: 0,
            batch_size,
            number_of_rows,
            row_selection,
            selection_index: 0,
            selection_consumed: 0,
        })
    }

    /// Skip the specified number of rows by calling skip_values on each decoder
    fn skip_rows(&mut self, count: usize) -> Result<()> {
        // Call skip_values on each decoder to efficiently skip rows
        // Top-level decoders don't have parent_present
        for decoder in &mut self.decoders {
            decoder.skip_values(count, None)?;
        }
        Ok(())
    }
}
