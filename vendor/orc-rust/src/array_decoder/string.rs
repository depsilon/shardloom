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

use std::io::Read;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, GenericByteArray, OffsetSizeTrait, StringArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{ByteArrayType, GenericBinaryType, GenericStringType};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::compression::Decompressor;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, IoSnafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::reader::memory::{
    decoded::{offsets, total_length, Memory, Values},
    invalid,
};
use crate::stripe::Stripe;

use super::{ArrayBatchDecoder, Int64ArrayDecoder, PresentDecoder};

#[cfg(test)]
#[path = "string_ownership_tests.rs"]
mod ownership_tests;

// TODO: reduce duplication with string below
pub fn new_binary_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let present = PresentDecoder::from_stripe(stripe, column)?;

    let lengths = stripe.stream_map().get_checked(column, Kind::Length)?;
    let lengths = super::decoder_memory::unsigned(
        lengths,
        column.rle_version(),
        stripe.stream_map().memory(),
    )?;

    let bytes = Box::new(stripe.stream_map().get_checked(column, Kind::Data)?);
    Ok(Box::new(BinaryArrayDecoder::new(
        bytes,
        lengths,
        present,
        stripe.stream_map().memory(),
    )))
}

pub fn new_string_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let kind = column.encoding().kind();
    let present = PresentDecoder::from_stripe(stripe, column)?;

    let lengths = stripe.stream_map().get_checked(column, Kind::Length)?;
    let lengths = super::decoder_memory::unsigned(
        lengths,
        column.rle_version(),
        stripe.stream_map().memory(),
    )?;

    match kind {
        ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => {
            let bytes = Box::new(stripe.stream_map().get_checked(column, Kind::Data)?);
            Ok(Box::new(DirectStringArrayDecoder::new(
                bytes,
                lengths,
                present,
                stripe.stream_map().memory(),
            )))
        }
        ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
            let bytes = Box::new(
                stripe
                    .stream_map()
                    .get_checked(column, Kind::DictionaryData)?,
            );
            // TODO: is this always guaranteed to be set for all dictionaries?
            let dictionary_size = column.dictionary_size();
            // We assume here we have fetched all the dictionary strings (according to size above)
            let dictionary_strings =
                DirectStringArrayDecoder::new(bytes, lengths, None, stripe.stream_map().memory())
                    .next_byte_batch(dictionary_size, None)?;
            let dictionary_strings = Arc::new(dictionary_strings);

            let indexes = stripe.stream_map().get_checked(column, Kind::Data)?;
            let indexes = super::decoder_memory::unsigned(
                indexes,
                column.rle_version(),
                stripe.stream_map().memory(),
            )?;
            let indexes = Int64ArrayDecoder::new(indexes, present, stripe.stream_map().memory());

            Ok(Box::new(DictionaryStringArrayDecoder::new(
                indexes,
                dictionary_strings,
            )?))
        }
    }
}

// TODO: check this offset size type
pub type DirectStringArrayDecoder = GenericByteArrayDecoder<GenericStringType<i32>>;
pub type BinaryArrayDecoder = GenericByteArrayDecoder<GenericBinaryType<i32>>;

pub struct GenericByteArrayDecoder<T: ByteArrayType> {
    bytes: Box<Decompressor>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    present: Option<PresentDecoder>,
    phantom: PhantomData<T>,
    memory: Memory,
}

impl<T: ByteArrayType> GenericByteArrayDecoder<T> {
    fn new(
        bytes: Box<Decompressor>,
        lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        present: Option<PresentDecoder>,
        memory: Memory,
    ) -> Self {
        Self {
            bytes,
            lengths,
            present,
            phantom: Default::default(),
            memory,
        }
    }

    fn next_byte_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<GenericByteArray<T>> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let mut lengths = Values::<i64>::zeroed(batch_size, &self.memory)?;
        if let Some(present) = &present {
            self.lengths.decode_spaced(&mut lengths, present)?;
        } else {
            self.lengths.decode(&mut lengths)?;
        }
        let total_length = total_length(&lengths, T::Offset::MAX_OFFSET)?;
        let offsets = offsets::<T::Offset>(&lengths, &self.memory)?;
        drop(lengths);
        // Fetch all data bytes at once
        let mut bytes = Values::<u8>::zeroed(total_length, &self.memory)?;
        self.bytes.read_exact(&mut bytes).context(IoSnafu)?;
        let bytes = bytes.into_buffer();

        let null_buffer = match present {
            // Edge case where keys of map cannot have a null buffer
            Some(present) if present.null_count() == 0 => None,
            _ => present,
        };
        let array =
            GenericByteArray::<T>::try_new(offsets, bytes, null_buffer).context(ArrowSnafu)?;
        Ok(array)
    }
}

impl<T: ByteArrayType> ArrayBatchDecoder for GenericByteArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self.next_byte_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use crate::array_decoder::skip_present_and_get_non_null_count;

        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;

        // Decode lengths to determine how many bytes to skip
        let total_bytes = super::skip_lengths(self.lengths.as_mut(), non_null_count)?;

        // Skip the data bytes
        // TODO: can we use the decompressor to skip the bytes?
        let skipped = std::io::copy(
            &mut self.bytes.by_ref().take(total_bytes as u64),
            &mut std::io::sink(),
        )
        .context(IoSnafu)?;
        if skipped != total_bytes as u64 {
            return Err(invalid("truncated skipped string payload")).context(IoSnafu);
        }

        Ok(())
    }
}

pub struct DictionaryStringArrayDecoder {
    indexes: Int64ArrayDecoder,
    dictionary: Arc<StringArray>,
}

impl DictionaryStringArrayDecoder {
    fn new(indexes: Int64ArrayDecoder, dictionary: Arc<StringArray>) -> Result<Self> {
        Ok(Self {
            indexes,
            dictionary,
        })
    }
}

impl ArrayBatchDecoder for DictionaryStringArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let keys = self
            .indexes
            .next_primitive_batch(batch_size, parent_present)?;
        // Preserve the reader's Utf8 schema, while admitting the expanded bytes
        // before allocation instead of hiding them in a dictionary cast.
        let memory = &self.indexes.memory;
        let mut lengths = Values::<i64>::zeroed(batch_size, memory)?;
        for (row, key) in keys.iter().enumerate() {
            if let Some(key) = key {
                let key = usize::try_from(key)
                    .ok()
                    .filter(|key| *key < self.dictionary.len())
                    .ok_or_else(|| invalid("string dictionary index is out of range"))
                    .context(IoSnafu)?;
                lengths[row] = self.dictionary.value(key).len() as i64;
            }
        }
        let length = total_length(&lengths, i32::MAX as usize)?;
        let offsets = offsets::<i32>(&lengths, memory)?;
        drop(lengths);
        let mut bytes = Values::<u8>::zeroed(length, memory)?;
        let mut position = 0;
        for key in keys.iter().flatten() {
            let value = self.dictionary.value(key as usize).as_bytes();
            bytes[position..position + value.len()].copy_from_slice(value);
            position += value.len();
        }
        let array = StringArray::try_new(offsets, bytes.into_buffer(), keys.nulls().cloned())
            .context(ArrowSnafu)?;
        Ok(Arc::new(array))
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        self.indexes.skip_values(n, parent_present)
    }
}
