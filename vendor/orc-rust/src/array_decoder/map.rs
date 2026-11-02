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

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Fields};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::reader::memory::decoded::{offsets, total_length, Memory, Values};
use crate::stripe::Stripe;

use super::{array_decoder_factory, ArrayBatchDecoder, PresentDecoder};

#[cfg(test)]
#[path = "map_ownership_tests.rs"]
mod ownership_tests;

pub struct MapArrayDecoder {
    keys: Box<dyn ArrayBatchDecoder>,
    values: Box<dyn ArrayBatchDecoder>,
    present: Option<PresentDecoder>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    memory: Memory,
    fields: Fields,
}

impl MapArrayDecoder {
    pub fn new(
        column: &Column,
        keys_field: Arc<Field>,
        values_field: Arc<Field>,
        stripe: &Stripe,
    ) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column)?;

        let children = column.children();
        let keys_column = &children[0];
        let keys = array_decoder_factory(keys_column, keys_field.data_type(), stripe)?;

        let values_column = &children[1];
        let values = array_decoder_factory(values_column, values_field.data_type(), stripe)?;

        let reader = stripe.stream_map().get_checked(column, Kind::Length)?;
        let lengths = super::decoder_memory::unsigned(
            reader,
            column.rle_version(),
            stripe.stream_map().memory(),
        )?;

        let fields = Fields::from(vec![keys_field, values_field]);

        Ok(Self {
            keys,
            values,
            present,
            lengths,
            memory: stripe.stream_map().memory(),
            fields,
        })
    }
}

impl ArrayBatchDecoder for MapArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let mut lengths = Values::<i64>::zeroed(batch_size, &self.memory)?;
        if let Some(present) = &present {
            self.lengths.decode_spaced(&mut lengths, present)?;
        } else {
            self.lengths.decode(&mut lengths)?;
        }
        let total_length = total_length(&lengths, i32::MAX as usize)?;
        let offsets = offsets::<i32>(&lengths, &self.memory)?;
        drop(lengths);
        // Fetch key and value arrays, each with total_length elements
        // Fetch child array as one Array with total_length elements
        let keys_array = self.keys.next_batch(total_length, None)?;
        let values_array = self.values.next_batch(total_length, None)?;
        // Compose the keys + values array into a StructArray with two entries
        let entries =
            StructArray::try_new(self.fields.clone(), vec![keys_array, values_array], None)
                .context(ArrowSnafu)?;

        let field = Arc::new(Field::new_struct("entries", self.fields.clone(), false));
        let array =
            MapArray::try_new(field, offsets, entries, present, false).context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use super::skip_present_and_get_non_null_count;

        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;

        // Decode lengths to determine how many entries to skip
        let total_length = super::skip_lengths(self.lengths.as_mut(), non_null_count)?;

        // Skip both keys and values (they don't have parent_present from map)
        self.keys.skip_values(total_length, None)?;
        self.values.skip_values(total_length, None)?;

        Ok(())
    }
}
