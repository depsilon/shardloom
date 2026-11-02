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

use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, FieldRef};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::encoding::PrimitiveValueDecoder;
use crate::proto::stream::Kind;

use crate::error::{ArrowSnafu, Result};
use crate::reader::memory::decoded::{offsets, total_length, Memory, Values};
use crate::stripe::Stripe;

use super::{array_decoder_factory, ArrayBatchDecoder, PresentDecoder};

#[cfg(test)]
#[path = "list_ownership_tests.rs"]
mod ownership_tests;

pub struct ListArrayDecoder {
    inner: Box<dyn ArrayBatchDecoder>,
    present: Option<PresentDecoder>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    memory: Memory,
    field: FieldRef,
}

impl ListArrayDecoder {
    pub fn new(column: &Column, field: Arc<Field>, stripe: &Stripe) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column)?;

        let child = &column.children()[0];
        let inner = array_decoder_factory(child, field.data_type(), stripe)?;

        let reader = stripe.stream_map().get_checked(column, Kind::Length)?;
        let lengths = super::decoder_memory::unsigned(
            reader,
            column.rle_version(),
            stripe.stream_map().memory(),
        )?;

        Ok(Self {
            inner,
            present,
            lengths,
            memory: stripe.stream_map().memory(),
            field,
        })
    }
}

impl ArrayBatchDecoder for ListArrayDecoder {
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
        // Fetch child array as one Array with total_length elements
        let child_array = self.inner.next_batch(total_length, None)?;
        let null_buffer = present;

        let array = ListArray::try_new(self.field.clone(), offsets, child_array, null_buffer)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use super::skip_present_and_get_non_null_count;

        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;

        // Decode lengths to determine how many child values to skip
        let total_length = super::skip_lengths(self.lengths.as_mut(), non_null_count)?;

        // Skip the child values (children don't have parent_present from list)
        self.inner.skip_values(total_length, None)?;

        Ok(())
    }
}
