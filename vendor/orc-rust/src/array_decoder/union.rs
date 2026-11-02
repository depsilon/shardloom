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

use arrow::array::{ArrayRef, UnionArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::UnionFields;
use snafu::ResultExt;

use crate::column::Column;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::ArrowSnafu;
use crate::error::Result;
use crate::proto::stream::Kind;
use crate::reader::memory::decoded::{bitmap, Memory, Values};
use crate::stripe::Stripe;

use super::{array_decoder_factory, derive_present_vec, ArrayBatchDecoder, PresentDecoder};

#[cfg(test)]
#[path = "union_ownership_tests.rs"]
mod ownership_tests;

/// Decode ORC Union column into batches of Arrow Sparse UnionArrays.
pub struct UnionArrayDecoder {
    // fields and variants should have same length
    // TODO: encode this assumption into types
    fields: UnionFields,
    variants: Vec<Box<dyn ArrayBatchDecoder>>,
    tags: Box<dyn PrimitiveValueDecoder<i8> + Send>,
    present: Option<PresentDecoder>,
    memory: Memory,
}

impl UnionArrayDecoder {
    pub fn new(column: &Column, fields: UnionFields, stripe: &Stripe) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column)?;

        let tags = stripe.stream_map().get_checked(column, Kind::Data)?;
        let tags = super::decoder_memory::byte(tags, stripe.stream_map().memory())?;

        let variants = column
            .children()
            .iter()
            .zip(fields.iter())
            .map(|(child, (_, field))| array_decoder_factory(child, field.data_type(), stripe))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            fields,
            variants,
            tags,
            present,
            memory: stripe.stream_map().memory(),
        })
    }
}

impl ArrayBatchDecoder for UnionArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;
        let mut tags = Values::<i8>::zeroed(batch_size, &self.memory)?;
        match &present {
            Some(present) => {
                // Since UnionArrays don't have nullability, we rely on child arrays.
                // So we default to first child (tag 0) for any nulls from this parent Union.
                self.tags.decode_spaced(&mut tags, present)?;
            }
            None => {
                self.tags.decode(&mut tags)?;
            }
        }

        for &tag in tags.iter() {
            validate_tag(tag, self.variants.len())?;
        }
        let mut child_arrays = Vec::with_capacity(self.variants.len());
        for (index, decoder) in self.variants.iter_mut().enumerate() {
            let child_present = NullBuffer::new(bitmap(batch_size, &self.memory, |row| {
                tags[row] as usize == index
                    && present
                        .as_ref()
                        .map_or(true, |present| present.is_valid(row))
            })?);
            child_arrays.push(decoder.next_batch(batch_size, Some(&child_present))?);
        }
        // ORC tags are child ordinals; Arrow permits other declared type IDs.
        for tag in tags.iter_mut() {
            *tag = self
                .fields
                .iter()
                .nth(*tag as usize)
                .ok_or_else(|| {
                    crate::error::OutOfSpecSnafu {
                        msg: "union field is missing",
                    }
                    .build()
                })?
                .0;
        }
        let type_ids = tags.into_buffer().into();
        let array = UnionArray::try_new(self.fields.clone(), type_ids, None, child_arrays)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        let mut remaining =
            super::skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;
        let mut counts = Values::<usize>::zeroed(self.variants.len(), &self.memory)?;
        let mut tags = [0i8; 1024];
        while remaining != 0 {
            let take = remaining.min(tags.len());
            self.tags.decode(&mut tags[..take])?;
            for &tag in &tags[..take] {
                counts[validate_tag(tag, self.variants.len())?] += 1;
            }
            remaining -= take;
        }
        for (decoder, &count) in self.variants.iter_mut().zip(counts.iter()) {
            decoder.skip_values(count, None)?;
        }

        Ok(())
    }
}

fn validate_tag(tag: i8, variants: usize) -> Result<usize> {
    usize::try_from(tag)
        .ok()
        .filter(|&tag| tag < variants)
        .ok_or_else(|| {
            crate::error::OutOfSpecSnafu {
                msg: "union tag is out of range",
            }
            .build()
        })
}
