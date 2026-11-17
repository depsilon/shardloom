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
//! Frozen recursive validation oracle from parquet-variant 59.3.2.

use super::*;
use crate::decoder::map_bytes_to_offsets;

#[rustfmt::skip]
impl<'m, 'v> VariantList<'m, 'v> {
    pub(crate) fn with_full_validation_reference(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Validate the metadata dictionary first, if not already validated, because we pass it
            // by value to all the children (who would otherwise re-validate it repeatedly).
            self.metadata = self.metadata.with_full_validation()?;

            let offset_buffer = slice_from_slice(
                self.value,
                self.header.first_offset_byte() as _..self.first_value_byte as _,
            )?;

            let value_buffer = slice_from_slice(self.value, self.first_value_byte as _..)?;

            // Validate whether values are valid variant objects
            //
            // Since we use offsets to slice into the value buffer, this also verifies all offsets are in-bounds
            // and monotonically increasing
            let mut offset_iter = map_bytes_to_offsets(offset_buffer, self.header.offset_size);
            let mut current_offset = offset_iter.next().unwrap_or(0);

            for next_offset in offset_iter {
                let value_bytes = slice_from_slice(value_buffer, current_offset..next_offset)?;
                Variant::try_new_with_metadata_reference(self.metadata.clone(), value_bytes)?;
                current_offset = next_offset;
            }

            self.validated = true;
        }
        Ok(self)
    }
}
