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

#[rustfmt::skip]
impl<'m, 'v> VariantObject<'m, 'v> {
    pub(crate) fn with_full_validation_reference(mut self) -> Result<Self, ArrowError> {
        if !self.validated {
            // Validate the metadata dictionary first, if not already validated, because we pass it
            // by value to all the children (who would otherwise re-validate it repeatedly).
            self.metadata = self.metadata.with_full_validation()?;

            let field_id_buffer = slice_from_slice(
                self.value,
                self.header.field_ids_start_byte() as _..self.first_field_offset_byte as _,
            )?;

            let mut field_ids_iter =
                map_bytes_to_offsets(field_id_buffer, self.header.field_id_size);

            // Validate all field ids exist in the metadata dictionary and the corresponding field names are lexicographically sorted
            if self.metadata.is_sorted() {
                // Since the metadata dictionary has unique and sorted field names, we can also guarantee this object's field names
                // are lexicographically sorted by their field id ordering
                let dictionary_size = self.metadata.len();

                if let Some(mut current_id) = field_ids_iter.next() {
                    for next_id in field_ids_iter {
                        if current_id >= dictionary_size {
                            return Err(ArrowError::InvalidArgumentError(
                                "field id is not valid".to_string(),
                            ));
                        }

                        if next_id <= current_id {
                            return Err(ArrowError::InvalidArgumentError(
                                "field names not sorted".to_string(),
                            ));
                        }
                        current_id = next_id;
                    }

                    if current_id >= dictionary_size {
                        return Err(ArrowError::InvalidArgumentError(
                            "field id is not valid".to_string(),
                        ));
                    }
                }
            } else {
                // The metadata dictionary can't guarantee uniqueness or sortedness, so we have to parse out the corresponding field names
                // to check lexicographical order
                //
                // Since we are probing the metadata dictionary by field id, this also verifies field ids are in-bounds
                let mut current_field_name = match field_ids_iter.next() {
                    Some(field_id) => Some(self.metadata.get(field_id)?),
                    None => None,
                };

                for field_id in field_ids_iter {
                    let next_field_name = self.metadata.get(field_id)?;

                    if let Some(current_name) = current_field_name {
                        if next_field_name < current_name {
                            return Err(ArrowError::InvalidArgumentError(
                                "field names not sorted".to_string(),
                            ));
                        }
                    }
                    current_field_name = Some(next_field_name);
                }
            }

            // Validate whether values are valid variant objects
            let field_offset_buffer = slice_from_slice(
                self.value,
                self.first_field_offset_byte as _..self.first_value_byte as _,
            )?;
            let num_offsets = field_offset_buffer.len() / self.header.field_offset_size() as usize;

            let value_buffer = slice_from_slice(self.value, self.first_value_byte as _..)?;

            map_bytes_to_offsets(field_offset_buffer, self.header.field_offset_size)
                .take(num_offsets.saturating_sub(1))
                .try_for_each(|offset| {
                    let value_bytes = slice_from_slice(value_buffer, offset..)?;
                    Variant::try_new_with_metadata_reference(self.metadata.clone(), value_bytes)?;

                    Ok::<_, ArrowError>(())
                })?;

            self.validated = true;
        }
        Ok(self)
    }
}
