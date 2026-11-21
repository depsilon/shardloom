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

//! Ordered full validation over borrowed container continuations.

use arrow_schema::ArrowError;

use super::{Variant, VariantMetadata};
use crate::decoder::{OffsetSizeBytes, map_bytes_to_offsets};
use crate::utils::slice_from_slice;

/// A suspended parent's original offset iterator. No descendant is parsed until
/// it is requested, and no frame owns a recursive value tree.
pub(super) struct Children<'v> {
    offsets: &'v [u8],
    values: &'v [u8],
    width: OffsetSizeBytes,
    next: usize,
    end: usize,
    list_offset: Option<usize>,
}

impl<'v> Children<'v> {
    pub(super) fn object(offsets: &'v [u8], values: &'v [u8], width: OffsetSizeBytes) -> Self {
        Self {
            offsets,
            values,
            width,
            next: 0,
            end: (offsets.len() / width as usize).saturating_sub(1),
            list_offset: None,
        }
    }

    pub(super) fn list(offsets: &'v [u8], values: &'v [u8], width: OffsetSizeBytes) -> Self {
        Self {
            offsets,
            values,
            width,
            next: 1,
            end: offsets.len() / width as usize,
            list_offset: Some(map_bytes_to_offsets(offsets, width).next().unwrap_or(0)),
        }
    }

    fn next<'m>(
        &mut self,
        metadata: &VariantMetadata<'m>,
    ) -> Result<Option<Variant<'m, 'v>>, ArrowError> {
        if self.next >= self.end {
            return Ok(None);
        }
        // Map/ChunksExact::nth advances directly to this offset. This preserves
        // the original iterator's decoding without inspecting later siblings.
        let offset = map_bytes_to_offsets(self.offsets, self.width)
            .nth(self.next)
            .expect("offset count was derived from the same slice and width");
        let bytes = match self.list_offset {
            Some(current) => slice_from_slice(self.values, current..offset)?,
            None => slice_from_slice(self.values, offset..)?,
        };
        let child = Variant::try_new_with_metadata_and_shallow_validation(metadata.clone(), bytes)?;
        self.next += 1;
        if self.list_offset.is_some() {
            self.list_offset = Some(offset);
        }
        Ok(Some(child))
    }
}

fn children<'v>(value: &mut Variant<'_, 'v>) -> Result<Option<Children<'v>>, ArrowError> {
    match value {
        Variant::Object(object) => object.validation_children(),
        Variant::List(list) => list.validation_children(),
        _ => Ok(None),
    }
}

pub(super) fn validate<'m, 'v>(mut value: Variant<'m, 'v>) -> Result<Variant<'m, 'v>, ArrowError> {
    let Some(mut current) = children(&mut value)? else {
        return Ok(value);
    };
    // Every child uses the same dictionary, validated by the root before child
    // traversal. Keep one inline header instead of repeating it in each frame.
    let metadata = match &value {
        Variant::Object(object) => object.metadata.clone(),
        Variant::List(list) => list.metadata.clone(),
        _ => unreachable!("only containers have validation children"),
    };
    let mut parents = Vec::new();
    loop {
        if let Some(mut child) = current.next(&metadata)? {
            if let Some(nested) = children(&mut child)? {
                parents.push(current);
                current = nested;
            }
        } else if let Some(parent) = parents.pop() {
            current = parent;
        } else {
            break;
        }
    }
    match &mut value {
        Variant::Object(object) => object.finish_validation(),
        Variant::List(list) => list.finish_validation(),
        _ => unreachable!("only containers have validation children"),
    }
    Ok(value)
}
