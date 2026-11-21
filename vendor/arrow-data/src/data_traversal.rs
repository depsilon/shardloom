// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered descriptor traversals. Public provider operations retain their
//! original contracts; caller memory admission belongs to the native boundary.

use std::slice;

use arrow_buffer::Buffer;
use arrow_schema::{ArrowError, DataType, UnionMode};

use super::{ArrayData, BufferSpec, checked_len_plus_offset, layout};

pub(super) struct Walk<'a> {
    next: Option<&'a ArrayData>,
    parents: Vec<slice::Iter<'a, ArrayData>>,
}

impl<'a> Walk<'a> {
    pub(super) fn new(root: &'a ArrayData) -> Self {
        Self {
            next: Some(root),
            parents: Vec::new(),
        }
    }
}

impl<'a> Iterator for Walk<'a> {
    type Item = &'a ArrayData;

    fn next(&mut self) -> Option<Self::Item> {
        let node = loop {
            if let Some(node) = self.next.take() {
                break node;
            }
            let parent = self.parents.last_mut()?;
            if let Some(node) = parent.next() {
                break node;
            }
            self.parents.pop();
        };
        if !node.child_data.is_empty() {
            self.parents.push(node.child_data.iter());
        }
        Some(node)
    }
}

pub(super) fn align_buffers(root: &mut ArrayData) {
    let mut cursors = vec![slice::from_mut(root).iter_mut()];
    while let Some(cursor) = cursors.last_mut() {
        let Some(data) = cursor.next() else {
            cursors.pop();
            continue;
        };
        let layout = layout(&data.data_type);
        for (buffer, spec) in data.buffers.iter_mut().zip(&layout.buffers) {
            if let BufferSpec::FixedWidth { alignment, .. } = spec {
                if buffer.as_ptr().align_offset(*alignment) != 0 {
                    *buffer = Buffer::from_slice_ref(buffer.as_ref());
                }
            }
        }
        if !data.child_data.is_empty() {
            cursors.push(data.child_data.iter_mut());
        }
    }
}

// Exactly the checks that precede the first get_valid_child_data call in the
// pinned implementation. Map schema checks intentionally precede child count.
fn children_prefix(data: &ArrayData) -> Result<(), ArrowError> {
    match &data.data_type {
        DataType::Map(field, _) => {
            let DataType::Struct(entries_fields) = field.data_type() else {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Map field should be a entries struct data type, got {:?} instead",
                    field.data_type()
                )));
            };
            if entries_fields.len() != 2 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Map entries data type should be a struct containing 2 fields, got {} fields",
                    entries_fields.len()
                )));
            }
            if entries_fields[0].is_nullable() {
                return Err(ArrowError::InvalidArgumentError(
                    "Map key field must not be nullable".to_string(),
                ));
            }
            data.validate_num_child_data(1)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Dictionary(_, _) => data.validate_num_child_data(1),
        DataType::Struct(fields) => data.validate_num_child_data(fields.len()),
        DataType::Union(fields, _) => data.validate_num_child_data(fields.len()),
        DataType::RunEndEncoded(_, _) => data.validate_num_child_data(2),
        _ if !data.child_data.is_empty() => Err(ArrowError::InvalidArgumentError(format!(
            "Expected no child arrays for type {} but got {}",
            data.data_type,
            data.child_data.len()
        ))),
        _ => Ok(()),
    }
}

fn child(data: &ArrayData, index: usize) -> Result<&ArrayData, ArrowError> {
    let expected = match &data.data_type {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => f.data_type(),
        DataType::Dictionary(_, value) => value,
        DataType::Struct(fields) => fields[index].data_type(),
        DataType::Union(fields, _) => fields.get(index).unwrap().1.data_type(),
        DataType::RunEndEncoded(ends, values) => {
            if index == 0 {
                ends.data_type()
            } else {
                values.data_type()
            }
        }
        _ => unreachable!("child count was checked before descending"),
    };
    data.get_child_data_of_type(index, expected)
}

// These checks occur immediately after this particular child's complete cheap
// validation, before the next sibling's type or layout can produce an error.
fn child_suffix(data: &ArrayData, index: usize) -> Result<(), ArrowError> {
    let values = &data.child_data[index];
    match &data.data_type {
        DataType::List(_) | DataType::Map(_, _) => data.validate_offsets::<i32>(values.len)?,
        DataType::LargeList(_) => data.validate_offsets::<i64>(values.len)?,
        DataType::ListView(_) => data.validate_offsets_and_sizes::<i32>(values.len)?,
        DataType::LargeListView(_) => data.validate_offsets_and_sizes::<i64>(values.len)?,
        DataType::FixedSizeList(_, list_size) => {
            let list_size: usize = (*list_size).try_into().map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "{} has a negative list_size {}",
                    data.data_type, list_size
                ))
            })?;
            let expected_values_len = data.len.checked_mul(list_size).expect(
                "integer overflow computing expected number of expected values in FixedListSize",
            );
            if values.len < expected_values_len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Values length {} is less than the length ({}) multiplied by the value size ({}) for {}",
                    values.len, data.len, list_size, data.data_type
                )));
            }
        }
        DataType::Struct(fields) if values.len < data.len => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{} child array #{} for field {} has length smaller than expected for struct array ({} < {})",
                data.data_type,
                index,
                fields[index].name(),
                values.len,
                data.len
            )));
        }
        DataType::RunEndEncoded(_, _) if index == 1 => {
            let ends = &data.child_data[0];
            if ends.len != values.len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "The run_ends array length should be the same as values array length. Run_ends array length is {}, values array length is {}",
                    ends.len, values.len
                )));
            }
            if ends.nulls.is_some() {
                return Err(ArrowError::InvalidArgumentError(
                    "Found null values in run_ends array. The run_ends array should not have null values.".to_string()
                ));
            }
        }
        DataType::Union(_, UnionMode::Sparse) => {
            let len_plus_offset = checked_len_plus_offset(&data.data_type, data.len, data.offset)?;
            if values.len < len_plus_offset {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Sparse union child array #{} has length smaller than expected for union array ({} < {})",
                    index, values.len, len_plus_offset
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn validate(root: &ArrayData) -> Result<(), ArrowError> {
    let mut parents = Vec::<(&ArrayData, usize)>::new();
    let mut data = root;
    'enter: loop {
        data.validate_layout()?;
        children_prefix(data)?;
        if !data.child_data.is_empty() {
            let next = child(data, 0)?;
            parents.push((data, 0));
            data = next;
            continue;
        }
        loop {
            data.validate_type()?;
            let Some((parent, index)) = parents.pop() else {
                return Ok(());
            };
            child_suffix(parent, index)?;
            if index + 1 < parent.child_data.len() {
                let next = child(parent, index + 1)?;
                parents.push((parent, index + 1));
                data = next;
                continue 'enter;
            }
            data = parent;
        }
    }
}

pub(super) fn validate_full(root: &ArrayData) -> Result<(), ArrowError> {
    // The original first validate_data already checked the complete immutable
    // layout tree. Repeating those successful cheap checks cannot fail later.
    root.validate()?;
    let mut parents = Vec::<(&ArrayData, usize)>::new();
    let mut data = root;
    loop {
        if let Err(mut error) = data.validate_nulls().and_then(|()| data.validate_values()) {
            for &(parent, index) in parents.iter().rev() {
                error = ArrowError::InvalidArgumentError(format!(
                    "{} child #{} invalid: {}",
                    parent.data_type, index, error
                ));
            }
            return Err(error);
        }
        if let Some(first) = data.child_data.first() {
            parents.push((data, 0));
            data = first;
            continue;
        }
        loop {
            let Some((parent, index)) = parents.pop() else {
                return Ok(());
            };
            if let Some(next) = parent.child_data.get(index + 1) {
                parents.push((parent, index + 1));
                data = next;
                break;
            }
        }
    }
}
