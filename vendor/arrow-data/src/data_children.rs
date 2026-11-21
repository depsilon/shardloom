// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Owned child descriptors retain the public Vec-based construction/parts API,
//! with intrinsic ordered release and iterative cloning of the descriptor tree.

use std::ops::{Deref, DerefMut};
use std::{slice, vec};

use arrow_buffer::{Buffer, NullBuffer};
use arrow_schema::DataType;

use super::ArrayData;

#[cfg(test)]
#[path = "data_children_tests.rs"]
pub(super) mod tests;

#[derive(Default)]
pub(super) struct Children(Vec<ArrayData>);

impl Children {
    pub(super) fn into_inner(mut self) -> Vec<ArrayData> {
        std::mem::take(&mut self.0)
    }
}

impl From<Vec<ArrayData>> for Children {
    fn from(value: Vec<ArrayData>) -> Self {
        Self(value)
    }
}

impl FromIterator<ArrayData> for Children {
    fn from_iter<T: IntoIterator<Item = ArrayData>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl std::fmt::Debug for Children {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for Children {
    type Target = Vec<ArrayData>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Children {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> IntoIterator for &'a Children {
    type Item = &'a ArrayData;
    type IntoIter = slice::Iter<'a, ArrayData>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a mut Children {
    type Item = &'a mut ArrayData;
    type IntoIter = slice::IterMut<'a, ArrayData>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl IntoIterator for Children {
    type Item = ArrayData;
    type IntoIter = vec::IntoIter<ArrayData>;
    fn into_iter(self) -> Self::IntoIter {
        self.into_inner().into_iter()
    }
}

// Declared in exactly ArrayData's owning-field order. A panicking external
// owner leaves later fields to ordinary ordered destruction. Child descriptors
// themselves contain Children, so their cleanup starts an iterative walk too.
struct ReleaseFrame {
    dtype: Option<DataType>,
    buffers: Vec<Buffer>,
    children: vec::IntoIter<ArrayData>,
    _nulls: Option<NullBuffer>,
}

struct ReleasePending(Vec<ReleaseFrame>);

impl Drop for ReleasePending {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

impl Drop for Children {
    fn drop(&mut self) {
        let children = std::mem::take(&mut self.0);
        if children.is_empty() {
            return;
        }
        // Declare outer work first so the current subtree unwinds before it.
        let mut pending = ReleasePending(Vec::new());
        let mut current = ReleaseFrame {
            dtype: None,
            buffers: Vec::new(),
            children: children.into_iter(),
            _nulls: None,
        };
        loop {
            // Remove each prefix owner from the frame before invoking its Drop.
            drop(current.dtype.take());
            drop(std::mem::take(&mut current.buffers));
            if let Some(child) = current.children.next() {
                pending.0.push(current);
                let (dtype, _, nulls, _, buffers, children) = child.into_parts();
                current = ReleaseFrame {
                    dtype: Some(dtype),
                    buffers,
                    children: children.into_iter(),
                    _nulls: nulls,
                };
            } else {
                // Release the exhausted vector allocation and then the nulls.
                drop(current);
                match pending.0.pop() {
                    Some(parent) => current = parent,
                    None => return,
                }
            }
        }
    }
}

struct CloneFrame<'a> {
    source: &'a ArrayData,
    result: ArrayData,
    next: usize,
    slice: Option<(usize, usize)>,
    children_slice: Option<(usize, usize)>,
}

impl<'a> CloneFrame<'a> {
    fn new(source: &'a ArrayData, slice: Option<(usize, usize)>) -> Self {
        if let Some((offset, length)) = slice {
            let end = offset
                .checked_add(length)
                .expect("offset + length overflow");
            assert!(end <= source.len(), "assertion failed: end <= self.len()");
        }
        let struct_slice = slice
            .filter(|_| matches!(source.data_type, DataType::Struct(_)))
            .map(|(offset, length)| (source.offset + offset, length));
        Self {
            source,
            result: ArrayData {
                data_type: source.data_type.clone(),
                len: struct_slice.map_or(source.len, |(_, len)| len),
                offset: if struct_slice.is_some() {
                    0
                } else {
                    source.offset
                },
                buffers: source.buffers.clone(),
                // Vec::clone reserved the complete child capacity before cloning
                // the first child. Nulls are cloned after all child descriptors.
                child_data: Vec::with_capacity(source.child_data.len()).into(),
                nulls: None,
            },
            next: 0,
            slice,
            children_slice: struct_slice,
        }
    }

    fn child_slice(&self) -> Option<(usize, usize)> {
        self.children_slice
    }

    fn finish(mut self) -> ArrayData {
        if let Some((offset, length)) = self.slice {
            if !matches!(self.source.data_type, DataType::Struct(_)) {
                // The original non-struct path cloned the complete descriptor
                // first, then replaced its length, offset and null slice.
                self.result.nulls = self.source.nulls.clone();
                self.result.len = length;
                self.result.offset = offset + self.source.offset;
            }
            self.result.nulls = self.source.nulls.as_ref().map(|x| x.slice(offset, length));
        } else {
            self.result.nulls = self.source.nulls.clone();
        }
        self.result
    }
}

struct ClonePending<'a>(Vec<CloneFrame<'a>>);

impl Drop for ClonePending<'_> {
    fn drop(&mut self) {
        while let Some(frame) = self.0.pop() {
            drop(frame);
        }
    }
}

impl Clone for ArrayData {
    fn clone(&self) -> Self {
        clone_or_slice(self, None)
    }
}

pub(super) fn clone_or_slice(source: &ArrayData, slice: Option<(usize, usize)>) -> ArrayData {
    let mut pending = ClonePending(Vec::new());
    let mut current = CloneFrame::new(source, slice);
    loop {
        if let Some(child) = current.source.child_data.get(current.next) {
            let slice = current.child_slice();
            current.next += 1;
            pending.0.push(current);
            current = CloneFrame::new(child, slice);
        } else {
            let result = current.finish();
            match pending.0.pop() {
                Some(mut parent) => {
                    parent.result.child_data.push(result);
                    current = parent;
                }
                None => return result,
            }
        }
    }
}
