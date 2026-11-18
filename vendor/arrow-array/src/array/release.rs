// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Intrinsic ordered release for built-in nested arrays. Public ArrayRef stays
//! Arc<dyn Array>; concrete extraction is performed by each typed implementation.

use std::ops::{Deref, DerefMut};
use std::vec;

use arrow_buffer::{Buffer, NullBuffer};
use arrow_schema::DataType;

use super::{ArrayRef, StructArray};

#[cfg(test)]
#[path = "release_tests.rs"]
pub(super) mod tests;

pub(crate) enum Part {
    Type(DataType),
    Buffer(Buffer),
    OptionalBuffer(Option<Buffer>),
    Nulls(Option<NullBuffer>),
    Child(ArrayRef),
    Children(vec::IntoIter<ArrayRef>),
    UnionChildren(vec::IntoIter<Option<ArrayRef>>),
    Struct(StructArray),
}

enum Work {
    Child(ArrayRef),
    Struct(StructArray),
    Drop(Part),
}

/// Opaque declaration-order release work returned by built-in nested arrays.
/// This is an internal provider hook, not a new Arrow representation.
#[doc(hidden)]
pub struct ArrayReleaseNode {
    parts: [Option<Part>; 5],
    next: usize,
}

impl ArrayReleaseNode {
    pub(crate) fn new<const N: usize>(parts: [Part; N]) -> Self {
        assert!(N <= 5);
        let mut slots = std::array::from_fn(|_| None);
        for (slot, part) in slots.iter_mut().zip(parts) {
            *slot = Some(part);
        }
        Self {
            parts: slots,
            next: 0,
        }
    }

    fn next(&mut self) -> Option<Work> {
        while self.next < self.parts.len() {
            let slot = &mut self.parts[self.next];
            match slot {
                Some(Part::Children(children)) => {
                    if let Some(child) = children.next() {
                        return Some(Work::Child(child));
                    }
                }
                Some(Part::UnionChildren(children)) => {
                    if let Some(child) = children.by_ref().flatten().next() {
                        return Some(Work::Child(child));
                    }
                }
                _ => {}
            }
            self.next += 1;
            if let Some(part) = slot.take() {
                return Some(match part {
                    Part::Child(child) => Work::Child(child),
                    Part::Struct(child) => Work::Struct(child),
                    other => Work::Drop(other),
                });
            }
        }
        None
    }
}

impl Drop for ArrayReleaseNode {
    fn drop(&mut self) {
        // A node with a failing external owner keeps its remaining parts in
        // declaration order. Nested children still use the iterative route.
        while let Some(work) = self.next() {
            match work {
                Work::Child(child) => release(child),
                Work::Struct(child) => release_node(Some(child.release_node())),
                Work::Drop(part) => drop_part(part),
            }
        }
    }
}

fn drop_part(part: Part) {
    match part {
        Part::Type(value) => drop(value),
        Part::Buffer(value) => drop(value),
        Part::OptionalBuffer(value) => drop(value),
        Part::Nulls(value) => drop(value),
        Part::Children(value) => drop(value),
        Part::UnionChildren(value) => drop(value),
        Part::Child(_) | Part::Struct(_) => unreachable!("children are visited separately"),
    }
}

#[derive(Default)]
struct Pending(Vec<ArrayReleaseNode>);

impl Drop for Pending {
    fn drop(&mut self) {
        while let Some(node) = self.0.pop() {
            drop(node);
        }
    }
}

fn release(array: ArrayRef) {
    release_node(array.into_release_node());
}

fn release_node(mut current: Option<ArrayReleaseNode>) {
    let mut pending = Pending::default();
    loop {
        if let Some(mut node) = current.take() {
            match node.next() {
                Some(Work::Child(child)) => {
                    pending.0.push(node);
                    current = child.into_release_node();
                }
                Some(Work::Struct(child)) => {
                    pending.0.push(node);
                    current = Some(child.release_node());
                }
                Some(Work::Drop(part)) => {
                    // Keep the node owned locally if the external owner panics.
                    drop_part(part);
                    current = Some(node);
                }
                None => {}
            }
        } else if let Some(node) = pending.0.pop() {
            current = Some(node);
        } else {
            return;
        }
    }
}

/// The option is only empty after consuming or detaching the private owner.
/// Live array methods always see the same public ArrayRef.
#[derive(Clone)]
pub(crate) struct Child(Option<ArrayRef>);

impl std::fmt::Debug for Child {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl Child {
    pub(crate) fn into_inner(mut self) -> ArrayRef {
        self.0.take().expect("live child owner")
    }
}

impl From<ArrayRef> for Child {
    fn from(value: ArrayRef) -> Self {
        Self(Some(value))
    }
}

impl Deref for Child {
    type Target = ArrayRef;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("live child owner")
    }
}

impl DerefMut for Child {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("live child owner")
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if let Some(child) = self.0.take() {
            release(child);
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Children(Vec<ArrayRef>);

impl std::fmt::Debug for Children {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Children {
    pub(crate) fn into_inner(mut self) -> Vec<ArrayRef> {
        std::mem::take(&mut self.0)
    }
}

impl From<Vec<ArrayRef>> for Children {
    fn from(value: Vec<ArrayRef>) -> Self {
        Self(value)
    }
}

impl Deref for Children {
    type Target = Vec<ArrayRef>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Children {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for Children {
    fn drop(&mut self) {
        let children = std::mem::take(&mut self.0);
        release_node(Some(ArrayReleaseNode::new([Part::Children(
            children.into_iter(),
        )])));
    }
}

impl FromIterator<ArrayRef> for Children {
    fn from_iter<T: IntoIterator<Item = ArrayRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

#[derive(Clone, Default)]
pub(crate) struct UnionChildren(Vec<Option<ArrayRef>>);

impl std::fmt::Debug for UnionChildren {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl UnionChildren {
    pub(crate) fn into_inner(mut self) -> Vec<Option<ArrayRef>> {
        std::mem::take(&mut self.0)
    }
}

impl From<Vec<Option<ArrayRef>>> for UnionChildren {
    fn from(value: Vec<Option<ArrayRef>>) -> Self {
        Self(value)
    }
}

impl Deref for UnionChildren {
    type Target = Vec<Option<ArrayRef>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UnionChildren {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for UnionChildren {
    fn drop(&mut self) {
        let children = std::mem::take(&mut self.0);
        release_node(Some(ArrayReleaseNode::new([Part::UnionChildren(
            children.into_iter(),
        )])));
    }
}

impl FromIterator<Option<ArrayRef>> for UnionChildren {
    fn from_iter<T: IntoIterator<Item = Option<ArrayRef>>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
