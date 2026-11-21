// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Opaque storage owners. They are not part of an Arrow field's logical value.

use std::any::Any;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct Retained(pub(crate) Vec<Arc<dyn Any + Send + Sync>>);

impl Retained {
    pub(crate) fn storage_size(&self) -> usize {
        self.0.capacity() * std::mem::size_of::<Arc<dyn Any + Send + Sync>>()
    }

    pub(crate) fn push(&mut self, owner: Arc<dyn Any + Send + Sync>) {
        // Native callers prepay this storage. Avoid Vec's geometric first growth
        // for the ordinary single-credit case.
        self.0.reserve_exact(1);
        self.0.push(owner);
    }
}

/// A sized final owner lets cleanup detach children safely with Arc::into_inner.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub(crate) struct FieldStorage<T> {
    pub(crate) fields: Vec<T>,
    #[cfg_attr(feature = "serde", serde(skip))]
    pub(crate) retained: Retained,
}

impl<T> From<Vec<T>> for FieldStorage<T> {
    fn from(fields: Vec<T>) -> Self {
        Self {
            fields,
            retained: Retained::default(),
        }
    }
}

impl<T> FieldStorage<T> {
    pub(crate) fn storage_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.fields.capacity() * std::mem::size_of::<T>()
            + self.retained.storage_size()
    }
}

impl<T> std::ops::Deref for FieldStorage<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for FieldStorage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fields.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for FieldStorage<T> {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl<T: Eq> Eq for FieldStorage<T> {}

impl<T: PartialOrd> PartialOrd for FieldStorage<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.fields.partial_cmp(&other.fields)
    }
}

impl<T: Ord> Ord for FieldStorage<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.fields.cmp(&other.fields)
    }
}

impl<T: Hash> Hash for FieldStorage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}
