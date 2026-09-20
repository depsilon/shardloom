//! Dense payloads for the compound reducer's private index directories.
//! Ordinals stay stable; only the small first page can move during growth.

use super::{allocate, failed};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::ops::{Index, IndexMut};

const PAGE_ITEMS: usize = 1024;

#[cfg(test)]
#[path = "compound_pages_tests.rs"]
mod tests;

struct Page<T> {
    values: Vec<T>,
    _lease: MemoryLease,
}

pub(super) struct DensePages<T> {
    pages: Vec<Page<T>>,
    len: usize,
    metadata: MemoryLease,
}

impl<T: Copy> DensePages<T> {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            pages: Vec::new(),
            len: 0,
            metadata: memory.reserve(0)?,
        })
    }

    /// Admit all capacity before the caller interns or publishes a record.
    pub(super) fn reserve_one(&mut self, memory: &LiveMemoryPool) -> Result<bool> {
        self.len
            .checked_add(1)
            .ok_or_else(|| failed("dense ordinal overflowed"))?;
        if self
            .pages
            .last()
            .is_some_and(|p| p.values.len() < p.values.capacity())
        {
            return Ok(true);
        }
        if self.pages.len() == 1 && self.pages[0].values.capacity() < PAGE_ITEMS {
            let capacity = self.pages[0].values.capacity() * 2;
            let Some((mut values, lease)) = allocate::<T>(capacity, memory)? else {
                return Ok(false);
            };
            values.extend_from_slice(&self.pages[0].values);
            self.pages[0] = Page {
                values,
                _lease: lease,
            };
            return Ok(true);
        }
        if self.pages.len() == self.pages.capacity() {
            let capacity = self
                .pages
                .capacity()
                .max(1)
                .checked_mul(2)
                .ok_or_else(|| failed("dense page metadata capacity overflowed"))?;
            let Some((mut pages, lease)) = allocate::<Page<T>>(capacity, memory)? else {
                return Ok(false);
            };
            pages.append(&mut self.pages);
            self.pages = pages;
            self.metadata = lease;
        }
        let capacity = if self.pages.is_empty() {
            16
        } else {
            PAGE_ITEMS
        };
        let Some((values, lease)) = allocate::<T>(capacity, memory)? else {
            return Ok(false);
        };
        self.pages.push(Page {
            values,
            _lease: lease,
        });
        Ok(true)
    }

    /// Infallible after reserve_one under the caller's partition lock.
    pub(super) fn push(&mut self, value: T) {
        let page = self.pages.last_mut().expect("dense capacity admitted");
        assert!(page.values.len() < page.values.capacity());
        page.values.push(value);
        self.len += 1;
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.pages.iter().flat_map(|page| page.values.iter())
    }

    pub(super) fn release(&mut self) -> Result<()> {
        self.pages = Vec::new();
        self.len = 0;
        self.metadata.resize(0)
    }
}

impl<T> Index<usize> for DensePages<T> {
    type Output = T;
    fn index(&self, index: usize) -> &T {
        &self.pages[index / PAGE_ITEMS].values[index % PAGE_ITEMS]
    }
}

impl<T> IndexMut<usize> for DensePages<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        &mut self.pages[index / PAGE_ITEMS].values[index % PAGE_ITEMS]
    }
}
