//! A fixed inline continuation prefix with credited, fallible heap growth.

use super::ReservedVec;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;

pub(crate) struct ReservedStack<T, const INLINE: usize> {
    // Spill frames are deeper than every inline frame and must release first.
    spill: ReservedVec<T>,
    inline: [Option<T>; INLINE],
    inline_len: usize,
}

impl<T, const INLINE: usize> ReservedStack<T, INLINE> {
    pub(crate) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            spill: ReservedVec::new(memory)?,
            inline: [const { None }; INLINE],
            inline_len: 0,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.inline_len + self.spill.values.len()
    }

    pub(crate) fn reserve_one(&mut self) -> Result<()> {
        if self.inline_len == INLINE {
            self.spill.reserve_one()?;
        }
        Ok(())
    }

    pub(crate) fn push_admitted(&mut self, value: T) {
        if self.inline_len < INLINE {
            self.inline[self.inline_len] = Some(value);
            self.inline_len += 1;
        } else {
            debug_assert!(self.spill.values.len() < self.spill.values.capacity());
            self.spill.values.push(value);
        }
    }

    pub(crate) fn push(&mut self, value: T) -> Result<()> {
        self.reserve_one()?;
        self.push_admitted(value);
        Ok(())
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        if let Some(value) = self.spill.values.pop() {
            return Some(value);
        }
        self.inline_len = self.inline_len.checked_sub(1)?;
        self.inline[self.inline_len].take()
    }
}

impl<T, const INLINE: usize> Drop for ReservedStack<T, INLINE> {
    fn drop(&mut self) {
        // Let ordinary slice drop glue finish every sibling after a panic.
        // Reversing first keeps inner-before-outer release during unwinding too.
        self.spill.values.reverse();
        self.inline[..self.inline_len].reverse();
    }
}

#[cfg(test)]
#[path = "native_capacity_stack_tests.rs"]
mod tests;
