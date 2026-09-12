//! Bounded UTF8 group selection after complete DISTINCT pair reconciliation.
//! Adapted from the validated native selected-row owner; no new result API.
use super::compound_count_partial::failed;
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{cmp::Ordering, collections::BinaryHeap};

struct Row {
    text: String,
    count: u64,
    text_lease: MemoryLease,
}

fn compare(left: (&str, u64), right: (&str, u64)) -> Ordering {
    right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0))
}
impl PartialEq for Row { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Row {}
impl PartialOrd for Row { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        compare((&self.text, self.count), (&other.text, other.count))
    }
}

pub(super) struct Selection {
    rows: BinaryHeap<Row>,
    limit: usize,
    memory: LiveMemoryPool,
    lease: MemoryLease,
}
pub(super) struct Rows {
    rows: Vec<Row>,
    lease: MemoryLease,
}

impl Selection {
    pub(super) fn new(memory: &LiveMemoryPool, limit: usize) -> Result<Self> {
        let bytes = limit.checked_mul(size_of::<Row>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() + size_of::<Rows>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("UTF8 DISTINCT selection capacity overflowed"))?;
        let lease = memory.reserve(bytes)?;
        let mut rows = BinaryHeap::new();
        rows.try_reserve_exact(limit).map_err(|error| failed(&error.to_string()))?;
        if rows.capacity() > limit { return Err(failed("UTF8 DISTINCT selection exceeded reserved capacity")); }
        Ok(Self { rows, limit, memory: memory.clone(), lease })
    }
    pub(super) fn insert(&mut self, text: &str, count: u64) -> Result<()> {
        if self.limit == 0 || (self.rows.len() == self.limit && self.rows.peek().is_some_and(|worst| {
            !compare((text, count), (&worst.text, worst.count)).is_lt()
        })) { return Ok(()); }
        // The old row and replacement remain admitted together until assignment.
        let lease = self.memory.reserve(u64::try_from(text.len()).map_err(|_| failed("text capacity overflowed"))?)?;
        let mut owned = String::new();
        owned.try_reserve_exact(text.len()).map_err(|error| failed(&error.to_string()))?;
        if owned.capacity() > text.len() { return Err(failed("UTF8 DISTINCT text exceeded reserved capacity")); }
        owned.push_str(text);
        let row = Row { text: owned, count, text_lease: lease };
        if self.rows.len() == self.limit {
            *self.rows.peek_mut().ok_or_else(|| failed("UTF8 DISTINCT selection lost worst row"))? = row;
        } else { self.rows.push(row); }
        Ok(())
    }
    pub(super) fn finish(self) -> Rows {
        Rows { rows: self.rows.into_sorted_vec(), lease: self.lease }
    }
}

impl Rows {
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lease.bytes() + self.rows.iter().map(|row| row.text_lease.bytes()).sum::<u64>()
    }
    pub(super) fn visit_utf8<'a>(&'a self, mut visit: impl FnMut(&'a str, u64) -> Result<()>) -> Result<()> {
        for row in &self.rows { visit(&row.text, row.count)?; }
        Ok(())
    }
    pub(super) fn len(&self) -> usize { self.rows.len() }
    pub(super) fn text_bytes(&self) -> usize { self.rows.iter().map(|row| row.text.len()).sum() }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(rows: &Rows) -> Vec<(String, u64)> {
        let mut result = Vec::new();
        rows.visit_utf8(|key, count| { result.push((key.to_owned(), count)); Ok(()) }).unwrap();
        result
    }

    #[test]
    fn utf8_integer_distinct_selected_owner_orders_exact_bytes_and_retains_credits() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut selection = Selection::new(&memory, 4).unwrap();
        for (key, count) in [("z", 1), ("東京", 2), ("e\u{301}", 2), ("é", 2), ("", 2), ("\0", 2)] {
            selection.insert(key, count).unwrap();
        }
        let rows = selection.finish();
        assert_eq!(values(&rows), vec![(String::new(), 2), ("\0".into(), 2), ("e\u{301}".into(), 2), ("é".into(), 2)]);
        assert_eq!(rows.reserved_bytes(), memory.snapshot().reserved_bytes);
        assert_eq!(rows.text_bytes(), 6);
        drop(rows);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn utf8_integer_distinct_selected_owner_denial_preserves_existing_complete_candidate() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let mut selection = Selection::new(&memory, 1).unwrap();
        selection.insert("old", 1).unwrap();
        let initial = memory.snapshot().reserved_bytes;
        let competing = memory.reserve(4096 - initial).unwrap();
        assert!(selection.insert("better", 2).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 4096);
        drop(competing);
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        let rows = selection.finish();
        assert_eq!(values(&rows), vec![("old".into(), 1)]);
        drop(rows);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let competing = memory.reserve(4096).unwrap();
        assert!(Selection::new(&memory, 1).is_err());
        drop(competing);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn utf8_integer_distinct_selected_owner_zero_limit_and_worse_rows_do_not_copy() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let mut selection = Selection::new(&memory, 0).unwrap();
        let initial = memory.snapshot().reserved_bytes;
        selection.insert("unselected", u64::MAX).unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, initial);
        let rows = selection.finish();
        assert_eq!(rows.len(), 0);
        drop(rows);
        let mut selection = Selection::new(&memory, 1).unwrap();
        selection.insert("a", 2).unwrap();
        let initial = memory.snapshot().reserved_bytes;
        let competing = memory.reserve(4096 - initial).unwrap();
        selection.insert("worse", 1).unwrap();
        drop(competing);
        assert_eq!(values(&selection.finish()), vec![("a".into(), 2)]);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
