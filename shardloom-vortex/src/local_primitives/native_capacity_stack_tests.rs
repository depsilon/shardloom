use super::*;
use shardloom_exec::live_memory::MemoryLease;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

#[test]
fn continuation_stack_replacement_refusal_preserves_values_and_refunds_capacity() {
    let minimal = LiveMemoryPool::new(1).unwrap();
    let mut inline = ReservedStack::<u64, 4>::new(&minimal).unwrap();
    for value in 0..4 {
        inline.push(value).unwrap();
    }
    assert!(inline.push(4).is_err());
    assert_eq!(minimal.snapshot().reserved_bytes, 0);
    for value in (0..4).rev() {
        assert_eq!(inline.pop(), Some(value));
    }
    drop(inline);

    let memory = LiveMemoryPool::new(128).unwrap();
    let mut stack = ReservedStack::<u64, 4>::new(&memory).unwrap();
    for value in 0..4 {
        stack.push(value).unwrap();
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for value in 4..12 {
        stack.push(value).unwrap();
    }
    assert!(stack.push(12).is_err());
    assert_eq!(stack.len(), 12);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    for value in (0..12).rev() {
        assert_eq!(stack.pop(), Some(value));
    }
    assert_eq!(stack.pop(), None);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(stack);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

struct Owner {
    id: usize,
    events: Arc<Mutex<Vec<usize>>>,
    panic_at: Option<usize>,
    _credit: MemoryLease,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.events.lock().unwrap().push(self.id);
        assert_ne!(self.panic_at, Some(self.id), "continuation owner");
    }
}

#[test]
fn continuation_stack_releases_inner_owners_before_parents_even_during_panic() {
    for panic_at in [None, Some(9), Some(2)] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut stack = ReservedStack::<Owner, 4>::new(&memory).unwrap();
        for id in 0..12 {
            stack
                .push(Owner {
                    id,
                    events: events.clone(),
                    panic_at,
                    _credit: memory.reserve(1).unwrap(),
                })
                .unwrap();
        }
        let result = catch_unwind(AssertUnwindSafe(|| drop(stack)));
        assert_eq!(result.is_err(), panic_at.is_some());
        assert_eq!(*events.lock().unwrap(), (0..12).rev().collect::<Vec<_>>());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
