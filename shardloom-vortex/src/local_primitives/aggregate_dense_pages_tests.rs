use super::super::compound_count_partial::failed;
use super::*;

#[test]
fn contiguous_binding_borrows_only_a_complete_single_page_without_new_credit() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut values = DensePages::new(&memory).unwrap();
    for len in [0, 1, 16, 17, 512, 1024, 1025, 2049] {
        while values.len() < len {
            assert!(values.reserve_one(&memory, failed).unwrap());
            values.push(values.len() as u64);
        }
        let before = memory.snapshot().reserved_bytes;
        let pressure = memory.reserve((1 << 20) - before).unwrap();
        let pointer = values.pages.first().map(|page| page.values.as_ptr());
        if let Some(slice) = values.single_page_mut() {
            assert!(len <= PAGE_ITEMS);
            assert_eq!(slice.len(), len);
            if len != 0 {
                assert_eq!(Some(slice.as_ptr()), pointer);
                slice[len - 1] += 7;
                assert_eq!(values[len - 1], (len - 1) as u64 + 7);
                values[len - 1] -= 7;
            }
        } else {
            assert!(len > PAGE_ITEMS);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 1 << 20);
        assert_eq!(
            values.iter().copied().collect::<Vec<_>>(),
            (0..len as u64).collect::<Vec<_>>()
        );
        drop(pressure);
        assert_eq!(memory.snapshot().reserved_bytes, before);
    }
    values.release().unwrap();
    assert_eq!(values.single_page_mut().unwrap(), [0_u64; 0]);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dense_ordinals_survive_first_page_growth_and_multiple_pages() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut values = DensePages::new(&memory).unwrap();
    for i in 0..(PAGE_ITEMS * 3 + 17) {
        assert!(values.reserve_one(&memory, failed).unwrap());
        values.push(i as u64);
        assert_eq!(values[i], i as u64);
    }
    for i in 0..values.len() {
        assert_eq!(values[i], i as u64);
        values[i] += 7;
    }
    assert_eq!(
        values.iter().copied().collect::<Vec<_>>(),
        (7..(PAGE_ITEMS * 3 + 24) as u64).collect::<Vec<_>>()
    );
    let charged = values.metadata.bytes()
        + values
            .pages
            .iter()
            .map(|p| (p.values.capacity() * size_of::<u64>()) as u64)
            .sum::<u64>();
    assert_eq!(memory.snapshot().reserved_bytes, charged);
    assert!(charged < 40_000);
    values.release().unwrap();
    assert_eq!(values.len(), 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dense_capacity_denials_preserve_payloads_and_release_every_owner() {
    // Empty metadata, first payload, first-page replacement, second page,
    // and outer page-directory replacement are distinct admission boundaries.
    for (len, remaining) in [(0, 0), (0, 80), (16, 0), (1024, 0), (2048, 0)] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut values = DensePages::new(&memory).unwrap();
        for i in 0..len {
            assert!(values.reserve_one(&memory, failed).unwrap());
            values.push(i as u64);
        }
        let before = memory.snapshot().reserved_bytes;
        let held = memory.reserve((1 << 20) - before - remaining).unwrap();
        assert!(!values.reserve_one(&memory, failed).unwrap());
        assert_eq!(values.len(), len);
        assert_eq!(
            values.iter().copied().collect::<Vec<_>>(),
            (0..len as u64).collect::<Vec<_>>()
        );
        drop(held);
        assert!(values.reserve_one(&memory, failed).unwrap());
        values.push(len as u64);
        assert_eq!(values[len], len as u64);
        drop(values);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
