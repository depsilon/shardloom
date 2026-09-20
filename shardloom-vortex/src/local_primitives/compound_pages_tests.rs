use super::*;

#[test]
fn dense_ordinals_survive_first_page_growth_and_multiple_pages() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut values = DensePages::new(&memory).unwrap();
    for i in 0..(PAGE_ITEMS * 3 + 17) {
        assert!(values.reserve_one(&memory).unwrap());
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
            assert!(values.reserve_one(&memory).unwrap());
            values.push(i as u64);
        }
        let before = memory.snapshot().reserved_bytes;
        let held = memory.reserve((1 << 20) - before - remaining).unwrap();
        assert!(!values.reserve_one(&memory).unwrap());
        assert_eq!(values.len(), len);
        assert_eq!(
            values.iter().copied().collect::<Vec<_>>(),
            (0..len as u64).collect::<Vec<_>>()
        );
        drop(held);
        assert!(values.reserve_one(&memory).unwrap());
        values.push(len as u64);
        assert_eq!(values[len], len as u64);
        drop(values);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
