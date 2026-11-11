//! Exact screening, original probe order, and charged replacement lifetimes.

use super::*;

fn scalar_group(values: &[u8], fingerprint: u8) -> (u64, usize) {
    let end = values
        .iter()
        .position(|value| *value == EMPTY_CONTROL)
        .unwrap_or(GROUP_BYTES);
    let matches = values[..end]
        .iter()
        .enumerate()
        .fold(0, |mask, (lane, value)| {
            mask | if *value == fingerprint {
                0x80u64 << (8 * lane)
            } else {
                0
            }
        });
    (matches, end)
}

#[test]
fn exact_byte_masks_match_every_fingerprint_byte_and_lane() {
    for fingerprint in 0..128 {
        for value in 0..=255 {
            for lane in 0..GROUP_BYTES {
                let mut bytes = [0x55; GROUP_BYTES];
                bytes[lane] = value;
                let expected = bytes.iter().enumerate().fold(0, |mask, (lane, value)| {
                    mask | if *value == fingerprint {
                        0x80u64 << (8 * lane)
                    } else {
                        0
                    }
                });
                assert_eq!(
                    matching_controls(u64::from_le_bytes(bytes), fingerprint),
                    expected,
                    "fingerprint {fingerprint}, byte {value}, lane {lane}"
                );
            }
        }
    }
}

#[test]
fn every_occupancy_pattern_and_wraparound_stops_at_the_first_empty_lane() {
    for mut pattern in 0..3usize.pow(8) {
        let mut values = [0; GROUP_BYTES];
        for value in &mut values {
            *value = [EMPTY_CONTROL, 45, 44][pattern % 3];
            pattern /= 3;
        }
        assert_eq!(probe_group(&values, 0, 45), scalar_group(&values, 45));
    }
    for capacity in [16, 32, 128, 256] {
        for start in 0..capacity {
            for values in [
                [45, 45, 45, 45, 45, 45, 45, EMPTY_CONTROL],
                [44, 45, 44, EMPTY_CONTROL, 45, 45, 45, 45],
                [EMPTY_CONTROL, 45, 45, 45, 45, 45, 45, 45],
                [45; GROUP_BYTES],
            ] {
                let mut controls = vec![EMPTY_CONTROL; capacity + GROUP_BYTES - 1];
                for (lane, value) in values.iter().copied().enumerate() {
                    controls[(start + lane) & (capacity - 1)] = value;
                }
                controls.copy_within(..GROUP_BYTES - 1, capacity);
                assert_eq!(probe_group(&controls, start, 45), scalar_group(&values, 45));
            }
        }
    }
}

#[test]
fn wrapped_tables_keep_exact_equality_order_and_duplicate_links() {
    let active = CancellationToken::default();
    for start in 0..128u64 {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut index = RowIndex::new(&memory).unwrap();
        // All keys share a bucket. Some share fingerprints but differ in their
        // full hash; some share the full hash and require the caller's equality.
        let hash = |row: usize| ((row % 5) as u64) << 57 | (((row / 5) % 2) as u64) << 32 | start;
        for row in 0..40 {
            assert_eq!(
                index
                    .insert(hash(row), row, &active, |_| Ok(false))
                    .unwrap(),
                row
            );
        }
        assert_eq!(index.buckets.values.len(), 128);
        let controls = &index.controls.as_ref().unwrap().values;
        assert_eq!(&controls[128..], &controls[..GROUP_BYTES - 1]);
        for row in 0..40 {
            assert_eq!(
                index
                    .insert(hash(row), 1000 + row, &active, |old| Ok(old == row))
                    .unwrap(),
                row
            );
            let mut comparisons = Vec::new();
            assert_eq!(
                index
                    .find(hash(row), &active, |old| {
                        comparisons.push(old);
                        Ok(old == row)
                    })
                    .unwrap(),
                Some(row)
            );
            assert_eq!(
                comparisons,
                (0..=row)
                    .filter(|old| hash(*old) == hash(row))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                index.rows(row).unwrap().collect::<Vec<_>>(),
                [row, 1000 + row]
            );
        }
        #[cfg(debug_assertions)]
        let observation = super::super::native_relational_observation::Session::start();
        assert_eq!(
            index
                .find((127 << 57) | start, &active, |_| panic!(
                    "full hash differs"
                ))
                .unwrap(),
            None
        );
        #[cfg(debug_assertions)]
        {
            let counts = observation.finish();
            assert_eq!(counts.row_index_probe_slots, 41);
            assert_eq!(counts.row_index_bucket_reads, 0);
            assert_eq!(counts.row_index_entry_reads, 0);
            assert_eq!(counts.row_index_equality_calls, 0);
            assert_eq!(counts.row_index_metadata_groups, 6);
        }
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn growth_admits_or_declines_metadata_at_the_frozen_credit_boundary() {
    const GRANT: u64 = 1 << 20;
    let active = CancellationToken::default();
    for decline in [true, false] {
        let memory = LiveMemoryPool::new(GRANT).unwrap();
        let mut index = RowIndex::new(&memory).unwrap();
        for row in 0..64 {
            assert_eq!(index.insert(7, row, &active, |_| Ok(false)).unwrap(), row);
        }
        index.insert(7, 1003, &active, |old| Ok(old == 3)).unwrap();
        assert_eq!(index.buckets.values.len(), 128);
        assert_eq!(index.controls.as_ref().unwrap().values.len(), 135);
        assert_eq!(index.controls.as_ref().unwrap().values.capacity(), 135);
        // Pre-admit entry growth. The next snapshot must include the old bucket
        // and control owners AND the replacement bucket reservation.
        index.entries.reserve_one().unwrap();
        let reserved = memory.snapshot().reserved_bytes;
        let replacement_bytes = u64::try_from(256 * size_of::<usize>()).unwrap();
        let remaining = 16 * 263 - u64::from(decline);
        let hold = memory
            .reserve(GRANT - reserved - replacement_bytes - remaining)
            .unwrap();
        assert_eq!(index.insert(7, 64, &active, |_| Ok(false)).unwrap(), 64);
        assert_eq!(index.buckets.values.len(), 256);
        assert_eq!(index.controls.is_none(), decline);
        if let Some(controls) = &index.controls {
            assert_eq!(controls.values.len(), 263);
            assert_eq!(controls.values.capacity(), 263);
            assert_eq!(&controls.values[256..], &controls.values[..GROUP_BYTES - 1]);
        }
        assert_eq!(memory.snapshot().denied_reservations, 0);
        assert_eq!(
            memory.snapshot().peak_reserved_bytes,
            GRANT - remaining + if decline { 0 } else { 263 }
        );
        for row in 0..65 {
            assert_eq!(
                index.find(7, &active, |old| Ok(old == row)).unwrap(),
                Some(row)
            );
            let expected = if row == 3 { vec![3, 1003] } else { vec![row] };
            assert_eq!(index.rows(row).unwrap().collect::<Vec<_>>(), expected);
        }
        drop(hold);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn metadata_lookup_errors_and_final_cancellation_do_not_publish_rows() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let active = CancellationToken::default();
    let mut index = RowIndex::new(&memory).unwrap();
    for row in 0..40 {
        index.insert(7, row, &active, |_| Ok(false)).unwrap();
    }
    let buckets = index.buckets.values.clone();
    let controls = index.controls.as_ref().unwrap().values.clone();
    let reserved = memory.snapshot().reserved_bytes;
    let mut comparisons = 0;
    assert!(
        index
            .insert(7, 40, &active, |_| {
                comparisons += 1;
                if comparisons == 17 {
                    Err(failed())
                } else {
                    Ok(false)
                }
            })
            .is_err()
    );
    assert_eq!(comparisons, 17);
    for matches in [false, true] {
        let cancelled = CancellationToken::default();
        assert!(
            index
                .insert(7, 40, &cancelled, |_| {
                    cancelled.cancel();
                    Ok(matches)
                })
                .is_err()
        );
        assert_eq!(index.buckets.values, buckets);
        assert_eq!(index.controls.as_ref().unwrap().values, controls);
        assert_eq!(index.unique_rows(), 40);
        assert!(index.links.values.is_empty());
        assert_eq!(memory.snapshot().reserved_bytes, reserved);
    }
    assert_eq!(index.insert(7, 40, &active, |_| Ok(false)).unwrap(), 40);
    drop(index);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
