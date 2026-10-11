use super::*;
use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds};
use std::sync::Arc;
use vortex::array::ArrayRef;

fn source(session: &ResidentVortexSession, rows: usize) -> ResidentMemorySource {
    let values = (0..rows)
        .map(|row| (!row.is_multiple_of(5)).then_some(i64::try_from(row).unwrap() - 2049))
        .collect::<Vec<_>>();
    let labels = (0..rows)
        .map(|row| {
            if row.is_multiple_of(7) {
                None
            } else {
                Some("λ\0猫")
            }
        })
        .collect::<Vec<_>>();
    ResidentMemorySource::from_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(&values),
            },
            MemoryColumn {
                name: "label",
                values: MemoryColumnValues::Utf8(&labels),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap()
}

fn rows(source: &ResidentMemorySource) -> serde_json::Value {
    let output = source
        .prepare_projection(&["n", "label"], None, None)
        .unwrap()
        .execute()
        .unwrap();
    serde_json::from_str(output.values_json.value()).unwrap()
}

#[test]
fn same_owner_source_transfer_shares_arrays_and_does_not_reserve_or_copy() {
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let input = source(&session, 4099);
    let before = session.snapshot().memory.reserved_bytes;
    let retained = input
        .in_session(&session, &CancellationToken::default())
        .unwrap();
    assert!(Arc::ptr_eq(&input.0, &retained.0));
    assert_eq!(session.snapshot().memory.reserved_bytes, before);
}

#[test]
fn foreign_source_transfer_preserves_explicit_output_bounds() {
    let original = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let input = ResidentMemorySource::from_columns(
        &original,
        &[MemoryColumn {
            name: "n",
            values: MemoryColumnValues::Int64(&[Some(1), Some(2)]),
        }],
        MemorySourceBounds {
            max_output_rows: 1,
            max_output_bytes: 17,
            ..MemorySourceBounds::default()
        },
    )
    .unwrap();
    let copied = input
        .in_session(&receiving, &CancellationToken::default())
        .unwrap();
    for source in [&input, &copied] {
        assert!(source.prepare_projection(&["n"], None, Some(2)).is_err());
        assert_eq!(source.0.bounds.max_output_bytes, 17);
    }
}

#[test]
fn foreign_native_transfer_preserves_all_rows_and_releases_the_original_grant() {
    for count in [0, 1, 4099] {
        let original = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let receiving = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let input = source(&original, count);
        let expected = rows(&input);
        let native = input
            .in_session(&receiving, &CancellationToken::default())
            .unwrap();
        assert!(!ArrayRef::ptr_eq(&input.0.array, &native.0.array));
        assert_eq!(native.dtype(), input.dtype());
        assert_eq!(native.row_count(), count);
        assert!(native.belongs_to_session(&receiving));
        assert!(!native.belongs_to_session(&original));
        if count > 0 {
            assert!(native.intake_columnar_buffer_bytes_copied() > 0);
        }
        drop(input);
        assert_eq!(original.snapshot().memory.reserved_bytes, 0);
        assert_eq!(rows(&native), expected);
        let credit = receiving.memory().clone();
        let escaped = native.0.array.slots()[1]
            .as_ref()
            .unwrap()
            .slice(0..count.min(1))
            .unwrap();
        drop((native, receiving));
        assert!(credit.snapshot().reserved_bytes > 0);
        drop(escaped);
        assert_eq!(credit.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn foreign_native_transfer_denial_and_cancellation_release_partial_receiving_credit() {
    let original = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let input = source(&original, 4099);
    let original_bytes = original.snapshot().memory.reserved_bytes;
    let receiving = ResidentVortexSession::new(32 << 10, 1).unwrap();
    assert!(
        input
            .in_session(&receiving, &CancellationToken::default())
            .is_err()
    );
    assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    assert!(receiving.snapshot().memory.denied_reservations > 0);
    assert_eq!(original.snapshot().memory.reserved_bytes, original_bytes);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let before = receiving.snapshot().memory;
    assert!(input.in_session(&receiving, &cancelled).is_err());
    assert_eq!(receiving.snapshot().memory, before);
    let retry = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let copied = input
        .in_session(&retry, &CancellationToken::default())
        .unwrap();
    assert_eq!(rows(&copied), rows(&input));
}

#[test]
fn foreign_generated_range_keeps_compact_metadata_and_exact_extreme_values() {
    for (start, step, count) in [(0, 1, 1_000_017), (i64::MIN, i64::MAX, 3), (4, -3, 0)] {
        let original = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let receiving = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let input =
            ResidentMemorySource::from_int64_range(&original, "n", start, step, count).unwrap();
        let copied = input
            .in_session(&receiving, &CancellationToken::default())
            .unwrap();
        assert_eq!(copied.row_count(), count);
        assert_eq!(copied.dtype(), input.dtype());
        assert!(copied.0.generated_range.is_some());
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 4096);
        assert_eq!(copied.intake_columnar_buffer_bytes_copied(), 0);
        let first = count.saturating_sub(3);
        let actual = copied.array_range(first..count, &|| Ok(())).unwrap();
        let values = actual.slots()[1].as_ref().unwrap().buffers()[0].clone();
        for (index, bytes) in values.as_slice().as_chunks::<8>().0.iter().enumerate() {
            let expected = i128::from(start) + (first + index) as i128 * i128::from(step);
            assert_eq!(i128::from(i64::from_ne_bytes(*bytes)), expected);
        }
        drop(input);
        assert_eq!(original.snapshot().memory.reserved_bytes, 0);
        drop((actual, values, copied));
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn completed_native_result_transfers_between_owners_without_retaining_source_credit() {
    for count in [0, 4099] {
        let original = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let receiving = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let input = source(&original, count);
        let expected = rows(&input);
        let result = input
            .prepare_projection(&["n", "label"], None, None)
            .unwrap()
            .execute_arrays()
            .unwrap();
        let copied = ResidentMemorySource::from_completed_result_in_session(
            &receiving,
            &result,
            &CancellationToken::default(),
        )
        .unwrap();
        drop((input, result));
        assert_eq!(original.snapshot().memory.reserved_bytes, 0);
        assert_eq!(rows(&copied), expected);
        drop(copied);
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    }
}
