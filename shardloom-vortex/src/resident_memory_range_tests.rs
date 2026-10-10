use super::*;
use std::cell::Cell;

fn values(array: &ArrayRef) -> Vec<i64> {
    let column = array.slots()[1].as_ref().unwrap();
    column.buffers()[0]
        .as_slice()
        .as_chunks::<8>()
        .0
        .iter()
        .map(|bytes| i64::from_ne_bytes(*bytes))
        .collect()
}

#[test]
fn generated_range_metadata_and_active_buffer_lifetimes_are_independent_of_total_rows() {
    for rows in [0, 1, 1_000_001]
        .into_iter()
        .chain(usize::try_from(1_000_000_000_000_u64).ok())
    {
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let source = source(&session, "n", 0, 1, rows).unwrap();
        assert_eq!(source.row_count(), rows);
        assert_eq!(source.input_logical_bytes(), rows * 8 + 1);
        assert_eq!(source.intake_payload_bytes_copied(), 0);
        assert_eq!(memory.snapshot().reserved_bytes, 4096);
        let first = rows.saturating_sub(3);
        let array = source.array_range(first..rows, &|| Ok(())).unwrap();
        assert_eq!(
            values(&array),
            (first..rows)
                .map(|value| i64::try_from(value).unwrap())
                .collect::<Vec<_>>()
        );
        assert!(memory.snapshot().peak_reserved_bytes < 16 << 10);
        let buffer = array.slots()[1].as_ref().unwrap().buffers()[0].clone();
        drop(array);
        drop(source);
        drop(session);
        assert!(memory.snapshot().reserved_bytes >= 2048);
        drop(buffer);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn generated_range_extreme_and_intermediate_overflow_cases_preserve_exact_values() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    for (start, step, expected) in [
        (i64::MIN, i64::MAX, vec![i64::MIN, -1, i64::MAX - 1]),
        (i64::MAX, -i64::MAX, vec![i64::MAX, 0, -i64::MAX]),
        (i64::MAX, i64::MIN, vec![i64::MAX, -1]),
        (i64::MIN, 1, vec![i64::MIN, i64::MIN + 1, i64::MIN + 2]),
        (i64::MAX, -1, vec![i64::MAX, i64::MAX - 1, i64::MAX - 2]),
        (4, -3, vec![4, 1, -2, -5]),
    ] {
        let source = source(&session, "n", start, step, expected.len()).unwrap();
        for first in 0..=expected.len() {
            let array = source
                .array_range(first..expected.len(), &|| Ok(()))
                .unwrap();
            assert_eq!(values(&array), expected[first..]);
        }
        let result = source
            .prepare_projection(&["n"], None, None)
            .unwrap()
            .execute()
            .unwrap();
        let expected_json = expected
            .into_iter()
            .map(|n| serde_json::json!({"n":n}))
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::from_str::<Vec<serde_json::Value>>(result.values_json.value()).unwrap(),
            expected_json,
        );
        drop(result);
        drop(source);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn generated_range_admission_cancellation_and_failed_materialization_release_all_credit() {
    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    for (name, start, step, rows) in [
        ("", 0, 1, 1),
        ("n", 0, 0, 1),
        ("n", i64::MAX, 1, 2),
        ("n", i64::MIN, -1, 2),
        ("n", 0, 1, usize::MAX),
    ] {
        assert!(source(&session, name, start, step, rows).is_err());
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
    let source = source(&session, "n", 0, 1, 1_000_001).unwrap();
    let initial = session.snapshot().memory.reserved_bytes;
    let checkpoints = Cell::new(0);
    let cancelled = source.array_range(0..8192, &|| {
        checkpoints.set(checkpoints.get() + 1);
        if checkpoints.get() == 4 {
            Err(memory_error("range test cancellation"))
        } else {
            Ok(())
        }
    });
    assert!(
        cancelled
            .err()
            .unwrap()
            .to_string()
            .contains("range test cancellation")
    );
    assert_eq!(checkpoints.get(), 4);
    assert_eq!(session.snapshot().memory.reserved_bytes, initial);
    assert!(source.array_range(0..1_000_002, &|| Ok(())).is_err());
    assert!(source.array_range(0..1_000_001, &|| Ok(())).is_err());
    assert!(session.snapshot().memory.denied_reservations > 0);
    assert_eq!(session.snapshot().memory.reserved_bytes, initial);
    let result = source
        .prepare_projection(&["n"], None, Some(3))
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
        serde_json::json!([{"n":0},{"n":1},{"n":2}]),
    );
    drop(result);
    drop(source);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
#[cfg(feature = "vortex-write")]
fn generated_range_memory_generation_uses_owned_values_and_checks_bounds_before_allocation() {
    use crate::memory_file_generation::{MemoryFileGenerationBounds, MemoryFileGenerationLayout};
    use std::sync::atomic::AtomicBool;

    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let small = source(&session, "n", i64::MIN, i64::MAX, 3).unwrap();
    let generation = small
        .file_generation(MemoryFileGenerationBounds::default())
        .unwrap();
    assert_eq!(generation.row_count(), 3);
    let result = generation.collect(&["n"], None, 3, 1 << 20).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
        serde_json::json!([{"n":i64::MIN},{"n":-1},{"n":i64::MAX-1}]),
    );
    drop(result);
    drop(generation);
    let initial = session.snapshot().memory.reserved_bytes;
    assert!(
        small
            .file_generation_with_layout(
                MemoryFileGenerationBounds::default(),
                MemoryFileGenerationLayout::default(),
                Some(&AtomicBool::new(true)),
            )
            .is_err()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, initial);
    drop(small);
    let large = source(&session, "n", 0, 1, 1_000_017).unwrap();
    let initial = session.snapshot().memory.reserved_bytes;
    assert!(
        large
            .file_generation(MemoryFileGenerationBounds::default())
            .is_err()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, initial);
    drop(large);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
