//! Complete join values, order and owners through the shared prepared runner.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use serde_json::{Value, json};

#[path = "local_primitive_relational_join_spill_semantics_tests.rs"]
mod semantics_tests;

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_join_spill_writer_tests.rs"]
mod writer_tests;

fn plan(left: &Fixture, right: &Fixture, kind: JoinKind) -> VortexRelationalPlan {
    let mut plan = join(left, right, kind);
    if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti)
        && let VortexRelationalPlan::Join(join) = &mut plan
    {
        join.columns.pop();
    }
    plan
}

fn ordered(left: &Fixture, plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    let workspace = left.0.join("join-runs");
    fs::create_dir_all(&workspace).unwrap();
    prepare_relational(plan, policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(workspace, 128 << 20, 1 << 20).unwrap())
        .unwrap()
}

fn complete(
    prepared: &PreparedVortexRelational,
    batch_rows: usize,
) -> (Vec<Value>, ExecutedVortexRelational) {
    let mut rows = Vec::new();
    let report = prepared
        .for_each_json_batch(
            &CancellationToken::default(),
            batch_rows,
            1 << 20,
            |batch| {
                let values = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
                assert!(values.len() <= batch_rows);
                rows.extend(values);
                Ok(())
            },
        )
        .unwrap();
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    let spill = report.spill.as_ref().unwrap();
    assert!(spill.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
    (rows, report)
}

pub(super) fn assert_ordered(fixture: &Fixture, plan: &VortexRelationalPlan, expected: &[Value]) {
    let prepared = ordered(fixture, plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for batch_rows in [1, 3, 2048] {
        let (rows, report) = complete(&prepared, batch_rows);
        assert_eq!(rows, expected, "batch_rows={batch_rows}");
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

/// Intentionally simple row-loop oracle, independent of native hashes and runs.
fn reference(
    left: &[(Option<u64>, u32)],
    right: &[(Option<u64>, u32)],
    kind: JoinKind,
) -> Vec<Value> {
    let mut result = Vec::new();
    let mut matched_right = vec![false; right.len()];
    for &(key, debit) in left {
        let mut matched = false;
        for (position, &(other, credit)) in right.iter().enumerate() {
            if kind == JoinKind::Cross || (key.is_some() && key == other) {
                matched = true;
                matched_right[position] = true;
                if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                    result.push(json!({"debit":debit,"credit":credit}));
                }
            }
        }
        match kind {
            JoinKind::Left | JoinKind::Full if !matched => {
                result.push(json!({"debit":debit,"credit":null}));
            }
            JoinKind::LeftSemi if matched => result.push(json!({"debit":debit})),
            JoinKind::LeftAnti if !matched => result.push(json!({"debit":debit})),
            _ => {}
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (position, &(_, credit)) in right.iter().enumerate() {
            if !matched_right[position] {
                result.push(json!({"debit":null,"credit":credit}));
            }
        }
    }
    result
}

fn fixture(rows: &[(Option<u64>, u32)], chunk_rows: usize) -> Fixture {
    Fixture::new(
        keyed(
            &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        ),
        chunk_rows,
    )
}

#[test]
fn ordered_join_all_seven_kinds_keep_complete_duplicate_null_and_unmatched_order() {
    let left_rows = [
        (Some(2), 10),
        (Some(1), 11),
        (Some(2), 12),
        (None, 13),
        (Some(u64::MAX), 14),
    ];
    let right_rows = [
        (Some(1), 20),
        (Some(2), 21),
        (None, 22),
        (Some(2), 23),
        (Some(3), 24),
        (Some(u64::MAX), 25),
    ];
    let left = fixture(&left_rows, 2);
    let right = fixture(&right_rows, 3);
    for kind in [
        JoinKind::Inner,
        JoinKind::Left,
        JoinKind::Right,
        JoinKind::Full,
        JoinKind::LeftSemi,
        JoinKind::LeftAnti,
        JoinKind::Cross,
    ] {
        let plan = plan(&left, &right, kind);
        let expected = reference(&left_rows, &right_rows, kind);
        let resident = prepare_relational(&plan, policy()).unwrap();
        assert_eq!(
            json_rows(
                &resident
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            ),
            expected
        );
        let ordered = ordered(&left, &plan);
        assert_eq!(ordered.output_dtype(), resident.output_dtype());
        let baseline = ordered.snapshot().memory.reserved_bytes;
        for batch_rows in [1, 2, 4, 2048] {
            let (actual, report) = complete(&ordered, batch_rows);
            assert_eq!(actual, expected, "{kind:?}, batch_rows={batch_rows}");
            assert_eq!(report.ordered_join_stages, 1);
            assert_eq!(report.ordered_join_build_rows, right_rows.len() as u64);
            assert_eq!(report.ordered_join_probe_rows, left_rows.len() as u64);
            drop(report);
            assert_eq!(ordered.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn ordered_join_empty_sides_and_root_nulls_keep_outer_and_semi_anti_semantics() {
    let rows = [(None, 10), (Some(1), 11)];
    for (left_rows, right_rows) in [
        (&rows[..], &[][..]),
        (&[][..], &rows[..]),
        (&[][..], &[][..]),
    ] {
        let left = fixture(left_rows, 2);
        let right = fixture(right_rows, 2);
        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
            JoinKind::LeftSemi,
            JoinKind::LeftAnti,
            JoinKind::Cross,
        ] {
            let prepared = ordered(&left, &plan(&left, &right, kind));
            let baseline = prepared.snapshot().memory.reserved_bytes;
            assert_eq!(
                complete(&prepared, 1).0,
                reference(left_rows, right_rows, kind),
                "{kind:?}"
            );
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
    let nullable = |ids: [u32; 3]| {
        let source = StructArray::new(
            FieldNames::from(["entity", "amount"]),
            vec![
                PrimitiveArray::from_iter([1u64, 1, 2]).into_array(),
                PrimitiveArray::from_iter(ids).into_array(),
            ],
            3,
            Validity::from_iter([true, false, true]),
        )
        .into_array();
        // The file provider requires a nonnullable root; normalize its logical
        // fields here. Direct nullable-root execution is covered by kernel tests.
        StructArray::new(
            FieldNames::from(["entity", "amount"]),
            ["entity", "amount"].map(|name| {
                crate::local_primitives::logical_field_from_native_array(&source, name).unwrap()
            }),
            source.len(),
            Validity::NonNullable,
        )
        .into_array()
    };
    let left = Fixture::new(nullable([10, 11, 12]), 1);
    let right = Fixture::new(nullable([20, 21, 22]), 2);
    let prepared = ordered(&left, &plan(&left, &right, JoinKind::Full));
    assert_eq!(
        complete(&prepared, 2).0,
        vec![
            json!({"debit":10,"credit":20}),
            json!({"debit":null,"credit":null}),
            json!({"debit":12,"credit":22}),
            json!({"debit":null,"credit":null}),
        ]
    );
}

#[test]
fn ordered_join_spilled_full_build_and_repeated_matches_restore_original_right_order() {
    let left_rows = [
        (Some(2), 10),
        (Some(1), 11),
        (Some(2), 12),
        (None, 13),
        (Some(u64::MAX), 14),
    ];
    let right_rows = (0..20_003u32)
        .rev()
        .map(|id| {
            (
                (!id.is_multiple_of(17)).then_some(u64::from(id % 7)),
                id + 20,
            )
        })
        .collect::<Vec<_>>();
    let left = fixture(&left_rows, 2);
    let right = fixture(&right_rows, 509);
    let plan = plan(&left, &right, JoinKind::Full);
    let prepared = ordered(&left, &plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let (actual, report) = complete(&prepared, 257);
    assert_eq!(actual, reference(&left_rows, &right_rows, JoinKind::Full));
    let spill = report.spill.as_ref().unwrap();
    assert!(spill.runs_written >= 3, "{spill:?}");
    assert!(spill.merge_passes > 0, "{spill:?}");
    assert!(spill.max_open_runs <= 4, "{spill:?}");
    assert!(spill.run_block_rows <= 257);
    assert!(report.ordered_join_lookup_blocks > 1);
    assert!(report.ordered_join_match_records > 257);
    assert_eq!(
        report.ordered_join_match_records,
        report.ordered_join_candidate_rows
    );
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn ordered_join_spilled_full_output_reopens_through_all_eight_writers() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    use std::fmt::Write as _;
    let left_rows = [(Some(1), 10), (None, 11), (Some(3), 12)];
    let right_rows = (0..20_003u32)
        .rev()
        .map(|row| (Some(u64::from(row)), row))
        .collect::<Vec<_>>();
    let left = fixture(&left_rows, 2);
    let right = fixture(&right_rows, 509);
    let prepared = ordered(&left, &plan(&left, &right, JoinKind::Full));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let expected = reference(&left_rows, &right_rows, JoinKind::Full);
    let mut csv = String::from("debit,credit\n");
    for row in &expected {
        writeln!(
            &mut csv,
            "{},{}",
            row["debit"]
                .as_u64()
                .map_or(String::new(), |value| value.to_string()),
            row["credit"]
                .as_u64()
                .map_or(String::new(), |value| value.to_string())
        )
        .unwrap();
    }
    for format in super::writer_tests::FORMATS {
        let path = left.0.join(format!("join-spill.{}", format.as_str()));
        let written = prepared.write(&path, format, false).unwrap();
        assert_eq!(written.output.rows_written, expected.len() as u64);
        assert_eq!(written.execution.ordered_join_stages, 1);
        let spill = written.execution.spill.as_ref().unwrap();
        assert!(spill.runs_written > 3 && spill.merge_passes > 0);
        assert!(spill.owned_cleanup_completed);
        if format == Format::Csv {
            assert_eq!(fs::read_to_string(path).unwrap(), csv);
        } else {
            assert_eq!(
                super::writer_tests::read_rows(&path, format, &prepared.output_dtype().unwrap()),
                expected,
                "{format:?}"
            );
        }
        drop(written);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(left.0.join("join-runs")).unwrap().count(), 0);
    }
}
