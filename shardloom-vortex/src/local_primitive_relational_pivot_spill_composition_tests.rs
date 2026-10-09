use super::*;
use crate::relational_query::{
    VortexRelationalFilter, VortexRelationalLimit, VortexRelationalOrderKey,
    VortexRelationalProject, VortexRelationalSort,
};
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
use std::fmt::Write as _;

fn column(name: &str) -> Expression {
    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap())
}

fn composed(fixture: &Fixture) -> VortexRelationalPlan {
    let VortexRelationalPlan::Unary(mut pivot) = plan(fixture, "sum") else {
        unreachable!()
    };
    pivot.input = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: pivot.input,
        predicate: Expression::new(
            ExprId::new("positive").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column("amount")),
                op: ComparisonOp::Gt,
                right: Box::new(Expression::literal(
                    ExprId::new("one").unwrap(),
                    ScalarValue::Float64(1.0),
                )),
            },
        ),
    }));
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: VortexRelationalPlan::Unary(pivot),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("entity").unwrap(),
            descending: true,
            nulls: None,
        }],
    }));
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: sorted,
            offset: 7,
            count: 11,
        })),
        expressions: vec![
            ("entity".into(), column("entity")),
            ("total".into(), column("pivot_a")),
        ],
    }))
}

#[test]
fn native_pivot_spill_counts_discovery_when_outer_zero_limit_skips_emission() {
    let fixture = fixture(&[Some(2.0), Some(3.0)], 257, 67);
    let limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: plan(&fixture, "sum"),
        offset: 0,
        count: 0,
    }));
    let prepared = prepare(&fixture, limited, true);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let (rows, report) = complete(&prepared);
    assert_eq!(rows, [] as [serde_json::Value; 0]);
    assert_eq!(report.spilled_pivot_stages, 1);
    assert_eq!(report.spilled_pivot_input_rows, 514);
    assert_eq!(report.spilled_pivot_index_rows, 257);
    assert_eq!(report.spilled_pivot_domains, 1);
    assert_eq!(report.spilled_pivot_cells, 257);
    assert_eq!(
        report.spilled_pivot_reader_opens,
        report.spill.as_ref().unwrap().runs_written
    );
    assert!(report.spill.as_ref().unwrap().merge_passes > 0);
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn native_pivot_spill_composes_filter_order_offset_projection_and_owned_reuse() {
    let fixture = fixture(&[Some(1.0), Some(2.0), Some(3.0)], 257, 41);
    let plan = composed(&fixture);
    let prepared = prepare(&fixture, plan.clone(), true);
    let resident = prepare(&fixture, plan, false);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let expected = (239..=249)
        .rev()
        .map(|index| json!({"entity":key(index), "total":5.0}))
        .collect::<Vec<_>>();
    for _ in 0..2 {
        let (actual, report) = complete(&prepared);
        assert_eq!(actual, expected);
        assert_eq!(complete(&resident).0, expected);
        assert_eq!(report.scan_rows_delivered, 771);
        assert_eq!(report.spilled_pivot_input_rows, 514);
        assert_eq!(report.spilled_pivot_index_rows, 257);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let memory = prepared.session.memory().clone();
    let owned = prepared.execute_owned().unwrap();
    assert_eq!(owned.result.row_count(), 11);
    drop(prepared);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(owned.result.row_count(), 11);
    drop(owned);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_pivot_spill_nested_pivots_share_one_owner_and_report_each_discovery_once() {
    let fixture = fixture(&[Some(2.0), Some(3.0)], 257, 61);
    let first = plan(&fixture, "sum");
    let VortexRelationalPlan::Unary(mut second) = plan(&fixture, "sum") else {
        unreachable!()
    };
    second.input = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: first,
        expressions: vec![
            ("entity".into(), column("entity")),
            (
                "category".into(),
                Expression::literal(ExprId::new("a").unwrap(), ScalarValue::Utf8("a".into())),
            ),
            ("amount".into(), column("pivot_a")),
        ],
    }));
    let prepared = prepare(&fixture, VortexRelationalPlan::Unary(second), true);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..257)
            .map(|index| json!({"entity":key(index), "pivot_a":5.0}))
            .collect::<Vec<_>>()
    );
    assert_eq!(result.execution.spilled_pivot_stages, 2);
    assert_eq!(result.execution.spilled_pivot_input_rows, 771);
    assert_eq!(result.execution.spilled_pivot_index_rows, 514);
    assert_eq!(result.execution.spilled_pivot_cells, 514);
    assert_eq!(result.execution.spilled_pivot_domains, 2);
    assert_eq!(result.execution.scan_rows_delivered, 514);
    assert!(
        result
            .execution
            .spill
            .as_ref()
            .unwrap()
            .owned_cleanup_completed
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_pivot_spill_composed_output_reopens_all_admitted_writers_and_protects_destinations() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let fixture = fixture(&[Some(1.0), Some(2.0), Some(3.0)], 257, 43);
    let expected = (239..=249)
        .rev()
        .map(|index| json!({"entity":key(index), "total":5.0}))
        .collect::<Vec<_>>();
    for empty in [false, true] {
        let plan = composed(&fixture);
        let plan = if empty {
            VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: plan,
                offset: 0,
                count: 0,
            }))
        } else {
            plan
        };
        let prepared = prepare(&fixture, plan, true);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let dtype = DType::struct_(
            [
                ("entity", DType::Utf8(Nullability::NonNullable)),
                ("total", DType::Primitive(PType::F64, Nullability::Nullable)),
            ],
            Nullability::NonNullable,
        );
        for format in writer_tests::FORMATS {
            let path = fixture.0.join(format!("pivot-{empty}.{}", format.as_str()));
            let written = prepared.write(&path, format, false).unwrap();
            assert_eq!(written.execution.spilled_pivot_stages, 1);
            assert_eq!(written.execution.spilled_pivot_input_rows, 514);
            assert!(
                written
                    .execution
                    .spill
                    .as_ref()
                    .unwrap()
                    .owned_cleanup_completed
            );
            if format == Format::Csv {
                let mut csv = String::from("entity,total\n");
                if !empty {
                    for index in (239..=249).rev() {
                        writeln!(csv, "{},5", key(index)).unwrap();
                    }
                }
                assert_eq!(fs::read_to_string(&path).unwrap(), csv);
            } else {
                assert_eq!(
                    writer_tests::read_rows(&path, format, &dtype),
                    if empty { vec![] } else { expected.clone() }
                );
            }
            drop(written);
            let contents = fs::read(&path).unwrap();
            let executions = prepared.snapshot().completed_executions;
            assert!(prepared.write(&path, format, false).is_err());
            assert_eq!(fs::read(&path).unwrap(), contents);
            assert_eq!(prepared.snapshot().completed_executions, executions);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(
                fs::read_dir(fixture.0.join("pivot-runs")).unwrap().count(),
                0
            );
        }
    }
}
