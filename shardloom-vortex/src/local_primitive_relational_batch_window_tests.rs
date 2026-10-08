//! Window state must own its input before a finite producer can advance.

use super::*;
use crate::{
    local_primitives::VortexLocalPrimitiveRowExportFormat as Format,
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFrameBound as Bound,
        VortexRelationalFrameFunction as FrameFunction, VortexRelationalFrameUnit as Unit,
        VortexRelationalLimit, VortexRelationalNullOrder as NullOrder,
        VortexRelationalOrderKey as OrderKey, VortexRelationalSort, VortexRelationalSpillPolicy,
        VortexRelationalWindow as Window, VortexRelationalWindowExpression as WindowExpression,
        VortexRelationalWindowFrame as Frame, VortexRelationalWindowFunction as Function,
    },
};
use serde_json::{Value, json};
use std::cell::Cell;

#[path = "local_primitive_relational_batch_window_pressure_tests.rs"]
mod pressure_tests;

const NUMBERS: [Option<i64>; 5] = [Some(3), Some(1), Some(1), None, Some(2)];
const TEXT: [Option<&str>; 5] = [Some("c"), Some("a"), Some("b"), None, Some("d")];

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}

fn expression(name: &str, function: Function, key: &str, descending: bool) -> WindowExpression {
    let frame = matches!(function, Function::Framed(_)).then_some(Frame {
        unit: Unit::Rows,
        end: Bound::CurrentRow,
        ..Frame::default()
    });
    WindowExpression {
        output_column: name.into(),
        function,
        partition_by: vec![],
        order_by: vec![OrderKey {
            column: column(key),
            descending,
            nulls: Some(if descending {
                NullOrder::First
            } else {
                NullOrder::Last
            }),
        }],
        frame,
    }
}

fn window(input: VortexRelationalPlan) -> VortexRelationalPlan {
    let mut expressions = vec![
        expression("rn", Function::RowNumber, "n", false),
        expression(
            "lag",
            Function::Lag {
                column: column("s"),
                offset: 1,
            },
            "n",
            false,
        ),
        expression("reverse", Function::RowNumber, "n", true),
    ];
    for (name, function) in [
        ("total", FrameFunction::Sum(column("n"))),
        ("distinct", FrameFunction::CountDistinct(column("s"))),
        ("minimum", FrameFunction::Min(column("s"))),
        ("maximum", FrameFunction::Max(column("s"))),
    ] {
        expressions.push(expression(name, Function::Framed(function), "n", false));
    }
    VortexRelationalPlan::Window(Box::new(Window {
        input,
        columns: vec![column("n"), column("s")],
        expressions,
    }))
}

fn expected() -> Vec<Value> {
    vec![
        json!({"n":3,"s":"c","rn":4,"lag":"d","reverse":2,"total":7.0,"distinct":4,"minimum":"a","maximum":"d"}),
        json!({"n":1,"s":"a","rn":1,"lag":null,"reverse":4,"total":1.0,"distinct":1,"minimum":"a","maximum":"a"}),
        json!({"n":1,"s":"b","rn":2,"lag":"a","reverse":5,"total":2.0,"distinct":2,"minimum":"a","maximum":"b"}),
        json!({"n":null,"s":null,"rn":5,"lag":"c","reverse":1,"total":7.0,"distinct":4,"minimum":"a","maximum":"d"}),
        json!({"n":2,"s":"d","rn":3,"lag":"b","reverse":3,"total":4.0,"distinct":3,"minimum":"a","maximum":"d"}),
    ]
}

fn prepared(
    fixture: &Fixture,
    plan: &VortexRelationalPlan,
    spill: bool,
) -> PreparedVortexRelational {
    let prepared = prepare(plan, 16 << 20).unwrap();
    if spill {
        let directory = fixture.0.join("window-runs");
        fs::create_dir_all(&directory).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(directory, 128 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

struct Input {
    calls: Cell<usize>,
    ended: Cell<bool>,
    prior: Option<Weak<MemoryLease>>,
}

impl Input {
    fn new() -> Self {
        Self {
            calls: Cell::new(0),
            ended: Cell::new(false),
            prior: None,
        }
    }

    fn next(&mut self, session: &ResidentVortexSession) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|prior| prior.strong_count() == 0)
        );
        let call = self.calls.replace(self.calls.get() + 1);
        let range = match call {
            0 | 2 => 0..0,
            1 => 0..2,
            3 => 2..5,
            4 => {
                self.ended.set(true);
                return Ok(None);
            }
            _ => panic!("window replayed a single-use source"),
        };
        let batch = source(session, &NUMBERS[range.clone()], &TEXT[range])?;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

fn clean(fixture: &Fixture, spill: bool) {
    if spill {
        assert_eq!(
            fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
            0
        );
    }
}

#[test]
fn streaming_window_releases_input_and_retains_complete_output_after_plan_drop() {
    for spill in [false, true] {
        let fixture = Fixture::new(
            single("n", PrimitiveArray::from_iter([1i64, 4]).into_array()),
            1,
        );
        let prepared = prepared(&fixture, &window(scan()), spill);
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let mut input = Input::new();
        let ended = Cell::new(false);
        let mut producer = |session: &ResidentVortexSession| {
            let next = input.next(session)?;
            ended.set(input.ended.get());
            Ok(next)
        };
        let mut retained = Vec::new();
        let mut actual = Vec::new();
        let report = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                assert!(ended.get(), "window output requires complete input");
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(), "spill={spill}");
        assert_eq!(input.calls.get(), 5);
        let source = report.input.as_ref().unwrap();
        assert_eq!(source.payload_batches, 4);
        assert_eq!(source.rows, 5);
        assert_eq!(source.window_rows_detached, 5);
        assert_eq!(source.window_batches_detached, 2);
        assert_eq!(source.max_retained_input_batches, 1);
        assert!(source.end_of_input_observed && source.output_ownership_detached);
        assert_eq!(report.ordered_window_stages, u64::from(spill));
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        assert!(
            report
                .native_io_certificate
                .source_pushdown_report
                .proof_basis
                .contains("streaming_window_rows_detached=5")
        );
        drop(report);
        assert!(memory.snapshot().reserved_bytes > baseline);
        let slice = retained[0].slice(0..1).unwrap();
        drop((retained, prepared));
        assert!(memory.snapshot().reserved_bytes > 0);
        let field =
            crate::local_primitives::logical_field_from_native_array(&slice, "lag").unwrap();
        let mut context = VortexSession::default().create_execution_ctx();
        assert_eq!(
            result_batch::scalar_value(&field, 0, &mut context)
                .unwrap()
                .into_json()
                .unwrap(),
            json!("d")
        );
        drop((field, context, slice));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        clean(&fixture, spill);
    }
}

fn composed(fixture: &Fixture) -> VortexRelationalPlan {
    let nested = VortexRelationalPlan::Window(Box::new(Window {
        input: window(scan()),
        columns: vec![column("s"), column("rn")],
        expressions: vec![expression("backwards", Function::RowNumber, "rn", true)],
    }));
    let joined = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: nested,
        right: fixture.scan(),
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: column("backwards"),
            right: column("n"),
        }],
        columns: vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: column("s"),
            output_column: "s".into(),
        }],
    }));
    let aggregate = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: joined,
        group_by: vec![column("s")],
        measures: vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".into(),
        )],
    }));
    let sort = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: aggregate,
        keys: vec![OrderKey {
            column: column("s"),
            descending: false,
            nulls: Some(NullOrder::Last),
        }],
    }));
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: sort,
        offset: 1,
        count: 1,
    }))
}

#[test]
fn streaming_window_composes_with_join_aggregate_order_limit_and_native_write_reopen() {
    for spill in [false, true] {
        let fixture = Fixture::new(
            single("n", PrimitiveArray::from_iter([1i64, 4]).into_array()),
            1,
        );
        let prepared = prepared(&fixture, &composed(&fixture), spill);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for write in [false, true] {
            let mut input = Input::new();
            let mut producer = |session: &ResidentVortexSession| input.next(session);
            let stream = prepared.with_batch_input(&mut producer).unwrap();
            let (actual, report) = if write {
                let target = fixture.0.join("windows.vortex");
                let written = stream
                    .write_controlled(
                        &target,
                        Format::Vortex,
                        false,
                        &CancellationToken::default(),
                    )
                    .unwrap();
                assert_eq!(written.output.rows_written, 1);
                let reopened = prepare_relational(
                    &VortexRelationalPlan::Scan(VortexRelationalScan {
                        source_uri: DatasetUri::new(target.display().to_string()).unwrap(),
                        projection: shardloom_plan::ProjectionRequest::All,
                        predicate: None,
                    }),
                    policy(),
                )
                .unwrap();
                assert_eq!(reopened.output_dtype(), prepared.output_dtype());
                (
                    json_rows(
                        &reopened
                            .collect_jsonl(&CancellationToken::default())
                            .unwrap(),
                    ),
                    written.execution,
                )
            } else {
                let collected = stream.collect_jsonl(&CancellationToken::default()).unwrap();
                (json_rows(&collected), collected.execution)
            };
            assert_eq!(actual, vec![json!({"s":null,"rows":1})]);
            assert_eq!(input.calls.get(), 5);
            assert_eq!(report.prepared_sources, 2);
            assert_eq!(report.ordered_window_stages, if spill { 2 } else { 0 });
            assert_eq!(report.ordered_join_stages, u64::from(spill));
            assert_eq!(report.ordered_aggregate_stages, u64::from(spill));
            let source = report.input.as_ref().unwrap();
            assert_eq!(source.window_rows_detached, 10);
            assert!(source.ordering_rows_detached > 0);
            assert!(source.join_build_rows_detached > 0);
            assert!(source.end_of_input_observed);
            assert!(report.native_io_certificate.is_certified());
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&fixture, spill);
        }
        assert_eq!(prepared.snapshot().completed_executions, 2);
    }
}

#[test]
fn streaming_window_failures_and_zero_limit_require_complete_input_and_refund_owners() {
    for spill in [false, true] {
        for failure in [
            "producer",
            "schema",
            "input-cancel",
            "consumer",
            "consumer-cancel",
            "zero-limit",
        ] {
            let fixture = Fixture::new(
                single("n", PrimitiveArray::from_iter([1i64]).into_array()),
                1,
            );
            let plan = if failure == "zero-limit" {
                VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                    input: window(scan()),
                    offset: 0,
                    count: 0,
                }))
            } else {
                window(scan())
            };
            let prepared = prepared(&fixture, &plan, spill);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            let mut calls = 0;
            let mut emitted = 0;
            let mut producer = |session: &ResidentVortexSession| {
                calls += 1;
                if calls == 1 {
                    return source(session, &NUMBERS, &TEXT).map(Some);
                }
                assert_eq!(calls, 2);
                match failure {
                    "producer" | "zero-limit" => {
                        Err(batch_input::failed("window producer failure"))
                    }
                    "schema" => ResidentMemorySource::from_batch_columns(
                        session,
                        &[MemoryColumn {
                            name: "wrong",
                            values: MemoryColumnValues::Int64(&[Some(1)]),
                        }],
                    )
                    .map(Some),
                    "input-cancel" => {
                        token.cancel();
                        Ok(None)
                    }
                    _ => Ok(None),
                }
            };
            let result = prepared
                .with_batch_input(&mut producer)
                .unwrap()
                .for_each_batch(&token, |_, _| {
                    emitted += 1;
                    if failure == "consumer" {
                        return Err(batch_input::failed("window consumer failure"));
                    }
                    if failure == "consumer-cancel" {
                        token.cancel();
                    }
                    Ok(())
                });
            let error = result
                .err()
                .expect("a failed window must not complete")
                .to_string();
            let expected = match failure {
                "producer" | "zero-limit" => "window producer failure",
                "schema" => "schema",
                "consumer" => "window consumer failure",
                _ => "cancel",
            };
            assert!(error.contains(expected), "{failure}: {error}");
            assert_eq!(calls, 2);
            assert_eq!(
                emitted,
                usize::from(matches!(failure, "consumer" | "consumer-cancel"))
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&fixture, spill);
        }
    }
}

#[test]
fn streaming_window_rejects_repeated_source_and_compatibility_sink_before_demand() {
    let joined = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: window(scan()),
        right: scan(),
        kind: JoinKind::Cross,
        keys: vec![],
        columns: vec![],
        condition: None,
    }));
    assert!(
        prepare(&joined, 16 << 20)
            .err()
            .unwrap()
            .to_string()
            .contains("repeated batch source")
    );
    let fixture = Fixture::new(
        single("n", PrimitiveArray::from_iter([1i64]).into_array()),
        1,
    );
    let prepared = prepared(&fixture, &window(scan()), true);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut producer = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
        panic!("unsupported sink must reject before demand")
    };
    let target = fixture.0.join("rejected.json");
    let result = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .write_controlled(&target, Format::Json, false, &CancellationToken::default());
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("native Vortex destination")
    );
    assert!(!target.exists());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    clean(&fixture, true);
}
