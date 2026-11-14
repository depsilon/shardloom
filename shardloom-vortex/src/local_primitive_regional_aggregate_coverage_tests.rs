use super::*;
use crate::local_primitives::{
    StatValue, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    native_capacity::ReservedVec,
};
use crate::{VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::DatasetUri;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::{DType, Nullability, PType, StructFields};

fn zone(start: u64, end: u64, lo: Option<i64>, hi: Option<i64>, nulls: Option<u64>) -> Zone {
    Zone {
        range: start..end,
        stats: SegmentStats {
            null_count: nulls,
            min_value: lo.map(StatValue::Int64),
            max_value: hi.map(StatValue::Int64),
            exactness: StatisticsExactness::Exact,
            ..SegmentStats::with_row_count(end - start)
        },
    }
}

fn columns<'a>(
    memory: &LiveMemoryPool,
    inputs: &[(&'a str, Vec<Zone>)],
) -> [Option<Column<'a>>; MAX_COLUMNS] {
    let mut columns = std::array::from_fn(|_| None);
    for (index, (name, zones)) in inputs.iter().enumerate() {
        let mut values = ReservedVec::try_with_capacity(memory, zones.len())
            .unwrap()
            .unwrap();
        values.values.extend(zones.iter().cloned());
        columns[index] = Some(Column {
            name,
            dtype: DType::Primitive(PType::I64, Nullability::Nullable),
            zones: values,
        });
    }
    columns
}

fn request() -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("regional-unit.vortex").unwrap(),
        VortexSimpleAggregateRequest::new(vec![
            VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
            VortexSimpleAggregateMeasure::new(
                "min",
                Some(ColumnRef::new("a").unwrap()),
                "lo".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "max",
                Some(ColumnRef::new("b").unwrap()),
                "hi".into(),
            ),
        ]),
    )
}

fn lowering(request: &VortexQueryPrimitiveRequest) -> AggregateLowering {
    let dtype = DType::Struct(
        StructFields::from_iter([
            ("a", DType::Primitive(PType::I64, Nullability::Nullable)),
            ("b", DType::Primitive(PType::I64, Nullability::Nullable)),
        ]),
        Nullability::NonNullable,
    );
    AggregateLowering::new(
        request,
        &dtype,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1)
            .unwrap()
            .resource_envelope(),
    )
    .unwrap()
}

fn plan(memory: &LiveMemoryPool, rows: u64) -> Plan {
    Plan {
        ranges: ReservedVec::try_with_capacity(memory, 64).unwrap().unwrap(),
        source_rows: rows,
        regions: 0,
        metadata_regions: 0,
        excluded_regions: 0,
        residual_regions: 0,
        metadata_rows: 0,
        excluded_rows: 0,
        residual_rows: 0,
        tables_read: 0,
        metadata_arrays: 0,
        metadata_rows_read: 0,
        measures: 3,
        metadata_bytes: 4096,
        _metadata: memory.reserve(4096).unwrap(),
    }
}

#[test]
fn regional_aggregate_coverage_rejects_gaps_overlaps_missing_and_short_rows() {
    let good = vec![
        zone(0, 4, Some(1), Some(2), Some(0)),
        zone(4, 7, Some(3), Some(4), Some(0)),
    ];
    validate_zones(&good, 7).unwrap();
    for broken in [
        vec![good[0].clone()],
        vec![good[1].clone()],
        vec![good[0].clone(), zone(3, 7, Some(3), Some(4), Some(0))],
        vec![good[0].clone(), zone(5, 7, Some(3), Some(4), Some(0))],
        vec![good[0].clone(), zone(4, 8, Some(3), Some(4), Some(0))],
    ] {
        assert!(validate_zones(&broken, 7).is_err());
    }
    assert!(combine(&good, 1, 7).is_err());
    assert!(combine(&good, 0, 6).is_err());
    let mut wrong_length = good;
    wrong_length[0].stats.row_count = Some(5);
    assert!(validate_zones(&wrong_length, 7).is_err());
}

#[test]
fn regional_aggregate_cross_column_boundaries_only_merge_whole_zones() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let columns = columns(
        &memory,
        &[
            (
                "a",
                vec![
                    zone(0, 4, Some(-9), Some(-1), Some(0)),
                    zone(4, 8, Some(1), Some(9), Some(0)),
                    zone(8, 10, None, None, Some(2)),
                ],
            ),
            (
                "b",
                vec![
                    zone(0, 6, Some(4), Some(20), Some(1)),
                    zone(6, 10, Some(6), Some(40), Some(0)),
                ],
            ),
        ],
    );
    let request = request();
    let lowering = lowering(&request);
    let mut states = SimpleAggregateStates::new(
        &lowering.rewrite.aggregate,
        &lowering.plan.projected_columns,
    )
    .unwrap();
    let mut plan = plan(&memory, 10);
    complete(&columns, &lowering, &mut states, None, &mut plan).unwrap();
    assert_eq!(plan.regions, 1);
    assert_eq!(plan.metadata_rows, 10);
    assert_eq!(plan.ranges.values, [] as [std::ops::Range<u64>; 0]);
    let values: serde_json::Value =
        serde_json::from_str(&states.result_summary(&[]).unwrap()).unwrap();
    assert_eq!(
        values["values"],
        serde_json::json!({"rows":10,"lo":-9,"hi":40})
    );
    drop(columns);
    drop(plan);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn regional_aggregate_missing_measure_never_commits_half_of_a_region() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let columns = columns(
        &memory,
        &[
            (
                "a",
                vec![
                    zone(0, 4, Some(1), Some(4), Some(0)),
                    zone(4, 8, Some(5), Some(8), Some(0)),
                ],
            ),
            (
                "b",
                vec![
                    zone(0, 4, Some(1), None, Some(0)),
                    zone(4, 8, Some(5), Some(8), Some(0)),
                ],
            ),
        ],
    );
    let request = request();
    let lowering = lowering(&request);
    let mut states = SimpleAggregateStates::new(
        &lowering.rewrite.aggregate,
        &lowering.plan.projected_columns,
    )
    .unwrap();
    let mut plan = plan(&memory, 8);
    complete(&columns, &lowering, &mut states, None, &mut plan).unwrap();
    assert_eq!(plan.metadata_rows, 4);
    assert_eq!(plan.residual_rows, 4);
    assert_eq!(plan.ranges.values, vec![0..4]);
    let values: serde_json::Value =
        serde_json::from_str(&states.result_summary(&[]).unwrap()).unwrap();
    assert_eq!(
        values["values"],
        serde_json::json!({"rows":4,"lo":5,"hi":8})
    );
}

#[test]
fn regional_aggregate_nullable_predicate_proofs_never_skip_or_include_wrong_rows() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let domain = [None, Some(-2), Some(-1), Some(0), Some(1), Some(2)];
    let ops = [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ];
    for left in domain {
        for middle in domain {
            for right in domain {
                let values = [left, middle, right];
                let zones = vec![zone(
                    0,
                    3,
                    values.iter().flatten().copied().min(),
                    values.iter().flatten().copied().max(),
                    Some(values.iter().filter(|value| value.is_none()).count() as u64),
                )];
                let columns = columns(&memory, &[("a", zones.clone())]);
                let mut facts = std::array::from_fn(|_| None);
                facts[0] = Some(combine(&zones, 0, 3).unwrap());
                for literal in -3..=3 {
                    for op in ops {
                        let predicate = PredicateExpr::Compare {
                            column: ColumnRef::new("a").unwrap(),
                            op,
                            value: StatValue::Int64(literal),
                        };
                        let selected = values
                            .iter()
                            .filter(|value| {
                                value.is_some_and(|value| match op {
                                    ComparisonOp::Eq => value == literal,
                                    ComparisonOp::NotEq => value != literal,
                                    ComparisonOp::Lt => value < literal,
                                    ComparisonOp::LtEq => value <= literal,
                                    ComparisonOp::Gt => value > literal,
                                    ComparisonOp::GtEq => value >= literal,
                                })
                            })
                            .count();
                        match qualify(&predicate, &columns, &facts).unwrap() {
                            Qualification::All => {
                                assert_eq!(selected, 3, "{values:?} {op:?} {literal}");
                            }
                            Qualification::None => {
                                assert_eq!(selected, 0, "{values:?} {op:?} {literal}");
                            }
                            Qualification::Unknown => {}
                        }
                    }
                }
            }
        }
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn regional_aggregate_conjunction_binds_each_columns_facts_and_wide_literals() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let columns = columns(
        &memory,
        &[
            ("a", vec![zone(0, 3, Some(-2), Some(-1), Some(0))]),
            ("b", vec![zone(0, 3, Some(20), Some(30), Some(0))]),
        ],
    );
    let mut facts = std::array::from_fn(|_| None);
    for (i, column) in columns.iter().flatten().enumerate() {
        facts[i] = Some(column.zones.values[0].stats.clone());
    }
    let comparison = |name, op, value| PredicateExpr::Compare {
        column: ColumnRef::new(name).unwrap(),
        op,
        value,
    };
    let predicate = PredicateExpr::And(vec![
        comparison("a", ComparisonOp::Lt, StatValue::UInt64(0)),
        comparison("b", ComparisonOp::Gt, StatValue::Int64(10)),
    ]);
    assert_eq!(
        qualify(&predicate, &columns, &facts).unwrap(),
        Qualification::All
    );
    let predicate = comparison("b", ComparisonOp::GtEq, StatValue::UInt64(u64::MAX));
    assert_eq!(
        qualify(&predicate, &columns, &facts).unwrap(),
        Qualification::None
    );
    facts[1].as_mut().unwrap().null_count = Some(1);
    let predicate = comparison("b", ComparisonOp::Gt, StatValue::Int64(10));
    assert_eq!(
        qualify(&predicate, &columns, &facts).unwrap(),
        Qualification::Unknown
    );
    let predicate = PredicateExpr::IsNull {
        column: ColumnRef::new("b").unwrap(),
    };
    assert_eq!(
        qualify(&predicate, &columns, &facts).unwrap(),
        Qualification::Unknown
    );
}

#[test]
fn regional_aggregate_cancellation_and_bad_coverage_preserve_original_states() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let columns = columns(
        &memory,
        &[
            ("a", vec![zone(0, 4, Some(1), Some(4), Some(0))]),
            ("b", vec![zone(0, 4, Some(1), Some(4), Some(0))]),
        ],
    );
    let request = request();
    let lowering = lowering(&request);
    let mut states = SimpleAggregateStates::new(
        &lowering.rewrite.aggregate,
        &lowering.plan.projected_columns,
    )
    .unwrap();
    let before = states.result_summary(&[]).unwrap();
    let mut bad = plan(&memory, 5);
    assert!(complete(&columns, &lowering, &mut states, None, &mut bad).is_err());
    assert_eq!(states.result_summary(&[]).unwrap(), before);
    let token = CancellationToken::default();
    token.cancel();
    let mut cancelled = plan(&memory, 4);
    assert!(
        complete(
            &columns,
            &lowering,
            &mut states,
            Some(&token),
            &mut cancelled
        )
        .is_err()
    );
    assert_eq!(states.result_summary(&[]).unwrap(), before);
}
