//! Ordinary binary joins preserve cyclic products, existence and source aliases.

use super::*;
use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds};
use serde_json::{Value, json};
use shardloom_core::ShardLoomError;

#[path = "local_primitive_relational_cyclic_exists_tests.rs"]
mod cyclic_exists_tests;

#[derive(Clone)]
struct Relation {
    x: Vec<Option<i64>>,
    y: Vec<Option<i64>>,
    id: Vec<i64>,
}

impl Relation {
    fn new(rows: &[(Option<i64>, Option<i64>)]) -> Self {
        Self {
            x: rows.iter().map(|row| row.0).collect(),
            y: rows.iter().map(|row| row.1).collect(),
            id: (0..i64::try_from(rows.len()).unwrap()).collect(),
        }
    }

    fn source(&self, session: &ResidentVortexSession) -> Result<ResidentMemorySource> {
        ResidentMemorySource::from_columns(
            session,
            &[
                MemoryColumn {
                    name: "x",
                    values: MemoryColumnValues::Int64(&self.x),
                },
                MemoryColumn {
                    name: "y",
                    values: MemoryColumnValues::Int64(&self.y),
                },
                MemoryColumn {
                    name: "id",
                    values: MemoryColumnValues::Int64NonNullable(&self.id),
                },
            ],
            MemorySourceBounds::default(),
        )
    }
}

fn uri(name: &str) -> DatasetUri {
    DatasetUri::new(format!("memory://cyclic-join/{name}")).unwrap()
}

fn scan(name: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri(name),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn column(side: Side, name: &str, alias: &str) -> VortexRelationalJoinColumn {
    VortexRelationalJoinColumn {
        side,
        column: ColumnRef::new(name).unwrap(),
        output_column: alias.to_owned(),
    }
}

fn key(left: &str, right: &str) -> VortexRelationalJoinKey {
    VortexRelationalJoinKey {
        left: ColumnRef::new(left).unwrap(),
        right: ColumnRef::new(right).unwrap(),
    }
}

fn plan(self_alias: bool, reverse: bool) -> VortexRelationalPlan {
    let inner = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan("r"),
        right: scan("s"),
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![key("y", "x")],
        columns: vec![
            column(Side::Left, "x", "ra"),
            column(Side::Left, "y", "rb"),
            column(Side::Left, "id", "rid"),
            column(Side::Right, "x", "sb"),
            column(Side::Right, "y", "sc"),
            column(Side::Right, "id", "sid"),
        ],
    }));
    let mut keys = vec![key("ra", "x"), key("sc", "y")];
    if reverse {
        keys.reverse();
    }
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: inner,
        right: scan(if self_alias { "r" } else { "t" }),
        kind: JoinKind::Inner,
        condition: None,
        keys,
        columns: ["ra", "rb", "rid", "sb", "sc", "sid"]
            .iter()
            .map(|name| column(Side::Left, name, name))
            .chain([
                column(Side::Right, "x", "ta"),
                column(Side::Right, "y", "tc"),
                column(Side::Right, "id", "tid"),
            ])
            .collect(),
    }))
}

fn expected_dtype() -> DType {
    DType::struct_(
        ["ra", "rb", "rid", "sb", "sc", "sid", "ta", "tc", "tid"]
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    DType::Primitive(
                        PType::I64,
                        if name.ends_with("id") {
                            Nullability::NonNullable
                        } else {
                            Nullability::Nullable
                        },
                    ),
                )
            })
            .collect::<Vec<_>>(),
        Nullability::NonNullable,
    )
}

fn literal_reference(r: &Relation, s: &Relation, t: &Relation) -> Vec<Value> {
    let equal = |left: Option<i64>, right| left.is_some() && left == right;
    let mut rows = Vec::new();
    for ri in 0..r.id.len() {
        for si in 0..s.id.len() {
            if !equal(r.y[ri], s.x[si]) {
                continue;
            }
            for ti in 0..t.id.len() {
                if equal(r.x[ri], t.x[ti]) && equal(s.y[si], t.y[ti]) {
                    rows.push(json!({"ra":r.x[ri],"rb":r.y[ri],"rid":r.id[ri],
                        "sb":s.x[si],"sc":s.y[si],"sid":s.id[si],
                        "ta":t.x[ti],"tc":t.y[ti],"tid":t.id[ti]}));
                }
            }
        }
    }
    rows
}

fn check(values: &[Relation; 3], self_alias: bool, reverse: bool) {
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri("r"), |session| values[0].source(session))?;
        schemas.register_memory_source(uri("s"), |session| values[1].source(session))?;
        if !self_alias {
            schemas.register_memory_source(uri("t"), |session| values[2].source(session))?;
        }
        Ok(plan(self_alias, reverse))
    })
    .unwrap();
    let memory = prepared.session.memory().clone();
    let expected = literal_reference(
        &values[0],
        &values[1],
        &values[if self_alias { 0 } else { 2 }],
    );
    for _ in 0..2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        assert_eq!(
            serde_json::from_str::<Value>(result.result_schema_json.value()).unwrap(),
            serde_json::to_value(expected_dtype()).unwrap()
        );
        assert_eq!(
            result.execution.prepared_sources,
            if self_alias { 2 } else { 3 }
        );
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(
            !result
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_binary_plan_preserves_complete_values_order_nulls_and_repeated_aliases() {
    let initial = [
        Relation::new(&[
            (Some(2), Some(7)),
            (Some(1), Some(7)),
            (None, Some(7)),
            (Some(2), None),
            (Some(2), Some(7)),
        ]),
        Relation::new(&[
            (Some(7), Some(3)),
            (Some(7), Some(4)),
            (Some(7), Some(3)),
            (None, Some(3)),
            (Some(9), None),
        ]),
        Relation::new(&[
            (Some(2), Some(3)),
            (Some(1), Some(3)),
            (Some(2), Some(3)),
            (Some(2), None),
            (None, Some(3)),
            (Some(-1), Some(4)),
        ]),
    ];
    let grid = Relation::new(
        &(0..16)
            .map(|row| (Some(row % 4), Some(row / 4)))
            .collect::<Vec<_>>(),
    );
    for reverse in [false, true] {
        for empty in [None, Some(0), Some(1), Some(2)] {
            let mut values = initial.clone();
            if let Some(index) = empty {
                values[index] = Relation::new(&[]);
            }
            check(&values, false, reverse);
        }
        check(&[grid.clone(), grid.clone(), grid.clone()], true, reverse);
    }
}

fn existence_plan(kind: JoinKind, reverse: bool) -> VortexRelationalPlan {
    // R(a,b) qualifies when a complete S(b,c), T(a,c) witness exists. The
    // inner child projects only the two columns the outer equality needs.
    let inner = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan("s"),
        right: scan("t"),
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![key("y", "y")],
        columns: vec![
            column(Side::Left, "x", "witness_b"),
            column(Side::Right, "x", "witness_a"),
        ],
    }));
    let mut keys = vec![key("y", "witness_b"), key("x", "witness_a")];
    if reverse {
        keys.reverse();
    }
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan("r"),
        right: inner,
        kind,
        condition: None,
        keys,
        columns: vec![
            column(Side::Left, "id", "outer_id"),
            column(Side::Left, "y", "b"),
            column(Side::Left, "x", "a"),
        ],
    }))
}

fn prepare_existence(
    values: &[Relation; 3],
    kind: JoinKind,
    reverse: bool,
) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |schemas| {
        for (name, value) in ["r", "s", "t"].into_iter().zip(values) {
            schemas.register_memory_source(uri(name), |session| value.source(session))?;
        }
        Ok(existence_plan(kind, reverse))
    })
    .unwrap()
}

fn existence_reference(values: &[Relation; 3], kind: JoinKind) -> Vec<Value> {
    let [r, s, t] = values;
    let equal = |left: Option<i64>, right| left.is_some() && left == right;
    (0..r.id.len())
        .filter(|&ri| {
            let witness = (0..s.id.len()).any(|si| {
                equal(r.y[ri], s.x[si])
                    && (0..t.id.len()).any(|ti| equal(r.x[ri], t.x[ti]) && equal(s.y[si], t.y[ti]))
            });
            witness == (kind == JoinKind::LeftSemi)
        })
        .map(|ri| json!({"outer_id":r.id[ri],"b":r.y[ri],"a":r.x[ri]}))
        .collect()
}

fn check_existence(values: &[Relation; 3], kind: JoinKind, reverse: bool) {
    let prepared = prepare_existence(values, kind, reverse);
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let expected = existence_reference(values, kind);
    let expected_schema = serde_json::to_value(DType::struct_(
        vec![
            (
                "outer_id".to_owned(),
                DType::Primitive(PType::I64, Nullability::NonNullable),
            ),
            (
                "b".to_owned(),
                DType::Primitive(PType::I64, Nullability::Nullable),
            ),
            (
                "a".to_owned(),
                DType::Primitive(PType::I64, Nullability::Nullable),
            ),
        ],
        Nullability::NonNullable,
    ))
    .unwrap();
    for batch_rows in [1, 2, 2048] {
        let mut rows = Vec::new();
        let execution = prepared
            .for_each_json_batch(
                &CancellationToken::default(),
                batch_rows,
                1 << 20,
                |batch| {
                    assert!(batch.rows <= batch_rows);
                    assert_eq!(
                        serde_json::from_str::<Value>(batch.result_schema_json.value()).unwrap(),
                        expected_schema
                    );
                    rows.extend(
                        serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap(),
                    );
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(rows, expected);
        assert_eq!(execution.output_rows, expected.len() as u64);
        assert_eq!(execution.prepared_sources, 3);
        assert!(execution.native_io_certificate.is_certified());
        assert!(
            !execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        drop(execution);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cyclic_existence_plan_preserves_outer_duplicates_nulls_aliases_and_batch_order() {
    let initial = [
        Relation::new(&[
            (Some(2), Some(7)),
            (Some(1), Some(7)),
            (None, Some(7)),
            (Some(2), None),
            (Some(2), Some(7)),
            (Some(-1), Some(9)),
        ]),
        Relation::new(&[
            (Some(7), Some(3)),
            (Some(7), Some(4)),
            (Some(7), Some(3)),
            (None, Some(3)),
            (Some(9), None),
            (Some(9), Some(5)),
        ]),
        Relation::new(&[
            (Some(2), Some(3)),
            (Some(1), Some(3)),
            (Some(2), Some(3)),
            (Some(2), None),
            (None, Some(3)),
            (Some(-1), Some(4)),
        ]),
    ];
    assert_eq!(
        existence_reference(&initial, JoinKind::LeftSemi),
        vec![
            json!({"outer_id":0,"b":7,"a":2}),
            json!({"outer_id":1,"b":7,"a":1}),
            json!({"outer_id":4,"b":7,"a":2}),
        ]
    );
    for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
        for reverse in [false, true] {
            check_existence(&initial, kind, reverse);
            for index in 0..3 {
                for replacement in [Relation::new(&[]), Relation::new(&[(None, None); 3])] {
                    let mut values = initial.clone();
                    values[index] = replacement;
                    check_existence(&values, kind, reverse);
                }
            }
        }
    }
    let abundant = [
        Relation::new(&vec![(Some(2), Some(7)); 2051]),
        Relation::new(&[(Some(7), Some(3)); 4]),
        Relation::new(&[(Some(2), Some(3)); 4]),
    ];
    check_existence(&abundant, JoinKind::LeftSemi, false);
}

#[test]
fn cyclic_existence_plan_drains_failed_consumers_cancellation_and_denial() {
    let values = [
        Relation::new(&[(Some(2), Some(7)); 5]),
        Relation::new(&[(Some(7), Some(3)); 4]),
        Relation::new(&[(Some(2), Some(3)); 4]),
    ];
    let prepared = prepare_existence(&values, JoinKind::LeftSemi, false);
    let memory = prepared.session.memory().clone();
    let baseline = memory.snapshot().reserved_bytes;
    let mut observed = 0;
    let failure = prepared.for_each_json_batch(&CancellationToken::default(), 2, 4096, |batch| {
        observed += batch.rows;
        Err(ShardLoomError::InvalidOperation(
            "cyclic existence consumer failed".into(),
        ))
    });
    assert!(
        failure
            .err()
            .unwrap()
            .to_string()
            .contains("cyclic existence consumer failed")
    );
    assert_eq!(observed, 2);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);

    let cancellation = CancellationToken::default();
    observed = 0;
    assert!(
        prepared
            .for_each_json_batch(&cancellation, 2, 4096, |batch| {
                observed += batch.rows;
                cancellation.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(observed, 2);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);

    let occupied = memory
        .reserve(memory.snapshot().limit_bytes - baseline)
        .unwrap();
    let denied = prepared.collect_jsonl(&CancellationToken::default());
    assert!(
        denied
            .err()
            .unwrap()
            .to_string()
            .contains("memory reservation denied")
    );
    drop(occupied);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        existence_reference(&values, JoinKind::LeftSemi)
    );
    drop((result, prepared));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
