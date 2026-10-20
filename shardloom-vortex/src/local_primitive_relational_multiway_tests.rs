//! Ordinary binary joins preserve complete cyclic results and shared source aliases.

use super::*;
use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds};
use serde_json::{Value, json};

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
