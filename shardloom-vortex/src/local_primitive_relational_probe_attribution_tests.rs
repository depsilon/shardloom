//! Complete native requests with independent answers and debug-only probe counts.
//! These observations establish work, not release timings or CPU shares.

#![cfg(debug_assertions)]

use super::*;
use crate::{
    local_primitives::native_relational_observation::Session,
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFrameBound as Bound,
        VortexRelationalFrameExclusion as Exclusion, VortexRelationalFrameFunction as Function,
        VortexRelationalFrameOffset as Offset, VortexRelationalFrameUnit as Unit,
        VortexRelationalOrderKey as OrderKey, VortexRelationalSetKind as SetKind,
        VortexRelationalSubquery as Subquery, VortexRelationalSubqueryKind as SubqueryKind,
        VortexRelationalWindow as Window, VortexRelationalWindowExpression as WindowExpression,
        VortexRelationalWindowFrame as Frame, VortexRelationalWindowFunction as WindowFunction,
    },
    resident_memory_source::{MemoryColumn, MemoryColumnValues},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write as _,
};
use vortex::array::dtype::Nullability;

const CASES: [&str; 11] = [
    "group_unique",
    "group_repeat",
    "inner_unique",
    "inner_duplicates",
    "semi_missing",
    "anti_missing",
    "set_intersect",
    "set_except",
    "window_partitions",
    "window_distinct",
    "correlated_in",
];
const URIS: [&str; 2] = ["memory://probe-left", "memory://probe-right"];
const GRANT: u64 = 64 << 20;
type Key = (Option<i64>, Option<String>);

struct Data {
    ids: Vec<i64>,
    keys: Vec<Option<i64>>,
    text: Vec<Option<String>>,
    values: Vec<i64>,
}

impl Data {
    fn new(case: &str, rows: usize, right: bool) -> Self {
        let domain = match case {
            "group_repeat" => 67,
            "inner_duplicates" | "window_partitions" | "correlated_in" => rows.div_ceil(4),
            _ => rows,
        };
        let missing = !right
            && matches!(
                case,
                "semi_missing" | "anti_missing" | "set_intersect" | "set_except" | "correlated_in"
            );
        let raw: Vec<_> = (0..rows)
            .map(|row| {
                if missing && !row.is_multiple_of(7) {
                    domain + row
                } else {
                    row % domain
                }
            })
            .collect();
        Self {
            ids: (0..rows).map(|row| i64::try_from(row).unwrap()).collect(),
            keys: raw
                .iter()
                .enumerate()
                .map(|(row, key)| (!row.is_multiple_of(31)).then(|| i64::try_from(*key).unwrap()))
                .collect(),
            text: raw
                .iter()
                .enumerate()
                .map(|(row, key)| {
                    (!row.is_multiple_of(29)).then(|| match key % 4 {
                        0 => format!("key-{key}-東京"),
                        1 => format!("key-{key}-{}", "long-".repeat(12)),
                        2 => format!("key-{key}\0suffix"),
                        _ => format!("key-{key}"),
                    })
                })
                .collect(),
            values: (0..rows)
                .map(|row| i64::try_from(row % 17).unwrap() - 8)
                .collect(),
        }
    }

    fn key(&self, row: usize) -> Key {
        (self.keys[row], self.text[row].clone())
    }

    fn source(
        &self,
        session: &ResidentVortexSession,
        range: std::ops::Range<usize>,
    ) -> Result<ResidentMemorySource> {
        let text: Vec<_> = self.text[range.clone()]
            .iter()
            .map(Option::as_deref)
            .collect();
        ResidentMemorySource::from_batch_columns(
            session,
            &[
                MemoryColumn {
                    name: "id",
                    values: MemoryColumnValues::Int64NonNullable(&self.ids[range.clone()]),
                },
                MemoryColumn {
                    name: "k",
                    values: MemoryColumnValues::Int64(&self.keys[range.clone()]),
                },
                MemoryColumn {
                    name: "s",
                    values: MemoryColumnValues::Utf8(&text),
                },
                MemoryColumn {
                    name: "v",
                    values: MemoryColumnValues::Int64NonNullable(&self.values[range]),
                },
            ],
        )
    }
}

fn scan(side: usize, keys_only: bool) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(URIS[side]).unwrap(),
        projection: if keys_only {
            shardloom_plan::ProjectionRequest::Columns(vec![
                ColumnRef::new("k").unwrap(),
                ColumnRef::new("s").unwrap(),
            ])
        } else {
            shardloom_plan::ProjectionRequest::All
        },
        predicate: None,
    })
}

fn plan(case: &str) -> VortexRelationalPlan {
    if case == "correlated_in" {
        let key = |name: &str| VortexRelationalJoinKey {
            left: ColumnRef::new(name).unwrap(),
            right: ColumnRef::new(name).unwrap(),
        };
        return VortexRelationalPlan::Subquery(Box::new(Subquery {
            input: scan(0, false),
            relation: scan(1, false),
            kind: SubqueryKind::In {
                columns: vec![key("k")],
            },
            evaluation_guard: None,
            correlation: vec![key("s")],
            negated: false,
            output_column: "predicate".into(),
        }));
    }
    if case.starts_with("window_") {
        return window_plan(case);
    }
    if case.starts_with("group_") {
        return VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: scan(0, false),
            group_by: vec![ColumnRef::new("k").unwrap(), ColumnRef::new("s").unwrap()],
            grouping_sets: None,
            measures: vec![
                Measure::new("count", None, "rows".into()),
                Measure::new("sum", Some(ColumnRef::new("v").unwrap()), "total".into()),
            ],
        }));
    }
    if case.starts_with("set_") {
        return VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: scan(0, true),
            right: scan(1, true),
            kind: if case == "set_intersect" {
                SetKind::Intersect
            } else {
                SetKind::Except
            },
        }));
    }
    let kind = match case {
        "semi_missing" => JoinKind::LeftSemi,
        "anti_missing" => JoinKind::LeftAnti,
        _ => JoinKind::Inner,
    };
    let mut columns = vec![VortexRelationalJoinColumn {
        side: Side::Left,
        column: ColumnRef::new("id").unwrap(),
        output_column: "left_id".into(),
    }];
    if kind == JoinKind::Inner {
        columns.push(VortexRelationalJoinColumn {
            side: Side::Right,
            column: ColumnRef::new("id").unwrap(),
            output_column: "right_id".into(),
        });
    }
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan(0, false),
        right: scan(1, false),
        kind,
        condition: None,
        columns,
        keys: ["k", "s"]
            .into_iter()
            .map(|name| VortexRelationalJoinKey {
                left: ColumnRef::new(name).unwrap(),
                right: ColumnRef::new(name).unwrap(),
            })
            .collect(),
    }))
}

fn expected(case: &str, left: &Data, right: &Data) -> (Vec<Value>, DType) {
    if case == "correlated_in" {
        return expected_correlated(left, right);
    }
    if case.starts_with("window_") {
        return expected_window(case, left);
    }
    let mut fields = vec![
        ("k", DType::Primitive(PType::I64, Nullability::Nullable)),
        ("s", DType::Utf8(Nullability::Nullable)),
    ];
    let mut result = Vec::new();
    if case.starts_with("group_") {
        // BTreeMap provides exact independent key identity; the ordinal preserves first occurrence.
        let mut groups: BTreeMap<Key, (usize, u64, i64)> = BTreeMap::new();
        for row in 0..left.ids.len() {
            let next = groups.len();
            let state = groups.entry(left.key(row)).or_insert((next, 0, 0));
            state.1 += 1;
            state.2 += left.values[row];
        }
        let mut ordered: Vec<_> = groups.into_iter().collect();
        ordered.sort_by_key(|(_, state)| state.0);
        result.extend(ordered.into_iter().map(
            |((k, s), (_, count, total))| json!({"k":k, "s":s, "rows":count, "total":f64::from(i32::try_from(total).unwrap())}),
        ));
        fields.push((
            "rows",
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ));
        fields.push(("total", DType::Primitive(PType::F64, Nullability::Nullable)));
    } else if case.starts_with("set_") {
        let members: BTreeSet<_> = (0..right.ids.len()).map(|row| right.key(row)).collect();
        let mut seen = BTreeSet::new();
        for row in 0..left.ids.len() {
            let key = left.key(row);
            if members.contains(&key) == (case == "set_intersect") && seen.insert(key.clone()) {
                result.push(json!({"k": key.0, "s": key.1}));
            }
        }
    } else {
        let mut members: BTreeMap<Key, Vec<i64>> = BTreeMap::new();
        for row in 0..right.ids.len() {
            let key = right.key(row);
            if key.0.is_some() && key.1.is_some() {
                members.entry(key).or_default().push(right.ids[row]);
            }
        }
        for row in 0..left.ids.len() {
            let matches = members.get(&left.key(row));
            match case {
                "semi_missing" if matches.is_some() => {
                    result.push(json!({"left_id":left.ids[row]}));
                }
                "anti_missing" if matches.is_none() => {
                    result.push(json!({"left_id":left.ids[row]}));
                }
                "inner_unique" | "inner_duplicates" => {
                    for id in matches.into_iter().flatten() {
                        result.push(json!({"left_id":left.ids[row], "right_id":id}));
                    }
                }
                _ => {}
            }
        }
        fields = vec![(
            "left_id",
            DType::Primitive(PType::I64, Nullability::NonNullable),
        )];
        if case.starts_with("inner_") {
            fields.push((
                "right_id",
                DType::Primitive(PType::I64, Nullability::NonNullable),
            ));
        }
    }
    (
        result,
        DType::struct_(
            fields
                .into_iter()
                .map(|(name, dtype)| (name.to_owned(), dtype))
                .collect::<Vec<_>>(),
            Nullability::NonNullable,
        ),
    )
}

fn window_plan(case: &str) -> VortexRelationalPlan {
    let distinct = case == "window_distinct";
    VortexRelationalPlan::Window(Box::new(Window {
        input: scan(0, false),
        columns: vec![ColumnRef::new("id").unwrap()],
        expressions: vec![WindowExpression {
            output_column: "observed".into(),
            function: if distinct {
                WindowFunction::Framed(Function::CountDistinct(ColumnRef::new("k").unwrap()))
            } else {
                WindowFunction::RowNumber
            },
            partition_by: if distinct {
                Vec::new()
            } else {
                vec![ColumnRef::new("k").unwrap(), ColumnRef::new("s").unwrap()]
            },
            order_by: vec![OrderKey {
                column: ColumnRef::new("id").unwrap(),
                descending: false,
                nulls: None,
            }],
            frame: distinct.then_some(Frame {
                unit: Unit::Rows,
                start: Bound::Preceding(Offset::Number(shardloom_core::ScalarValue::UInt64(7))),
                end: Bound::CurrentRow,
                exclusion: Exclusion::NoOthers,
            }),
        }],
    }))
}

fn expected_correlated(left: &Data, right: &Data) -> (Vec<Value>, DType) {
    // Resolve SQL IN truth from the complete correlated multiset. A null
    // correlation key selects no rows; an empty IN relation is false even
    // when the input value is null. No engine hashes or indices are reused.
    let mut groups = BTreeMap::<String, Vec<Option<i64>>>::new();
    for row in 0..right.ids.len() {
        if let Some(group) = &right.text[row] {
            groups
                .entry(group.clone())
                .or_default()
                .push(right.keys[row]);
        }
    }
    let rows = (0..left.ids.len())
        .map(|row| {
            let members = left.text[row].as_ref().and_then(|group| groups.get(group));
            let key = left.keys[row];
            let predicate = match members {
                Some(values) if key.is_some() && values.contains(&key) => Some(true),
                Some(values) if key.is_none() || values.contains(&None) => None,
                None | Some(_) => Some(false),
            };
            json!({"id":left.ids[row], "k":key, "s":left.text[row],
                "v":left.values[row], "predicate":predicate})
        })
        .collect();
    (
        rows,
        DType::struct_(
            vec![
                (
                    "id".to_owned(),
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ),
                (
                    "k".to_owned(),
                    DType::Primitive(PType::I64, Nullability::Nullable),
                ),
                ("s".to_owned(), DType::Utf8(Nullability::Nullable)),
                (
                    "v".to_owned(),
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ),
                ("predicate".to_owned(), DType::Bool(Nullability::Nullable)),
            ],
            Nullability::NonNullable,
        ),
    )
}

fn expected_window(case: &str, left: &Data) -> (Vec<Value>, DType) {
    let mut partition_counts = BTreeMap::<Key, u64>::new();
    let rows = (0..left.ids.len())
        .map(|row| {
            let observed = if case == "window_distinct" {
                u64::try_from(
                    left.keys[row.saturating_sub(7)..=row]
                        .iter()
                        .flatten()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .len(),
                )
                .unwrap()
            } else {
                let count = partition_counts.entry(left.key(row)).or_default();
                *count += 1;
                *count
            };
            json!({"id":left.ids[row], "observed":observed})
        })
        .collect();
    (
        rows,
        DType::struct_(
            vec![
                (
                    "id".to_owned(),
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ),
                (
                    "observed".to_owned(),
                    DType::Primitive(
                        if case == "window_distinct" {
                            PType::U64
                        } else {
                            PType::I64
                        },
                        Nullability::NonNullable,
                    ),
                ),
            ],
            Nullability::NonNullable,
        ),
    )
}

fn check_counts(
    case: &str,
    rows: usize,
    counts: &crate::local_primitives::native_relational_observation::Counts,
) {
    assert!(
        counts.row_index_lookups > 0,
        "{case}/{rows} did not activate RowIndex"
    );
    assert!(counts.row_index_entry_reads >= counts.row_index_equality_calls);
    assert!(counts.row_index_fingerprint_candidates >= counts.row_index_equality_calls);
    assert!(counts.row_index_entry_reads >= counts.row_index_fingerprint_candidates);
    if rows == 17 {
        assert_eq!(
            counts.row_index_large_lookups, 0,
            "{case} tiny index control"
        );
        assert_eq!(
            counts.row_index_metadata_groups, 0,
            "{case} tiny index control"
        );
    } else {
        assert!(
            counts.row_index_large_lookups > 0,
            "{case} large index not reached"
        );
        assert!(
            counts.row_index_large_entry_reads > 0,
            "{case} large entries not read"
        );
    }
}

fn result_identity(rows: &[Value]) -> String {
    use std::fmt::Write as _;
    let mut value = String::with_capacity(64);
    for byte in Sha256::digest(serde_json::to_vec(rows).unwrap()) {
        write!(value, "{byte:02x}").unwrap();
    }
    value
}

fn observe(case: &str, rows: usize) -> Value {
    let inputs = [Data::new(case, rows, false), Data::new(case, rows, true)];
    let (expected, schema) = expected(case, &inputs[0], &inputs[1]);
    let sides = if case.starts_with("group_") || case.starts_with("window_") {
        1
    } else {
        2
    };
    let mut resources = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    resources.resource_envelope.memory_budget_bytes = GRANT;
    let prepared = prepare_relational_with_schema(resources, |schemas| {
        for side in 0..sides {
            schemas.register_batch_source(DatasetUri::new(URIS[side])?, |session| {
                inputs[side].source(session, 0..0)
            })?;
        }
        Ok(plan(case))
    })
    .unwrap();
    assert!(
        prepared.provider.is_none(),
        "diagnostic targets the shared native owner"
    );
    assert_eq!(prepared.output_dtype().as_ref(), Some(&schema));
    let memory = prepared.session.memory().clone();
    let initial = memory.snapshot().reserved_bytes;
    let owner_thread = std::thread::current().id();
    let mut calls = [0u64; 2];
    let mut positions = [0usize; 2];
    let mut ended = [false; 2];
    let mut input = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        assert_eq!(std::thread::current().id(), owner_thread);
        let side = URIS.iter().position(|name| uri.as_str() == *name).unwrap();
        assert_eq!(ordinal, calls[side]);
        assert!(!ended[side]);
        calls[side] += 1;
        if ordinal == 0 {
            return inputs[side].source(session, 0..0).map(Some);
        }
        let start = positions[side];
        if start == rows {
            ended[side] = true;
            return Ok(None);
        }
        let end = (start + 257).min(rows);
        positions[side] = end;
        inputs[side].source(session, start..end).map(Some)
    };
    let observation = Session::start();
    let result = prepared
        .with_batch_inputs(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let counts = observation.finish();
    let actual = json_rows(&result);
    assert_eq!(actual, expected, "{case}/{rows} complete ordered values");
    assert_eq!(
        serde_json::from_str::<Value>(result.result_schema_json.value()).unwrap(),
        serde_json::to_value(&schema).unwrap()
    );
    assert!(ended[..sides].iter().all(|value| *value));
    assert!(positions[..sides].iter().all(|value| *value == rows));
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
    check_counts(case, rows, &counts);
    let actual_identity = result_identity(&actual);
    let expected_identity = result_identity(&expected);
    assert_eq!(actual_identity, expected_identity);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, initial);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(prepared.snapshot().admitted_max_parallelism, 1);
    assert_eq!(prepared.snapshot().provider_background_workers, 0);
    drop(prepared);
    let final_memory = memory.snapshot();
    assert_eq!(final_memory.reserved_bytes, 0);
    assert_eq!(final_memory.denied_reservations, 0);
    assert!(final_memory.peak_reserved_bytes <= GRANT);
    json!({"case":case, "input_rows_per_side":rows, "output_rows":actual.len(), "observations":counts,
        "expected_sha256":expected_identity, "actual_sha256":actual_identity, "complete_values_and_schema_match":true,
        "peak_charged_bytes":final_memory.peak_reserved_bytes, "final_charged_bytes":0,
        "observation_scope":"debug-test caller thread; one admitted worker; zero provider background workers",
        "engine_timing_claimed":false, "no_fallback":true})
}

#[test]
#[ignore = "guarded serial attribution screen; never a release timing benchmark"]
fn native_row_index_complete_request_attribution() {
    let destination = PathBuf::from(std::env::var("SHARDLOOM_PROBE_ATTRIBUTION_OUTPUT").unwrap());
    assert!(destination.is_absolute() && !destination.exists());
    let mut observations = Vec::new();
    for rows in [17, 4097] {
        for case in CASES {
            let observation = observe(case, rows);
            eprintln!("ROW_INDEX_WORK {observation}");
            observations.push(observation);
        }
    }
    fs::OpenOptions::new().create_new(true).write(true).open(destination).unwrap()
        .write_all(&serde_json::to_vec_pretty(&json!({"schema":"shardloom.row_index_attribution.v1", "observations":observations})).unwrap()).unwrap();
}
