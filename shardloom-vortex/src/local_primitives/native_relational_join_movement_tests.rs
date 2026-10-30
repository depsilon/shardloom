//! Measure existing output construction; no runtime path or timing is changed.

use super::*;
use crate::{
    local_primitives::{native_payload, native_relational_aggregate as aggregate},
    resident_memory_source::{MemoryColumn, MemoryColumnValues, ResidentMemorySource},
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_exec::live_memory::MemoryLease;
use std::{collections::BTreeMap, fmt::Write as _, sync::Weak};

#[derive(Clone, Copy, Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)] // Independent fixture axes, not runtime state.
struct Case {
    name: &'static str,
    products_first: bool,
    report: bool,
    wide: bool,
    outer: bool,
    rows: usize,
    batch_rows: usize,
}

impl Case {
    fn groups(self) -> Vec<(&'static str, DType)> {
        let mut groups = vec![("r", integer())];
        if !self.report {
            groups.push(("c", integer()));
        }
        if self.wide {
            groups.push(("t", DType::Utf8(Nullability::Nullable)));
        }
        groups
    }
}

#[derive(Default, Debug, Serialize)]
struct Stage {
    rows: u64,
    batches: u64,
    cells: u64,
    visible_buffer_bytes: u64,
    largest_output_buffer_bytes: u64,
}

impl Stage {
    fn record(&mut self, array: &ArrayRef) {
        let rows = u64::try_from(array.len()).unwrap();
        self.rows += rows;
        self.batches += 1;
        self.cells +=
            rows * u64::try_from(array.dtype().as_struct_fields_opt().unwrap().nfields()).unwrap();
        let bytes = array.nbytes();
        self.visible_buffer_bytes += bytes;
        self.largest_output_buffer_bytes = self.largest_output_buffer_bytes.max(bytes);
    }
}

#[derive(Debug, Serialize)]
struct Report {
    case: Case,
    stages: Vec<Stage>,
    aggregate_output: Stage,
    producer_batches_released: usize,
    complete_result_rows: usize,
    actual: Vec<Value>,
    expected: Vec<Value>,
    complete_result_sha256: String,
    source_release_before_next_batch: bool,
    retained_values_after_session_close: bool,
    final_output_credits_released: bool,
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    Consumer(usize),
    Cancel(usize),
    Deny,
}

fn integer() -> DType {
    DType::Primitive(PType::I64, Nullability::Nullable)
}

fn dimensions(products: bool, active: bool) -> Vec<(Option<i64>, Option<i64>)> {
    let mut rows = vec![(None, Some(777))];
    let (count, duplicate, groups) = if products { (61, 11, 5) } else { (127, 13, 8) };
    for key in (0..count).rev() {
        if active && key % 3 == 0 {
            continue;
        }
        for _ in 0..if key % duplicate == 0 { 2 } else { 1 } {
            rows.push((Some(key), (key % 17 != 0).then_some(key % groups)));
        }
    }
    rows
}

fn fact(row: usize) -> (Option<i64>, Option<i64>, Option<i64>, Option<String>) {
    let row = i64::try_from(row).unwrap();
    (
        (row % 29 != 0).then_some((row * 17 + 43) % 137),
        (row % 31 != 0).then_some((row * 7 + 23) % 71),
        (row % 11 != 0).then_some((row * 13 + 19) % 1001 - 500),
        (row % 23 != 0).then(|| format!("μλ:{row:05}:{}", "AbÇ".repeat(24))),
    )
}

fn source(
    session: &ResidentVortexSession,
    context: &NativeExecutionContext<'_>,
    columns: &[MemoryColumn<'_>],
    witnesses: &mut Vec<Weak<MemoryLease>>,
) -> Result<ArrayRef> {
    assert!(witnesses.iter().all(|witness| witness.strong_count() == 0));
    let source = ResidentMemorySource::from_batch_columns(session, columns)?;
    witnesses.push(source.batch_release_witness()?);
    let array = source.batch_array(context)?.clone();
    drop(source);
    assert!(witnesses.last().unwrap().strong_count() > 0);
    Ok(array)
}

fn join_spec(case: Case, first: bool) -> Spec {
    let products = if first {
        case.products_first
    } else {
        !case.products_first
    };
    let mut columns = Vec::new();
    if first && !case.report {
        columns.push((Side::Left, if products { "e" } else { "p" }.to_owned()));
    }
    columns.push((Side::Left, "v".to_owned()));
    if case.wide {
        columns.push((Side::Left, "t".to_owned()));
    }
    if !first {
        columns.push((Side::Left, if products { "r" } else { "c" }.to_owned()));
    }
    columns.push((Side::Right, if products { "c" } else { "r" }.to_owned()));
    Spec {
        kind: if case.outer { Kind::Left } else { Kind::Inner },
        asof: None,
        condition: None,
        left_keys: vec![if products { "p" } else { "e" }.into()],
        right_keys: vec!["k".into()],
        right_key_fields: vec![("k".into(), integer())],
        fields: columns
            .iter()
            .map(|(_, name)| {
                (
                    name.clone(),
                    if name == "t" {
                        DType::Utf8(Nullability::Nullable)
                    } else {
                        integer()
                    },
                )
            })
            .collect(),
        columns,
    }
}

fn aggregate_spec(case: Case) -> aggregate::Spec {
    let groups: Vec<_> = case
        .groups()
        .into_iter()
        .map(|(name, dtype)| (name.to_owned(), dtype))
        .collect();
    let mut fields = groups.clone();
    let measures = [aggregate::Function::Count, aggregate::Function::Sum]
        .into_iter()
        .map(|function| {
            let count = matches!(function, aggregate::Function::Count);
            let dtype = if count {
                DType::Primitive(PType::U64, Nullability::NonNullable)
            } else {
                DType::Primitive(PType::F64, Nullability::Nullable)
            };
            fields.push((if count { "count" } else { "sum" }.into(), dtype.clone()));
            aggregate::Measure {
                function,
                column: (!count).then(|| "v".to_owned()),
                secondary_column: None,
                quantile_level: None,
                separator: None,
                filter_column: None,
                selection: None,
                dtype,
                input_ptype: (!count).then_some(PType::I64),
                decimal_source: None,
                decimal_output: None,
                distinct_fields: vec![],
                distinct_names: vec![],
            }
        })
        .collect();
    aggregate::Spec {
        group_names: groups.iter().map(|(name, _)| name.clone()).collect(),
        groups,
        fields,
        measures,
    }
}

fn json_rows(array: &ArrayRef) -> Vec<Value> {
    let mut execution = vortex::array::array_session().create_execution_ctx();
    let names = array
        .dtype()
        .as_struct_fields_opt()
        .unwrap()
        .names()
        .clone();
    let columns: Vec<_> = names
        .iter()
        .map(|name| logical_field_from_native_array(array, name.as_ref()).unwrap())
        .collect();
    (0..array.len())
        .map(|row| {
            Value::Object(
                names
                    .iter()
                    .zip(&columns)
                    .map(|(name, column)| {
                        (
                            name.to_string(),
                            result_batch::scalar_value(column, row, &mut execution)
                                .unwrap()
                                .into_json()
                                .unwrap(),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

fn canonical(rows: &mut [Value]) {
    rows.sort_by_cached_key(Value::to_string);
}

struct ReferenceGroup {
    key: serde_json::Map<String, Value>,
    count: u64,
    sum: Option<i64>,
}

fn reference(case: Case) -> (Vec<Value>, Vec<u64>) {
    let entities = dimensions(false, case.report);
    let products = dimensions(true, false);
    let mut groups: BTreeMap<String, ReferenceGroup> = BTreeMap::new();
    let mut stages = vec![0; if case.report { 1 } else { 2 }];
    for row in 0..case.rows {
        let (e, p, value, text) = fact(row);
        let matches = |key: Option<i64>, domain: &[(Option<i64>, Option<i64>)]| {
            let mut values: Vec<_> = domain
                .iter()
                .filter(|(other, _)| key.is_some() && *other == key)
                .map(|(_, value)| *value)
                .collect();
            if case.outer && values.is_empty() {
                values.push(None);
            }
            values
        };
        let regions = matches(e, &entities);
        let categories = if case.report {
            vec![None]
        } else {
            matches(p, &products)
        };
        stages[0] += u64::try_from(if case.products_first {
            categories.len()
        } else {
            regions.len()
        })
        .unwrap();
        if !case.report {
            stages[1] += u64::try_from(regions.len() * categories.len()).unwrap();
        }
        for region in &regions {
            for category in &categories {
                let mut key = serde_json::Map::new();
                key.insert("r".into(), json!(region));
                if !case.report {
                    key.insert("c".into(), json!(category));
                }
                if case.wide {
                    key.insert("t".into(), json!(text));
                }
                let entry = groups
                    .entry(Value::Object(key.clone()).to_string())
                    .or_insert(ReferenceGroup {
                        key,
                        count: 0,
                        sum: None,
                    });
                entry.count += 1;
                if let Some(value) = value {
                    entry.sum = Some(entry.sum.unwrap_or(0) + value);
                }
            }
        }
    }
    let mut output: Vec<_> = groups
        .into_values()
        .map(
            |ReferenceGroup {
                 mut key,
                 count,
                 sum,
             }| {
                key.insert("count".into(), json!(count));
                // This bounded independent integer oracle remains exactly representable.
                assert!(sum.is_none_or(|sum| sum.unsigned_abs() < (1 << 53)));
                #[allow(clippy::cast_precision_loss)]
                key.insert("sum".into(), json!(sum.map(|sum| sum as f64)));
                Value::Object(key)
            },
        )
        .collect();
    canonical(&mut output);
    (output, stages)
}

fn observe(
    stage: usize,
    array: &ArrayRef,
    counts: &mut [Stage],
    retained: &mut Vec<(ArrayRef, Vec<Value>)>,
    fault: Fault,
    cancellation: &CancellationToken,
) -> Result<()> {
    counts[stage].record(array);
    if counts[stage].batches == 1 {
        let sample = array.slice(0..1).map_err(vortex_error)?;
        retained.push((sample.clone(), json_rows(&sample)));
    }
    if counts[stage].batches == 2 {
        match fault {
            Fault::Consumer(selected) if selected == stage => {
                return Err(failed("movement consumer refused batch"));
            }
            Fault::Cancel(selected) if selected == stage => cancellation.cancel(),
            _ => {}
        }
    }
    cancellation.check()
}

#[allow(clippy::too_many_lines)] // One measurement keeps construction, consumption and lifetime checks together.
fn exercise(case: Case, fault: Fault) -> Result<Report> {
    let (expected, stage_rows) = reference(case);
    let session = ResidentVortexSession::new(64 << 20, 1)?;
    let memory = session.memory().clone();
    let cancellation = CancellationToken::default();
    let first_spec = join_spec(case, true);
    let second_spec = (!case.report).then(|| join_spec(case, false));
    let aggregate_spec = aggregate_spec(case);
    let mut stages: Vec<_> = stage_rows.iter().map(|_| Stage::default()).collect();
    let mut output = Stage::default();
    let mut witnesses = Vec::new();
    let mut retained = Vec::new();
    let mut actual = Vec::new();
    let result = session.with_native_execution_context(&cancellation, |context| {
        let mut first = Join::new(&first_spec, &memory)?;
        let mut second = second_spec
            .as_ref()
            .map(|spec| Join::new(spec, &memory))
            .transpose()?;
        for (join, products) in std::iter::once((&mut first, case.products_first))
            .chain(second.as_mut().map(|join| (join, !case.products_first)))
        {
            let rows = dimensions(products, case.report);
            for rows in rows.chunks(17) {
                let keys: Vec<_> = rows.iter().map(|row| row.0).collect();
                let values: Vec<_> = rows.iter().map(|row| row.1).collect();
                let array = source(
                    &session,
                    context,
                    &[
                        MemoryColumn {
                            name: "k",
                            values: MemoryColumnValues::Int64(&keys),
                        },
                        MemoryColumn {
                            name: if products { "c" } else { "r" },
                            values: MemoryColumnValues::Int64(&values),
                        },
                    ],
                    &mut witnesses,
                )?;
                let detached = native_payload::detach(&array, context)?;
                drop(array);
                assert_eq!(witnesses.last().unwrap().strong_count(), 0);
                join.build(detached, context, case.batch_rows)?;
            }
        }
        let mut aggregate = aggregate::Aggregate::new(&aggregate_spec, &memory)?;
        for start in (0..case.rows).step_by(case.batch_rows) {
            let facts: Vec<_> = (start..(start + case.batch_rows).min(case.rows))
                .rev()
                .map(fact)
                .collect();
            let e: Vec<_> = facts.iter().map(|row| row.0).collect();
            let p: Vec<_> = facts.iter().map(|row| row.1).collect();
            let v: Vec<_> = facts.iter().map(|row| row.2).collect();
            let t: Vec<_> = facts.iter().map(|row| row.3.as_deref()).collect();
            let mut columns = vec![
                MemoryColumn {
                    name: "e",
                    values: MemoryColumnValues::Int64(&e),
                },
                MemoryColumn {
                    name: "p",
                    values: MemoryColumnValues::Int64(&p),
                },
                MemoryColumn {
                    name: "v",
                    values: MemoryColumnValues::Int64(&v),
                },
            ];
            if case.wide {
                columns.push(MemoryColumn {
                    name: "t",
                    values: MemoryColumnValues::Utf8(&t),
                });
            }
            let array = source(&session, context, &columns, &mut witnesses)?;
            let snapshot = memory.snapshot();
            let _denial = matches!(fault, Fault::Deny)
                .then(|| memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes))
                .transpose()?;
            first.consume(array, context, case.batch_rows, &mut |array| {
                observe(0, &array, &mut stages, &mut retained, fault, &cancellation)?;
                if let Some(second) = &mut second {
                    second
                        .consume(array, context, case.batch_rows, &mut |array| {
                            observe(1, &array, &mut stages, &mut retained, fault, &cancellation)?;
                            aggregate.consume(&array, context, case.batch_rows)
                        })
                        .map(|_| ())
                } else {
                    aggregate.consume(&array, context, case.batch_rows)
                }
            })?;
            assert_eq!(witnesses.last().unwrap().strong_count(), 0);
        }
        aggregate.finish(context, case.batch_rows, &mut |array| {
            output.record(&array);
            actual.extend(json_rows(&array));
            if output.batches == 1 && !array.is_empty() {
                let sample = array.slice(0..1).map_err(vortex_error)?;
                retained.push((sample.clone(), json_rows(&sample)));
            }
            Ok(())
        })?;
        Ok(())
    });
    assert!(witnesses.iter().all(|witness| witness.strong_count() == 0));
    if let Err(error) = result {
        retained.clear();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        return Err(error);
    }
    canonical(&mut actual);
    assert_eq!(actual, expected, "{case:?}");
    assert_eq!(
        stages.iter().map(|stage| stage.rows).collect::<Vec<_>>(),
        stage_rows
    );
    drop(session);
    for (array, wanted) in &retained {
        assert_eq!(&json_rows(array), wanted);
    }
    if !retained.is_empty() {
        assert!(memory.snapshot().reserved_bytes > 0);
    }
    let alias = retained
        .last()
        .map(|(array, wanted)| (array.clone(), wanted.clone()));
    drop(retained);
    if let Some((array, wanted)) = &alias {
        assert!(memory.snapshot().reserved_bytes > 0);
        assert_eq!(json_rows(array), *wanted);
    }
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut digest = String::new();
    for byte in Sha256::digest(serde_json::to_vec(&actual).unwrap()) {
        write!(digest, "{byte:02x}").unwrap();
    }
    Ok(Report {
        case,
        stages,
        aggregate_output: output,
        producer_batches_released: witnesses.len(),
        complete_result_rows: actual.len(),
        actual,
        expected,
        complete_result_sha256: digest,
        source_release_before_next_batch: true,
        retained_values_after_session_close: true,
        final_output_credits_released: true,
    })
}

fn cases() -> [Case; 4] {
    [
        Case {
            name: "star_entity_first",
            products_first: false,
            report: false,
            wide: false,
            outer: false,
            rows: 0,
            batch_rows: 1,
        },
        Case {
            name: "star_product_first",
            products_first: true,
            report: false,
            wide: false,
            outer: false,
            rows: 0,
            batch_rows: 1,
        },
        Case {
            name: "active_region_report_prefix",
            products_first: false,
            report: true,
            wide: false,
            outer: false,
            rows: 0,
            batch_rows: 1,
        },
        Case {
            name: "wide_outer_star",
            products_first: false,
            report: false,
            wide: true,
            outer: true,
            rows: 0,
            batch_rows: 1,
        },
    ]
}

#[test]
fn successive_join_output_measurements_preserve_complete_rows_and_buffer_lifetimes() {
    for base in cases() {
        for (rows, batch_rows) in [(0, 17), (17, 1), (257, 17), (8191, 1024)] {
            let report = exercise(
                Case {
                    rows,
                    batch_rows,
                    ..base
                },
                Fault::None,
            )
            .unwrap();
            println!("JOIN_MOVEMENT {}", serde_json::to_string(&report).unwrap());
        }
    }
}

#[test]
fn successive_join_consumer_failure_cancellation_and_denial_release_sources() {
    let case = Case {
        rows: 257,
        batch_rows: 17,
        ..cases()[3]
    };
    for fault in [
        Fault::Consumer(0),
        Fault::Consumer(1),
        Fault::Cancel(0),
        Fault::Cancel(1),
        Fault::Deny,
    ] {
        let error = exercise(case, fault).unwrap_err();
        let message = error.to_string();
        match fault {
            Fault::Consumer(_) => assert!(message.contains("movement consumer refused batch")),
            Fault::Cancel(_) => assert!(message.to_lowercase().contains("cancel")),
            Fault::Deny => assert!(message.contains("memory") || message.contains("Memory")),
            Fault::None => unreachable!(),
        }
        // A fresh ordinary action remains usable after every refused action.
        exercise(Case { rows: 17, ..case }, Fault::None).unwrap();
    }
}
