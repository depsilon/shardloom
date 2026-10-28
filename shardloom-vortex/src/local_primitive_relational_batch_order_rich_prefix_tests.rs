//! Rich prefix payloads remain independently owned across producer boundaries.

use super::*;
use serde_json::{Value as Json, json};

fn rich_source(session: &ResidentVortexSession, rows: &[Json]) -> Result<ResidentMemorySource> {
    let ids = rows
        .iter()
        .map(|row| row["id"].as_i64())
        .collect::<Vec<_>>();
    let keys = rows
        .iter()
        .map(|row| row["key"].as_i64())
        .collect::<Vec<_>>();
    let amount = rows
        .iter()
        .map(|row| row["amount"].to_string())
        .collect::<Vec<_>>();
    let amounts = amount
        .iter()
        .map(|value| Some(value.as_str()))
        .collect::<Vec<_>>();
    let payload = rows
        .iter()
        .map(|row| row["payload"].to_string())
        .collect::<Vec<_>>();
    let payloads = payload
        .iter()
        .map(|value| Some(value.as_str()))
        .collect::<Vec<_>>();
    let decimal = r#"{"Decimal":[{"precision":18,"scale":3},true]}"#;
    let rich = r#"{"Struct":[{"names":["items","pair","label"],"dtypes":[{"List":[{"Utf8":true},true]},{"FixedSizeList":[{"Primitive":["i16",false]},2,true]},{"Utf8":false}]},true]}"#;
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "id",
                values: MemoryColumnValues::Int64(&ids),
            },
            MemoryColumn {
                name: "key",
                values: MemoryColumnValues::Int64(&keys),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::TypedJson {
                    dtype_json: decimal,
                    values: &amounts,
                },
            },
            MemoryColumn {
                name: "payload",
                values: MemoryColumnValues::TypedJson {
                    dtype_json: rich,
                    values: &payloads,
                },
            },
        ],
    )
}

fn rich_rows(count: usize, text: &str) -> Vec<Json> {
    (0..count)
        .map(|id| {
            let key = if id % 13 == 0 {
                None
            } else {
                Some(i64::try_from(id * 37 % 41).unwrap() - 20)
            };
            let amount = (id % 17 != 0)
                .then(|| format!("decimal128(18,3):{}", i64::try_from(id).unwrap() * 29 - 901));
            let items = if id % 5 == 0 {
                vec![]
            } else {
                vec![json!(text), Json::Null, json!(id.to_string())]
            };
            let pair = if id % 7 == 0 {
                Json::Null
            } else {
                json!([-32768, id % 1000])
            };
            let payload = if id % 11 == 0 {
                Json::Null
            } else {
                json!({"items": items, "pair": pair, "label":"λ\0猫"})
            };
            json!({"id":id, "key":key, "amount":amount, "payload":payload})
        })
        .collect()
}

fn prepare_rich(plan: &VortexRelationalPlan, grant: u64) -> PreparedVortexRelational {
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = grant;
    prepare_relational_with_schema(resources, |preparation| {
        preparation.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
            rich_source(session, &[])
        })?;
        Ok(plan.clone())
    })
    .unwrap()
}

fn assert_ordering_certificate(execution: &ExecutedVortexRelational) {
    let report = execution.input.as_ref().unwrap();
    for (name, value) in [
        (
            "payload_buffer_bytes_copied",
            report.ordering_payload_buffer_bytes_copied,
        ),
        ("prefix_batches", report.ordering_prefix_batches),
        (
            "prefix_rows_validated",
            report.ordering_prefix_rows_validated,
        ),
        (
            "prefix_peak_retained_buffer_bytes",
            report.ordering_prefix_peak_retained_buffer_bytes,
        ),
    ] {
        assert!(
            execution
                .native_io_certificate
                .source_pushdown_report
                .proof_basis
                .split_whitespace()
                .any(|field| field == format!("streaming_ordering_{name}={value}"))
        );
    }
}

#[test]
fn streamed_rich_prefix_keeps_stable_offsets_releases_sources_and_retains_child_ownership() {
    let rows = rich_rows(1025, &"λ\"\n".repeat(32));
    let mut expected = rows.clone();
    expected
        .sort_by(|a, b| reference(a["key"].as_i64(), b["key"].as_i64(), false, NullOrder::Last));
    for batch in [1, 17, 257] {
        let prepared = prepare_rich(
            &limited(
                sorted(scan(), vec![key("key", false, NullOrder::Last)]),
                3,
                7,
            ),
            8 << 20,
        );
        let memory = prepared.session.memory().clone();
        let mut chunks = std::iter::once(&rows[..0]).chain(rows.chunks(batch));
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut input = |session: &ResidentVortexSession| {
            assert!(prior.as_ref().is_none_or(|w| w.strong_count() == 0));
            chunks
                .next()
                .map(|rows| {
                    let source = rich_source(session, rows)?;
                    prior = Some(source.batch_release_witness()?);
                    Ok(source)
                })
                .transpose()
        };
        let mut actual = Vec::new();
        let mut child = None;
        let execution = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let mut sink = crate::local_primitives::collect::JsonRows::new(
                    context.memory(),
                    1 << 20,
                    false,
                )?;
                sink.append_native(&array, context)?;
                actual.extend(serde_json::from_str::<Vec<Json>>(sink.finish()?.value()).unwrap());
                child = Some(crate::local_primitives::logical_field_from_native_array(
                    &array, "payload",
                )?);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected[3..10]);
        let report = execution.input.as_ref().unwrap();
        assert!(report.end_of_input_observed && report.output_ownership_detached);
        assert_eq!(report.max_retained_input_batches, 1);
        assert_eq!(report.ordering_prefix_rows_validated, 1025);
        assert!(report.ordering_prefix_batches >= rows.len().div_ceil(batch) as u64);
        assert!(report.ordering_payload_buffer_bytes_copied > report.ordering_rows_detached * 32);
        assert!(report.ordering_prefix_peak_retained_buffer_bytes > 0);
        assert!(report.ordering_prefix_peak_retained_buffer_bytes < 32 << 10);
        assert_ordering_certificate(&execution);
        if batch == 257 {
            assert!(report.ordering_rows_detached < 1025 / 8);
            assert!(report.ordering_payload_buffer_bytes_copied < report.input_logical_bytes / 4);
        }
        drop(execution);
        drop(prepared);
        assert!(prior.as_ref().is_none_or(|w| w.strong_count() == 0));
        assert!(memory.snapshot().reserved_bytes > 0);
        let child = child.unwrap();
        assert_eq!(child.len(), 7);
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let record = StructArray::new(
                    FieldNames::from(["payload"]),
                    vec![child.clone()],
                    child.len(),
                    Validity::NonNullable,
                )
                .into_array();
                let mut sink = crate::local_primitives::collect::JsonRows::new(
                    context.memory(),
                    1 << 20,
                    false,
                )?;
                sink.append_native(&record, context)?;
                let values = serde_json::from_str::<Vec<Json>>(sink.finish()?.value()).unwrap();
                assert_eq!(
                    values,
                    expected[3..10]
                        .iter()
                        .map(|row| json!({"payload":row["payload"]}))
                        .collect::<Vec<_>>()
                );
                Ok(())
            })
            .unwrap();
        drop(child);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn streamed_rich_prefix_zero_demand_empty_input_and_large_cap_keep_their_contracts() {
    let rows = rich_rows(129, "payload");
    let mut expected = rows.clone();
    expected
        .sort_by(|a, b| reference(a["key"].as_i64(), b["key"].as_i64(), false, NullOrder::Last));
    for (offset, count) in [(0, 0), (0, 1), (121, 30), (0, 16_385)] {
        for empty in [false, true] {
            let prepared = prepare_rich(
                &limited(
                    sorted(scan(), vec![key("key", false, NullOrder::Last)]),
                    offset,
                    count,
                ),
                8 << 20,
            );
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let values = if empty { &rows[..0] } else { &rows[..] };
            let mut chunks = std::iter::once(&values[..0]).chain(values.chunks(17));
            let mut input = |session: &ResidentVortexSession| {
                chunks
                    .next()
                    .map(|chunk| rich_source(session, chunk))
                    .transpose()
            };
            let result = prepared
                .with_batch_input(&mut input)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let wanted = if empty {
                vec![]
            } else {
                expected
                    .iter()
                    .skip(offset)
                    .take(count)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            assert_eq!(json_rows(&result), wanted);
            let report = result.execution.input.as_ref().unwrap();
            assert!(report.end_of_input_observed && report.output_ownership_detached);
            assert_eq!(report.rows, values.len() as u64);
            if count == 16_385 {
                assert_eq!(report.ordering_prefix_batches, 0);
                assert_eq!(report.ordering_prefix_peak_retained_buffer_bytes, 0);
                assert_eq!(report.ordering_rows_detached, values.len() as u64);
            } else {
                assert_eq!(report.ordering_prefix_rows_validated, values.len() as u64);
                if count == 0 {
                    assert_eq!(report.ordering_rows_detached, 0);
                    assert_eq!(report.ordering_prefix_peak_retained_buffer_bytes, 0);
                }
            }
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn streamed_rich_prefix_failure_cleanup_allows_fresh_retry() {
    let rows = rich_rows(65, &"λ".repeat(16));
    let mut expected = rows.clone();
    expected
        .sort_by(|a, b| reference(a["key"].as_i64(), b["key"].as_i64(), false, NullOrder::Last));
    for fault in ["denied", "cancel", "consumer"] {
        let prepared = prepare_rich(
            &limited(
                sorted(scan(), vec![key("key", false, NullOrder::Last)]),
                0,
                7,
            ),
            4 << 20,
        );
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let mut held = None;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls == 4 {
                return Ok(None);
            }
            let source = rich_source(session, &rows)?;
            if calls == 2 {
                if fault == "denied" {
                    let snapshot = memory.snapshot();
                    held = Some(memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?);
                } else if fault == "cancel" {
                    token.cancel();
                }
            }
            Ok(Some(source))
        };
        let mut emitted = 0;
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&token, |_, _| {
                emitted += 1;
                Err(batch_input::failed("rich prefix consumer failure"))
            })
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains(match fault {
                "denied" => "reservation denied",
                "cancel" => "cancel",
                _ => "rich prefix consumer failure",
            }),
            "{error}"
        );
        assert_eq!(emitted, usize::from(fault == "consumer"));
        assert_eq!(calls, if fault == "consumer" { 4 } else { 2 });
        assert_eq!(prepared.snapshot().completed_executions, 0);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        let mut once = true;
        let mut input = |session: &ResidentVortexSession| {
            if std::mem::take(&mut once) {
                rich_source(session, &rows).map(Some)
            } else {
                Ok(None)
            }
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected[..7]);
        drop(result);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
}

#[test]
fn streamed_rich_prefix_handles_a_population_larger_than_its_grant() {
    // The typed JSON producer reserves conversion workspace before delivering
    // a native batch. Keep each producer admission below the same small grant;
    // ordering must still drain a population much larger than that grant.
    let rows = rich_rows(17, &"λ".repeat(256));
    let mut expected = rows.clone();
    expected
        .sort_by(|a, b| reference(a["key"].as_i64(), b["key"].as_i64(), false, NullOrder::Last));
    let prepared = prepare_rich(
        &limited(
            sorted(scan(), vec![key("key", false, NullOrder::Last)]),
            0,
            7,
        ),
        4 << 20,
    );
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut calls = 0;
    let mut prior: Option<Weak<MemoryLease>> = None;
    let mut input = |session: &ResidentVortexSession| {
        assert!(prior.as_ref().is_none_or(|w| w.strong_count() == 0));
        calls += 1;
        if calls > 4097 {
            return Ok(None);
        }
        let source = rich_source(session, &rows)?;
        prior = Some(source.batch_release_witness()?);
        Ok(Some(source))
    };
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    // Every batch repeats these rows. Its smallest nonnull key occurs once,
    // so the complete stable prefix contains that row from the first 7 batches.
    assert_eq!(json_rows(&result), vec![expected[0].clone(); 7]);
    let report = result.execution.input.as_ref().unwrap();
    assert!(report.end_of_input_observed);
    assert_eq!(report.rows, 4097 * 17);
    assert_eq!(report.ordering_prefix_rows_validated, report.rows);
    assert!(report.input_logical_bytes > 4 * (4 << 20));
    // Tiny batches retain pending owned rows between compactions. They need
    // not reduce copy traffic, but their retained payload remains bounded.
    assert!(report.ordering_rows_detached <= report.rows * 2);
    assert!(report.ordering_prefix_peak_retained_buffer_bytes < 32 << 10);
    assert!(result.execution.runtime.memory.peak_reserved_bytes <= 4 << 20);
    assert!(result.execution.spill.is_none());
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
