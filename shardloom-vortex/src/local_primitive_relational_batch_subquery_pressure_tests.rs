//! Replay ownership and resident subquery state have separate resource contracts.

use super::*;

fn large_source(
    session: &ResidentVortexSession,
    ordinal: u64,
    rows: usize,
    width: usize,
) -> Result<Option<ResidentMemorySource>> {
    let start = usize::try_from(ordinal).unwrap() * 127;
    if start >= rows {
        return Ok(None);
    }
    let end = (start + 127).min(rows);
    let numbers = (start..end)
        .map(|row| Some(i64::try_from(row).unwrap()))
        .collect::<Vec<_>>();
    let strings = (start..end)
        .map(|row| format!("{row:08}{}", "p".repeat(width)))
        .collect::<Vec<_>>();
    let refs = strings
        .iter()
        .map(|value| Some(value.as_str()))
        .collect::<Vec<_>>();
    source(session, &numbers, &refs).map(Some)
}

fn spill(
    prepared: PreparedVortexRelational,
    directory: &std::path::Path,
) -> PreparedVortexRelational {
    prepared
        .with_spill(VortexRelationalSpillPolicy::new(directory, 64 << 20, 1 << 20).unwrap())
        .unwrap()
}

#[test]
fn streaming_subqueries_shared_replay_supports_nested_resident_and_spilled_readers() {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("subquery-replay");
    fs::create_dir(&directory).unwrap();
    let plan = VortexRelationalPlan::Subquery(Box::new(query(
        project(input_scan(0), "n"),
        limit(project(input_scan(0), "n"), 1),
        Kind::Scalar,
    )));
    let rows = 1025;
    for spilling in [false, true] {
        let prepared = prepare_sources(&plan, 1, 16 << 20).unwrap();
        let prepared = if spilling {
            spill(prepared, &directory)
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for execution in 1..=2 {
            let mut calls = 0;
            let mut prior: Option<Weak<MemoryLease>> = None;
            let mut delivered = 0;
            let result = prepared
                .with_batch_input(&mut |session| {
                    assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
                    let batch = large_source(session, calls, rows, 2048)?;
                    calls += 1;
                    prior = batch
                        .as_ref()
                        .map(ResidentMemorySource::batch_release_witness)
                        .transpose()?;
                    Ok(batch)
                })
                .unwrap()
                .for_each_batch(&CancellationToken::default(), |array, context| {
                    for row in values(&array, context)? {
                        assert_eq!(row, json!({"n": delivered, "answer": 0}));
                        delivered += 1;
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(delivered, rows);
            assert_eq!(calls, (rows.div_ceil(127) + 1) as u64);
            let input = result.input.as_ref().unwrap();
            assert_eq!(input.rows, rows as u64);
            assert_eq!(input.replay_sources, 1);
            assert_eq!(input.replay_prefilled_sources, 1);
            assert_eq!(input.replay_spilled_sources, u64::from(spilling));
            assert_eq!(input.replay_passes, 2);
            assert_eq!(input.replay_rows, (2 * rows) as u64);
            assert_eq!(input.subquery_outer_batches_detached, 0);
            assert_eq!(input.subquery_build_rows_detached, 1);
            assert_eq!(result.runtime.completed_executions, execution);
            assert!(result.runtime.memory.peak_reserved_bytes <= 16 << 20);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        }
    }
}

#[test]
fn streaming_subqueries_bounded_inner_aggregate_consumes_more_than_grant_without_replay() {
    let rows = 8193;
    let inner = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: input_scan(1),
        group_by: vec![],
        measures: vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "total".into(),
        )],
    }));
    let plan = VortexRelationalPlan::Subquery(Box::new(query(input_scan(0), inner, Kind::Scalar)));
    let prepared = prepare_sources(&plan, 2, 4 << 20).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut calls = [0_u64; 2];
    let mut prior: Option<Weak<MemoryLease>> = None;
    let result = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            let index = usize::from(uri.as_str() == URIS[1]);
            assert_eq!(ordinal, calls[index]);
            calls[index] += 1;
            let batch = if index == 1 {
                large_source(session, ordinal, rows, 2048)?
            } else if ordinal == 0 {
                Some(source(session, &[Some(1)], &[Some("outer")])?)
            } else {
                None
            };
            prior = batch
                .as_ref()
                .map(ResidentMemorySource::batch_release_witness)
                .transpose()?;
            Ok(batch)
        })
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![json!({"n":1,"s":"outer","answer":rows})]
    );
    let input = result.execution.input.as_ref().unwrap();
    assert!(input.input_logical_bytes > 4 * (4 << 20));
    assert_eq!(input.replay_sources, 0);
    assert_eq!(input.subquery_outer_rows_detached, 1);
    assert_eq!(input.subquery_build_rows_detached, 1);
    assert_eq!(calls, [2, (rows.div_ceil(127) + 1) as u64]);
    assert!(result.execution.runtime.memory.peak_reserved_bytes <= 4 << 20);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn streaming_subqueries_resident_membership_denial_is_not_hidden_by_replay_spill() {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("resident-state");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"keep").unwrap();
    let plan = VortexRelationalPlan::Subquery(Box::new(query(
        input_scan(0),
        input_scan(1),
        Kind::In {
            columns: vec![key("n")],
        },
    )));
    for spilling in [false, true] {
        let prepared = prepare_sources(&plan, 2, 4 << 20).unwrap();
        let prepared = if spilling {
            spill(prepared, &directory)
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = [0_u64; 2];
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                let index = usize::from(uri.as_str() == URIS[1]);
                assert_eq!(ordinal, calls[index]);
                calls[index] += 1;
                large_source(session, ordinal, 8193, 2048)
            })
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| {
                panic!("eager build denial must precede output")
            });
        let error = result
            .err()
            .expect("resident membership must exceed its grant")
            .to_string();
        assert!(error.contains("reservation denied"), "{error}");
        assert_eq!(calls[0], 0, "inner build is eager");
        assert!(calls[1] > 1 && calls[1] < 8193_usize.div_ceil(127) as u64);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
        let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected(&[Value::Null, json!(true), Value::Null, Value::Null])
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
