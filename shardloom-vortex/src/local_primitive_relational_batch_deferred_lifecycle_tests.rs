//! Declaration references retain their metadata credits and execution identity.

use super::*;

#[test]
fn streaming_dynamic_subqueries_reject_misplaced_reused_and_unconsumed_declarations() {
    for mode in ["misplaced", "reused", "unconsumed"] {
        let prepared = prepare_streams(2, move |schemas| {
            let relation = schemas.defer_subquery_with_sources(
                &[DatasetUri::new(URIS[1])?],
                65_536,
                |_| panic!("invalid declaration must fail before per-parameter lowering"),
            )?;
            match mode {
                "misplaced" => Ok(relation),
                "unconsumed" => Ok(scan(0)),
                _ => {
                    let first = VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                        relation.clone(),
                        Kind::Exists,
                    )));
                    let mut second = query(relation, Kind::Exists);
                    second.input = first;
                    second.output_column = "second_answer".into();
                    Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(second)))
                }
            }
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let error = prepared
            .with_batch_inputs(&mut |_, _, _| panic!("invalid declarations must precede demand"))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap()
            .to_string();
        let message = match mode {
            "misplaced" => "require a correlated subquery relation",
            "reused" => "consumed twice",
            _ => "unconsumed",
        };
        assert!(error.contains(message), "{mode}: {error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_retained_reference_keeps_credits_and_cannot_cross_executions() {
    let retained = Arc::new(Mutex::new(None::<VortexRelationalPlan>));
    let held = retained.clone();
    let calls = AtomicUsize::new(0);
    let prepared = prepare_streams(2, move |schemas| {
        let call = calls.fetch_add(1, Ordering::SeqCst);
        let relation = if call == 1 {
            held.lock().unwrap().as_ref().unwrap().clone()
        } else {
            let relation = schemas.defer_subquery_with_sources(
                &[DatasetUri::new(URIS[1])?],
                65_536,
                |schemas| schemas.resolve_output(&inner(1)).map(|r| r.0),
            )?;
            if call == 0 {
                *held.lock().unwrap() = Some(relation.clone());
            }
            relation
        };
        Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
            relation,
            Kind::Exists,
        ))))
    });
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let (result, calls) = collect(&prepared);
    assert_eq!(json_rows(&result), expected(true));
    assert_eq!(calls, [4, 4]);
    drop(result);
    let retained_bytes = prepared.snapshot().memory.reserved_bytes;
    assert!(
        retained_bytes > baseline,
        "retained source identities must retain their credits"
    );
    let error = prepared
        .with_batch_inputs(&mut |_, _, _| panic!("foreign references must precede source demand"))
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("different execution"), "{error}");
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, retained_bytes);
    drop(retained.lock().unwrap().take());
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let (result, _) = collect(&prepared);
    assert_eq!(json_rows(&result), expected(true));
    assert_eq!(result.execution.runtime.completed_executions, 2);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn streaming_dynamic_subqueries_reject_replay_declared_after_unrecorded_discovery() {
    let prepared = prepare_streams(1, |schemas| {
        let (outer, _) = schemas.resolve_output(&pivot(scan(0)))?;
        let relation =
            schemas.defer_subquery_with_sources(&[DatasetUri::new(URIS[0])?], 65_536, |_| {
                panic!("late replay declaration must fail before inner lowering")
            })?;
        let mut query = query(relation, Kind::Exists);
        query.input = outer;
        Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query)))
    });
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut calls = 0;
    let error = prepared
        .with_batch_input(&mut |session| {
            let batch = next(session, calls, 1)?;
            calls += 1;
            Ok(batch)
        })
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("source reuse must be declared before"),
        "{error}"
    );
    assert_eq!(calls, 4);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
