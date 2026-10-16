//! Nested demand failures retain neither original input nor successful completion.

use super::*;

#[test]
fn streaming_subqueries_selected_scalar_cardinality_and_expression_failures_are_not_skipped() {
    for divide in [false, true] {
        let mut plan = skipped_scalar(false, divide);
        let VortexRelationalPlan::Subquery(declaration) = &mut plan else {
            unreachable!()
        };
        declaration.evaluation_guard = None;
        let prepared = prepare_sources(&plan, 2, 8 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
        let error = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .expect("selected invalid scalar must fail")
            .to_string();
        assert!(
            error.contains(if divide {
                "division by zero"
            } else {
                "scalar subquery cardinality"
            }),
            "{error}"
        );
        assert_eq!(inputs.calls, [2, 2]);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(inputs.prior.as_ref().unwrap().strong_count(), 0);
    }
}

#[test]
fn streaming_subqueries_consumer_failure_and_cancellation_release_nested_and_shared_state() {
    for shared in [false, true] {
        let plan = VortexRelationalPlan::Subquery(Box::new(query(
            input_scan(0),
            limit(project(input_scan(usize::from(!shared)), "n"), 1),
            Kind::Scalar,
        )));
        let prepared = prepare_sources(&plan, if shared { 1 } else { 2 }, 8 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for cancel in [false, true] {
            let cancellation = CancellationToken::default();
            let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
            let mut emitted = 0;
            let error = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
                .unwrap()
                .for_each_batch(&cancellation, |array, _| {
                    if array.is_empty() {
                        return Ok(());
                    }
                    emitted += array.len();
                    if cancel {
                        cancellation.cancel();
                        Ok(())
                    } else {
                        Err(batch_input::failed("nested consumer sentinel"))
                    }
                })
                .err()
                .expect("consumer failure must prevent completion")
                .to_string();
            assert!(
                error.contains(if cancel {
                    "cancel"
                } else {
                    "nested consumer sentinel"
                }),
                "{error}"
            );
            assert!(emitted > 0);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(inputs.prior.as_ref().unwrap().strong_count(), 0);
        }
        let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected(&vec![json!(if shared { 1 } else { 2 }); 4])
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_subqueries_outer_copy_denial_precedes_inner_demand_and_releases_source() {
    let plan = VortexRelationalPlan::Subquery(Box::new(query(
        input_scan(0),
        limit(project(input_scan(1), "n"), 1),
        Kind::Scalar,
    )));
    let grant = 4 << 20;
    let prepared = prepare_sources(&plan, 2, grant).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut held_credit = None;
    let mut prior = None;
    let mut calls = 0;
    let error = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert_eq!(
                uri.as_str(),
                URIS[0],
                "copy denial must precede inner demand"
            );
            assert_eq!(ordinal, 0);
            calls += 1;
            let batch = source(session, &[Some(1)], &[Some(&"wide".repeat(1024))])?;
            prior = Some(batch.batch_release_witness()?);
            let available = grant - session.memory().snapshot().reserved_bytes;
            held_credit = Some(session.memory().reserve(available - 128)?);
            Ok(Some(batch))
        })
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .expect("outer detachment must use credited capacity")
        .to_string();
    assert!(error.contains("reservation denied"), "{error}");
    assert_eq!(calls, 1);
    assert_eq!(prior.unwrap().strong_count(), 0);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(held_credit);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
