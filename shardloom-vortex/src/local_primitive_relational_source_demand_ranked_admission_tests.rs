//! Optional ranked setup cannot consume the ordinary source on denial.
use super::*;
use crate::local_primitives::prepared_relational::batch_input::{Execution, Input};

#[test]
fn ranked_admission_declines_before_source_work_and_preserves_native_execution() {
    for (groups, names, predicate, eligible) in [
        (397, vec![], None, false),
        (7, vec!["id"], None, false),
        (7, vec!["id", "tie"], Some(equal("tie", 1)), false),
        (7, vec![], None, true),
    ] {
        let fixture = fixture(0, 397, groups);
        let filtered = predicate.is_some();
        let plan = limit(
            sorted(
                fixture.scan(&names, predicate),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            2,
        );
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let PreparedRoot::Bound(root) = &prepared.root else {
            panic!("fixture requires an ordinary bound query");
        };
        let NodeKind::Limit { input: sort, .. } = &root.kind else {
            panic!("fixture requires a bounded sort");
        };
        prepared
            .session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let input = Execution::new(&prepared, None)?;
                let metrics = Metrics {
                    input: Some(&input),
                    ..Metrics::default()
                };
                let memory = context.memory().snapshot();
                let pressure = context
                    .memory()
                    .reserve(memory.limit_bytes - memory.reserved_bytes)?;
                let before = context.memory().snapshot();
                assert!(!prepared.run_ranked_sort(
                    sort,
                    context,
                    &metrics,
                    7,
                    None,
                    &mut |_| panic!("declined admission must not emit output"),
                )?);
                let after = context.memory().snapshot();
                assert_eq!(after.reserved_bytes, before.reserved_bytes);
                assert_eq!(
                    after.denied_reservations - before.denied_reservations,
                    u64::from(eligible),
                    "ineligible inputs must not even request optional metadata",
                );
                drop(pressure);
                // The exact same once-only producer is still available. This
                // executes the ordinary native read, not a newly opened query.
                let mut rows = 0;
                input.run(0, context, &metrics, &mut |array| {
                    rows += array.len();
                    Ok(())
                })?;
                assert_eq!(rows, 397);
                let report = input.completed(context, &metrics)?;
                assert_eq!(report.ranked_sources, 0);
                assert!(report.end_of_input_observed);
                Ok(())
            })
            .unwrap();
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let ids = if filtered { [1, 4] } else { [0, 1] };
        let expected: Vec<_> = ids
            .into_iter()
            .map(|id| {
                let mut value = expected(id);
                if !names.is_empty() {
                    value
                        .as_object_mut()
                        .unwrap()
                        .retain(|name, _| names.contains(&name.as_str()));
                }
                value
            })
            .collect();
        assert_eq!(json_rows(&result), expected);
        assert_eq!(
            result.execution.input.as_ref().unwrap().ranked_sources,
            u64::from(eligible)
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
}

#[test]
fn ranked_admission_adapter_budget_refusal_preserves_validation_and_cancellation() {
    let fixture = fixture(0, 397, 7);
    let prepared = fixture.prepare(&fixture.scan(&[], None));
    let source = prepared.batch_sources[0].local.as_ref().unwrap();
    let token = CancellationToken::default();
    let initial = prepared.snapshot().memory.reserved_bytes;
    let cancelled = prepared
        .session
        .with_native_execution_context(&token, |context| {
            let memory = context.memory().snapshot();
            let pressure = context
                .memory()
                .reserve(memory.limit_bytes - memory.reserved_bytes)?;
            assert!(
                source
                    .prepare_ranking(&["id".into()], 2, context)?
                    .is_none()
            );
            let invalid = source.prepare_ranking(&["missing".into()], 2, context);
            assert!(
                invalid
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("unadmitted source column")
            );
            token.cancel();
            assert!(source.prepare_ranking(&["id".into()], 2, context).is_err());
            drop(pressure);
            Ok(())
        })
        .expect_err("cancellation also prevents context completion");
    assert!(cancelled.to_string().contains("execution cancelled"));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    prepared
        .session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            assert!(
                source
                    .prepare_ranking(&["id".into()], 2, context)?
                    .is_some()
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}
