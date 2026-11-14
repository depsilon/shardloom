use super::*;

#[test]
#[allow(clippy::too_many_lines)] // One full-value matrix over every keyed join kind and condition.
fn adaptive_join_all_keyed_kinds_keep_complete_values_nulls_and_hot_duplicate_order() {
    let right_keys = (0..128)
        .map(Some)
        .chain([Some(u64::MAX)])
        .chain(std::iter::repeat_n(Some(7), 69))
        .chain([None])
        .collect::<Vec<_>>();
    let left_keys = [Some(7), Some(100), Some(u64::MAX), None]
        .into_iter()
        .chain((10_000..10_512).map(Some))
        .collect::<Vec<_>>();
    for condition in [false, true] {
        for kind in [
            Kind::Inner,
            Kind::Left,
            Kind::Right,
            Kind::Full,
            Kind::LeftSemi,
            Kind::LeftAnti,
        ] {
            let mut binding = spec(kind);
            if condition {
                binding.condition = Some(Condition {
                    expression: Expression::new(
                        DType::Bool(Nullability::NonNullable),
                        super::super::super::native_relational_expression::Kind::Literal(
                            true.into(),
                        ),
                    ),
                    fields: vec![],
                    columns: vec![],
                });
            }
            let mut expected = Vec::new();
            let mut right_matched = vec![false; right_keys.len()];
            for (left, key) in left_keys.iter().enumerate() {
                let matches = right_keys
                    .iter()
                    .enumerate()
                    .filter(|(_, right)| key.is_some() && key == *right)
                    .map(|(right, _)| right)
                    .collect::<Vec<_>>();
                if matches!(kind, Kind::LeftSemi | Kind::LeftAnti) {
                    if matches.is_empty() == (kind == Kind::LeftAnti) {
                        expected.push((Some(left as u64), None));
                    }
                } else {
                    for right in &matches {
                        right_matched[*right] = true;
                        expected.push((Some(left as u64), Some(*right as u64)));
                    }
                    if matches.is_empty() && matches!(kind, Kind::Left | Kind::Full) {
                        expected.push((Some(left as u64), None));
                    }
                }
            }
            if matches!(kind, Kind::Right | Kind::Full) {
                expected.extend(
                    right_matched
                        .iter()
                        .enumerate()
                        .filter(|(_, matched)| !**matched)
                        .map(|(row, _)| (None, Some(row as u64))),
                );
            }
            let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let mut retained = Vec::new();
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut join = Join::new(&binding, context.memory())?;
                    for (batch, keys) in right_keys.chunks(17).enumerate() {
                        let start = batch * 17;
                        join.build(
                            input(
                                keys,
                                &(u32::try_from(start).unwrap()
                                    ..u32::try_from(start + keys.len()).unwrap())
                                    .collect::<Vec<_>>(),
                            ),
                            context,
                            17,
                        )?;
                    }
                    join.finish_build(context)?;
                    assert!(join.build(input(&[], &[]), context, 17).is_err());
                    let mut output = |array| {
                        retained.push(array);
                        Ok(())
                    };
                    for (batch, keys) in left_keys.chunks(31).enumerate() {
                        let start = batch * 31;
                        join.consume(
                            input(
                                keys,
                                &(u32::try_from(start).unwrap()
                                    ..u32::try_from(start + keys.len()).unwrap())
                                    .collect::<Vec<_>>(),
                            ),
                            context,
                            19,
                            &mut output,
                        )?;
                    }
                    join.finish(context, 19, &mut output)?;
                    let work = join.join_work()?;
                    assert_eq!(work.runtime_filter.admitted, 1);
                    assert_eq!(work.runtime_filter.build_keys, 129);
                    assert_eq!(work.runtime_filter.probe_rows, 515);
                    assert!(work.runtime_filter.rejected_rows > 450);
                    let ranges = !matches!(kind, Kind::LeftSemi | Kind::LeftAnti) || condition;
                    assert_eq!(work.skew.admitted_groups, u64::from(ranges));
                    assert_eq!(work.skew.range_visits, u64::from(ranges));
                    if ranges {
                        assert!(work.skew.rows_visited > 0);
                    }
                    Ok(())
                })
                .unwrap();
            drop(session);
            assert_eq!(
                retained.iter().flat_map(pairs).collect::<Vec<_>>(),
                expected,
                "{kind:?} condition={condition}"
            );
            drop(retained);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn adaptive_join_keeps_full_key_validation_cancellation_and_terminal_consumer_errors() {
    for failure in [0, 1, 2] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let cancellation = CancellationToken::default();
        let mut calls = 0;
        let result = session.with_native_execution_context(&cancellation, |context| {
            let mut binding = spec(Kind::Left);
            if failure == 0 {
                binding.left_keys.push("bad".into());
                binding.right_keys.push("bad".into());
            }
            let array = |keys: &[Option<u64>], bad: f64| {
                let ordinary = input(keys, &vec![0; keys.len()]);
                if failure != 0 {
                    return ordinary;
                }
                StructArray::try_new(
                    FieldNames::from(["key", "id", "bad"]),
                    vec![
                        logical_field_from_native_array(&ordinary, "key").unwrap(),
                        logical_field_from_native_array(&ordinary, "id").unwrap(),
                        PrimitiveArray::from_iter(std::iter::repeat_n(bad, keys.len()))
                            .into_array(),
                    ],
                    keys.len(),
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array()
            };
            let mut join = Join::new(&binding, context.memory())?;
            join.build(
                array(&(0..128).map(Some).collect::<Vec<_>>(), 0.0),
                context,
                16,
            )?;
            join.finish_build(context)?;
            assert_eq!(join.join_work()?.runtime_filter.admitted, 1);
            if failure == 1 {
                cancellation.cancel();
            }
            join.consume(array(&[Some(10_000)], f64::NAN), context, 1, &mut |_| {
                calls += 1;
                Err(failed("intentional consumer failure"))
            })
        });
        assert!(result.is_err());
        assert_eq!(calls, usize::from(failure == 2));
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
