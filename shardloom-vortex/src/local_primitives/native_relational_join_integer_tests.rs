use super::super::super::native_relational_join_cost::{self as cost, Phase};
use super::*;
use vortex::array::arrays::{ChunkedArray, DictArray};

fn signed_spec(kind: Kind) -> Spec {
    let mut binding = spec(kind);
    if matches!(kind, Kind::LeftSemi | Kind::LeftAnti) {
        binding.right_key_fields[0].1 = DType::Primitive(PType::I64, Nullability::Nullable);
    } else {
        binding.right_key_fields.clear();
    }
    binding
}

fn signed_input(keys: &[Option<i64>], first_id: usize, encoding: usize) -> ArrayRef {
    let primitive =
        |keys: &[Option<i64>]| PrimitiveArray::from_option_iter(keys.iter().copied()).into_array();
    let key = match encoding {
        0 => primitive(keys),
        1 => DictArray::try_new(
            PrimitiveArray::from_iter((0..u32::try_from(keys.len()).unwrap()).rev()).into_array(),
            PrimitiveArray::from_option_iter(keys.iter().rev().copied()).into_array(),
        )
        .unwrap()
        .into_array(),
        2 => ChunkedArray::try_new(
            vec![
                primitive(&keys[..keys.len() / 2]),
                primitive(&keys[keys.len() / 2..]),
            ],
            DType::Primitive(PType::I64, Nullability::Nullable),
        )
        .unwrap()
        .into_array(),
        _ => panic!("unknown fixture encoding"),
    };
    StructArray::new(
        FieldNames::from(["key", "id"]),
        vec![
            key,
            PrimitiveArray::from_iter(
                (first_id..first_id + keys.len()).map(|id| u32::try_from(id).unwrap()),
            )
            .into_array(),
        ],
        keys.len(),
        Validity::NonNullable,
    )
    .into_array()
}

fn reference(left: &[Option<i64>], right: &[Option<i64>], kind: Kind) -> Vec<Pair> {
    let mut result = Vec::new();
    let mut right_matched = vec![false; right.len()];
    for (left_row, left_key) in left.iter().enumerate() {
        let mut matched = false;
        for (right_row, right_key) in right.iter().enumerate() {
            if kind == Kind::Cross || (left_key.is_some() && left_key == right_key) {
                matched = true;
                right_matched[right_row] = true;
                if !matches!(kind, Kind::LeftSemi | Kind::LeftAnti) {
                    result.push((Some(left_row as u64), Some(right_row as u64)));
                }
            }
        }
        if (matched && kind == Kind::LeftSemi)
            || (!matched && matches!(kind, Kind::Left | Kind::Full | Kind::LeftAnti))
        {
            result.push((Some(left_row as u64), None));
        }
    }
    if matches!(kind, Kind::Right | Kind::Full) {
        result.extend(
            right_matched
                .iter()
                .enumerate()
                .filter(|(_, matched)| !**matched)
                .map(|(row, _)| (None, Some(row as u64))),
        );
    }
    result
}

#[test]
fn dense_integer_join_ordinary_kinds_preserve_complete_order_across_encodings_and_extremes() {
    for minimum in [i64::MIN, -128, i64::MAX - 254] {
        let right = (0..128)
            .map(|key| Some(minimum + key * 2))
            .chain(std::iter::repeat_n(Some(minimum + 14), 69))
            .chain([None])
            .collect::<Vec<_>>();
        let outside = if minimum == i64::MIN {
            i64::MAX
        } else {
            i64::MIN
        };
        let left = [
            Some(minimum + 14),
            Some(minimum + 33),
            Some(minimum + 254),
            None,
            Some(outside),
            Some(minimum),
        ];
        for encoding in 0..3 {
            for kind in [Kind::Inner, Kind::Left, Kind::Right, Kind::Full] {
                let binding = signed_spec(kind);
                let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
                let memory = session.memory().clone();
                let observation = cost::Session::start();
                let node = cost::node("ordinary", 1, false, false);
                let mut retained = Vec::new();
                session
                    .with_native_execution_context(&CancellationToken::default(), |context| {
                        let mut join = Join::new(&binding, context.memory())?;
                        for (batch, keys) in right.chunks(17).enumerate() {
                            join.build(signed_input(keys, batch * 17, encoding), context, 19)?;
                        }
                        join.finish_build(context)?;
                        let scope = node.scope(Phase::ProbeMatch);
                        // An empty callback must not spend or freeze optional admission.
                        join.consume(signed_input(&[], 0, 0), context, 19, &mut |_| {
                            panic!("empty")
                        })?;
                        for (batch, keys) in left.chunks(2).enumerate() {
                            join.consume(
                                signed_input(keys, batch * 2, encoding),
                                context,
                                19,
                                &mut |array| {
                                    assert!(array.len() <= 19);
                                    assert_eq!(
                                        array.dtype(),
                                        &DType::struct_(
                                            binding.fields.clone(),
                                            Nullability::NonNullable
                                        )
                                    );
                                    retained.push(array);
                                    Ok(())
                                },
                            )?;
                        }
                        drop(scope);
                        join.finish(context, 19, &mut |array| {
                            retained.push(array);
                            Ok(())
                        })?;
                        let work = join.join_work()?;
                        assert_eq!(work.runtime_filter.admitted, 1);
                        assert_eq!(work.runtime_filter.probe_rows, 0);
                        assert_eq!(work.skew.range_visits, 1);
                        Ok(())
                    })
                    .unwrap();
                let report = observation.finish();
                let work = report.integer_lookup_work[0];
                assert_eq!((work.considered, work.admitted), (1, 1));
                assert_eq!(
                    (work.build_keys, work.directory_slots, work.retained_bytes),
                    (128, 255, 255 * size_of::<usize>() as u64)
                );
                assert_eq!(
                    (
                        work.probe_batches,
                        work.probe_rows,
                        work.null_rows,
                        work.matched_rows
                    ),
                    (3, 6, 1, 3)
                );
                drop(session);
                assert_eq!(
                    retained.iter().flat_map(pairs).collect::<Vec<_>>(),
                    reference(&left, &right, kind),
                    "{kind:?}, encoding={encoding}, minimum={minimum}"
                );
                drop(retained);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                assert_eq!(memory.snapshot().denied_reservations, 0);
            }
        }
    }
}

#[test]
fn dense_integer_join_size_and_density_boundaries_keep_exact_answers_without_reconsideration() {
    let cases = [
        (vec![], "small"),
        (vec![None; 256], "small"),
        ((0..127).map(Some).collect(), "small"),
        ((0..128).map(Some).collect(), "admitted"),
        ((0..127).map(Some).chain([Some(255)]).collect(), "admitted"),
        ((0..127).map(Some).chain([Some(256)]).collect(), "range"),
        (
            (0..126)
                .map(Some)
                .chain([Some(i64::MIN), Some(i64::MAX)])
                .collect(),
            "range",
        ),
    ];
    for (right, decision) in cases {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let observation = cost::Session::start();
        let node = cost::node("left", 1, false, false);
        let left = [Some(0), Some(128), None];
        let mut observed = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let binding = signed_spec(Kind::Left);
                let mut join = Join::new(&binding, context.memory())?;
                join.build(signed_input(&right, 0, 0), context, 7)?;
                let _scope = node.scope(Phase::ProbeMatch);
                for (row, key) in left.iter().enumerate() {
                    join.consume(signed_input(&[*key], row, 0), context, 7, &mut |array| {
                        observed.extend(pairs(&array));
                        Ok(())
                    })?;
                }
                Ok(())
            })
            .unwrap();
        let work = observation.finish().integer_lookup_work[0];
        assert_eq!(work.considered, 1);
        assert_eq!(work.admitted, u64::from(decision == "admitted"));
        assert_eq!(work.declined_small, u64::from(decision == "small"));
        assert_eq!(work.declined_range, u64::from(decision == "range"));
        assert_eq!(observed, reference(&left, &right, Kind::Left));
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dense_integer_join_other_shapes_keep_existing_native_paths() {
    for shape in [
        "semi",
        "anti",
        "cross",
        "condition",
        "composite",
        "u64",
        "i32",
    ] {
        let kind = match shape {
            "semi" => Kind::LeftSemi,
            "anti" => Kind::LeftAnti,
            "cross" => Kind::Cross,
            _ => Kind::Inner,
        };
        let mut binding = signed_spec(kind);
        if shape == "condition" {
            binding.condition = Some(Condition {
                expression: Expression::new(
                    DType::Bool(Nullability::NonNullable),
                    super::super::super::native_relational_expression::Kind::Literal(true.into()),
                ),
                fields: vec![],
                columns: vec![],
            });
        }
        if shape == "composite" {
            binding.left_keys.push("id".into());
            binding.right_keys.push("id".into());
        }
        let make_input = |keys: &[Option<i64>], first: usize| {
            if shape == "u64" {
                return input(
                    &keys
                        .iter()
                        .map(|key| key.map(|key| u64::try_from(key).unwrap()))
                        .collect::<Vec<_>>(),
                    &(first..first + keys.len())
                        .map(|id| u32::try_from(id).unwrap())
                        .collect::<Vec<_>>(),
                );
            }
            if shape == "i32" {
                return StructArray::new(
                    FieldNames::from(["key", "id"]),
                    vec![
                        PrimitiveArray::from_option_iter(
                            keys.iter()
                                .map(|key| key.map(|key| i32::try_from(key).unwrap())),
                        )
                        .into_array(),
                        PrimitiveArray::from_iter(
                            (first..first + keys.len()).map(|id| u32::try_from(id).unwrap()),
                        )
                        .into_array(),
                    ],
                    keys.len(),
                    Validity::NonNullable,
                )
                .into_array();
            }
            signed_input(keys, first, 0)
        };
        let right = (0..128).map(Some).collect::<Vec<_>>();
        let left = [Some(0), Some(1)];
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let observation = cost::Session::start();
        let node = cost::node(shape, binding.left_keys.len(), false, shape == "condition");
        let mut observed = Vec::new();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut join = Join::new(&binding, context.memory())?;
                join.build(make_input(&right, 0), context, 7)?;
                let _scope = node.scope(Phase::ProbeMatch);
                join.consume(make_input(&left, 0), context, 7, &mut |array| {
                    observed.extend(pairs(&array));
                    Ok(())
                })?;
                Ok(())
            })
            .unwrap();
        let work = observation.finish().integer_lookup_work[0];
        assert_eq!(work.admitted, 0, "{shape}");
        assert_eq!(
            work.declined_shape,
            u64::from(shape != "condition"),
            "{shape}"
        );
        assert_eq!(observed, reference(&left, &right, kind), "{shape}");
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dense_integer_join_memory_decline_keeps_results_and_does_not_retry_after_grant_growth() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let observation = cost::Session::start();
    let node = cost::node("inner", 1, false, false);
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let binding = signed_spec(Kind::Inner);
            let mut join = Join::new(&binding, context.memory())?;
            join.build(
                signed_input(&(0..128).map(Some).collect::<Vec<_>>(), 0, 0),
                context,
                1,
            )?;
            join.finish_build(context)?;
            let snapshot = context.memory().snapshot();
            // The session's grant is intentionally fixed. Hold an ordinary
            // credited owner, then release it to restore available headroom.
            let mut pressure = Some(
                context
                    .memory()
                    .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 8192)?,
            );
            let _scope = node.scope(Phase::ProbeMatch);
            for _ in 0..2 {
                let mut observed = Vec::new();
                join.consume(signed_input(&[Some(7)], 0, 0), context, 1, &mut |array| {
                    observed.extend(pairs(&array));
                    Ok(())
                })?;
                assert_eq!(observed, [(Some(0), Some(7))]);
                drop(pressure.take());
            }
            Ok(())
        })
        .unwrap();
    let work = observation.finish().integer_lookup_work[0];
    assert_eq!(
        (work.considered, work.declined_memory, work.admitted),
        (1, 1, 0)
    );
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn dense_integer_join_consumer_failure_and_cancellation_release_published_state() {
    for mode in ["cancel_before", "cancel_consumer", "consumer_error"] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let cancellation = CancellationToken::default();
        let observation = cost::Session::start();
        let node = cost::node("inner", 1, false, false);
        let mut calls = 0;
        let result = session.with_native_execution_context(&cancellation, |context| {
            let binding = signed_spec(Kind::Inner);
            let mut join = Join::new(&binding, context.memory())?;
            join.build(
                signed_input(&(0..128).map(Some).collect::<Vec<_>>(), 0, 0),
                context,
                1,
            )?;
            join.finish_build(context)?;
            if mode == "cancel_before" {
                cancellation.cancel();
            }
            let _scope = node.scope(Phase::ProbeMatch);
            join.consume(
                signed_input(&[Some(0), Some(1)], 0, 0),
                context,
                1,
                &mut |_| {
                    calls += 1;
                    if mode == "cancel_consumer" {
                        cancellation.cancel();
                        Ok(())
                    } else {
                        Err(failed("intentional dense join consumer failure"))
                    }
                },
            )
        });
        assert!(result.is_err());
        assert_eq!(calls, usize::from(mode != "cancel_before"));
        let work = observation.finish().integer_lookup_work[0];
        assert_eq!(work.admitted, u64::from(mode != "cancel_before"));
        assert_eq!(work.probe_rows, u64::from(mode != "cancel_before"));
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn dense_integer_join_keeps_selected_payload_errors_even_when_output_is_masked() {
    for selected in [false, true] {
        for masked in [false, true] {
            let mut binding = signed_spec(Kind::Inner);
            binding.fields.push((
                "bad".into(),
                DType::Primitive(PType::F64, Nullability::NonNullable),
            ));
            binding.columns.push((Side::Right, "bad".into()));
            binding.retained = masked.then_some(vec![true, true, false]);
            let ordinary = signed_input(&(0..128).map(Some).collect::<Vec<_>>(), 0, 0);
            let right = StructArray::new(
                FieldNames::from(["key", "id", "bad"]),
                vec![
                    logical_field_from_native_array(&ordinary, "key").unwrap(),
                    logical_field_from_native_array(&ordinary, "id").unwrap(),
                    PrimitiveArray::from_iter(
                        (0..128).map(|row| if row == 7 { f64::NAN } else { 0.0 }),
                    )
                    .into_array(),
                ],
                128,
                Validity::NonNullable,
            )
            .into_array();
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let memory = session.memory().clone();
            let observation = cost::Session::start();
            let node = cost::node("inner", 1, false, false);
            let mut calls = 0;
            let result =
                session.with_native_execution_context(&CancellationToken::default(), |context| {
                    let mut join = Join::new(&binding, context.memory())?;
                    join.build(right.clone(), context, 1)?;
                    let _scope = node.scope(Phase::ProbeMatch);
                    join.consume(
                        signed_input(&[Some(if selected { 7 } else { 8 })], 0, 0),
                        context,
                        1,
                        &mut |_| {
                            calls += 1;
                            Ok(())
                        },
                    )
                });
            assert_eq!(result.is_err(), selected);
            if let Err(error) = result {
                assert!(error.to_string().contains("finite declared dtype"));
            }
            assert_eq!(calls, usize::from(!selected));
            assert_eq!(observation.finish().integer_lookup_work[0].admitted, 1);
            drop(session);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn dense_integer_join_parent_validity_masks_build_and_probe_keys() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let observation = cost::Session::start();
    let node = cost::node("inner", 1, false, false);
    let parent_masked = |keys: &[Option<i64>], invalid: usize| {
        let ordinary = signed_input(keys, 0, 0);
        StructArray::new(
            FieldNames::from(["key", "id"]),
            vec![
                logical_field_from_native_array(&ordinary, "key").unwrap(),
                logical_field_from_native_array(&ordinary, "id").unwrap(),
            ],
            keys.len(),
            Validity::from_iter((0..keys.len()).map(|row| row != invalid)),
        )
        .into_array()
    };
    let mut observed = Vec::new();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let binding = signed_spec(Kind::Inner);
            let mut join = Join::new(&binding, context.memory())?;
            join.build(
                parent_masked(&(0..129).map(Some).collect::<Vec<_>>(), 7),
                context,
                3,
            )?;
            let _scope = node.scope(Phase::ProbeMatch);
            join.consume(
                parent_masked(&[Some(7), Some(8), Some(128)], 1),
                context,
                3,
                &mut |array| {
                    observed.extend(pairs(&array));
                    Ok(())
                },
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(observed, [(Some(2), Some(128))]);
    let work = observation.finish().integer_lookup_work[0];
    assert_eq!(
        (
            work.admitted,
            work.build_keys,
            work.null_rows,
            work.matched_rows
        ),
        (1, 128, 1, 1)
    );
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
