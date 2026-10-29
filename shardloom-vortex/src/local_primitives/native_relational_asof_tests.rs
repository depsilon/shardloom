//! Independent exhaustive selection checks for resident as-of ownership.

use super::*;
use crate::relational_query::{
    VortexRelationalAsOfDirection as Direction, VortexRelationalAsOfOptions as Options,
};

type Row = (Option<u8>, Option<i64>, u32);

fn array(rows: &[Row]) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["group", "key", "id"]),
        vec![
            PrimitiveArray::from_option_iter(rows.iter().map(|row| row.0)).into_array(),
            PrimitiveArray::from_option_iter(rows.iter().map(|row| row.1)).into_array(),
            PrimitiveArray::from_iter(rows.iter().map(|row| row.2)).into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn binding(options: Options, partitioned: bool) -> Spec {
    let mut bound = spec(Kind::Left);
    bound.asof = Some(options);
    if partitioned {
        bound.left_keys.insert(0, "group".into());
        bound.right_keys.insert(0, "group".into());
    }
    bound
}

fn reference(left: &[Row], right: &[Row], options: Options, partitioned: bool) -> Vec<Pair> {
    left.iter()
        .map(|&(group, key, id)| {
            let selected = key.and_then(|key| {
                right
                    .iter()
                    .enumerate()
                    .filter_map(|(position, &(other_group, other, other_id))| {
                        let other = other?;
                        if (partitioned && (group.is_none() || group != other_group))
                            || (!options.allow_exact_matches && key == other)
                            || (options.direction == Direction::Backward && other > key)
                            || (options.direction == Direction::Forward && other < key)
                        {
                            return None;
                        }
                        let distance = i128::from(key).abs_diff(i128::from(other));
                        if options
                            .tolerance
                            .is_some_and(|limit| distance > u128::from(limit))
                        {
                            return None;
                        }
                        // Enumerate all candidates, with explicit direction and source-row ties.
                        let after = other > key || options.direction == Direction::Forward;
                        let tie = if after {
                            position
                        } else {
                            usize::MAX - position
                        };
                        Some(((distance, after, tie), u64::from(other_id)))
                    })
                    .min_by_key(|(rank, _)| *rank)
                    .map(|(_, id)| id)
            });
            (Some(u64::from(id)), selected)
        })
        .collect()
}

fn check(left: &[Row], right: &[Row], options: Options, partitioned: bool, chunk: usize) {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let bound = binding(options, partitioned);
    let mut held = vec![];
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut join = asof::Join::new(&bound, context.memory())?;
            join.build(array(&[]), context)?;
            for rows in right.chunks(chunk) {
                join.build(array(rows), context)?;
            }
            join.seal(context)?;
            for rows in left.chunks(chunk) {
                join.consume(array(rows), context, 3, &mut |array| {
                    assert!(array.len() <= 3);
                    assert_eq!(
                        array.dtype(),
                        &DType::struct_(bound.fields.clone(), Nullability::NonNullable)
                    );
                    held.push(array);
                    Ok(())
                })?;
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    let observed = held.iter().flat_map(pairs).collect::<Vec<_>>();
    assert_eq!(
        observed,
        reference(left, right, options, partitioned),
        "{options:?}, partitioned={partitioned}, chunk={chunk}"
    );
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn asof_directions_tolerance_nulls_duplicate_ties_and_unsorted_batches_match_reference() {
    let left = [
        (Some(1), Some(5), 10),
        (Some(1), Some(4), 11),
        (Some(2), Some(5), 12),
        (None, Some(5), 13),
        (Some(1), None, 14),
        (Some(3), Some(5), 15),
        (Some(1), Some(i64::MIN), 16),
        (Some(1), Some(i64::MAX), 17),
        (Some(1), Some(-3), 18),
    ];
    let right = [
        (Some(1), Some(5), 20),
        (Some(2), Some(6), 21),
        (Some(1), Some(3), 22),
        (Some(1), Some(5), 23),
        (Some(1), Some(7), 24),
        (None, Some(5), 25),
        (Some(1), None, 26),
        (Some(1), Some(i64::MAX), 27),
        (Some(1), Some(i64::MIN), 28),
        (Some(1), Some(-4), 29),
        (Some(1), Some(7), 30),
    ];
    for direction in [Direction::Backward, Direction::Forward, Direction::Nearest] {
        for allow_exact_matches in [true, false] {
            for tolerance in [None, Some(0), Some(1), Some(4), Some(u64::MAX)] {
                let options = Options {
                    direction,
                    tolerance,
                    allow_exact_matches,
                };
                for partitioned in [false, true] {
                    for chunk in [1, 4, 64] {
                        check(&left, &right, options, partitioned, chunk);
                    }
                    check(&left, &[], options, partitioned, 3);
                    check(&[], &right, options, partitioned, 3);
                }
            }
        }
    }
}

#[test]
fn asof_fresh_partition_population_matches_exhaustive_reference() {
    let mut seed = 0x2026_1029_0173_u64;
    let mut rows = |count: u32, offset: u32| {
        (0..count)
            .map(|id| {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let group = (!seed.is_multiple_of(13)).then_some((seed % 7) as u8);
                let key = (!seed.is_multiple_of(17))
                    .then_some(i64::try_from((seed >> 16) % 79).unwrap() - 39);
                (group, key, id + offset)
            })
            .collect::<Vec<_>>()
    };
    let right = rows(257, 1000);
    let left = rows(193, 0);
    for direction in [Direction::Backward, Direction::Forward, Direction::Nearest] {
        for allow_exact_matches in [true, false] {
            for tolerance in [None, Some(0), Some(3)] {
                check(
                    &left,
                    &right,
                    Options {
                        direction,
                        tolerance,
                        allow_exact_matches,
                    },
                    true,
                    17,
                );
            }
        }
    }
}

#[test]
fn asof_mixed_integer_extremes_use_exact_distance_and_inclusive_tolerance() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for tolerance in [None, Some(u64::MAX), Some(1_u64 << 63)] {
                let bound = binding(
                    Options {
                        direction: Direction::Nearest,
                        tolerance,
                        allow_exact_matches: true,
                    },
                    false,
                );
                let mut join = asof::Join::new(&bound, context.memory())?;
                join.build(
                    array(&[(None, Some(i64::MIN), 1), (None, Some(i64::MAX), 2)]),
                    context,
                )?;
                join.seal(context)?;
                let mut observed = vec![];
                join.consume(
                    input(&[Some(u64::MAX), Some(0), None], &[10, 11, 12]),
                    context,
                    2,
                    &mut |array| {
                        observed.extend(pairs(&array));
                        Ok(())
                    },
                )?;
                assert_eq!(
                    observed,
                    [(Some(10), Some(2)), (Some(11), Some(2)), (Some(12), None)]
                );
            }
            let bound = binding(
                Options {
                    tolerance: Some(u64::MAX),
                    ..Options::default()
                },
                false,
            );
            let mut join = asof::Join::new(&bound, context.memory())?;
            join.build(array(&[(None, Some(i64::MIN), 1)]), context)?;
            join.seal(context)?;
            let mut observed = vec![];
            join.consume(input(&[Some(u64::MAX)], &[10]), context, 1, &mut |array| {
                observed.extend(pairs(&array));
                Ok(())
            })?;
            assert_eq!(observed, [(Some(10), None)]);
            Ok(())
        })
        .unwrap();
}

#[test]
fn asof_failure_cancellation_and_denied_allocation_release_owners() {
    for cancelled in [false, true] {
        let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let cancellation = CancellationToken::default();
        let result = session.with_native_execution_context(&cancellation, |context| {
            let bound = binding(Options::default(), false);
            let mut join = asof::Join::new(&bound, context.memory())?;
            join.build(array(&[(None, Some(1), 1)]), context)?;
            join.seal(context)?;
            if cancelled {
                cancellation.cancel();
            }
            join.consume(array(&[(None, Some(1), 2)]), context, 1, &mut |_| {
                Err(failed("asof consumer failure"))
            })
        });
        assert!(result.is_err());
        if !cancelled {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("asof consumer failure")
            );
        }
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let session = ResidentVortexSession::new(128 << 10, 1).unwrap();
    let memory = session.memory().clone();
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let bound = binding(Options::default(), false);
        let mut join = asof::Join::new(&bound, context.memory())?;
        join.build(array(&vec![(None, Some(1), 1); 100_000]), context)
    });
    assert!(result.is_err());
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    check(
        &[(None, Some(1), 2)],
        &[(None, Some(1), 1)],
        Options::default(),
        false,
        1,
    );
}
