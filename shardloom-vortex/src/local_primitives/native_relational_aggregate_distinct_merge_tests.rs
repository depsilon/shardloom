//! Range-local exact sets must union values after remapping group identities.

use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use std::ops::Range;
use vortex::array::arrays::{BoolArray, DictArray, PrimitiveArray, VarBinViewArray};

fn measure(function: Function, column: Option<&str>, dtype: DType) -> Measure {
    let distinct = function == Function::CountDistinct;
    Measure {
        function,
        column: column.map(str::to_owned),
        secondary_column: None,
        quantile_level: None,
        separator: None,
        filter_column: None,
        selection: None,
        dtype,
        input_ptype: (column == Some("n")).then_some(PType::I64),
        decimal_source: None,
        decimal_output: None,
        distinct_fields: if distinct {
            vec![
                (
                    "group".into(),
                    DType::Primitive(PType::U64, Nullability::NonNullable),
                ),
                ("value".into(), DType::Utf8(Nullability::Nullable)),
            ]
        } else {
            vec![]
        },
        distinct_names: if distinct {
            vec!["group".into(), "value".into()]
        } else {
            vec![]
        },
    }
}

fn specification(grouped: bool) -> Spec {
    let count_type = DType::Primitive(PType::U64, Nullability::NonNullable);
    let float_type = DType::Primitive(PType::F64, Nullability::Nullable);
    let group_type = DType::Primitive(PType::I64, Nullability::Nullable);
    let mut filtered = measure(Function::CountDistinct, Some("v"), count_type.clone());
    filtered.filter_column = Some("keep".into());
    let mut fields = if grouped {
        vec![("g".into(), group_type.clone())]
    } else {
        vec![]
    };
    fields.extend([
        ("unique".into(), count_type.clone()),
        ("same_unique".into(), count_type.clone()),
        ("filtered_unique".into(), count_type.clone()),
        ("rows".into(), count_type.clone()),
        ("sum".into(), float_type.clone()),
        ("avg".into(), float_type.clone()),
    ]);
    Spec {
        fields,
        groups: if grouped {
            vec![("g".into(), group_type)]
        } else {
            vec![]
        },
        group_names: if grouped { vec!["g".into()] } else { vec![] },
        measures: vec![
            measure(Function::CountDistinct, Some("v"), count_type.clone()),
            measure(Function::CountDistinct, Some("v"), count_type.clone()),
            filtered,
            measure(Function::Count, None, count_type),
            measure(Function::Sum, Some("n"), float_type.clone()),
            measure(Function::Avg, Some("n"), float_type),
        ],
    }
}

fn input(groups: &[Option<i64>], values: ArrayRef, keep: &[Option<bool>]) -> ArrayRef {
    let rows = groups.len();
    StructArray::try_new(
        FieldNames::from(["g", "v", "keep", "n"]),
        vec![
            PrimitiveArray::from_option_iter(groups.iter().copied()).into_array(),
            values,
            BoolArray::from_iter(keep.iter().copied()).into_array(),
            PrimitiveArray::from_iter((0..rows).map(|row| i64::try_from(row + 1).unwrap()))
                .into_array(),
        ],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn fixture(dictionary: bool) -> ArrayRef {
    let values = if dictionary {
        DictArray::try_new(
            PrimitiveArray::from_option_iter([
                Some(0_u8),
                Some(0),
                Some(1),
                Some(2),
                Some(1),
                Some(0),
                Some(3),
                Some(2),
                Some(0),
                Some(4),
                None,
                None,
            ])
            .into_array(),
            VarBinViewArray::from_iter_nullable_str([
                Some("a"),
                Some("b"),
                Some("n"),
                Some("z"),
                Some("x"),
            ])
            .into_array(),
        )
        .unwrap()
        .into_array()
    } else {
        VarBinViewArray::from_iter_nullable_str([
            Some("a"),
            Some("a"),
            Some("b"),
            Some("n"),
            Some("b"),
            Some("a"),
            Some("z"),
            Some("n"),
            Some("a"),
            Some("x"),
            None,
            None,
        ])
        .into_array()
    };
    input(
        &[
            Some(2),
            Some(1),
            Some(2),
            None,
            Some(1),
            Some(2),
            Some(3),
            None,
            Some(1),
            Some(3),
            Some(4),
            Some(4),
        ],
        values,
        &[
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
            Some(true),
            None,
        ],
    )
}

fn output(
    aggregate: &Aggregate<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<Vec<Cell>>> {
    let mut result = Vec::new();
    aggregate.finish(context, 2, &mut |array| {
        let columns = aggregate
            .spec
            .fields
            .iter()
            .map(|(name, _)| keys(&logical_field_from_native_array(&array, name)?, context))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..array.len() {
            result.push(
                columns
                    .iter()
                    .map(|column| column.raw_cell(row))
                    .collect::<Result<_>>()?,
            );
        }
        Ok(())
    })?;
    Ok(result)
}

fn merged(
    spec: &Spec,
    array: &ArrayRef,
    ranges: &[Range<usize>],
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<Vec<Cell>>> {
    let shared = spec
        .try_clone_for_parallel_merge()
        .expect("exact sets admit merge");
    let mut aggregate = Aggregate::new_for_execution(spec, context.memory())?;
    for range in ranges {
        let mut partial = Aggregate::new_shared(shared.clone(), context.memory())?;
        if range.is_empty() {
            partial.consume(
                &array.slice(range.clone()).map_err(vortex_error)?,
                context,
                2,
            )?;
        } else {
            // Persistent range state spans several decoder batches. The merge
            // sees only unique partial pairs, with unrelated local group IDs.
            for start in (range.start..range.end).step_by(2) {
                partial.consume(
                    &array
                        .slice(start..(start + 2).min(range.end))
                        .map_err(vortex_error)?,
                    context,
                    2,
                )?;
            }
        }
        aggregate.merge_from(partial, context, 2)?;
    }
    output(&aggregate, context)
}

fn counts_and_totals(unique: u64, filtered: u64, count: u64, sum: f64) -> Vec<Cell> {
    vec![
        Cell::NonnegativeInteger(unique),
        Cell::NonnegativeInteger(unique),
        Cell::NonnegativeInteger(filtered),
        Cell::NonnegativeInteger(count),
        Cell::Float(sum.to_bits()),
        Cell::Float((sum / f64::from(u32::try_from(count).unwrap())).to_bits()),
    ]
}

#[test]
fn count_distinct_range_merge_unions_overlaps_filters_and_canonical_measures() {
    let session = ResidentVortexSession::new(16 << 20, 4).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for grouped in [false, true] {
                for dictionary in [false, true] {
                    let spec = specification(grouped);
                    let array = fixture(dictionary);
                    let mut serial = Aggregate::new_for_execution(&spec, context.memory())?;
                    serial.consume(&array, context, 2)?;
                    let actual = merged(&spec, &array, &[0..0, 0..4, 4..4, 4..7, 7..12], context)?;
                    assert_eq!(actual, output(&serial, context)?);
                    let expected = if grouped {
                        [
                            (Cell::NonnegativeInteger(2), 2, 1, 3, 10.0),
                            (Cell::NonnegativeInteger(1), 2, 1, 3, 16.0),
                            (Cell::Null, 1, 1, 2, 12.0),
                            (Cell::NonnegativeInteger(3), 2, 1, 2, 17.0),
                            (Cell::NonnegativeInteger(4), 0, 0, 2, 23.0),
                        ]
                        .into_iter()
                        .map(|(group, unique, filtered, count, sum)| {
                            let mut row = vec![group];
                            row.extend(counts_and_totals(unique, filtered, count, sum));
                            row
                        })
                        .collect::<Vec<_>>()
                    } else {
                        vec![counts_and_totals(5, 3, 12, 78.0)]
                    };
                    assert_eq!(
                        actual, expected,
                        "grouped={grouped}, dictionary={dictionary}"
                    );
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_distinct_range_merge_preserves_empty_and_all_null_results() {
    let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let array = input(
                &[Some(2), Some(1), Some(2), None, Some(1), None],
                VarBinViewArray::from_iter_nullable_str([None::<&str>; 6]).into_array(),
                &[Some(true); 6],
            );
            for grouped in [false, true] {
                let spec = specification(grouped);
                for (input, ranges) in [
                    (array.clone(), vec![0..2, 2..2, 2..6]),
                    (array.slice(0..0).map_err(vortex_error)?, vec![0..0, 0..0]),
                ] {
                    let mut serial = Aggregate::new_for_execution(&spec, context.memory())?;
                    serial.consume(&input, context, 2)?;
                    let actual = merged(&spec, &input, &ranges, context)?;
                    assert_eq!(actual, output(&serial, context)?);
                    for row in actual {
                        let offset = usize::from(grouped);
                        assert_eq!(
                            &row[offset..offset + 3],
                            &[
                                Cell::NonnegativeInteger(0),
                                Cell::NonnegativeInteger(0),
                                Cell::NonnegativeInteger(0),
                            ],
                        );
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_distinct_range_merge_releases_memory_on_budget_and_cancellation_errors() {
    for cancel in [false, true] {
        let session = ResidentVortexSession::new(4 << 20, 2).unwrap();
        let memory = session.memory().clone();
        let cancellation = CancellationToken::default();
        let completion = session.with_native_execution_context(&cancellation, |context| {
            let spec = specification(true);
            let shared = spec.try_clone_for_parallel_merge().unwrap();
            let array = fixture(false);
            let mut target = Aggregate::new_for_execution(&spec, context.memory())?;
            let mut source = Aggregate::new_shared(shared, context.memory())?;
            target.consume(&array.slice(0..4).map_err(vortex_error)?, context, 2)?;
            source.consume(&array.slice(4..12).map_err(vortex_error)?, context, 2)?;
            let pressure = if cancel {
                cancellation.cancel();
                None
            } else {
                let snapshot = memory.snapshot();
                Some(memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?)
            };
            let error = target.merge_from(source, context, 2).unwrap_err();
            if !cancel {
                assert!(error.to_string().contains("reservation denied"), "{error}");
            }
            drop(pressure);
            Ok(())
        });
        if cancel {
            assert!(completion.is_err());
        } else {
            completion.unwrap();
        }
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn count_distinct_range_merge_preserves_floating_key_identity() {
    let session = ResidentVortexSession::new(4 << 20, 2).unwrap();
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut spec = specification(false);
            spec.measures.truncate(1);
            spec.fields.truncate(1);
            spec.measures[0].distinct_fields[1].1 =
                DType::Primitive(PType::F64, Nullability::Nullable);
            spec.measures[0].input_ptype = Some(PType::F64);
            let array = input(
                &[Some(1); 6],
                PrimitiveArray::from_option_iter([
                    Some(-0.0_f64),
                    None,
                    Some(1.0),
                    Some(0.0),
                    Some(1.0),
                    Some(2.0),
                ])
                .into_array(),
                &[Some(true); 6],
            );
            let mut serial = Aggregate::new_for_execution(&spec, context.memory())?;
            serial.consume(&array, context, 2)?;
            let actual = merged(&spec, &array, &[0..3, 3..6], context)?;
            assert_eq!(actual, output(&serial, context)?);
            assert_eq!(actual, vec![vec![Cell::NonnegativeInteger(3)]]);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn count_distinct_range_merge_declines_order_sensitive_distinct_and_float_sums() {
    let mut spec = specification(false);
    assert!(spec.admits_parallel_range_merge());
    spec.measures[4].input_ptype = Some(PType::F64);
    assert!(!spec.admits_parallel_range_merge());
    spec.measures[4].input_ptype = Some(PType::I64);
    spec.measures[5].input_ptype = Some(PType::F64);
    assert!(!spec.admits_parallel_range_merge());
    spec.measures[5].input_ptype = Some(PType::I64);
    for function in [Function::SumDistinct, Function::AvgDistinct] {
        spec.measures[0].function = function;
        assert!(!spec.admits_parallel_range_merge());
    }
    spec.measures[0].function = Function::CountDistinct;
    spec.measures[0].distinct_fields.swap(0, 1);
    assert!(!spec.admits_parallel_range_merge());
    spec.measures[0].distinct_fields.swap(0, 1);
    spec.measures[0].distinct_fields[1].1 = DType::List(
        std::sync::Arc::new(DType::Utf8(Nullability::Nullable)),
        Nullability::Nullable,
    );
    assert!(!spec.admits_parallel_range_merge());
}
