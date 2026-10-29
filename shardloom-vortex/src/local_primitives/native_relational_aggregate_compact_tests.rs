//! Complete grouped values under a bounded state budget, including growth.

use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{BoolArray, ChunkedArray, DictArray, PrimitiveArray},
    dtype::Nullability,
};

pub(super) fn measure(function: Function, column: Option<&str>) -> Measure {
    Measure {
        function,
        column: column.map(str::to_owned),
        secondary_column: None,
        quantile_level: None,
        separator: None,
        filter_column: None,
        selection: None,
        dtype: if function == Function::Count {
            DType::Primitive(PType::U64, Nullability::NonNullable)
        } else {
            DType::Primitive(PType::F64, Nullability::Nullable)
        },
        decimal_source: None,
        decimal_output: None,
        distinct_fields: vec![],
        distinct_names: vec![],
    }
}

pub(super) fn binding() -> Spec {
    let groups = vec![
        (
            "identity".into(),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ),
        (
            "zone".into(),
            DType::Primitive(PType::U32, Nullability::NonNullable),
        ),
    ];
    let measures = vec![
        measure(Function::Count, None),
        measure(Function::Sum, Some("amount")),
        measure(Function::Avg, Some("width")),
    ];
    let mut fields = groups.clone();
    fields.extend(
        ["count", "total", "average"]
            .into_iter()
            .zip(&measures)
            .map(|(name, measure)| (name.into(), measure.dtype.clone())),
    );
    Spec {
        fields,
        groups,
        group_names: vec!["identity".into(), "zone".into()],
        measures,
    }
}

fn input(start: u32, end: u32) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["identity", "zone", "amount", "width"]),
        vec![
            PrimitiveArray::from_iter((start..end).map(u64::from)).into_array(),
            PrimitiveArray::from_iter((start..end).map(|row| row % 97)).into_array(),
            PrimitiveArray::from_iter((start..end).map(|row| u8::try_from(row % 5).unwrap()))
                .into_array(),
            PrimitiveArray::from_option_iter((start..end).map(|row| {
                (!row.is_multiple_of(11)).then(|| u16::try_from(row % 16000 + 1).unwrap())
            }))
            .into_array(),
        ],
        (end - start) as usize,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

#[test]
fn grouped_additive_many_groups_fit_declared_memory_and_preserve_every_result() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let spec = binding();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            for _ in 0..2 {
                for start in (0..65536).step_by(1024) {
                    aggregate.consume(&input(start, start + 1024), context, 1024)?;
                }
            }
            let mut checked = 0u64;
            aggregate.finish(context, 1024, &mut |batch| {
                let values = spec
                    .fields
                    .iter()
                    .map(|(name, _)| keys(&logical_field_from_native_array(&batch, name)?, context))
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..batch.len() {
                    assert_eq!(values[0].raw_cell(row)?, Cell::NonnegativeInteger(checked));
                    assert_eq!(
                        values[1].raw_cell(row)?,
                        Cell::NonnegativeInteger(checked % 97)
                    );
                    assert_eq!(values[2].raw_cell(row)?, Cell::NonnegativeInteger(2));
                    assert_eq!(
                        number(&values[3].raw_cell(row)?)?,
                        f64::from(u32::try_from(2 * (checked % 5)).unwrap())
                    );
                    if checked.is_multiple_of(11) {
                        assert_eq!(values[4].raw_cell(row)?, Cell::Null);
                    } else {
                        assert_eq!(
                            number(&values[4].raw_cell(row)?)?,
                            f64::from(u32::try_from(checked % 16000 + 1).unwrap())
                        );
                    }
                    checked += 1;
                }
                Ok(())
            })?;
            assert_eq!(checked, 65536);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn nullable_column(phase: usize) -> ArrayRef {
    let dictionary = [None, Some(1i64), Some(-2), Some(9)];
    if phase == 0 {
        DictArray::try_new(
            PrimitiveArray::from_iter((0..8193).map(|row| u8::try_from(row % 4).unwrap()))
                .into_array(),
            PrimitiveArray::from_option_iter(dictionary).into_array(),
        )
        .unwrap()
        .into_array()
    } else {
        let chunks = [0..4097, 4097..8193]
            .into_iter()
            .map(|range| {
                PrimitiveArray::from_option_iter(range.map(|row| dictionary[(row + phase) % 4]))
                    .into_array()
            })
            .collect::<Vec<_>>();
        let dtype = chunks[0].dtype().clone();
        ChunkedArray::try_new(chunks, dtype).unwrap().into_array()
    }
}

#[test]
fn compact_groups_preserve_filtered_nullable_dictionary_and_chunked_measures() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let mut spec = binding();
    spec.measures = vec![
        measure(Function::Count, None),
        measure(Function::Count, Some("n")),
        measure(Function::Sum, Some("n")),
        measure(Function::Avg, Some("n")),
    ];
    spec.measures[2].filter_column = Some("keep".into());
    spec.measures[3].filter_column = Some("keep".into());
    spec.fields = spec.groups.clone();
    spec.fields.extend(
        ["rows", "nonnull", "total", "average"]
            .into_iter()
            .zip(&spec.measures)
            .map(|(name, measure)| (name.into(), measure.dtype.clone())),
    );
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            let dictionary = [None, Some(1i64), Some(-2), Some(9)];
            for phase in 0..2usize {
                let column = nullable_column(phase);
                let input = StructArray::try_new(
                    FieldNames::from(["identity", "zone", "n", "keep"]),
                    vec![
                        PrimitiveArray::from_iter(0u64..8193).into_array(),
                        PrimitiveArray::from_iter((0..8193).map(|row| row % 97u32)).into_array(),
                        column,
                        BoolArray::from_iter((0..8193).map(|row| (row + phase) % 5 != 0))
                            .into_array(),
                    ],
                    8193,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array();
                aggregate.consume(&input, context, 257)?;
                assert!(aggregate.compact.is_some());
                assert!(aggregate.states.values.is_empty());
            }
            let mut checked = 0usize;
            aggregate.finish(context, 257, &mut |batch| {
                let values = spec
                    .fields
                    .iter()
                    .map(|(name, _)| keys(&logical_field_from_native_array(&batch, name)?, context))
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..batch.len() {
                    let expected = [dictionary[checked % 4], dictionary[(checked + 1) % 4]];
                    let selected = expected
                        .into_iter()
                        .enumerate()
                        .filter_map(|(phase, value)| {
                            (!(checked + phase).is_multiple_of(5))
                                .then_some(value)
                                .flatten()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        values[0].raw_cell(row)?,
                        Cell::NonnegativeInteger(checked as u64)
                    );
                    assert_eq!(values[2].raw_cell(row)?, Cell::NonnegativeInteger(2));
                    assert_eq!(
                        values[3].raw_cell(row)?,
                        Cell::NonnegativeInteger(
                            expected.iter().filter(|value| value.is_some()).count() as u64
                        )
                    );
                    if selected.is_empty() {
                        assert_eq!(values[4].raw_cell(row)?, Cell::Null);
                        assert_eq!(values[5].raw_cell(row)?, Cell::Null);
                    } else {
                        let total = f64::from(i32::try_from(selected.iter().sum::<i64>()).unwrap());
                        assert_eq!(number(&values[4].raw_cell(row)?)?, total);
                        assert_eq!(
                            number(&values[5].raw_cell(row)?)?,
                            total / f64::from(u32::try_from(selected.len()).unwrap())
                        );
                    }
                    checked += 1;
                }
                Ok(())
            })?;
            assert_eq!(checked, 8193);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compact_adjacent_aliases_reuse_only_completed_equal_numeric_states() {
    let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let mut spec = binding();
    spec.measures = vec![
        measure(Function::Sum, Some("n")),
        measure(Function::Avg, Some("alias")),
        measure(Function::Sum, Some("n")),
    ];
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new(&spec, context.memory())?;
            let groups = (0u64..8193).cycle().take(8193 * 3).collect::<Vec<_>>();
            let numbers = PrimitiveArray::from_iter(
                groups
                    .iter()
                    .map(|row| i64::try_from(row % 17).unwrap() - 8),
            )
            .into_array();
            let input = StructArray::try_new(
                FieldNames::from(["identity", "zone", "n", "alias"]),
                vec![
                    PrimitiveArray::from_iter(groups.iter().copied()).into_array(),
                    PrimitiveArray::from_iter(groups.iter().map(|row| (row % 97) as u32))
                        .into_array(),
                    numbers.clone(),
                    numbers,
                ],
                groups.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&input, context, 257)?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_numeric_bound_batches, 1);
                assert_eq!(counts.aggregate_numeric_state_reuses, 2);
                assert_eq!(counts.aggregate_numeric_reused_rows, (8193 * 3 * 2) as u64);
            }
            let table = aggregate
                .compact
                .as_ref()
                .expect("large grouped state promoted");
            for group in 0..8193 {
                let expected = f64::from(u32::try_from(group % 17).unwrap()) - 8.0;
                for index in [0, 2] {
                    assert!(matches!(table.finish(index, group, Function::Sum)?,
                        Value::Float(value) if value.to_bits() == (expected * 3.0).to_bits()));
                }
                assert!(matches!(table.finish(1, group, Function::Avg)?,
                    Value::Float(value) if value.to_bits() == expected.to_bits()));
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
