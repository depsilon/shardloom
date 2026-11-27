use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{BoolArray, ChunkedArray, ConstantArray, DictArray, PrimitiveArray, VarBinViewArray},
    dtype::Nullability,
    scalar::Scalar,
};
use vortex::buffer::ByteBuffer;

fn measure(function: Function, column: Option<&str>, dtype: DType) -> Measure {
    Measure {
        function,
        column: column.map(str::to_owned),
        secondary_column: None,
        filter_column: None,
        quantile_level: None,
        selection: None,
        separator: None,
        dtype,
        decimal_source: None,
        input_ptype: None,
        decimal_output: None,
        distinct_fields: vec![],
        distinct_names: vec![],
    }
}

fn spec(measures: Vec<Measure>) -> Spec {
    Spec {
        fields: vec![],
        groups: vec![],
        group_names: vec![],
        measures,
    }
}

#[test]
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn metadata_count_native_result_preserves_the_full_unsigned_domain_and_overflow_errors() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let mut binding = spec(vec![
        measure(Function::Count, None, dtype.clone()),
        measure(Function::Count, None, dtype.clone()),
    ]);
    binding.fields = vec![("rows".into(), dtype.clone()), ("also".into(), dtype)];
    let memory = session.memory().clone();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for rows in [(1 << 53) + 1, u64::MAX] {
                let mut aggregate = Aggregate::new(&binding, context.memory())?;
                aggregate.add_metadata_count(rows, context)?;
                aggregate.finish(context, 1, &mut |array| {
                    assert_eq!(array.len(), 1);
                    for name in ["rows", "also"] {
                        let value = keys(&logical_field_from_native_array(&array, name)?, context)?;
                        assert_eq!(value.raw_cell(0)?, Cell::NonnegativeInteger(rows));
                    }
                    Ok(())
                })?;
                if rows == u64::MAX {
                    assert!(
                        aggregate
                            .add_metadata_count(1, context)
                            .unwrap_err()
                            .to_string()
                            .contains("aggregate count overflow")
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
fn aggregate_reuse_scalar_count_uses_checked_batch_addition_without_row_sized_state() {
    let session = ResidentVortexSession::new(16 << 10, 1).unwrap();
    let memory = session.memory().clone();
    let binding = spec(vec![measure(
        Function::Count,
        None,
        DType::Primitive(PType::U64, Nullability::NonNullable),
    )]);
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            let initial = context.memory().snapshot().reserved_bytes;
            let array = StructArray::try_new(
                FieldNames::from(["unused"]),
                vec![ConstantArray::new(Scalar::from(7i64), 1_000_000).into_array()],
                1_000_000,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            aggregate.consume(&array, context, 1024)?;
            assert_eq!(aggregate.states.values[0].count, 1_000_000);
            assert_eq!(context.memory().snapshot().reserved_bytes, initial);
            aggregate.consume(&array.slice(0..0).map_err(vortex_error)?, context, 1024)?;
            assert_eq!(aggregate.states.values[0].count, 1_000_000);
            aggregate.states.values[0].count = u64::MAX;
            assert!(
                aggregate
                    .consume(&array, context, 1024)
                    .unwrap_err()
                    .to_string()
                    .contains("aggregate count overflow")
            );
            assert_eq!(aggregate.states.values[0].count, u64::MAX);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_column_count_uses_native_validity_cardinality_across_encodings() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let binding = spec(vec![measure(
        Function::Count,
        Some("n"),
        DType::Primitive(PType::U64, Nullability::NonNullable),
    )]);
    let nullable =
        PrimitiveArray::from_option_iter([Some(7i64), None, Some(-2), None, Some(9)]).into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_option_iter([Some(2u8), None, Some(0), Some(1), Some(2)]).into_array(),
        PrimitiveArray::from_option_iter([None, Some(4i64), Some(8)]).into_array(),
    )
    .unwrap()
    .into_array();
    let chunked = ChunkedArray::try_new(
        vec![nullable.slice(0..2).unwrap(), nullable.slice(2..5).unwrap()],
        nullable.dtype().clone(),
    )
    .unwrap()
    .into_array();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            for (column, expected) in [
                (nullable.clone(), 3),
                (nullable.slice(1..4).unwrap(), 1),
                (nullable.slice(0..0).unwrap(), 0),
                (dictionary, 3),
                (chunked, 3),
                (
                    ConstantArray::new(Scalar::from(9i64), 100_000).into_array(),
                    100_000,
                ),
                (
                    ConstantArray::new(
                        Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
                        4096,
                    )
                    .into_array(),
                    0,
                ),
            ] {
                let input = StructArray::try_new(
                    FieldNames::from(["n"]),
                    vec![column.clone()],
                    column.len(),
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array();
                let mut aggregate = Aggregate::new_for_execution(&binding, context.memory())?;
                #[cfg(debug_assertions)]
                let observation = super::super::native_relational_observation::Session::start();
                aggregate.consume(&input, context, 1024)?;
                assert_eq!(aggregate.states.values[0].count, expected);
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.aggregate_count_updates, 1);
                    assert_eq!(counts.aggregate_count_contributions, expected);
                    assert_eq!(counts.aggregate_value_preparations, 0);
                }
            }
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_column_count_composes_parent_validity_and_measure_filters() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let count_type = DType::Primitive(PType::U64, Nullability::NonNullable);
    let mut filtered_count = measure(Function::Count, Some("n"), count_type.clone());
    filtered_count.filter_column = Some("keep".into());
    let mut filtered_sum = measure(
        Function::Sum,
        Some("n"),
        DType::Primitive(PType::F64, Nullability::Nullable),
    );
    filtered_sum.filter_column = Some("keep".into());
    let mut filtered_rows = measure(Function::Count, None, count_type.clone());
    filtered_rows.filter_column = Some("keep".into());
    let binding = spec(vec![
        filtered_count,
        filtered_sum,
        measure(Function::Count, Some("n"), count_type),
        filtered_rows,
    ]);
    let input = StructArray::try_new(
        FieldNames::from(["n", "keep"]),
        vec![
            PrimitiveArray::from_option_iter([Some(4i64), None, Some(-2), Some(8), Some(5)])
                .into_array(),
            BoolArray::from_iter([true, true, false, true, true]).into_array(),
        ],
        5,
        Validity::Array(BoolArray::from_iter([true, true, true, false, true]).into_array()),
    )
    .unwrap()
    .into_array();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new_for_execution(&binding, context.memory())?;
            aggregate.consume(&input, context, 2)?;
            assert_eq!(aggregate.states.values[0].count, 2);
            assert_eq!(aggregate.states.values[1].count, 2);
            assert_eq!(aggregate.states.values[1].sum.total(), 9.0);
            assert_eq!(aggregate.states.values[2].count, 3);
            assert_eq!(aggregate.states.values[3].count, 3);
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn scalar_column_count_keeps_checked_overflow_and_cancellation() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let binding = spec(vec![
        measure(Function::Count, Some("n"), dtype.clone()),
        measure(Function::Count, Some("n"), dtype),
    ]);
    let input = StructArray::try_new(
        FieldNames::from(["n"]),
        vec![PrimitiveArray::from_option_iter([Some(1i64), None, Some(2)]).into_array()],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut aggregate = Aggregate::new_for_execution(&binding, &memory).unwrap();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            aggregate.states.values[0].count = u64::MAX - 1;
            let error = aggregate.consume(&input, context, 2).unwrap_err();
            assert!(error.to_string().contains("aggregate count overflow"));
            assert_eq!(aggregate.states.values[0].count, u64::MAX - 1);
            assert_eq!(aggregate.states.values[1].count, 0);
            aggregate.states.values[0].count = u64::MAX - 2;
            aggregate.consume(&input, context, 2)?;
            assert_eq!(aggregate.states.values[0].count, u64::MAX);
            assert_eq!(aggregate.states.values[1].count, 2);
            aggregate.consume(&input.slice(0..0).map_err(vortex_error)?, context, 2)?;
            Ok(())
        })
        .unwrap();
    let token = CancellationToken::default();
    token.cancel();
    let error = session
        .with_native_execution_context(&token, |context| aggregate.consume(&input, context, 2))
        .unwrap_err();
    assert!(error.to_string().contains("execution cancelled"));
    assert_eq!(aggregate.states.values[0].count, u64::MAX);
    assert_eq!(aggregate.states.values[1].count, 2);
    drop(aggregate);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn aggregate_reuse_preserves_repeated_and_interleaved_measure_results_and_releases_owners() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let binding = spec(vec![
        measure(Function::Min, Some("s"), DType::Utf8(Nullability::Nullable)),
        measure(Function::Max, Some("s"), DType::Utf8(Nullability::Nullable)),
        measure(
            Function::Count,
            Some("s"),
            DType::Primitive(PType::U64, Nullability::NonNullable),
        ),
        measure(
            Function::Sum,
            Some("n"),
            DType::Primitive(PType::F64, Nullability::Nullable),
        ),
        measure(
            Function::Avg,
            Some("n"),
            DType::Primitive(PType::F64, Nullability::Nullable),
        ),
        measure(Function::Min, Some("s"), DType::Utf8(Nullability::Nullable)),
    ]);
    let array = StructArray::try_new(
        FieldNames::from(["s", "n"]),
        vec![
            VarBinViewArray::from_iter_nullable_str([Some("東京"), None, Some("a")]).into_array(),
            PrimitiveArray::from_option_iter([Some(3i64), Some(-1), None]).into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate = Aggregate::new(&binding, context.memory())?;
            #[cfg(debug_assertions)]
            let observation = super::super::native_relational_observation::Session::start();
            aggregate.consume(&array, context, 2)?;
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.aggregate_value_preparations, 3);
                assert_eq!(counts.variable_hash_slot_bytes, 0);
            }
            assert_eq!(
                aggregate.states.values[0].extreme,
                Some(Cell::Utf8(ByteBuffer::from(b"a".to_vec())))
            );
            assert_eq!(
                aggregate.states.values[1].extreme,
                Some(Cell::Utf8(ByteBuffer::from("東京".as_bytes().to_vec())))
            );
            assert_eq!(aggregate.states.values[2].count, 2);
            assert_eq!(aggregate.states.values[3].sum.total(), 2.0);
            assert_eq!(aggregate.states.values[4].sum.total(), 2.0);
            assert_eq!(aggregate.states.values[4].count, 2);
            assert_eq!(
                aggregate.states.values[0].extreme,
                aggregate.states.values[5].extreme
            );
            Ok(())
        })
        .unwrap();
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn aggregate_reuse_nested_count_keeps_parent_only_validation_and_measure_error_order() {
    let nested = StructArray::try_new(
        FieldNames::from(["value"]),
        vec![PrimitiveArray::from_iter([0.0f64, f64::NAN]).into_array()],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let nested_type = nested.dtype().clone();
    let array = StructArray::try_new(
        FieldNames::from(["nested"]),
        vec![nested],
        2,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    for functions in [
        vec![Function::Count],
        vec![Function::Count, Function::Min],
        vec![Function::Min, Function::Count],
    ] {
        let measures = functions
            .iter()
            .map(|function| {
                measure(
                    *function,
                    Some("nested"),
                    if *function == Function::Count {
                        DType::Primitive(PType::U64, Nullability::NonNullable)
                    } else {
                        nested_type.clone()
                    },
                )
            })
            .collect();
        let binding = spec(measures);
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut aggregate = Aggregate::new(&binding, context.memory())?;
                let result = aggregate.consume(&array, context, 2);
                if functions.len() == 1 {
                    result?;
                } else {
                    let error = result.unwrap_err();
                    assert!(error.to_string().contains("nonfinite"), "{error}");
                }
                let count = functions
                    .iter()
                    .position(|f| *f == Function::Count)
                    .unwrap();
                assert_eq!(
                    aggregate.states.values[count].count,
                    if count == 0 { 2 } else { 0 }
                );
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
