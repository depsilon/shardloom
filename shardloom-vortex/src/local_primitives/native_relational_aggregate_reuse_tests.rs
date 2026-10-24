use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    arrays::{ConstantArray, PrimitiveArray, VarBinViewArray},
    dtype::Nullability,
    scalar::Scalar,
};
use vortex::buffer::ByteBuffer;

fn measure(function: Function, column: Option<&str>, dtype: DType) -> Measure {
    Measure {
        function,
        column: column.map(str::to_owned),
        dtype,
        decimal_source: None,
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
