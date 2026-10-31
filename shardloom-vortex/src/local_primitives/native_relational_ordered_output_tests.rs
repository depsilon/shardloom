//! Metadata admission must stay linear when sources and outputs use tiny batches.

use super::*;
use crate::{
    relational_query::{VortexRelationalNullOrder, VortexRelationalOrderKey},
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        arrays::{PrimitiveArray, VarBinViewArray},
        dtype::Nullability,
    },
    buffer::ByteBuffer,
};

fn collection(descending: bool) -> Measure {
    let mut measure = super::compact_tests::measure(Function::StringAgg, Some("text"));
    measure.dtype = DType::Utf8(Nullability::Nullable);
    measure.separator = Some(",".into());
    measure.selection = Some(SelectionSpec {
        fields: vec![
            ("text".into(), DType::Utf8(Nullability::NonNullable)),
            (
                "key".into(),
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ),
        ],
        names: vec!["key".into()],
        order: vec![VortexRelationalOrderKey {
            column: shardloom_core::ColumnRef::new("key").unwrap(),
            descending,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    });
    measure
}

#[test]
fn ordered_input_collection_domain_proof_is_once_per_state_across_output_chunks() {
    for output_rows in [1, 7, 33] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let group_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let spec = Spec {
            fields: vec![
                ("group".into(), group_dtype.clone()),
                ("forward".into(), DType::Utf8(Nullability::Nullable)),
                ("backward".into(), DType::Utf8(Nullability::Nullable)),
            ],
            groups: vec![("group".into(), group_dtype)],
            group_names: vec!["group".into()],
            measures: vec![collection(false), collection(true)],
        };
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut aggregate = Aggregate::new(&spec, context.memory())?;
                for row in 0_u64..66 {
                    let array = StructArray::new(
                        FieldNames::from(["group", "key", "text"]),
                        vec![
                            PrimitiveArray::from_iter([row % 33]).into_array(),
                            PrimitiveArray::from_iter([row]).into_array(),
                            VarBinViewArray::from_iter_str([format!("v{row}")]).into_array(),
                        ],
                        1,
                        Validity::NonNullable,
                    )
                    .into_array();
                    aggregate.consume(&array, context, 1)?;
                }
                #[cfg(debug_assertions)]
                let observation = super::super::native_relational_observation::Session::start();
                let (mut checked, mut batches) = (0_u64, 0_usize);
                aggregate.finish(context, output_rows, &mut |batch| {
                    batches += 1;
                    assert!(batch.len() <= output_rows);
                    let columns = spec
                        .fields
                        .iter()
                        .map(|(name, _)| {
                            keys(&logical_field_from_native_array(&batch, name)?, context)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for row in 0..batch.len() {
                        assert_eq!(columns[0].raw_cell(row)?, Cell::NonnegativeInteger(checked));
                        for (index, text) in [
                            format!("v{checked},v{}", checked + 33),
                            format!("v{},v{checked}", checked + 33),
                        ]
                        .into_iter()
                        .enumerate()
                        {
                            assert_eq!(
                                columns[index + 1].raw_cell(row)?,
                                Cell::Utf8(ByteBuffer::from(text.into_bytes()))
                            );
                        }
                        checked += 1;
                    }
                    Ok(())
                })?;
                assert_eq!(checked, 33);
                assert_eq!(batches, 33_usize.div_ceil(output_rows));
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.order_domain_checks, 2, "{counts:?}");
                    assert_eq!(counts.order_checks, 66, "{counts:?}");
                    assert_eq!(counts.ordered_input_sorts, 33, "{counts:?}");
                    assert_eq!(counts.order_heap_sorts, 33, "{counts:?}");
                }
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn ordered_input_window_domain_proof_is_shared_across_order_groups() {
    use super::super::native_relational_window as window;
    use crate::relational_query::VortexRelationalWindowFunction;
    for output_rows in [1, 7, 33] {
        let session = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let spec = window::Spec {
            fields: vec![
                (
                    "id".into(),
                    DType::Primitive(PType::U64, Nullability::NonNullable),
                ),
                (
                    "forward".into(),
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ),
                (
                    "backward".into(),
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ),
            ],
            columns: vec!["id".into()],
            keys: vec!["id".into()],
            parent_validity_keys: vec![],
            functions: vec![VortexRelationalWindowFunction::RowNumber; 2],
            groups: [false, true]
                .into_iter()
                .enumerate()
                .map(|(index, descending)| window::Group {
                    partition: vec![],
                    order: vec![window::OrderKey {
                        key: 0,
                        descending,
                        nulls: None,
                    }],
                    functions: vec![index],
                })
                .collect(),
            frames: vec![None, None],
            fills: vec![None, None],
            ordering_keys: 1,
        };
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let mut window = window::Window::new(&spec, context.memory())?;
                for row in 0_u64..33 {
                    window.build(
                        StructArray::new(
                            FieldNames::from(["id"]),
                            vec![PrimitiveArray::from_iter([row]).into_array()],
                            1,
                            Validity::NonNullable,
                        )
                        .into_array(),
                        context,
                    )?;
                }
                #[cfg(debug_assertions)]
                let observation = super::super::native_relational_observation::Session::start();
                let (mut checked, mut batches) = (0_u64, 0_usize);
                window.finish(context, output_rows, &mut |batch| {
                    batches += 1;
                    assert!(batch.len() <= output_rows);
                    let columns = spec
                        .fields
                        .iter()
                        .map(|(name, _)| {
                            keys(&logical_field_from_native_array(&batch, name)?, context)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for row in 0..batch.len() {
                        for (index, value) in
                            [checked, checked + 1, 33 - checked].into_iter().enumerate()
                        {
                            assert_eq!(
                                columns[index].raw_cell(row)?,
                                Cell::NonnegativeInteger(value)
                            );
                        }
                        checked += 1;
                    }
                    Ok(())
                })?;
                assert_eq!(checked, 33);
                assert_eq!(batches, 33_usize.div_ceil(output_rows));
                #[cfg(debug_assertions)]
                {
                    let counts = observation.finish();
                    assert_eq!(counts.order_domain_checks, 1, "{counts:?}");
                    assert_eq!(counts.order_checks, 2, "{counts:?}");
                    assert_eq!(counts.ordered_input_sorts, 1, "{counts:?}");
                    assert_eq!(counts.order_heap_sorts, 1, "{counts:?}");
                }
                Ok(())
            })
            .unwrap();
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
