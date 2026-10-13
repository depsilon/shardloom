use super::*;

#[test]
fn native_selection_preserves_values_nulls_and_row_order() {
    let input = keyed(&[Some(11), None, Some(29), Some(41)], &[5, 6, 7, 8]);
    let fields = vec![
        (
            "entity".to_owned(),
            DType::Primitive(PType::U64, Nullability::Nullable),
        ),
        (
            "amount".to_owned(),
            DType::Primitive(PType::U32, Nullability::NonNullable),
        ),
    ];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for rows in [
        vec![],
        vec![0, 1, 2, 3],
        vec![1, 2],
        vec![3],
        vec![0, 2],
        vec![2, 2, 1],
        vec![3, 2, 1, 0],
    ] {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let selected = select_batch(&input, &fields, &rows, context)?;
                assert_eq!(selected.dtype(), input.dtype());
                assert_eq!(selected.len(), rows.len());
                let mut execution = context.native_session().create_execution_ctx();
                for (output, input_row) in rows.iter().enumerate() {
                    assert_eq!(
                        selected.execute_scalar(output, &mut execution).unwrap(),
                        input.execute_scalar(*input_row, &mut execution).unwrap()
                    );
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn consecutive_native_selection_needs_no_row_index_buffer() {
    let column = vortex::array::arrays::ConstantArray::new(37i64, 4096).into_array();
    let input = StructArray::from_fields(&[("value", column)])
        .unwrap()
        .into_array();
    let fields = vec![(
        "value".to_owned(),
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let rows = (17..2065).collect::<Vec<_>>();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let selected = select_batch(&input, &fields, &rows, context)?;
            assert_eq!(selected.len(), rows.len());
            let value =
                crate::local_primitives::logical_field_from_native_array(&selected, "value")?;
            assert_eq!(value.encoding_id().as_ref(), "vortex.constant");
            assert_eq!(value.nchildren(), 0);
            Ok(())
        })
        .unwrap();
    assert!(session.snapshot().memory.peak_reserved_bytes < (rows.len() * 8) as u64);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_selection_rejects_cancelled_and_out_of_bounds_slices() {
    let input = keyed(&[Some(1), None], &[5, 6]);
    let fields = vec![(
        "entity".to_owned(),
        DType::Primitive(PType::U64, Nullability::Nullable),
    )];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let cancelled =
        session.with_native_execution_context(&CancellationToken::default(), |context| {
            assert!(select_batch(&input, &fields, &[2], context).is_err());
            assert!(select_batch(&input, &fields, &[1, 2], context).is_err());
            context.cancellation().cancel();
            assert!(select_batch(&input, &fields, &[0, 1], context).is_err());
            assert!(select_batch(&input, &fields, &[], context).is_err());
            Ok(())
        });
    assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
