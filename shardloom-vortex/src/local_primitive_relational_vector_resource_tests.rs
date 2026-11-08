//! Admission refusal, retry and extracted-result ownership at expression scope.

use super::*;

#[test]
fn native_vector_denied_scratch_and_selected_expansion_release_before_retry() {
    let input = batch(
        &[(
            "v",
            list(
                PrimitiveArray::from_iter(vec![0.5f64; 4096]).into_array(),
                &[(0, 4096)],
                Validity::NonNullable,
            ),
        )],
        1,
    );
    for name in [
        "vector_dot",
        "vector_norm",
        "vector_l2_distance",
        "vector_cosine_similarity",
        "vector_normalize",
    ] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let mut binder = Binder::new(&session).unwrap();
        let fields = [(
            "v".into(),
            crate::local_primitives::logical_field_from_native_array(&input, "v")
                .unwrap()
                .dtype()
                .clone(),
        )];
        let args = if matches!(name, "vector_norm" | "vector_normalize") {
            vec![col("v")]
        } else {
            vec![col("v"), col("v")]
        };
        let bound = binder.expression(&call(name, args), &fields, 0).unwrap();
        let baseline = memory.snapshot().reserved_bytes;
        for available in [0, 512, 8192, 16_384] {
            let held = memory
                .reserve(memory.snapshot().limit_bytes - baseline - available)
                .unwrap();
            let before = memory.snapshot().reserved_bytes;
            let error = session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    bound.evaluate(&input, context)
                })
                .unwrap_err();
            assert!(error.to_string().contains("memory"), "{name}: {error}");
            assert_eq!(
                memory.snapshot().reserved_bytes,
                before,
                "{name} {available}"
            );
            drop(held);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
        }
        let result = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                bound.evaluate(&input, context)
            })
            .unwrap();
        assert_eq!(result.len(), 1);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop((bound, binder));
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_vector_result_children_slices_and_bare_buffers_outlive_the_expression() {
    for name in [
        "vector_values",
        "vector_slice",
        "vector_normalize",
        "vector_element",
        "vector_norm",
    ] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let literal = list_literal(&[Some(3.0), Some(4.0)]);
        let args = if matches!(name, "vector_slice" | "vector_element") {
            vec![literal, lit(ScalarValue::Int64(0))]
        } else {
            vec![literal]
        };
        let output = evaluate(&session, &call(name, args), &batch(&[], 3)).unwrap();
        let slice = output.slice(1..2).unwrap();
        drop((output, session));
        assert!(memory.snapshot().reserved_bytes > 0, "{name}");
        let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
        let child = if matches!(
            slice.dtype(),
            DType::List(_, _) | DType::FixedSizeList(_, _, _)
        ) {
            crate::local_primitives::native_list::Column::new(slice.clone(), &mut execution)
                .unwrap()
                .elements
        } else {
            slice.clone()
        };
        let primitive = child
            .clone()
            .execute::<PrimitiveArray>(&mut execution)
            .unwrap();
        let bytes = primitive.to_buffer::<f64>();
        assert!(!bytes.is_empty());
        drop((slice, child, primitive, execution));
        assert!(memory.snapshot().reserved_bytes > 0, "bare {name}");
        assert!(bytes.iter().all(|v| v.is_finite()));
        drop(bytes);
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{name}");
    }
}
