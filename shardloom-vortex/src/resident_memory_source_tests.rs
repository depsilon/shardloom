use super::*;
use vortex::{
    VortexSessionDefault as _,
    expr::{get_item, gt_eq, lit},
    session::VortexSession,
};

fn fixture(session: &ResidentVortexSession, bounds: MemorySourceBounds) -> ResidentMemorySource {
    ResidentMemorySource::from_columns(
        session,
        &[
            MemoryColumn {
                name: "identifier",
                values: MemoryColumnValues::Int64(&[Some(i64::MAX), None, Some(i64::MIN), Some(7)]),
            },
            MemoryColumn {
                name: "measurement",
                values: MemoryColumnValues::Float64(&[Some(1.25), None, Some(-0.0), Some(1.5)]),
            },
            MemoryColumn {
                name: "admitted",
                values: MemoryColumnValues::Bool(&[Some(true), None, Some(false), Some(false)]),
            },
            MemoryColumn {
                name: "label",
                values: MemoryColumnValues::Utf8(&[
                    Some("λ\"\n東京"),
                    None,
                    Some("kept"),
                    Some(""),
                ]),
            },
        ],
        bounds,
    )
    .unwrap()
}

#[test]
fn typed_snapshot_copies_borrowed_input_and_preserves_complete_scalar_values() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 2).unwrap();
    let mut ids = vec![Some(i64::MAX), None, Some(i64::MIN)];
    let source = ResidentMemorySource::from_columns(
        &session,
        &[MemoryColumn {
            name: "renamed",
            values: MemoryColumnValues::Int64(&ids),
        }],
        MemorySourceBounds::default(),
    )
    .unwrap();
    ids[0] = Some(1);
    drop(ids);
    let projection = source.prepare_projection(&["renamed"], None, None).unwrap();
    for _ in 0..3 {
        let result = projection.execute_arrays().unwrap();
        assert_eq!(result.row_count(), 3);
        let mut context = result.create_execution_ctx();
        let field = result.arrays()[0].named_children()[0].1.clone();
        assert_eq!(
            field.execute_scalar(0, &mut context).unwrap(),
            i64::MAX.into()
        );
        assert!(field.execute_scalar(1, &mut context).unwrap().is_null());
        assert_eq!(
            field.execute_scalar(2, &mut context).unwrap(),
            i64::MIN.into()
        );
    }
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    assert_eq!(session.snapshot().completed_executions, 3);
}

#[test]
fn nullable_native_filter_and_limit_keep_source_order_and_real_array_ownership() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 2).unwrap();
    let memory = session.memory().clone();
    let source = fixture(&session, MemorySourceBounds::default());
    let fields = source.dtype().as_struct_fields();
    for (name, dtype) in [
        (
            "identifier",
            DType::Primitive(vortex::array::dtype::PType::I64, Nullability::Nullable),
        ),
        (
            "measurement",
            DType::Primitive(vortex::array::dtype::PType::F64, Nullability::Nullable),
        ),
        ("admitted", DType::Bool(Nullability::Nullable)),
        ("label", DType::Utf8(Nullability::Nullable)),
    ] {
        assert_eq!(fields.field(name), Some(dtype));
    }
    let projection = source
        .prepare_projection(
            &["label", "admitted", "measurement", "identifier"],
            Some(gt_eq(get_item("identifier", root()), lit(0_i64))),
            Some(1),
        )
        .unwrap();
    let result = projection.execute_arrays().unwrap();
    assert_eq!(result.row_count(), 1);
    let arrays = result.arrays().to_vec();
    drop(result);
    drop(projection);
    drop(source);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    let provider = VortexSession::default();
    let mut context = provider.create_execution_ctx();
    let fields = arrays[0]
        .named_children()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        fields["identifier"]
            .execute_scalar(0, &mut context)
            .unwrap(),
        i64::MAX.into()
    );
    assert_eq!(
        fields["measurement"]
            .execute_scalar(0, &mut context)
            .unwrap(),
        1.25_f64.into()
    );
    assert_eq!(
        fields["admitted"].execute_scalar(0, &mut context).unwrap(),
        true.into()
    );
    assert_eq!(
        fields["label"].execute_scalar(0, &mut context).unwrap(),
        "λ\"\n東京".into()
    );
    drop(fields);
    drop(context);
    drop(arrays);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn invalid_input_fails_before_native_allocation_and_partial_admission_releases_credits() {
    let session = ResidentVortexSession::new(1024 * 1024, 1).unwrap();
    let integers = [Some(1_i64), None];
    let one = MemoryColumn {
        name: "id",
        values: MemoryColumnValues::Int64(&integers),
    };
    for columns in [
        vec![one, one],
        vec![
            one,
            MemoryColumn {
                name: "other",
                values: MemoryColumnValues::Bool(&[]),
            },
        ],
        vec![MemoryColumn {
            name: "x",
            values: MemoryColumnValues::Float64(&[Some(f64::NAN)]),
        }],
    ] {
        assert!(
            ResidentMemorySource::from_columns(&session, &columns, MemorySourceBounds::default())
                .is_err()
        );
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        assert_eq!(session.snapshot().memory.peak_reserved_bytes, 0);
    }
    for bounds in [
        MemorySourceBounds {
            max_input_rows: 1,
            ..MemorySourceBounds::default()
        },
        MemorySourceBounds {
            max_input_bytes: 1,
            ..MemorySourceBounds::default()
        },
    ] {
        assert!(ResidentMemorySource::from_columns(&session, &[one], bounds).is_err());
        assert_eq!(session.snapshot().memory.peak_reserved_bytes, 0);
    }
    let tight = ResidentVortexSession::new(270, 1).unwrap();
    assert!(
        ResidentMemorySource::from_columns(&tight, &[one], MemorySourceBounds::default()).is_err()
    );
    assert_eq!(tight.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn output_bounds_and_nonboolean_filters_fail_explicitly() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let source = fixture(
        &session,
        MemorySourceBounds {
            max_output_rows: 2,
            ..MemorySourceBounds::default()
        },
    );
    assert!(
        source
            .prepare_projection(&["identifier"], None, Some(3))
            .is_err()
    );
    assert!(source.prepare_projection(&["missing"], None, None).is_err());
    assert!(
        source
            .prepare_projection(&["identifier", "identifier"], None, None)
            .is_err()
    );
    assert!(
        source
            .prepare_projection(&["identifier"], Some(lit(1_i64)), None)
            .is_err()
    );
    let operation = source
        .prepare_projection(&["identifier"], None, None)
        .unwrap();
    assert!(operation.execute_arrays().is_err());
    let source = fixture(
        &session,
        MemorySourceBounds {
            max_output_bytes: 1,
            ..MemorySourceBounds::default()
        },
    );
    assert!(
        source
            .prepare_projection(&["identifier"], None, None)
            .unwrap()
            .execute_arrays()
            .is_err()
    );
}

#[test]
fn empty_typed_sources_and_empty_limits_return_empty_owned_arrays() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let source = ResidentMemorySource::from_columns(
        &session,
        &[MemoryColumn {
            name: "empty",
            values: MemoryColumnValues::Utf8(&[]),
        }],
        MemorySourceBounds::default(),
    )
    .unwrap();
    assert_eq!(source.row_count(), 0);
    assert_eq!(
        source
            .prepare_projection(&["empty"], None, None)
            .unwrap()
            .execute_arrays()
            .unwrap()
            .row_count(),
        0
    );
    let source = fixture(&session, MemorySourceBounds::default());
    assert_eq!(
        source
            .prepare_projection(&["label"], None, Some(0))
            .unwrap()
            .execute_arrays()
            .unwrap()
            .row_count(),
        0
    );
}

#[test]
fn completed_json_keeps_exact_nullable_values_and_owned_credits_after_source_drop() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let memory = session.memory().clone();
    let source = fixture(&session, MemorySourceBounds::default());
    let projection = source
        .prepare_projection(
            &["identifier", "measurement", "admitted", "label"],
            None,
            None,
        )
        .unwrap();
    let result = projection.execute().unwrap();
    let expected = serde_json::json!([
        {"identifier": i64::MAX, "measurement": 1.25, "admitted": true, "label": "λ\"\n東京"},
        {"identifier": null, "measurement": null, "admitted": null, "label": null},
        {"identifier": i64::MIN, "measurement": -0.0, "admitted": false, "label": "kept"},
        {"identifier": 7, "measurement": 1.5, "admitted": false, "label": ""}
    ]);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
        expected
    );
    assert_eq!(
        projection.execute().unwrap().values_json.value(),
        result.values_json.value()
    );
    let certificate = &result.native_io_certificate;
    assert_eq!(
        certificate.source_capability_report.source_kind,
        "typed_memory_batch"
    );
    assert!(!certificate.source_capability_report.range_read_capability);
    assert!(
        !certificate
            .source_capability_report
            .encoded_representation_preserved
    );
    assert!(!certificate.side_effects.write_io);
    assert!(!certificate.side_effects.fallback_attempted);
    drop(projection);
    drop(source);
    drop(session);
    assert!(memory.snapshot().reserved_bytes >= result.values_json.value().len() as u64);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(result.values_json.value()).unwrap(),
        expected
    );
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn json_escape_expansion_obeys_output_cap_and_releases_failed_result() {
    let session = ResidentVortexSession::new(2 * 1024 * 1024, 1).unwrap();
    let escaped = "\n".repeat(128);
    let source = ResidentMemorySource::from_columns(
        &session,
        &[MemoryColumn {
            name: "label",
            values: MemoryColumnValues::Utf8(&[Some(&escaped)]),
        }],
        MemorySourceBounds {
            max_output_bytes: 200,
            ..MemorySourceBounds::default()
        },
    )
    .unwrap();
    let projection = source.prepare_projection(&["label"], None, None).unwrap();
    assert!(projection.execute_arrays().is_ok());
    let before = session.snapshot().memory.reserved_bytes;
    assert!(projection.execute().is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, before);
}
