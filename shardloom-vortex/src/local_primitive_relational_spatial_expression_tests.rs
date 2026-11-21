use super::*;
use crate::{
    native_spatial::{self, Kind as Geometry},
    resident_session::ResidentVortexSession,
};
use shardloom_core::{ColumnRef, ExprId};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, ExtensionArray, ListViewArray, PrimitiveArray, StructArray},
        dtype::FieldNames,
        validity::Validity,
    },
};

fn col(name: &str) -> Input {
    Input::new(
        ExprId::new("spatial-test").unwrap(),
        ExpressionKind::Column(ColumnRef::new(name).unwrap()),
    )
}
fn lit(value: ScalarValue) -> Input {
    Input::new(
        ExprId::new("spatial-test").unwrap(),
        ExpressionKind::Literal(value),
    )
}
fn call(name: &str, args: Vec<Input>) -> Input {
    Input::new(
        ExprId::new("spatial-test").unwrap(),
        ExpressionKind::FunctionCall {
            name: name.into(),
            args,
        },
    )
}
fn batch(columns: &[(&str, ArrayRef)], rows: usize) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(columns.iter().map(|v| v.0).collect::<Vec<_>>()),
        columns.iter().map(|v| v.1.clone()),
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}
fn coordinates(values: &[(f64, f64)], valid: Validity, dictionary: bool) -> ArrayRef {
    let axis = |x| {
        let values = PrimitiveArray::from_iter(values.iter().map(|v| if x { v.0 } else { v.1 }))
            .into_array();
        if dictionary {
            DictArray::try_new(
                PrimitiveArray::from_iter(0..values.len() as u64).into_array(),
                values,
            )
            .unwrap()
            .into_array()
        } else {
            values
        }
    };
    StructArray::try_new(
        ["x", "y"].into(),
        vec![axis(true), axis(false)],
        values.len(),
        valid,
    )
    .unwrap()
    .into_array()
}
fn list(values: ArrayRef, spans: &[(u64, u64)], valid: Validity) -> ArrayRef {
    ListViewArray::try_new(
        values,
        PrimitiveArray::from_iter(spans.iter().map(|s| s.0)).into_array(),
        PrimitiveArray::from_iter(spans.iter().map(|s| s.1)).into_array(),
        valid,
        &mut vortex::session::VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array()
}
fn geometry(kind: Geometry, storage: ArrayRef, metadata: &str) -> ArrayRef {
    let DType::Extension(dtype) = &native_spatial::dtype(
        native_spatial::Metadata::parse(kind, metadata).unwrap(),
        storage.dtype().clone(),
    )
    .unwrap() else {
        unreachable!()
    };
    ExtensionArray::try_new(dtype.clone(), storage)
        .unwrap()
        .into_array()
}
fn points(values: &[(f64, f64)], valid: Validity, encoded: bool) -> ArrayRef {
    geometry(Geometry::Point, coordinates(values, valid, encoded), "{}")
}
fn polygon(rings: &[Vec<(f64, f64)>], rows: usize) -> ArrayRef {
    let values = rings.iter().flatten().copied().collect::<Vec<_>>();
    let mut start = 0;
    let spans = rings
        .iter()
        .map(|r| {
            let span = (start, r.len() as u64);
            start += span.1;
            span
        })
        .collect::<Vec<_>>();
    let rings = list(
        coordinates(&values, Validity::NonNullable, false),
        &spans,
        Validity::NonNullable,
    );
    let count = rings.len() as u64;
    geometry(
        Geometry::Polygon,
        list(rings, &vec![(0, count); rows], Validity::NonNullable),
        "{}",
    )
}
fn evaluate(
    session: &ResidentVortexSession,
    expression: &Input,
    input: &ArrayRef,
) -> Result<ArrayRef> {
    let DType::Struct(fields, _) = input.dtype() else {
        panic!("record");
    };
    let fields = fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(n, d)| (n.to_string(), d))
        .collect::<Vec<_>>();
    let mut binder = Binder::new(session)?;
    let expression = binder.expression(expression, &fields, 0)?;
    session.with_native_execution_context(&CancellationToken::default(), |context| {
        expression.evaluate(input, context)
    })
}
fn numbers(array: &ArrayRef) -> Vec<Option<f64>> {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|i| {
            array
                .execute_scalar(i, &mut execution)
                .unwrap()
                .as_primitive()
                .as_::<f64>()
        })
        .collect()
}
fn booleans(array: &ArrayRef) -> Vec<Option<bool>> {
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|i| {
            array
                .execute_scalar(i, &mut execution)
                .unwrap()
                .as_bool()
                .value()
        })
        .collect()
}

#[test]
fn native_spatial_point_distance_empty_null_and_encoded_coordinates_agree() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for encoded in [false, true] {
        let a = points(
            &[
                (0.0, 0.0),
                (f64::NAN, f64::NAN),
                (f64::INFINITY, 0.0),
                (-0.0, 0.0),
            ],
            Validity::from_iter([true, true, false, true]),
            encoded,
        );
        let b = points(&[(3.0, 4.0); 4], Validity::NonNullable, encoded);
        let input = batch(&[("a", a), ("b", b)], 4);
        assert_eq!(
            numbers(
                &evaluate(
                    &session,
                    &call("st_distance", vec![col("a"), col("b")]),
                    &input
                )
                .unwrap()
            ),
            vec![Some(5.0), None, None, Some(5.0)]
        );
        assert_eq!(
            booleans(&evaluate(&session, &call("st_isempty", vec![col("a")]), &input).unwrap()),
            vec![Some(false), Some(true), None, Some(false)]
        );
        assert_eq!(
            booleans(
                &evaluate(
                    &session,
                    &call(
                        "st_dwithin",
                        vec![col("a"), col("b"), lit(ScalarValue::Float64(5.0))]
                    ),
                    &input
                )
                .unwrap()
            ),
            vec![Some(true), Some(false), None, Some(true)]
        );
        assert_eq!(
            numbers(&evaluate(&session, &call("st_x", vec![col("a")]), &input).unwrap())[3]
                .unwrap()
                .to_bits(),
            (-0.0_f64).to_bits()
        );
    }
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_distance_predicate_does_not_round_away_extreme_differences() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let tiny = f64::from_bits(1);
    let input = batch(
        &[
            (
                "a",
                points(
                    &[(tiny, 0.0), (f64::MAX, 0.0), (0.0, 0.0)],
                    Validity::NonNullable,
                    false,
                ),
            ),
            (
                "b",
                points(
                    &[(0.0, 0.0), (-f64::MAX, 0.0), (3.0, 4.0)],
                    Validity::NonNullable,
                    false,
                ),
            ),
        ],
        3,
    );
    assert_eq!(
        booleans(
            &evaluate(
                &session,
                &call(
                    "st_dwithin",
                    vec![col("a"), col("b"), lit(ScalarValue::Float64(0.0))]
                ),
                &input
            )
            .unwrap()
        ),
        vec![Some(false); 3]
    );
    assert_eq!(
        booleans(
            &evaluate(
                &session,
                &call(
                    "st_dwithin",
                    vec![col("a"), col("b"), lit(ScalarValue::Float64(f64::MAX))]
                ),
                &input
            )
            .unwrap()
        ),
        vec![Some(true), Some(false), Some(true)]
    );
    assert!(
        evaluate(
            &session,
            &call("st_distance", vec![col("a"), col("b")]),
            &input
        )
        .is_err()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_polygon_holes_boundary_orientation_and_bounds() {
    let exterior = vec![
        (0.0, 0.0),
        (10.0, 0.0),
        (10.0, 10.0),
        (0.0, 10.0),
        (0.0, 0.0),
    ];
    let hole = vec![(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)];
    let p = [
        (1.0, 1.0),
        (5.0, 5.0),
        (0.0, 2.0),
        (3.0, 4.0),
        (11.0, 1.0),
        (f64::NAN, f64::NAN),
    ];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for reverse in [false, true] {
        let mut rings = vec![exterior.clone(), hole.clone()];
        if reverse {
            for ring in &mut rings {
                ring.reverse();
            }
        }
        let input = batch(
            &[
                ("g", polygon(&rings, p.len())),
                ("p", points(&p, Validity::NonNullable, false)),
            ],
            p.len(),
        );
        assert_eq!(
            booleans(
                &evaluate(
                    &session,
                    &call("st_contains", vec![col("g"), col("p")]),
                    &input
                )
                .unwrap()
            ),
            vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(false),
                Some(false)
            ]
        );
        assert_eq!(
            booleans(
                &evaluate(
                    &session,
                    &call("st_covers", vec![col("g"), col("p")]),
                    &input
                )
                .unwrap()
            ),
            vec![
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                Some(false),
                Some(false)
            ]
        );
        assert_eq!(
            numbers(&evaluate(&session, &call("st_xmax", vec![col("g")]), &input).unwrap()),
            vec![Some(10.0); p.len()]
        );
    }
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_invalid_topology_cannot_hide_behind_rejected_bounds() {
    let square = vec![(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)];
    let invalid = [
        vec![vec![
            (0.0, 0.0),
            (4.0, 4.0),
            (0.0, 4.0),
            (4.0, 0.0),
            (0.0, 0.0),
        ]],
        vec![vec![(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (0.0, 0.0)]],
        vec![
            square.clone(),
            vec![(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 5.0)],
        ],
        vec![
            square.clone(),
            vec![(0.0, 0.0), (1.0, 1.0), (1.0, 2.0), (0.0, 0.0)],
        ],
        vec![
            square,
            vec![(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)],
            vec![(1.5, 1.5), (2.0, 1.5), (2.0, 2.0), (1.5, 1.5)],
        ],
    ];
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for rings in invalid {
        let input = batch(
            &[
                ("g", polygon(&rings, 1)),
                ("p", points(&[(100.0, 100.0)], Validity::NonNullable, false)),
            ],
            1,
        );
        assert!(
            evaluate(
                &session,
                &call("st_covers", vec![col("g"), col("p")]),
                &input
            )
            .is_err()
        );
        evaluate(&session, &call("st_xmin", vec![col("g")]), &input).unwrap();
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn native_spatial_constructor_length_and_interpretation_refusals() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let input = batch(&[], 2);
    let make = call(
        "st_point",
        vec![lit(ScalarValue::Int64(3)), lit(ScalarValue::Int64(4))],
    );
    assert_eq!(
        numbers(&evaluate(&session, &call("st_x", vec![make]), &input).unwrap()),
        vec![Some(3.0); 2]
    );
    assert_eq!(
        booleans(
            &evaluate(
                &session,
                &call("st_isempty", vec![call("st_emptypoint", vec![])]),
                &input
            )
            .unwrap()
        ),
        vec![Some(true); 2]
    );
    assert!(
        evaluate(
            &session,
            &call(
                "st_point",
                vec![
                    lit(ScalarValue::UInt64(u64::MAX)),
                    lit(ScalarValue::Int64(0))
                ]
            ),
            &input
        )
        .is_err()
    );
    let line = geometry(
        Geometry::LineString,
        list(
            coordinates(
                &[(0.0, 0.0), (3.0, 4.0), (6.0, 0.0)],
                Validity::NonNullable,
                true,
            ),
            &[(0, 3), (0, 0)],
            Validity::NonNullable,
        ),
        "{}",
    );
    assert_eq!(
        numbers(
            &evaluate(
                &session,
                &call("st_length", vec![col("v")]),
                &batch(&[("v", line)], 2)
            )
            .unwrap()
        ),
        vec![Some(10.0), Some(0.0)]
    );
    for raw in [r#"{"edges":"spherical"}"#, r#"{"crs":"other"}"#] {
        let a = geometry(
            Geometry::Point,
            coordinates(&[(0.0, 0.0)], Validity::NonNullable, false),
            raw,
        );
        let input = batch(
            &[
                ("a", a),
                ("b", points(&[(1.0, 0.0)], Validity::NonNullable, false)),
            ],
            1,
        );
        assert!(
            evaluate(
                &session,
                &call("st_distance", vec![col("a"), col("b")]),
                &input
            )
            .is_err()
        );
        evaluate(&session, &call("st_x", vec![col("a")]), &input).unwrap();
    }
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_selected_payloads_preserve_empty_and_ignore_hidden_invalid_values() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = points(
        &[
            (f64::NAN, f64::NAN),
            (f64::INFINITY, 0.0),
            (f64::INFINITY, 1.0),
            (-0.0, 2.0),
        ],
        Validity::from_iter([true, true, false, true]),
        true,
    );
    let selected = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let indices = PrimitiveArray::from_iter([0_u64, 2, 3]).into_array();
            crate::local_primitives::native_payload::take(
                &source,
                &indices,
                source.dtype(),
                context,
            )
        })
        .unwrap();
    let input = batch(&[("g", selected)], 3);
    assert_eq!(
        booleans(&evaluate(&session, &call("st_isempty", vec![col("g")]), &input).unwrap()),
        vec![Some(true), None, Some(false)]
    );
    let result = evaluate(&session, &call("st_x", vec![col("g")]), &input).unwrap();
    assert_eq!(numbers(&result)[0..2], [None, None]);
    assert_eq!(numbers(&result)[2].unwrap().to_bits(), (-0.0_f64).to_bits());
    assert!(
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let indices = PrimitiveArray::from_iter([1_u64]).into_array();
                crate::local_primitives::native_payload::take(
                    &source,
                    &indices,
                    source.dtype(),
                    context,
                )
            })
            .is_err()
    );
    drop((input, result));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_constructor_null_mask_precedes_conversion_and_radius_is_checked() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let input = batch(
        &[
            ("x", PrimitiveArray::from_iter([u64::MAX, 3]).into_array()),
            (
                "y",
                PrimitiveArray::from_option_iter([None, Some(4_i64)]).into_array(),
            ),
        ],
        2,
    );
    let make = call("st_point", vec![col("x"), col("y")]);
    assert_eq!(
        numbers(&evaluate(&session, &call("st_y", vec![make.clone()]), &input).unwrap()),
        vec![None, Some(4.0)]
    );
    assert_eq!(
        booleans(
            &evaluate(
                &session,
                &call(
                    "st_dwithin",
                    vec![make.clone(), make.clone(), lit(ScalarValue::Float64(0.0))]
                ),
                &input
            )
            .unwrap()
        ),
        vec![None, Some(true)]
    );
    assert!(
        evaluate(
            &session,
            &call(
                "st_dwithin",
                vec![make.clone(), make, lit(ScalarValue::Float64(-1.0))]
            ),
            &input
        )
        .is_err()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_vertex_counts_and_y_bounds_cover_empty_lines_and_polygons() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let line = geometry(
        Geometry::LineString,
        list(
            coordinates(&[(2.0, -3.0), (4.0, 5.0)], Validity::NonNullable, true),
            &[(0, 2), (0, 0)],
            Validity::NonNullable,
        ),
        "{}",
    );
    let input = batch(&[("g", line)], 2);
    let result = evaluate(&session, &call("st_npoints", vec![col("g")]), &input).unwrap();
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    assert_eq!(
        (0..2)
            .map(|i| result
                .execute_scalar(i, &mut execution)
                .unwrap()
                .as_primitive()
                .as_::<u64>())
            .collect::<Vec<_>>(),
        vec![Some(2), Some(0)]
    );
    assert_eq!(
        numbers(&evaluate(&session, &call("st_ymin", vec![col("g")]), &input).unwrap()),
        vec![Some(-3.0), None]
    );
    assert_eq!(
        numbers(&evaluate(&session, &call("st_ymax", vec![col("g")]), &input).unwrap()),
        vec![Some(5.0), None]
    );
    let empty = batch(&[("g", polygon(&[], 1))], 1);
    assert_eq!(
        booleans(&evaluate(&session, &call("st_isempty", vec![col("g")]), &empty).unwrap()),
        vec![Some(true)]
    );
    drop((result, execution));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_spatial_cancellation_denial_and_final_buffer_release_refund_all_credits() {
    let geometry = points(&vec![(3.0, 4.0); 2048], Validity::NonNullable, false);
    let input = batch(&[("g", geometry.clone())], 2048);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    let error = session
        .with_native_execution_context(&cancellation, |context| {
            crate::local_primitives::native_spatial_input::Column::new(&geometry, context)
                .map(|_| ())
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let denied = ResidentVortexSession::new(4096, 1).unwrap();
    assert!(evaluate(&denied, &call("st_x", vec![col("g")]), &input).is_err());
    assert_eq!(denied.snapshot().memory.reserved_bytes, 0);
    let result = evaluate(&session, &call("st_x", vec![col("g")]), &input).unwrap();
    let mut execution = vortex::session::VortexSession::default().create_execution_ctx();
    let primitive = result
        .clone()
        .execute::<PrimitiveArray>(&mut execution)
        .unwrap();
    let retained = primitive.to_buffer::<f64>();
    drop((result, primitive, session, execution));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(
        retained
            .iter()
            .all(|value| value.to_bits() == 3.0_f64.to_bits())
    );
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_spatial_polygon_orientation_survives_underflow_and_overflow() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let tiny = f64::from_bits(1);
    for (ring, queries) in [
        (
            vec![(0.0, 0.0), (4.0 * tiny, 0.0), (0.0, 4.0 * tiny), (0.0, 0.0)],
            [
                (tiny, tiny),
                (2.0 * tiny, 2.0 * tiny),
                (4.0 * tiny, 4.0 * tiny),
            ],
        ),
        (
            vec![
                (-f64::MAX, -f64::MAX),
                (f64::MAX, -f64::MAX),
                (0.0, f64::MAX),
                (-f64::MAX, -f64::MAX),
            ],
            [(0.0, 0.0), (0.0, -f64::MAX), (f64::MAX, f64::MAX)],
        ),
    ] {
        for reverse in [false, true] {
            let mut ring = ring.clone();
            if reverse {
                ring.reverse();
            }
            let input = batch(
                &[
                    ("g", polygon(&[ring], 3)),
                    ("p", points(&queries, Validity::NonNullable, false)),
                ],
                3,
            );
            assert_eq!(
                booleans(
                    &evaluate(
                        &session,
                        &call("st_contains", vec![col("g"), col("p")]),
                        &input
                    )
                    .unwrap()
                ),
                vec![Some(true), Some(false), Some(false)]
            );
            assert_eq!(
                booleans(
                    &evaluate(
                        &session,
                        &call("st_covers", vec![col("g"), col("p")]),
                        &input
                    )
                    .unwrap()
                ),
                vec![Some(true), Some(true), Some(false)]
            );
        }
    }
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
