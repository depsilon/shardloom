//! Raw vector results keep their selected bits at the real delivery boundaries.

use super::*;
use crate::local_primitives::{logical_field_from_native_array, native_list};
use crate::relational_query::{
    VortexRelationalPipeline, VortexRelationalSpillPolicy, VortexRelationalStage,
};
use vortex::array::arrays::FixedSizeListArray;

const BITS: [[u64; 2]; 4] = [
    [0x7ff0_0000_0000_0042, 0x8000_0000_0000_0000],
    [0, 0],
    [1.0_f64.to_bits(), 2.0_f64.to_bits()],
    [f64::NEG_INFINITY.to_bits(), 0xfff8_0000_0000_0081],
];

fn raw_fixture(rows: usize) -> Fixture {
    let values =
        PrimitiveArray::from_iter((0..rows).flat_map(|row| BITS[row % 4].map(f64::from_bits)))
            .into_array();
    let vectors = FixedSizeListArray::try_new(
        values,
        2,
        Validity::from_iter((0..rows).map(|row| row % 4 != 1)),
        rows,
    )
    .unwrap()
    .into_array();
    Fixture::new(
        StructArray::try_new(
            FieldNames::from(["id", "v", "bad"]),
            vec![
                PrimitiveArray::from_iter((0..rows).map(|row| row as u64)).into_array(),
                vectors,
                PrimitiveArray::from_iter((0..rows).map(|_| f64::INFINITY)).into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        257,
    )
}

fn projection(input: VortexRelationalPlan) -> VortexRelationalPlan {
    project(
        input,
        vec![
            ("id", col("id")),
            ("values", function("vector_values", vec![col("v")])),
            (
                "first",
                function("vector_element", vec![col("v"), lit(ScalarValue::Int64(0))]),
            ),
            (
                "part",
                function(
                    "vector_slice",
                    vec![
                        col("v"),
                        lit(ScalarValue::Int64(0)),
                        lit(ScalarValue::Int64(2)),
                    ],
                ),
            ),
        ],
    )
}

fn ordered(input: VortexRelationalPlan, name: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new(name).unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }))
}

fn aliased(input: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::Pipeline(Box::new(VortexRelationalPipeline {
        input,
        stages: vec![
            VortexRelationalStage::Project(vec![
                ("id".into(), col("id")),
                ("carried".into(), col("values")),
                ("first".into(), col("first")),
                ("part".into(), col("part")),
            ]),
            VortexRelationalStage::Filter(compare(
                col("id"),
                ComparisonOp::NotEq,
                lit(ScalarValue::UInt64(2)),
            )),
            VortexRelationalStage::Project(vec![
                ("id".into(), col("id")),
                ("values".into(), col("carried")),
                ("first".into(), col("first")),
                ("part".into(), col("part")),
            ]),
            VortexRelationalStage::Limit {
                offset: 0,
                count: 3,
            },
        ],
    }))
}

fn check_batch(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Vec<u64>> {
    let mut execution = context.native_session().create_execution_ctx();
    let ids = logical_field_from_native_array(array, "id")?
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(vortex_error)?;
    let first = logical_field_from_native_array(array, "first")?
        .execute::<PrimitiveArray>(&mut execution)
        .map_err(vortex_error)?;
    let values = native_list::Column::new(
        logical_field_from_native_array(array, "values")?,
        &mut execution,
    )?;
    let part = native_list::Column::new(
        logical_field_from_native_array(array, "part")?,
        &mut execution,
    )?;
    for (row, id) in ids.to_buffer::<u64>().iter().enumerate() {
        let index = usize::try_from(id % 4).unwrap();
        let valid = index != 1;
        assert_eq!(
            first
                .validity()
                .map_err(vortex_error)?
                .execute_is_valid(row, &mut execution)
                .map_err(vortex_error)?,
            valid
        );
        if valid {
            assert_eq!(first.to_buffer::<f64>()[row].to_bits(), BITS[index][0]);
        }
        for list in [&values, &part] {
            let span = list.coordinates(row, &mut execution)?;
            assert_eq!(span.is_some(), valid);
            if let Some((start, len)) = span {
                assert_eq!(len, 2);
                let child = list
                    .elements
                    .clone()
                    .execute::<PrimitiveArray>(&mut execution)
                    .map_err(vortex_error)?;
                assert_eq!(
                    child.to_buffer::<f64>()[start..start + len]
                        .iter()
                        .map(|v| v.to_bits())
                        .collect::<Vec<_>>(),
                    BITS[index]
                );
            }
        }
    }
    Ok(ids.to_buffer::<u64>().to_vec())
}

#[test]
fn native_vector_delivery_preserves_strict_raw_fields_through_aliases_filter_order_and_limit() {
    let fixture = raw_fixture(4);
    for mode in ["direct", "aliases", "ordered", "ordered_limit"] {
        let mut plan = projection(fixture.scan());
        let expected = match mode {
            "direct" => vec![0, 1, 2, 3],
            "aliases" => {
                plan = aliased(plan);
                vec![0, 1, 3]
            }
            "ordered" => {
                plan = ordered(plan, "id");
                vec![3, 2, 1, 0]
            }
            _ => {
                plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                    input: ordered(plan, "id"),
                    offset: 1,
                    count: 2,
                }));
                vec![2, 1]
            }
        };
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let memory = prepared.session.memory().clone();
        let mut held = Vec::new();
        let mut actual = Vec::new();
        let report = prepared
            .for_each_batch(&CancellationToken::default(), |array, context| {
                actual.extend(check_batch(&array, context)?);
                held.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected, "{mode}");
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        drop(report);
        drop(prepared);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_vector_delivery_does_not_exempt_siblings_arithmetic_or_order_keys() {
    let fixture = raw_fixture(4);
    for plan in [
        project(
            fixture.scan(),
            vec![
                ("raw", function("vector_values", vec![col("v")])),
                ("bad", col("bad")),
            ],
        ),
        project(
            fixture.scan(),
            vec![(
                "observed",
                function(
                    "vector_norm",
                    vec![function("vector_values", vec![col("v")])],
                ),
            )],
        ),
        ordered(projection(fixture.scan()), "first"),
    ] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let before = prepared.snapshot().memory.reserved_bytes;
        let error = prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                panic!("observed nonfinite input must refuse");
            })
            .err()
            .expect("strict observation error")
            .to_string();
        assert!(error.contains("finite"), "{error}");
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn native_vector_delivery_preserves_strict_raw_fields_through_real_spill() {
    let rows = 32_769;
    let fixture = raw_fixture(rows);
    let workspace = fixture.0.join("spill");
    fs::create_dir(&workspace).unwrap();
    let prepared = prepare_relational(&ordered(projection(fixture.scan()), "id"), policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let mut actual = Vec::new();
    let report = prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            actual.extend(check_batch(&array, context)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, (0..rows as u64).rev().collect::<Vec<_>>());
    let spill = report.spill.as_ref().expect("explicit spill report");
    assert!(spill.runs_written > 0);
    assert!(spill.owned_cleanup_completed);
    assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
}
