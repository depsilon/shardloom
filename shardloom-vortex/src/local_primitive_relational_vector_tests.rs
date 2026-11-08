//! Untrusted native files must validate selected vector relations at observation.

use super::*;
use crate::{
    local_primitives::VortexLocalPrimitiveRowExportFormat as Format,
    native_vector::{Kind, Metadata},
};
use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
use vortex::array::arrays::{ExtensionArray, ListArray};

fn listed(values: ArrayRef, offsets: &[u64]) -> ArrayRef {
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    ListArray::try_new(
        values,
        PrimitiveArray::from_iter(offsets.iter().copied()).into_array(),
        Validity::NonNullable,
        &mut execution,
    )
    .unwrap()
    .into_array()
}

fn fixture(kind: Kind, blocked: bool, hide_bad: bool) -> Fixture {
    let (names, fields, raw) = match kind {
        Kind::Sparse => (
            ["size", "indices", "values"],
            vec![
                PrimitiveArray::from_iter([u64::MAX, 0, 2]).into_array(),
                listed(
                    PrimitiveArray::from_iter([u64::MAX - 1, 0, u64::MAX - 1, 2]).into_array(),
                    &[0, 3, 3, 4],
                ),
                listed(
                    PrimitiveArray::from_option_iter([Some(-0.0_f64), None, Some(1.5), Some(2.0)])
                        .into_array(),
                    &[0, 3, 3, 4],
                ),
            ],
            r#"{"version":1,"index_policy":"preserve","space_id":"source Ω"}"#,
        ),
        Kind::Quantized => {
            let codes = listed(
                PrimitiveArray::from_option_iter([Some(u64::MAX), None, Some(0), Some(1)])
                    .into_array(),
                &[0, 3, 3, 4],
            );
            if blocked {
                (
                    ["codes", "scale", "zero_point"],
                    vec![
                        codes,
                        listed(
                            PrimitiveArray::from_iter([0.5_f32, 2.0, 0.0]).into_array(),
                            &[0, 2, 2, 3],
                        ),
                        listed(
                            PrimitiveArray::from_iter([0_u64, u64::MAX, 0]).into_array(),
                            &[0, 2, 2, 3],
                        ),
                    ],
                    r#"{"version":1,"parameter_mode":"blocked","block_size":2}"#,
                )
            } else {
                (
                    ["codes", "scale", "zero_point"],
                    vec![
                        codes,
                        PrimitiveArray::from_iter([0.5_f32, 1.0, 0.0]).into_array(),
                        PrimitiveArray::from_iter([u64::MAX, 0, 0]).into_array(),
                    ],
                    r#"{"version":1,"parameter_mode":"per_vector"}"#,
                )
            }
        }
    };
    let storage =
        StructArray::new(FieldNames::from(names), fields, 3, Validity::NonNullable).into_array();
    let DType::Extension(dtype) =
        crate::native_vector::dtype(Metadata::parse(kind, raw).unwrap(), storage.dtype().clone())
            .unwrap()
    else {
        unreachable!()
    };
    let vector = ExtensionArray::try_new(dtype, storage)
        .unwrap()
        .into_array();
    let parent = StructArray::new(
        FieldNames::from(["vector"]),
        vec![vector],
        3,
        Validity::from_iter([true, true, !hide_bad]),
    )
    .into_array();
    // The low-level writer intentionally bypasses public intake validation.
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "parent"]),
            vec![
                PrimitiveArray::from_iter([1_i64, 2, 3]).into_array(),
                parent,
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        3,
    )
}

fn expected(kind: Kind, blocked: bool, hidden: bool) -> Vec<serde_json::Value> {
    let vector = match (kind, blocked) {
        (Kind::Sparse, _) => [
            serde_json::json!({"size":u64::MAX,"indices":[u64::MAX-1,0,u64::MAX-1],"values":[-0.0,null,1.5]}),
            serde_json::json!({"size":0,"indices":[],"values":[]}),
        ],
        (Kind::Quantized, false) => [
            serde_json::json!({"codes":[u64::MAX,null,0],"scale":0.5,"zero_point":u64::MAX}),
            serde_json::json!({"codes":[],"scale":1.0,"zero_point":0}),
        ],
        (Kind::Quantized, true) => [
            serde_json::json!({"codes":[u64::MAX,null,0],"scale":[0.5,2.0],"zero_point":[0,u64::MAX]}),
            serde_json::json!({"codes":[],"scale":[],"zero_point":[]}),
        ],
    };
    let mut rows = vector
        .into_iter()
        .enumerate()
        .map(|(row, vector)| serde_json::json!({"id":row+1,"parent":{"vector":vector}}))
        .collect::<Vec<_>>();
    if hidden {
        rows.push(serde_json::json!({"id":3,"parent":null}));
    }
    rows
}

fn filtered(fixture: &Fixture) -> VortexRelationalPlan {
    let VortexRelationalPlan::Scan(mut scan) = fixture.scan() else {
        unreachable!()
    };
    scan.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("id").unwrap(),
        op: ComparisonOp::Lt,
        value: StatValue::Int64(3),
    });
    VortexRelationalPlan::Scan(scan)
}

#[test]
fn native_vector_native_file_selection_collection_stream_and_writes_ignore_hidden_invalid_values() {
    for (kind, blocked) in [
        (Kind::Sparse, false),
        (Kind::Quantized, false),
        (Kind::Quantized, true),
    ] {
        for hidden in [false, true] {
            let fixture = fixture(kind, blocked, hidden);
            let plan = if hidden {
                fixture.scan()
            } else {
                filtered(&fixture)
            };
            let prepared = prepare_relational(&plan, policy()).unwrap();
            let memory = prepared.session.memory().clone();
            let expected = expected(kind, blocked, hidden);
            assert_eq!(
                json_rows(
                    &prepared
                        .collect_jsonl(&CancellationToken::default())
                        .unwrap()
                ),
                expected
            );
            let retained = prepared.execute_owned().unwrap();
            assert_eq!(
                retained.result.row_count(),
                u64::try_from(expected.len()).unwrap()
            );
            let mut streamed = 0;
            prepared
                .for_each_batch(&CancellationToken::default(), |array, _| {
                    streamed += array.len();
                    Ok(())
                })
                .unwrap();
            assert_eq!(streamed, expected.len());
            for format in [Format::Vortex, Format::ArrowIpc, Format::Parquet] {
                let path = fixture.0.join(format!("good.{}", format.as_str()));
                let report = prepared.write(&path, format, false).unwrap();
                assert_eq!(
                    report.output.rows_written,
                    u64::try_from(expected.len()).unwrap()
                );
                assert!(report.execution.native_io_certificate.is_certified());
            }
            drop(retained);
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_vector_native_file_visible_invalid_values_fail_before_retention_and_publication() {
    for (kind, blocked) in [
        (Kind::Sparse, false),
        (Kind::Quantized, false),
        (Kind::Quantized, true),
    ] {
        let fixture = fixture(kind, blocked, false);
        let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
        let memory = prepared.session.memory().clone();
        let message = if kind == Kind::Sparse {
            "dimension"
        } else {
            "positive"
        };
        let Err(error) = prepared.execute_owned() else {
            panic!("invalid vector retained");
        };
        assert!(error.to_string().contains(message), "{error}");
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        let mut provisional = 0;
        assert!(
            prepared
                .for_each_batch(&CancellationToken::default(), |array, _| {
                    provisional += array.len();
                    Ok(())
                })
                .is_err()
        );
        assert!(provisional <= 2, "invalid row reached consumer");
        for format in [Format::Vortex, Format::ArrowIpc, Format::Parquet] {
            let path = fixture.0.join(format!("invalid.{}", format.as_str()));
            let Err(error) = prepared.write(&path, format, false) else {
                panic!("invalid vector published");
            };
            assert!(error.to_string().contains(message), "{error}");
            assert!(!path.exists());
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
        }
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
