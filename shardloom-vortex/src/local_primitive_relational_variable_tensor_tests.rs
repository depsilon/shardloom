//! Native-file observation must apply the same cross-field tensor checks as intake.

use super::*;
use crate::{
    local_primitives::VortexLocalPrimitiveRowExportFormat as Format,
    native_variable_tensor::{Metadata, VariableShapeTensor},
};
use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
use vortex::array::{
    arrays::{ExtensionArray, FixedSizeListArray, ListArray},
    dtype::extension::ExtDType,
};

fn malformed_tensor_fixture(hide_bad: bool) -> Fixture {
    let mut execution = vortex::session::VortexSession::empty().create_execution_ctx();
    let data = ListArray::try_new(
        PrimitiveArray::from_iter([10_i32, 20, 30, 40]).into_array(),
        PrimitiveArray::from_iter([0_u64, 1, 3, 4]).into_array(),
        Validity::NonNullable,
        &mut execution,
    )
    .unwrap()
    .into_array();
    let shape = FixedSizeListArray::try_new(
        PrimitiveArray::from_iter([1_i32, 1, 1, 2, -1, 1]).into_array(),
        2,
        Validity::NonNullable,
        3,
    )
    .unwrap()
    .into_array();
    let storage = StructArray::new(
        FieldNames::from(["data", "shape"]),
        vec![data, shape],
        3,
        Validity::NonNullable,
    )
    .into_array();
    let dtype = ExtDType::<VariableShapeTensor>::try_new(
        Metadata::parse("{}").unwrap(),
        storage.dtype().clone(),
    )
    .unwrap()
    .erased();
    let tensor = ExtensionArray::try_new(dtype, storage)
        .unwrap()
        .into_array();
    let parent = StructArray::new(
        FieldNames::from(["tensor"]),
        vec![tensor],
        3,
        Validity::from_iter([true, true, !hide_bad]),
    )
    .into_array();
    // This low-level writer intentionally bypasses public typed/Arrow intake.
    // The file is structurally sound but its last present tensor is malformed.
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
        1,
    )
}

fn selected(fixture: &Fixture) -> VortexRelationalPlan {
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

fn assert_good_outputs(fixture: &Fixture, plan: &VortexRelationalPlan, hidden: bool) {
    let prepared = prepare_relational(plan, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let mut expected = vec![
        serde_json::json!({"id":1,"parent":{"tensor":{"data":[10],"shape":[1,1]}}}),
        serde_json::json!({"id":2,"parent":{"tensor":{"data":[20,30],"shape":[1,2]}}}),
    ];
    if hidden {
        expected.push(serde_json::json!({"id":3,"parent":null}));
    }
    assert_eq!(
        prepared.execute_owned().unwrap().result.row_count(),
        u64::try_from(expected.len()).unwrap()
    );
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        expected
    );
    let mut rows = 0;
    prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            rows += array.len();
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, expected.len());
    for format in [Format::Vortex, Format::ArrowIpc, Format::Parquet] {
        let path = fixture.0.join(format!("valid.{}", format.as_str()));
        let output = prepared.write(&path, format, false).unwrap();
        assert_eq!(
            output.output.rows_written,
            u64::try_from(expected.len()).unwrap()
        );
        assert!(output.execution.native_io_certificate.is_certified());
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn variable_tensor_native_file_collection_stream_and_writers_observe_only_visible_rows() {
    let visible = malformed_tensor_fixture(false);
    assert_good_outputs(&visible, &selected(&visible), false);
    let hidden = malformed_tensor_fixture(true);
    assert_good_outputs(&hidden, &hidden.scan(), true);
}

#[test]
fn variable_tensor_native_file_malformed_visible_results_fail_before_publication() {
    let fixture = malformed_tensor_fixture(false);
    let prepared = prepare_relational(&fixture.scan(), policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let Err(error) = prepared.execute_owned() else {
        panic!("malformed tensor was retained");
    };
    assert!(error.to_string().contains("nonnegative"), "{error}");
    let mut provisional = 0;
    let Err(error) = prepared.for_each_batch(&CancellationToken::default(), |array, _| {
        provisional += array.len();
        Ok(())
    }) else {
        panic!("malformed tensor stream was accepted");
    };
    assert!(error.to_string().contains("nonnegative"), "{error}");
    assert!(provisional <= 2, "invalid row reached the stream consumer");
    for format in [Format::Vortex, Format::ArrowIpc, Format::Parquet] {
        let output = fixture.0.join(format!("invalid.{}", format.as_str()));
        let Err(error) = prepared.write(&output, format, false) else {
            panic!("malformed tensor was published as {format:?}");
        };
        assert!(error.to_string().contains("nonnegative"), "{error}");
        assert!(!output.exists());
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    }
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
