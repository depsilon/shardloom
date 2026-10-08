//! Stored window output uses the same complete, transactional native sinks.

use super::*;
use crate::{
    local_primitives::VortexLocalPrimitiveRowExportFormat as Format,
    relational_query::{VortexRelationalFrameBound as Bound, VortexRelationalLimit},
};
use vortex::array::arrays::{ListViewArray, VarBinArray};

fn verify(
    fixture: &Fixture,
    plan: &VortexRelationalPlan,
    expected: &[Value],
    csv: &str,
    nested: bool,
) {
    for count in [expected.len(), 0] {
        let VortexRelationalPlan::Window(mut window) = plan.clone() else {
            panic!("writer fixture requires a window");
        };
        window.input = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: window.input,
            offset: 0,
            count,
        }));
        let prepared = ordered(fixture, &VortexRelationalPlan::Window(window));
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut executions = 0;
        for format in writer_tests::FORMATS {
            let path = fixture
                .0
                .join(format!("stored-{count}.{}", format.as_str()));
            if nested && format == Format::Orc {
                let error = prepared.write(&path, format, false).err().unwrap();
                assert!(error.to_string().contains("nested"), "{error}");
                assert!(!path.exists());
                assert_eq!(prepared.snapshot().completed_executions, executions);
            } else {
                let written = prepared.write(&path, format, false).unwrap();
                executions += 1;
                assert_eq!(written.execution.ordered_window_stages, 1);
                assert_eq!(written.output.rows_written, count as u64);
                assert!(written.execution.native_io_certificate.is_certified());
                assert!(
                    written
                        .execution
                        .spill
                        .as_ref()
                        .unwrap()
                        .owned_cleanup_completed
                );
                assert_eq!(prepared.snapshot().completed_executions, executions);
                if format == Format::Csv {
                    assert_eq!(
                        fs::read_to_string(&path).unwrap(),
                        if count == 0 { "out\n" } else { csv }
                    );
                } else if nested && format == Format::Vortex {
                    let VortexRelationalPlan::Scan(mut scan) = fixture.scan() else {
                        panic!("writer fixture requires a native scan");
                    };
                    scan.source_uri = DatasetUri::new(path.display().to_string()).unwrap();
                    let reopened =
                        prepare_relational(&VortexRelationalPlan::Scan(scan), policy()).unwrap();
                    assert_eq!(reopened.output_dtype(), prepared.output_dtype());
                    assert_eq!(
                        json_rows(
                            &reopened
                                .collect_jsonl(&CancellationToken::default())
                                .unwrap()
                        ),
                        expected[..count]
                    );
                } else {
                    assert_eq!(
                        writer_tests::read_rows(&path, format, &prepared.output_dtype().unwrap()),
                        expected[..count],
                        "{format:?} {count}"
                    );
                }
                drop(written);
            }
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(
                fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
                0
            );
        }
    }
}

#[test]
fn ordered_window_flat_and_nested_output_roundtrips_every_admitted_writer() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_option_iter([Some(1i64), None, Some(3)]).into_array(),
        ),
        1,
    );
    let plan = window_frame_tests::one(
        &fixture,
        VortexRelationalWindowFunction::Framed(Function::Sum(ColumnRef::new("value").unwrap())),
        Some(Frame {
            unit: Unit::Rows,
            start: Bound::UnboundedPreceding,
            end: Bound::UnboundedFollowing,
            exclusion: Exclusion::CurrentRow,
        }),
        None,
    );
    verify(
        &fixture,
        &plan,
        &[
            json!({"out": 3.0}),
            json!({"out": 4.0}),
            json!({"out": 1.0}),
        ],
        "out\n3\n4\n1\n",
        false,
    );

    let lists = ListViewArray::try_new(
        PrimitiveArray::from_option_iter([Some(9i64), None, Some(-4)]).into_array(),
        PrimitiveArray::from_iter([0u64, 2, 2, 2]).into_array(),
        PrimitiveArray::from_iter([2u64, 0, 0, 1]).into_array(),
        Validity::from_iter([true, true, false, true]),
    )
    .unwrap()
    .into_array();
    let values = StructArray::new(
        FieldNames::from(["items", "label"]),
        vec![
            lists,
            VarBinArray::from(vec![Some("東京"), Some("hidden"), None, Some("a'b")]).into_array(),
        ],
        4,
        Validity::from_iter([true, false, true, true]),
    )
    .into_array();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "value"]),
            vec![PrimitiveArray::from_iter(0u64..4).into_array(), values],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let plan = window_frame_tests::one(
        &fixture,
        VortexRelationalWindowFunction::Lag {
            column: ColumnRef::new("value").unwrap(),
            offset: 1,
        },
        None,
        Some("id"),
    );
    verify(
        &fixture,
        &plan,
        &[
            json!({"out":null}),
            json!({"out":{"items":[9,null],"label":"東京"}}),
            json!({"out":null}),
            json!({"out":{"items":null,"label":null}}),
        ],
        "out\n\"\"\n\"{\"\"items\"\":[9,null],\"\"label\"\":\"\"東京\"\"}\"\n\"\"\n\"{\"\"items\"\":null,\"\"label\"\":null}\"\n",
        true,
    );
}

#[test]
fn ordered_window_writer_arithmetic_failure_preserves_destinations_and_refunds_state() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([f64::MAX, f64::MAX]).into_array(),
        ),
        1,
    );
    let plan = window_frame_tests::one(
        &fixture,
        VortexRelationalWindowFunction::Framed(Function::Sum(ColumnRef::new("value").unwrap())),
        None,
        None,
    );
    let prepared = ordered(&fixture, &plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for format in writer_tests::FORMATS {
        let path = fixture.0.join(format!("rollback.{}", format.as_str()));
        assert!(prepared.write(&path, format, false).is_err());
        assert!(!path.exists());
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
        fs::write(&path, b"existing destination").unwrap();
        assert!(prepared.write(&path, format, true).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing destination");
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 3);
        fs::remove_file(path).unwrap();
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(fixture.0.join("window-runs")).unwrap().count(),
            0
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}
