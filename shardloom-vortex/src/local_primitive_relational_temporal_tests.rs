//! Complete native plans, retained results and independent temporal readback.

use super::*;
use shardloom_core::temporal::{TemporalDType, TemporalValue};

#[path = "local_primitive_relational_temporal_key_tests.rs"]
mod key_tests;

fn dtype(name: &str) -> TemporalDType {
    TemporalDType::parse(name).unwrap()
}
fn text(value: &str) -> Expression {
    literal(ScalarValue::Utf8(value.into()))
}
fn number(value: i64) -> Expression {
    literal(ScalarValue::Int64(value))
}
fn value(name: &str, ticks: i64) -> Expression {
    literal(
        TemporalValue::new(ticks, dtype(name))
            .unwrap()
            .into_scalar(),
    )
}
fn temporal_array(name: &str, values: &[Option<i64>]) -> ArrayRef {
    let native = crate::native_temporal::vortex_dtype(&dtype(name), Nullability::Nullable).unwrap();
    let storage = if crate::native_temporal::temporal_storage(&native) == Some(PType::I32) {
        PrimitiveArray::from_option_iter(
            values
                .iter()
                .map(|value| value.map(|value| i32::try_from(value).unwrap())),
        )
        .into_array()
    } else {
        PrimitiveArray::from_option_iter(values.iter().copied()).into_array()
    };
    let DType::Extension(extension) = native else {
        panic!("temporal extension")
    };
    ExtensionArray::try_new(extension, storage)
        .unwrap()
        .into_array()
}

#[test]
fn native_temporal_discarded_payloads_validate_domains_and_release_credit() {
    use crate::local_primitives::result_batch::{Value, build_column_prefix};
    use crate::resident_session::ResidentVortexSession;
    use vortex::array::memory::HostMemorySessionExt as _;

    let session = ResidentVortexSession::new(2 << 20, 1).unwrap();
    for (name, invalid) in [
        ("date32", i64::from(i32::MAX) + 1),
        ("date64", 1),
        ("time(s)", -1),
        ("time(ms)", 86_400_000),
        ("time(us)", 86_400_000_000),
        ("time(ns)", 86_400_000_000_000),
    ] {
        for retained in [0, 1] {
            let before = session.snapshot().memory.reserved_bytes;
            session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    let dtype =
                        crate::native_temporal::vortex_dtype(&dtype(name), Nullability::Nullable)?;
                    let result = build_column_prefix(
                        &dtype,
                        2,
                        retained,
                        &context.native_session().host_allocator(),
                        |row| Ok(Value::Int(if row == 0 { 0 } else { invalid })),
                    );
                    assert!(result.is_err(), "{name}: discarded row must be validated");
                    Ok(())
                })
                .unwrap();
            assert_eq!(session.snapshot().memory.reserved_bytes, before, "{name}");
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One complete workflow checks both DST transitions and ownership.
fn native_temporal_calendar_workflow_keeps_units_nulls_zones_and_dst_days() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["instant"]),
            vec![temporal_array(
                "timestamp(s,America/New_York)",
                &[Some(1_710_046_800), Some(1_730_606_400), None],
            )],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let expressions = vec![
        ("hour", function("timestamp_hour", vec![col("instant")])),
        (
            "day",
            function("temporal_trunc", vec![col("instant"), text("day")]),
        ),
        (
            "calendar",
            function("calendar_add_days", vec![col("instant"), number(1)]),
        ),
        (
            "fixed",
            function(
                "temporal_add_duration",
                vec![col("instant"), value("duration(s)", 86_400)],
            ),
        ),
        (
            "utc",
            function(
                "timestamp_convert_timezone",
                vec![col("instant"), text("UTC")],
            ),
        ),
        (
            "civil",
            function("timestamp_remove_timezone", vec![col("instant")]),
        ),
        (
            "elapsed",
            function(
                "timestamp_difference",
                vec![
                    function("calendar_add_days", vec![col("instant"), number(1)]),
                    col("instant"),
                ],
            ),
        ),
    ];
    let plan = project(fixture.scan(), expressions.clone());
    assert_eq!(
        collect(&plan),
        vec![
            json!({"hour":0,"day":1_710_046_800,"calendar":1_710_129_600,"fixed":1_710_133_200,"utc":1_710_046_800,"civil":1_710_028_800,"elapsed":82_800}),
            json!({"hour":0,"day":1_730_606_400,"calendar":1_730_696_400,"fixed":1_730_692_800,"utc":1_730_606_400,"civil":1_730_592_000,"elapsed":90_000}),
            json!({"hour":null,"day":null,"calendar":null,"fixed":null,"utc":null,"civil":null,"elapsed":null}),
        ]
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let expected = prepared.output_dtype().unwrap();
    let fields = expected.as_struct_fields_opt().unwrap();
    for (index, name) in [
        (1, "timestamp(s,America/New_York)"),
        (2, "timestamp(s,America/New_York)"),
        (3, "timestamp(s,America/New_York)"),
        (4, "timestamp(s,UTC)"),
        (5, "timestamp(s)"),
        (6, "duration(s)"),
    ] {
        assert_eq!(
            fields.field_by_index(index).unwrap(),
            crate::native_temporal::vortex_dtype(&dtype(name), Nullability::Nullable).unwrap()
        );
    }
    let empty = prepare_relational(&project(empty(fixture.scan()), expressions), policy()).unwrap();
    assert_eq!(empty.output_dtype().unwrap(), expected);
    assert_eq!(
        empty
            .execute_native(&CancellationToken::default())
            .unwrap()
            .result
            .row_count(),
        0
    );
    let memory = prepared.session.memory().clone();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    let proof = &completed
        .execution
        .native_io_certificate
        .source_pushdown_report
        .proof_basis;
    assert!(proof.contains("temporal_calendar_provider=jiff-0.2.35"));
    assert!(proof.contains("temporal_timezone_database=2026a"));
    drop((prepared, empty));
    assert_eq!(completed.result.dtype(), &expected);
    assert_eq!(completed.result.row_count(), 3);
    drop(completed);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_temporal_selected_branches_and_calendar_policy_binding_are_strict() {
    let fixture = decimals();
    let bad = function(
        "timestamp_assign_timezone",
        vec![
            cast(
                text("2024-03-10T02:30:00"),
                dtype("timestamp(s)").logical(),
                false,
            ),
            text("America/New_York"),
        ],
    );
    let good = value("timestamp(s,America/New_York)", 0);
    let selected = function(
        "case_when",
        vec![literal(ScalarValue::Boolean(false)), bad.clone(), good],
    );
    assert_eq!(
        collect(&project(fixture.scan(), vec![("selected", selected)])),
        vec![json!({"selected":0}); 4]
    );
    let prepared =
        prepare_relational(&project(fixture.scan(), vec![("gap", bad)]), policy()).unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(prepared.execute_native(&cancellation).is_err());
    for expression in [
        function(
            "timestamp_assign_timezone",
            vec![value("timestamp(s)", 0), text("No/Such_Zone")],
        ),
        function("temporal_extract", vec![value("time(ns)", 0), text("year")]),
        function(
            "temporal_trunc",
            vec![value("timestamp(s)", 0), text("day"), text("guess")],
        ),
        function(
            "calendar_add_months",
            vec![value("date32", 0), number(1), text("guess")],
        ),
        function(
            "timestamp_difference",
            vec![value("timestamp(s)", 0), value("timestamp(ns)", 0)],
        ),
        function(
            "temporal_extract",
            vec![value("timestamp(s)", 0), col("text")],
        ),
    ] {
        assert!(
            prepare_relational(
                &project(empty(fixture.scan()), vec![("invalid", expression)]),
                policy()
            )
            .is_err()
        );
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_temporal_all_units_keep_exact_persisted_payloads_and_independent_arrow_schema() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let mut declarations = vec!["date32".to_owned(), "date64".to_owned()];
    for family in ["time", "timestamp", "duration"] {
        for unit in ["s", "ms", "us", "ns"] {
            declarations.push(format!("{family}({unit})"));
        }
    }
    declarations.push("timestamp(ns,America/New_York)".to_owned());
    let values = declarations
        .iter()
        .map(|name| {
            if name == "date32" {
                i64::from(i32::MAX)
            } else if name == "date64" {
                -86_400_000
            } else if name.starts_with("time(") {
                1
            } else {
                i64::MAX
            }
        })
        .collect::<Vec<_>>();
    let names = (0..declarations.len())
        .map(|index| format!("c{index}"))
        .collect::<Vec<_>>();
    let fixture = Fixture::new(
        StructArray::new(
            names.iter().map(String::as_str).collect::<FieldNames>(),
            declarations
                .iter()
                .zip(&values)
                .map(|(name, value)| temporal_array(name, &[Some(*value), None]))
                .collect::<Vec<_>>(),
            2,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    let expected = vec![
        Value::Object(
            names
                .iter()
                .zip(&values)
                .map(|(name, value)| (name.clone(), json!(*value)))
                .collect(),
        ),
        Value::Object(
            names
                .iter()
                .map(|name| (name.clone(), Value::Null))
                .collect(),
        ),
    ];
    for empty_input in [false, true] {
        let plan = if empty_input {
            empty(fixture.scan())
        } else {
            fixture.scan()
        };
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let dtype = prepared.output_dtype().unwrap();
        let expected = if empty_input {
            &[][..]
        } else {
            expected.as_slice()
        };
        for format in [Format::Vortex, Format::ArrowIpc, Format::Parquet] {
            let path = fixture
                .0
                .join(format!("all-temporal-{empty_input}.{}", format.as_str()));
            let report = prepared
                .write(&path, format, false)
                .unwrap_or_else(|error| panic!("{format:?}: {error}"));
            assert_eq!(
                usize::try_from(report.output.rows_written).unwrap(),
                expected.len()
            );
            assert!(report.execution.native_io_certificate.is_certified());
            assert_eq!(
                super::super::io_tests::reopen(&path, format, &dtype),
                expected,
                "{format:?}/{empty_input}"
            );
            if format == Format::ArrowIpc {
                let reader =
                    arrow_ipc::reader::FileReader::try_new(fs::File::open(&path).unwrap(), None)
                        .unwrap();
                for (field, declared) in reader.schema().fields().iter().zip(&declarations) {
                    assert_eq!(
                        field.data_type(),
                        &crate::temporal_arrow::data_type(&TemporalDType::parse(declared).unwrap())
                    );
                }
            }
        }
    }
}
