use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use std::path::Path;

const FORMATS: [Format; 7] = [
    Format::Vortex,
    Format::Parquet,
    Format::ArrowIpc,
    Format::Avro,
    Format::Json,
    Format::Jsonl,
    Format::Csv,
];

fn write(
    fixture: &Fixture,
    request: &VortexQueryPrimitiveRequest,
    direct: bool,
    path: &Path,
    format: Format,
    overwrite: bool,
) -> Result<u64> {
    if direct {
        let mut request = request.clone();
        request.source_uri = Some(DatasetUri::new(fixture.path().display().to_string()).unwrap());
        let report = prepare_unary(&request, policy())?.write(path, format, overwrite)?;
        assert!(!report.evidence.side_effects.fallback_attempted);
        Ok(report.rows_written)
    } else {
        let report = prepared_composed(fixture, request)?.write(path, format, overwrite)?;
        assert!(report.execution.native_io_certificate.is_certified());
        assert!(
            !report
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        Ok(report.output.rows_written)
    }
}

fn expected_csv(rows: &[Value]) -> String {
    let mut result = format!("{}\n", ALL.join(","));
    for row in rows {
        for (index, name) in ALL.iter().enumerate() {
            if index > 0 {
                result.push(',');
            }
            let value = &row[*name];
            if value.is_null() {
                continue;
            }
            let text = if value.is_string() {
                serde_json::to_string(value).unwrap()
            } else {
                value.to_string()
            };
            if text.contains(['"', ',', '\n']) {
                result.push('"');
                result.push_str(&text.replace('"', "\"\""));
                result.push('"');
            } else {
                result.push_str(&text);
            }
        }
        result.push('\n');
    }
    result
}

#[test]
fn typed_unary_writers_reopen_complete_typed_values_and_empty_schemas() {
    for empty in [false, true] {
        let array = source_array();
        let fixture = Fixture::new(
            if empty {
                array.slice(0..0).unwrap()
            } else {
                array
            },
            3,
        );
        let req = rewritten(
            TYPED
                .iter()
                .map(|name| Rewrite::ForwardFillNull {
                    target_column: ColumnRef::new(*name).unwrap(),
                    limit: None,
                })
                .collect(),
        );
        let mut expected = if empty { vec![] } else { oracle() };
        if !empty {
            for name in TYPED {
                expected[3][name] = expected[2][name].clone();
            }
        }
        for direct in [true, false] {
            let dtype = boundary_tests::dtype(&fixture, &req, direct);
            for format in FORMATS {
                let path = fixture
                    .0
                    .join(format!("output-{direct}.{}", format.as_str()));
                assert_eq!(
                    write(&fixture, &req, direct, &path, format, false).unwrap(),
                    expected.len() as u64
                );
                if format == Format::Csv {
                    assert_eq!(fs::read_to_string(&path).unwrap(), expected_csv(&expected));
                } else {
                    assert_eq!(
                        super::super::io_tests::reopen(&path, format, &dtype),
                        expected,
                        "{direct}/{format:?}/{empty}"
                    );
                }
            }
            let path = fixture.0.join(format!("denied-{direct}.orc"));
            assert!(
                write(&fixture, &req, direct, &path, Format::Orc, false)
                    .unwrap_err()
                    .to_string()
                    .contains("ORC does not admit decimal or temporal")
            );
            assert!(!path.exists());
        }
    }
}

#[test]
fn typed_unary_binary_rewrite_roundtrips_through_orc() {
    let fixture = source();
    let mut req = request(Kind::ExpressionProjectRows, &["bytes"]);
    req.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::ForwardFillNull {
            target_column: ColumnRef::new("bytes").unwrap(),
            limit: None,
        },
    ]));
    let expected = [
        Some("00ff"),
        Some(""),
        Some("00ff"),
        Some("00ff"),
        Some(""),
        Some("0a00"),
    ]
    .map(|value| json!({"bytes":value}));
    for direct in [true, false] {
        let path = fixture.0.join(format!("binary-{direct}.orc"));
        assert_eq!(
            write(&fixture, &req, direct, &path, Format::Orc, false).unwrap(),
            6
        );
        assert_eq!(
            super::super::io_tests::reopen(
                &path,
                Format::Orc,
                &boundary_tests::dtype(&fixture, &req, direct)
            ),
            expected
        );
    }
}

#[test]
fn typed_unary_writer_failure_after_first_batch_preserves_destination_and_releases_state() {
    let input = StructArray::new(
        FieldNames::from(["id", "decimal"]),
        vec![
            PrimitiveArray::from_iter(0..2049u64).into_array(),
            DecimalArray::from_option_iter(
                std::iter::repeat_n(None, 2048).chain([Some(123i128)]),
                DecimalDType::new(20, 2),
            )
            .into_array(),
        ],
        2049,
        Validity::NonNullable,
    )
    .into_array();
    let fixture = Fixture::new(input, 2048);
    let mut req = request(Kind::ExpressionProjectRows, &["id", "decimal"]);
    req.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::NumericScalarArithmetic {
            target_column: ColumnRef::new("decimal").unwrap(),
            operator: "/".into(),
            operand: decimal(0, 3, 2),
        },
    ]));
    let prepared = prepared_composed(&fixture, &req).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            delivered += array.len();
            Ok(())
        })
        .err()
        .unwrap();
    assert!(error.to_string().contains("zero"), "{error}");
    assert_eq!(delivered, 2048);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    for direct in [true, false] {
        for format in FORMATS {
            let path = fixture.0.join(format!("preserved.{}", format.as_str()));
            fs::write(&path, b"original destination").unwrap();
            let error = write(&fixture, &req, direct, &path, format, true).unwrap_err();
            assert!(
                error.to_string().contains("existing destination"),
                "{format:?}: {error}"
            );
            assert_eq!(fs::read(&path).unwrap(), b"original destination");
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
            fs::remove_file(&path).unwrap();
            let error = write(&fixture, &req, direct, &path, format, false).unwrap_err();
            assert!(error.to_string().contains("zero"), "{format:?}: {error}");
            assert!(!path.exists());
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
        }
    }
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
