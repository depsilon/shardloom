use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;

fn csv(columns: &[&str], rows: &[Value]) -> String {
    let mut result = format!("{}\n", columns.join(","));
    for row in rows {
        for (index, name) in columns.iter().enumerate() {
            if index > 0 {
                result.push(',');
            }
            let value = &row[*name];
            if value.is_null() {
                continue;
            }
            // UTF8 uses plain CSV text; decimal values keep the documented
            // tagged JSON scalar encoding inside their escaped CSV cells.
            let text = if *name == "group" {
                value.as_str().unwrap().to_owned()
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

fn roundtrip(
    fixture: &Fixture,
    req: &VortexQueryPrimitiveRequest,
    columns: &[&str],
    expected: &[Value],
) {
    for direct in [false, true] {
        let dtype = boundary_tests::dtype(fixture, req, direct);
        for format in io_tests::FORMATS {
            let path = fixture
                .0
                .join(format!("output-{direct}.{}", format.as_str()));
            assert_eq!(
                io_tests::write(fixture, req, direct, &path, format, false).unwrap(),
                expected.len() as u64
            );
            if format == Format::Csv {
                assert_eq!(fs::read_to_string(&path).unwrap(), csv(columns, expected));
            } else {
                assert_eq!(
                    super::super::io_tests::reopen(&path, format, &dtype),
                    expected,
                    "{direct}/{format:?}"
                );
            }
            fs::remove_file(path).unwrap();
        }
        let path = fixture.0.join("denied.orc");
        let error = io_tests::write(fixture, req, direct, &path, Format::Orc, false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ORC does not admit decimal or temporal"),
            "{error}"
        );
        assert!(!path.exists());
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
    }
}

#[test]
fn native_decimal_unary_writers_preserve_full_values_scales_nulls_and_empty_schemas() {
    for empty in [false, true] {
        let fixture = decimal_rolling_tests::fixture(
            if empty {
                &[]
            } else {
                &[Some(100), None, Some(300), Some(400)]
            },
            12,
            2,
            2,
        );
        for (aggregate, precision, scale, values) in [
            ("sum", 38, 2, [100, 100, 400, 700]),
            ("mean", 38, 6, [1_000_000, 1_000_000, 2_000_000, 3_500_000]),
            ("min", 12, 2, [100, 100, 100, 300]),
            ("max", 12, 2, [100, 100, 300, 400]),
        ] {
            let req = decimal_rolling_tests::rolling(aggregate, 3, 1, false);
            let expected = if empty {
                vec![]
            } else {
                values
                    .map(
                        |value| json!({"value":format!("decimal128({precision},{scale}):{value}")}),
                    )
                    .to_vec()
            };
            roundtrip(&fixture, &req, &["value"], &expected);
        }
        let fixture = if empty {
            decimal_pivot_tests::fixture(&[], &[], &[], 12, 2, 1)
        } else {
            decimal_pivot_tests::fixture(
                &["a", "a", "b", "b"],
                &["x", "x", "y", "y"],
                &[Some(100), Some(300), Some(500), Some(700)],
                12,
                2,
                2,
            )
        };
        for (aggregate, precision, scale, values) in [
            ("sum", 38, 2, [400, 1200, 1600]),
            ("mean", 38, 6, [2_000_000, 6_000_000, 4_000_000]),
            ("min", 12, 2, [100, 500, 100]),
            ("max", 12, 2, [300, 700, 700]),
        ] {
            let req = decimal_pivot_tests::pivot(aggregate, true);
            let [a, b, total] =
                values.map(|value| format!("decimal128({precision},{scale}):{value}"));
            let expected = if empty {
                vec![]
            } else {
                vec![
                    json!({"group":"a", "pivot_x":a, "pivot_y":null, "pivot_total":a}),
                    json!({"group":"b", "pivot_x":null, "pivot_y":b, "pivot_total":b}),
                    json!({"group":"total", "pivot_x":a, "pivot_y":b, "pivot_total":total}),
                ]
            };
            roundtrip(
                &fixture,
                &req,
                if empty {
                    &["group", "pivot_total"]
                } else {
                    &["group", "pivot_x", "pivot_y", "pivot_total"]
                },
                &expected,
            );
        }
    }
}

#[test]
fn native_decimal_unary_writer_final_overflow_after_delivery_publishes_nothing() {
    let maximum = 10i128.pow(38) - 1;
    for kind in [Kind::RollingWindowRows, Kind::PivotRows] {
        let (fixture, req) = if kind == Kind::RollingWindowRows {
            let mut values = vec![Some(1); 2048];
            values.push(Some(maximum));
            (
                decimal_rolling_tests::fixture(&values, 38, 2, 2048),
                decimal_rolling_tests::rolling("sum", 2, 1, false),
            )
        } else {
            let mut groups = (0..2048).map(|n| format!("{n:04}")).collect::<Vec<_>>();
            groups.extend(["zzzz".into(), "zzzz".into()]);
            let mut values = vec![Some(1); 2048];
            values.extend([Some(maximum), Some(1)]);
            (
                decimal_pivot_tests::fixture(
                    &groups.iter().map(String::as_str).collect::<Vec<_>>(),
                    &vec!["x"; groups.len()],
                    &values,
                    38,
                    2,
                    2048,
                ),
                decimal_pivot_tests::pivot("sum", false),
            )
        };
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
        assert!(
            error.to_string().contains("precision overflow"),
            "{kind:?}: {error}"
        );
        assert_eq!(delivered, 2048, "{kind:?}");
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        for direct in [false, true] {
            for format in io_tests::FORMATS {
                let path = fixture.0.join(format!("denied.{}", format.as_str()));
                fs::write(&path, b"original destination").unwrap();
                let error =
                    io_tests::write(&fixture, &req, direct, &path, format, true).unwrap_err();
                assert!(
                    error.to_string().contains("existing destination"),
                    "{error}"
                );
                assert_eq!(fs::read(&path).unwrap(), b"original destination");
                fs::remove_file(&path).unwrap();
                let error =
                    io_tests::write(&fixture, &req, direct, &path, format, false).unwrap_err();
                assert!(
                    error.to_string().contains("precision overflow"),
                    "{kind:?}/{direct}/{format:?}: {error}"
                );
                assert!(!path.exists());
                assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
            }
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
