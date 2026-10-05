use super::*;

pub(super) fn fixture(
    indices: &[&str],
    domains: &[&str],
    values: &[Option<i128>],
    precision: u8,
    scale: i8,
    chunk: usize,
) -> Fixture {
    assert_eq!(indices.len(), values.len());
    assert_eq!(domains.len(), values.len());
    Fixture::new(
        StructArray::new(
            FieldNames::from(["group", "domain", "decimal"]),
            vec![
                VarBinArray::from(indices.to_vec()).into_array(),
                VarBinArray::from(domains.to_vec()).into_array(),
                DecimalArray::from_option_iter(
                    values.iter().copied(),
                    DecimalDType::new(precision, scale),
                )
                .into_array(),
            ],
            values.len(),
            Validity::NonNullable,
        )
        .into_array(),
        chunk,
    )
}

pub(super) fn pivot(aggregate: &str, margins: bool) -> VortexQueryPrimitiveRequest {
    let mut req = request(Kind::PivotRows, &["group", "domain", "decimal"]);
    let mut projection = VortexPivotProjectionRequest::new(
        ColumnRef::new("group").unwrap(),
        ColumnRef::new("domain").unwrap(),
        ColumnRef::new("decimal").unwrap(),
        aggregate,
    );
    projection.margins = margins;
    projection.margins_name = "total".into();
    req.pivot_projection = Some(projection);
    req
}

#[test]
fn native_decimal_pivot_weighted_margins_fills_order_and_limits_match_independent_values() {
    for chunk in [1, 4] {
        let fixture = fixture(
            &["a", "a", "a", "a", "b", "b"],
            &["x", "x", "x", "y", "x", "x"],
            &[
                Some(100),
                Some(300),
                Some(500),
                Some(700),
                Some(900),
                Some(1100),
            ],
            8,
            2,
            chunk,
        );
        for (aggregate, precision, scale, values) in [
            (
                "sum",
                38,
                2,
                [[900, 700, 1600], [2000, 0, 2000], [2900, 700, 3600]],
            ),
            (
                "mean",
                38,
                6,
                [
                    [3_000_000, 7_000_000, 4_000_000],
                    [10_000_000, 0, 10_000_000],
                    [5_800_000, 7_000_000, 6_000_000],
                ],
            ),
            (
                "min",
                8,
                2,
                [[100, 700, 100], [900, 0, 900], [100, 700, 100]],
            ),
            (
                "max",
                8,
                2,
                [[500, 700, 700], [1100, 0, 1100], [1100, 700, 1100]],
            ),
        ] {
            let render = |group: &str, values: [i128; 3]| {
                json!({
                    "group":group,
                    "pivot_x":format!("decimal128({precision},{scale}):{}", values[0]),
                    "pivot_y":format!("decimal128({precision},{scale}):{}", values[1]),
                    "pivot_total":format!("decimal128({precision},{scale}):{}", values[2]),
                })
            };
            let expected = [
                render("a", values[0]),
                render("b", values[1]),
                render("total", values[2]),
            ];
            let mut req = pivot(aggregate, true);
            req.pivot_projection.as_mut().unwrap().fill_value = Some(decimal(0, 2, 2));
            for dropna in [false, true] {
                req.pivot_projection.as_mut().unwrap().dropna = dropna;
                for direct in [false, true] {
                    assert_eq!(
                        rows(&fixture, &req, direct),
                        expected,
                        "{aggregate}/{chunk}/{direct}/{dropna}"
                    );
                    let dtype = boundary_tests::dtype(&fixture, &req, direct);
                    assert_eq!(
                        dtype
                            .as_struct_fields_opt()
                            .unwrap()
                            .field("pivot_x")
                            .unwrap(),
                        DType::Decimal(DecimalDType::new(precision, scale), Nullability::Nullable)
                    );
                    let mut limited = req.clone();
                    limited.source_order_limit = Some(2);
                    assert_eq!(
                        rows(&fixture, &limited, direct),
                        [render("a", values[0]), render("total", values[0])]
                    );
                }
            }
        }
    }
}

#[test]
fn native_decimal_pivot_wide_cells_and_margins_delay_precision_check_until_finalization() {
    let maximum = 10i128.pow(38) - 1;
    for (values, aggregate) in [
        (vec![Some(maximum), Some(maximum), Some(-maximum)], "sum"),
        (vec![Some(maximum), Some(maximum)], "mean"),
    ] {
        let fixture = fixture(
            &vec!["a"; values.len()],
            &vec!["x"; values.len()],
            &values,
            38,
            6,
            1,
        );
        let req = pivot(aggregate, true);
        let value = format!("decimal128(38,6):{maximum}");
        for direct in [false, true] {
            assert_eq!(
                rows(&fixture, &req, direct),
                [
                    json!({"group":"a", "pivot_x":value, "pivot_total":value}),
                    json!({"group":"total", "pivot_x":value, "pivot_total":value}),
                ]
            );
        }
    }
    // Each cell is exact and representable; combining their unrounded totals
    // still has to widen before computing the column and grand averages.
    let fixture = fixture(
        &["a", "b"],
        &["x", "x"],
        &[Some(maximum), Some(maximum)],
        38,
        6,
        1,
    );
    for direct in [false, true] {
        let actual = rows(&fixture, &pivot("mean", true), direct);
        assert_eq!(actual.len(), 3);
        for row in actual {
            assert_eq!(row["pivot_x"], json!(format!("decimal128(38,6):{maximum}")));
            assert_eq!(row["pivot_total"], row["pivot_x"]);
        }
    }
}

#[test]
fn native_decimal_pivot_empty_null_and_inexact_or_overflow_failures_are_explicit() {
    let maximum = 10i128.pow(38) - 1;
    for (groups, values, aggregate, message) in [
        (
            vec!["a", "a"],
            vec![Some(maximum), Some(maximum)],
            "sum",
            "precision overflow",
        ),
        (
            vec!["a", "b"],
            vec![Some(maximum), Some(maximum)],
            "sum",
            "precision overflow",
        ),
        (
            vec!["a", "a", "a"],
            vec![Some(1), Some(0), Some(0)],
            "mean",
            "nonzero fractional digits",
        ),
        (
            vec!["a", "b", "b"],
            vec![Some(1), Some(0), Some(0)],
            "mean",
            "nonzero fractional digits",
        ),
    ] {
        let fixture = fixture(&groups, &vec!["x"; values.len()], &values, 38, 6, 1);
        let req = pivot(aggregate, true);
        for direct in [false, true] {
            assert!(
                denied(&fixture, &req, direct).contains(message),
                "{aggregate}/{message}"
            );
        }
    }
    for scale in [0, 6, 38] {
        let empty = fixture(&[], &[], &[], 38, scale, 1);
        for aggregate in ["sum", "mean", "min", "max"] {
            let req = pivot(aggregate, true);
            for direct in [false, true] {
                assert_eq!(rows(&empty, &req, direct), Vec::<Value>::new());
                let dtype = boundary_tests::dtype(&empty, &req, direct);
                assert_eq!(
                    dtype
                        .as_struct_fields_opt()
                        .unwrap()
                        .field("pivot_total")
                        .unwrap(),
                    DType::Decimal(
                        DecimalDType::new(
                            38,
                            if aggregate == "mean" {
                                scale.max(6)
                            } else {
                                scale
                            }
                        ),
                        Nullability::Nullable
                    )
                );
            }
        }
    }
    let null = fixture(&["a"], &["x"], &[None], 8, 2, 1);
    for direct in [false, true] {
        for aggregate in ["sum", "mean", "min", "max"] {
            assert!(denied(&null, &pivot(aggregate, false), direct).contains("non-null"));
        }
        assert_eq!(
            rows(&null, &pivot("count", true), direct),
            [
                json!({"group":"a", "pivot_x":1, "pivot_total":1}),
                json!({"group":"total", "pivot_x":1, "pivot_total":1}),
            ]
        );
    }
}
