//! Typed and nested payloads cross actual join runs and compatible file writers.

use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use vortex::array::{
    arrays::{DecimalArray, ExtensionArray, ListViewArray, VarBinArray},
    dtype::DecimalDType,
    extension::datetime::{Date, TimeUnit, Timestamp},
};

const ROWS: u32 = 16_385;

fn row(id: u32, left: Option<u32>) -> Value {
    json!({"left_id":left,"right_id":id,"binary":"00ff10",
        "money":format!("decimal128(38,6):{}", -i128::from(id)),
        "day":i64::from(id)-20_000,"instant":i64::from(id)*1_000_000-1,
        "items":if id.is_multiple_of(7) { Value::Null } else {json!([-i64::from(id),null])}})
}

fn right() -> Fixture {
    let rows = ROWS as usize;
    Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "id", "binary", "money", "day", "instant", "items"]),
            vec![
                PrimitiveArray::from_option_iter(
                    (0..ROWS)
                        .rev()
                        .map(|id| (!id.is_multiple_of(31)).then_some(u64::from(id))),
                )
                .into_array(),
                PrimitiveArray::from_iter((0..ROWS).rev()).into_array(),
                VarBinArray::from(vec![Some(&b"\x00\xff\x10"[..]); rows]).into_array(),
                DecimalArray::from_option_iter(
                    (0..ROWS).rev().map(|id| Some(-i128::from(id))),
                    DecimalDType::new(38, 6),
                )
                .into_array(),
                ExtensionArray::new(
                    Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                    PrimitiveArray::from_option_iter(
                        (0..ROWS)
                            .rev()
                            .map(|id| Some(i32::try_from(id).unwrap() - 20_000)),
                    )
                    .into_array(),
                )
                .into_array(),
                ExtensionArray::new(
                    Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
                    PrimitiveArray::from_option_iter(
                        (0..ROWS)
                            .rev()
                            .map(|id| Some(i64::from(id) * 1_000_000 - 1)),
                    )
                    .into_array(),
                )
                .into_array(),
                ListViewArray::try_new(
                    PrimitiveArray::from_option_iter(
                        (0..ROWS).rev().flat_map(|id| [Some(-i64::from(id)), None]),
                    )
                    .into_array(),
                    PrimitiveArray::from_iter((0..u64::from(ROWS)).map(|index| index * 2))
                        .into_array(),
                    PrimitiveArray::from_iter(vec![2u64; rows]).into_array(),
                    Validity::from_iter((0..ROWS).rev().map(|id| !id.is_multiple_of(7))),
                )
                .unwrap()
                .into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array(),
        257,
    )
}

fn csv(expected: &[Value]) -> String {
    use std::fmt::Write as _;
    let mut csv = String::from("left_id,right_id,binary,money,day,instant,items\n");
    for row in expected {
        let cell = |name| {
            let value = &row[name];
            if value.is_null() {
                String::new()
            } else if matches!(name, "binary" | "money" | "items") {
                format!(
                    "\"{}\"",
                    serde_json::to_string(value).unwrap().replace('"', "\"\"")
                )
            } else {
                value.to_string()
            }
        };
        writeln!(
            &mut csv,
            "{},{},{},{},{},{},{}",
            cell("left_id"),
            cell("right_id"),
            cell("binary"),
            cell("money"),
            cell("day"),
            cell("instant"),
            cell("items")
        )
        .unwrap();
    }
    csv
}

#[test]
fn ordered_join_spilled_typed_nested_payloads_roundtrip_every_admitted_writer() {
    let left = fixture(&[(Some(1), 10), (Some(2), 11), (None, 12)], 2);
    let right = right();
    let mut plan = plan(&left, &right, JoinKind::Full);
    let VortexRelationalPlan::Join(join) = &mut plan else {
        unreachable!()
    };
    join.columns = std::iter::once((Side::Left, "amount", "left_id"))
        .chain(
            ["id", "binary", "money", "day", "instant", "items"].map(|name| {
                (
                    Side::Right,
                    name,
                    if name == "id" { "right_id" } else { name },
                )
            }),
        )
        .map(|(side, name, output)| VortexRelationalJoinColumn {
            side,
            column: ColumnRef::new(name).unwrap(),
            output_column: output.into(),
        })
        .collect();
    let prepared = ordered(&left, &plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut expected = vec![
        row(1, Some(10)),
        row(2, Some(11)),
        json!({
            "left_id":12,"right_id":null,"binary":null,"money":null,"day":null,"instant":null,"items":null
        }),
    ];
    expected.extend(
        (0..ROWS)
            .rev()
            .filter(|id| ![1, 2].contains(id))
            .map(|id| row(id, None)),
    );
    let expected_csv = csv(&expected);
    for format in super::super::writer_tests::FORMATS {
        let path = left.0.join(format!("rich-join.{}", format.as_str()));
        let result = prepared.write(&path, format, false);
        if format == Format::Orc {
            assert!(
                result
                    .err()
                    .expect("ORC richer types remain explicitly unsupported")
                    .to_string()
                    .contains("ORC")
            );
            assert!(!path.exists());
            continue;
        }
        let report = result.unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(report.output.rows_written, expected.len() as u64);
        assert_eq!(report.execution.ordered_join_stages, 1);
        let spill = report.execution.spill.as_ref().unwrap();
        assert!(
            spill.runs_written > 3 && spill.merge_passes > 0,
            "{spill:?}"
        );
        assert!(spill.owned_cleanup_completed);
        if format == Format::Vortex {
            let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: DatasetUri::new(path.display().to_string()).unwrap(),
                projection: shardloom_plan::ProjectionRequest::All,
                predicate: None,
            });
            let reopened = prepare_relational(&scan, policy()).unwrap();
            assert_eq!(reopened.output_dtype(), prepared.output_dtype());
            assert_eq!(
                json_rows(
                    &reopened
                        .collect_jsonl(&CancellationToken::default())
                        .unwrap()
                ),
                expected
            );
        } else if format == Format::Csv {
            assert_eq!(fs::read_to_string(path).unwrap(), expected_csv);
        } else {
            assert_eq!(
                super::super::writer_tests::read_rows(
                    &path,
                    format,
                    &prepared.output_dtype().unwrap()
                ),
                expected,
                "{format:?}"
            );
        }
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(left.0.join("join-runs")).unwrap().count(), 0);
    }
}
