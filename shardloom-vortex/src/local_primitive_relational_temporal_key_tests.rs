//! Native relational key coverage for the complete temporal dtype family.

use super::*;
use crate::relational_query::{
    VortexRelationalAggregate, VortexRelationalJoin, VortexRelationalJoinColumn,
    VortexRelationalJoinKey, VortexRelationalOrderKey, VortexRelationalSpillPolicy,
};

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}
fn sort(input: VortexRelationalPlan, key: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: column(key),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }))
}
fn table(dtype: &str, vals: &[Option<i64>]) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "k"]),
            vec![
                PrimitiveArray::from_iter((0..vals.len()).map(|x| u32::try_from(x).unwrap()))
                    .into_array(),
                temporal_array(dtype, vals),
            ],
            vals.len(),
            Validity::NonNullable,
        )
        .into_array(),
        1,
    )
}
fn aggregate(input: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: vec![column("k")],
        grouping_sets: None,
        measures: vec![
            crate::query_primitive::VortexSimpleAggregateMeasure::new("count", None, "n".into()),
            crate::query_primitive::VortexSimpleAggregateMeasure::new(
                "min",
                Some(column("k")),
                "low".into(),
            ),
            crate::query_primitive::VortexSimpleAggregateMeasure::new(
                "max",
                Some(column("k")),
                "high".into(),
            ),
        ],
    }))
}

#[test]
#[allow(clippy::too_many_lines)] // One finite domain matrix checks shared key semantics and schemas.
fn temporal_keys_sort_group_and_join_preserve_domains_and_metadata() {
    let mut cases = vec![
        ("date32", i64::from(i32::MIN), i64::from(i32::MAX)),
        (
            "date64",
            -9_223_372_036_828_800_000,
            9_223_372_036_828_800_000,
        ),
        ("time(s)", 0, 86_399),
        ("time(ms)", 0, 86_399_999),
        ("time(us)", 0, 86_399_999_999),
        ("time(ns)", 0, 86_399_999_999_999),
    ];
    for name in [
        "timestamp(s)",
        "timestamp(ms)",
        "timestamp(us)",
        "timestamp(ns)",
        "timestamp(us,America/New_York)",
        "duration(s)",
        "duration(ms)",
        "duration(us)",
        "duration(ns)",
    ] {
        cases.push((name, i64::MIN, i64::MAX));
    }
    for (name, minimum, maximum) in cases {
        let vals = [Some(minimum), Some(minimum), Some(maximum), None];
        let fixture = table(name, &vals);
        assert_eq!(
            collect(&sort(fixture.scan(), "k")),
            vec![
                json!({"id":2,"k":maximum}),
                json!({"id":0,"k":minimum}),
                json!({"id":1,"k":minimum}),
                json!({"id":3,"k":null}),
            ],
            "{name}"
        );
        let grouped = collect(&aggregate(fixture.scan()));
        let prepared = prepare_relational(&aggregate(fixture.scan()), policy()).unwrap();
        let expected_dtype =
            crate::native_temporal::vortex_dtype(&dtype(name), Nullability::Nullable).unwrap();
        let output_dtype = prepared.output_dtype().unwrap();
        let fields = output_dtype.as_struct_fields_opt().unwrap();
        for index in [0, 2, 3] {
            assert_eq!(
                fields.field_by_index(index).unwrap(),
                expected_dtype,
                "{name}"
            );
        }
        assert_eq!(grouped.len(), 3, "{name}");
        for key in [vals[0], vals[2], None] {
            let row = grouped
                .iter()
                .find(|r| r["k"] == key.map_or(Value::Null, |v| json!(v)))
                .unwrap();
            let expected_count = vals.iter().filter(|v| **v == key).count();
            assert_eq!(row["n"], json!(expected_count), "{name}/{key:?}");
            assert_eq!(
                row["low"],
                key.map_or(Value::Null, |v| json!(v)),
                "{name}/{key:?}"
            );
            assert_eq!(
                row["high"],
                key.map_or(Value::Null, |v| json!(v)),
                "{name}/{key:?}"
            );
        }
        let join = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: fixture.scan(),
            right: fixture.scan(),
            kind: JoinKind::Inner,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: column("k"),
                right: column("k"),
            }],
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: column("id"),
                    output_column: "l".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: column("id"),
                    output_column: "r".into(),
                },
            ],
        }));
        let mut pairs = collect(&join)
            .iter()
            .map(|row| (row["l"].as_u64().unwrap(), row["r"].as_u64().unwrap()))
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            [(0, 0), (0, 1), (1, 0), (1, 1), (2, 2)],
            "null keys must not join: {name}"
        );
    }

    for (left, right) in [
        ("timestamp(s)", "timestamp(ms)"),
        ("timestamp(us)", "timestamp(us,America/New_York)"),
    ] {
        let l = table(left, &[Some(1), None]);
        let r = table(right, &[Some(1), None]);
        let join = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: l.scan(),
            right: r.scan(),
            kind: JoinKind::Inner,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: column("k"),
                right: column("k"),
            }],
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: column("id"),
                    output_column: "left_id".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: column("id"),
                    output_column: "right_id".into(),
                },
            ],
        }));
        assert!(
            prepare_relational(&join, policy()).is_err(),
            "{left} vs {right}"
        );
    }
}

#[test]
fn temporal_timestamp_duration_keys_force_owned_spill_merge() {
    let ids = (0..24_013u32).rev().collect::<Vec<_>>();
    let keys = [Some(i64::MIN), Some(-1), Some(i64::MAX), None];
    let vals = ids
        .iter()
        .map(|id| keys[(*id % 4) as usize])
        .collect::<Vec<_>>();
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["id", "ts", "dur"]),
            vec![
                PrimitiveArray::from_iter(ids.iter().copied()).into_array(),
                temporal_array("timestamp(ns,America/New_York)", &vals),
                temporal_array("duration(ns)", &vals),
            ],
            ids.len(),
            Validity::NonNullable,
        )
        .into_array(),
        512,
    );
    let prepared = prepare_relational(&sort(fixture.scan(), "ts"), policy())
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 64 << 20, 1 << 20).unwrap())
        .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let mut expected = ids.clone();
    expected.sort_by_key(|id| {
        (
            keys[(*id % 4) as usize].is_none(),
            std::cmp::Reverse(keys[(*id % 4) as usize]),
        )
    });
    assert_eq!(
        json_rows(&collected)
            .iter()
            .map(|r| u32::try_from(r["id"].as_u64().unwrap()).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    let spill = collected.execution.spill.as_ref().unwrap();
    assert!(spill.runs_written >= 3 && spill.merge_passes >= 1);
    assert!(spill.peak_disk_bytes <= spill.quota_bytes && spill.owned_cleanup_completed);
    drop(collected);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(
        fs::read_dir(&fixture.0)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>(),
        [std::ffi::OsString::from("input.vortex")]
    );
}
