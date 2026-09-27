use super::*;

fn request(reversed: bool) -> VortexSimpleAggregateRequest {
    let names = if reversed {
        ["label", "number"]
    } else {
        ["number", "label"]
    };
    VortexSimpleAggregateRequest::grouped(
        names.map(|name| ColumnRef::new(name).unwrap()).to_vec(),
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
}

fn strings(codes: Vec<u32>, values: &[&str]) -> AggregateDirectColumnAccessor {
    AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: codes,
        values: values
            .iter()
            .map(|value| std::sync::Arc::<str>::from(*value))
            .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    }
}

#[test]
fn compact_code_directory_matches_scalar_counts_across_changed_domains() {
    let columns = vec!["number".to_string(), "label".to_string()];
    for reversed in [false, true] {
        for signed in [false, true] {
            for retained in ["", "東京🙂", "embedded\0nul", "é"] {
                let request = request(reversed);
                let mut native =
                    GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
                let mut scalar =
                    GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
                for (numbers, codes, values) in [
                    (
                        vec![u64::MAX, 2, 99, u64::MAX, 3, 2],
                        vec![0, 0, 1, 0, 0, 0],
                        vec![retained, "absent"],
                    ),
                    (
                        vec![2, u64::MAX, 2, 7, u64::MAX],
                        vec![2, 1, 0, 3, 3],
                        vec![retained, retained, retained, "e\u{301}"],
                    ),
                ] {
                    let numeric = if signed {
                        AggregateDirectColumnAccessor::Int64(
                            numbers
                                .iter()
                                .map(|value| i64::from_ne_bytes(value.to_ne_bytes()))
                                .collect(),
                        )
                    } else {
                        AggregateDirectColumnAccessor::UInt64(numbers.clone())
                    };
                    let decoded = vec![
                        numbers
                            .iter()
                            .map(|value| {
                                if signed {
                                    StatValue::Int64(i64::from_ne_bytes(value.to_ne_bytes()))
                                } else {
                                    StatValue::UInt64(*value)
                                }
                            })
                            .collect(),
                        codes
                            .iter()
                            .map(|code| StatValue::Utf8(values[*code as usize].to_string()))
                            .collect(),
                    ];
                    for row in 0..numbers.len() {
                        scalar.update_row(&decoded, row).unwrap();
                    }
                    assert!(
                        native
                            .update_source_order_numeric_utf8_count_from_accessors(
                                &[numeric, strings(codes, &values)],
                                None
                            )
                            .unwrap()
                    );
                }
                let (_, actual) = native.result_row_count_and_summary(Some(2)).unwrap();
                let (_, expected) = scalar.result_row_count_and_summary(Some(2)).unwrap();
                let actual: serde_json::Value = serde_json::from_str(&actual).unwrap();
                let expected: serde_json::Value = serde_json::from_str(&expected).unwrap();
                assert_eq!(actual["values"], expected["values"]);
                assert_eq!(actual["values"][0]["rows"], 3);
                assert_eq!(actual["values"][1]["rows"], 4);
            }
        }
    }
}

#[test]
fn compact_code_directory_rejects_out_of_domain_code_after_admission_closes() {
    let request = request(false);
    let columns = vec!["number".to_string(), "label".to_string()];
    let mut state = GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
    assert!(
        state
            .update_source_order_numeric_utf8_count_from_accessors(
                &[
                    AggregateDirectColumnAccessor::UInt64(vec![1]),
                    strings(vec![0], &["kept"]),
                ],
                None
            )
            .unwrap()
    );
    let error = state
        .update_source_order_numeric_utf8_count_from_accessors(
            &[
                AggregateDirectColumnAccessor::UInt64(vec![1]),
                strings(vec![1], &["kept"]),
            ],
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("dictionary code was out of bounds"));
    assert!(error.contains("no fallback execution was attempted"));
}

#[test]
fn materialized_source_order_retains_owned_string_groups_after_admission_closes() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("label").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    );
    let declared = vec!["label".to_string()];
    let mut state =
        GroupedAggregateStates::new(&request, Some(1), &declared, false, false).unwrap();
    let columns = vec![
        ["東京", "absent", "東京", "東京"]
            .map(|value| StatValue::Utf8(value.to_string()))
            .to_vec(),
    ];
    state.update(&columns, 4).unwrap();
    let (_, summary) = state.result_row_count_and_summary(Some(1)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([{"label":"東京", "rows":3}])
    );
}

#[test]
fn materialized_distinct_updates_existing_general_keys_without_admitting_new_strings() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("label").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("number").unwrap()),
            "uniques".to_string(),
        )],
    );
    let declared = vec!["label".to_string(), "number".to_string()];
    for direct_seed in [false, true] {
        let mut state =
            GroupedAggregateStates::new(&request, Some(1), &declared, false, false).unwrap();
        if direct_seed {
            assert!(
                state
                    .update_general_direct_from_accessors(
                        &[
                            strings(vec![0], &["kept"]),
                            AggregateDirectColumnAccessor::UInt64(vec![1]),
                        ],
                        None,
                        1
                    )
                    .unwrap()
            );
        } else {
            state
                .update_row(
                    &[
                        vec![StatValue::Utf8("kept".to_string())],
                        vec![StatValue::UInt64(1)],
                    ],
                    0,
                )
                .unwrap();
        }
        let interned = state.string_interner.values.len();
        let columns = vec![
            ["absent", "kept", "kept", "absent"]
                .map(|value| StatValue::Utf8(value.to_string()))
                .to_vec(),
            [9, 2, 2, 3].map(StatValue::UInt64).to_vec(),
        ];
        state.update(&columns, 4).unwrap();
        assert_eq!(state.string_interner.values.len(), interned);
        assert_eq!(state.group_order.len(), 1);
        assert_eq!(state.groups.len(), 1);
        let (_, summary) = state.result_row_count_and_summary(Some(1)).unwrap();
        let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            summary["values"],
            serde_json::json!([{"label":"kept", "uniques":2}])
        );
    }
}

#[test]
fn compact_directory_retains_only_matched_lists_for_small_later_domains() {
    let request = request(false);
    let columns = vec!["number".to_string(), "label".to_string()];
    let mut state =
        GroupedAggregateStates::new(&request, Some(128), &columns, false, false).unwrap();
    let labels: Vec<_> = (0..128).map(|index| format!("retained-{index}")).collect();
    let label_refs: Vec<_> = labels.iter().map(String::as_str).collect();
    let initial = [
        AggregateDirectColumnAccessor::UInt64((0..128).collect()),
        strings((0..128).collect(), &label_refs),
    ];
    assert!(
        state
            .update_source_order_numeric_utf8_count_from_accessors(&initial, None)
            .unwrap()
    );
    let roles = state
        .source_order_numeric_utf8_group_roles_for_accessors(&initial, None)
        .unwrap();
    let absent = [std::sync::Arc::<str>::from("absent")];
    let directory = state
        .source_order_numeric_utf8_candidate_slots(&absent, roles)
        .unwrap()
        .unwrap();
    assert_eq!(directory.by_code, [usize::MAX]);
    assert_eq!(directory.by_string.capacity(), 0);
    let labels = [
        std::sync::Arc::<str>::from("retained-7"),
        std::sync::Arc::<str>::from("retained-7"),
        std::sync::Arc::<str>::from("absent"),
    ];
    let directory = state
        .source_order_numeric_utf8_candidate_slots(&labels, roles)
        .unwrap()
        .unwrap();
    assert_eq!(directory.by_code, [0, 0, usize::MAX]);
    assert_eq!(directory.by_string.len(), 1);
    assert_eq!(directory.by_string[0].len(), 1);
    assert!(
        state
            .update_source_order_numeric_utf8_count_from_accessors(
                &[
                    AggregateDirectColumnAccessor::UInt64(vec![7, 7, 0]),
                    strings(vec![0, 1, 2], &["retained-7", "retained-7", "absent"]),
                ],
                None
            )
            .unwrap()
    );
    let (_, summary) = state.result_row_count_and_summary(Some(128)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let rows = summary["values"].as_array().unwrap();
    assert_eq!(rows.len(), 128);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["number"], index);
        assert_eq!(row["label"], format!("retained-{index}"));
        assert_eq!(row["rows"], if index == 7 { 3 } else { 1 });
    }
}

#[test]
fn materialized_closed_admission_skips_dependent_output_expression() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("number").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "next".to_string(),
            ColumnRef::new("number").unwrap(),
            "add_offset",
        )
        .with_argument_offset(1),
    ]);
    let declared = vec!["number".to_string()];
    let mut state =
        GroupedAggregateStates::new(&request, Some(1), &declared, false, false).unwrap();
    assert_eq!(state.group_key_indices.len(), 1);
    state
        .update(
            &[vec![
                StatValue::UInt64(0),
                StatValue::UInt64(u64::MAX),
                StatValue::UInt64(0),
            ]],
            3,
        )
        .unwrap();
    let (_, summary) = state.result_row_count_and_summary(Some(1)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([{"number":0, "next":1, "rows":2}])
    );
}

#[test]
fn interned_closed_admission_skips_later_key_expression_for_unknown_string() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("label").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("number").unwrap()),
            "uniques".to_string(),
        )],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "next".to_string(),
            ColumnRef::new("number").unwrap(),
            "add_offset",
        )
        .with_argument_offset(1),
    ]);
    let declared = vec!["label".to_string(), "number".to_string()];
    let mut state =
        GroupedAggregateStates::new(&request, Some(1), &declared, false, false).unwrap();
    assert_eq!(state.group_key_indices.len(), 2);
    assert!(
        state
            .update_general_direct_from_accessors(
                &[
                    strings(vec![0], &["kept"]),
                    AggregateDirectColumnAccessor::UInt64(vec![0]),
                ],
                None,
                1,
            )
            .unwrap()
    );
    state
        .update(
            &[
                vec![
                    StatValue::Utf8("absent".to_string()),
                    StatValue::Utf8("kept".to_string()),
                ],
                vec![StatValue::UInt64(u64::MAX), StatValue::UInt64(0)],
            ],
            2,
        )
        .unwrap();
    let (_, summary) = state.result_row_count_and_summary(Some(1)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([{"label":"kept", "next":1, "uniques":1}])
    );
}

#[test]
fn owned_closed_admission_skips_later_key_expression_for_unknown_strings() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![
            ColumnRef::new("label").unwrap(),
            ColumnRef::new("second").unwrap(),
        ],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "next".to_string(),
            ColumnRef::new("number").unwrap(),
            "add_offset",
        )
        .with_argument_offset(1),
    ]);
    let declared = vec![
        "label".to_string(),
        "second".to_string(),
        "number".to_string(),
    ];
    let mut state =
        GroupedAggregateStates::new(&request, Some(2), &declared, false, false).unwrap();
    assert_eq!(state.group_key_indices.len(), 3);
    state
        .update(
            &[
                [
                    "kept", "other", "absent", "kept", "known", "kept", "kept", "other",
                ]
                .map(|s| StatValue::Utf8(s.to_string()))
                .to_vec(),
                [
                    "known",
                    "second-other",
                    "known",
                    "unknown",
                    "kept",
                    "second-other",
                    "known",
                    "second-other",
                ]
                .map(|s| StatValue::Utf8(s.to_string()))
                .to_vec(),
                [0, 0, u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0, 0]
                    .map(StatValue::UInt64)
                    .to_vec(),
            ],
            8,
        )
        .unwrap();
    assert!(state.string_interner.values.is_empty());
    assert_eq!(state.groups.len(), 2);
    let (_, summary) = state.result_row_count_and_summary(Some(2)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([
            {"label":"kept", "second":"known", "next":1, "rows":2},
            {"label":"other", "second":"second-other", "next":1, "rows":2},
        ])
    );
    let error = state
        .update_row(
            &[
                vec![StatValue::Utf8("kept".to_string())],
                vec![StatValue::Utf8("known".to_string())],
                vec![StatValue::UInt64(u64::MAX)],
            ],
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("overflow"));
}

#[test]
fn owned_numeric_prefix_miss_does_not_retry_the_same_later_expression() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("category").unwrap()],
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "next".to_string(),
            ColumnRef::new("number").unwrap(),
            "add_offset",
        )
        .with_argument_offset(1),
    ]);
    let declared = vec!["category".to_string(), "number".to_string()];
    let mut state =
        GroupedAggregateStates::new(&request, Some(1), &declared, false, false).unwrap();
    assert_eq!(state.group_key_indices.len(), 2);
    state
        .update(
            &[
                [0, 99, 0].map(StatValue::UInt64).to_vec(),
                [0, u64::MAX, 0].map(StatValue::UInt64).to_vec(),
            ],
            3,
        )
        .unwrap();
    let (_, summary) = state.result_row_count_and_summary(Some(1)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([{"category":0, "next":1, "rows":2}])
    );
}

#[test]
fn closed_owned_key_probe_is_read_only_for_wide_retained_groups() {
    let request = VortexSimpleAggregateRequest::grouped(
        ["category", "label", "other", "tag"]
            .map(|name| ColumnRef::new(name).unwrap())
            .to_vec(),
        vec![crate::VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".to_string(),
        )],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "next".to_string(),
            ColumnRef::new("number").unwrap(),
            "add_offset",
        )
        .with_argument_offset(1),
    ]);
    let declared = ["category", "label", "other", "tag", "number"].map(str::to_string);
    let retained = 512;
    let mut state =
        GroupedAggregateStates::new(&request, Some(retained), &declared, false, false).unwrap();
    assert_eq!(state.group_key_indices.len(), 5);
    let input = vec![
        (0..512).map(StatValue::UInt64).collect(),
        (0..512)
            .map(|index| StatValue::Utf8(format!("kept-{index}")))
            .collect(),
        vec![StatValue::Utf8("shared".to_string()); retained],
        vec![StatValue::Utf8("tag".to_string()); retained],
        vec![StatValue::UInt64(0); retained],
    ];
    state.update(&input, retained).unwrap();
    let bytes = state.estimated_group_key_storage_bytes();
    // The shared borrow is part of the contract: probing closed admission cannot
    // create a persistent index proportional to retained groups or key width.
    let probe: &GroupedAggregateStates = &state;
    for row in 0..retained {
        assert!(
            probe
                .grouped_owned_key_for_materialized_row(&input, row)
                .unwrap()
                .is_some()
        );
    }
    let mut row = vec![
        vec![StatValue::UInt64(7)],
        vec![StatValue::Utf8("kept-8".to_string())],
        vec![StatValue::Utf8("shared".to_string())],
        vec![StatValue::Utf8("tag".to_string())],
        vec![StatValue::UInt64(u64::MAX)],
    ];
    assert!(
        probe
            .grouped_owned_key_for_materialized_row(&row, 0)
            .unwrap()
            .is_none()
    );
    row[1][0] = StatValue::Utf8("kept-7".to_string());
    assert!(
        probe
            .grouped_owned_key_for_materialized_row(&row, 0)
            .unwrap_err()
            .to_string()
            .contains("overflow")
    );
    row[4].clear();
    assert!(
        probe
            .grouped_owned_key_for_materialized_row(&row, 0)
            .unwrap_err()
            .to_string()
            .contains("row was missing")
    );
    row[1][0] = StatValue::Utf8("kept-8".to_string());
    assert!(
        probe
            .grouped_owned_key_for_materialized_row(&row, 0)
            .unwrap()
            .is_none()
    );
    assert_eq!(probe.estimated_group_key_storage_bytes(), bytes);
    assert_eq!(probe.groups.len(), retained);
    assert!(probe.string_interner.values.is_empty());
}
