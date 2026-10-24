//! Exact membership policy, ordering and lifetime controls with spill permission.

use super::pressure_tests::{clean, with_spill, workspace};
use super::*;

#[path = "local_primitive_relational_unary_membership_failure_tests.rs"]
mod failure_tests;

#[path = "local_primitive_relational_unary_membership_typed_tests.rs"]
pub(in crate::local_primitives) mod typed_tests;

fn membership_cases() -> Vec<(String, VortexQueryPrimitiveRequest)> {
    cases(&ROWS)
        .into_iter()
        .filter(|(_, request, _)| {
            matches!(
                request.kind,
                Kind::DistinctRows | Kind::DropDuplicateRows | Kind::DuplicateMaskRows
            )
        })
        .map(|(name, mut req, _)| {
            if req.kind == Kind::DropDuplicateRows {
                req.deduplicate_key_projection = Some(columns(&["s"]));
            } else {
                req.projection = columns(&["s"]);
            }
            (name, req)
        })
        .collect()
}

#[test]
fn unary_membership_small_state_preserves_empty_null_nested_and_retained_output() {
    let nulls = [(None, None, None); 3];
    for rows in [&ROWS[..], &[][..], &nulls[..]] {
        for width in [1, 3, 8] {
            for (name, req, expected) in cases(rows) {
                if matches!(
                    req.kind,
                    Kind::DistinctRows | Kind::DropDuplicateRows | Kind::DuplicateMaskRows
                ) {
                    retained_case(rows, width, &name, req, &expected, true);
                }
            }
        }
    }
}

fn key_text(key: usize, width: usize) -> String {
    format!("{key:08}{}", "u".repeat(width - 8))
}

fn ordinal_source(
    session: &ResidentVortexSession,
    start: usize,
    keys: &[usize],
    width: usize,
) -> Result<ResidentMemorySource> {
    let numbers = (start..start + keys.len())
        .map(|n| Some(i64::try_from(n).unwrap()))
        .collect::<Vec<_>>();
    let strings = keys
        .iter()
        .map(|&key| key_text(key, width))
        .collect::<Vec<_>>();
    let text = strings.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>();
    let lists = (start..start + keys.len())
        .map(|n| format!("[{n},null]"))
        .collect::<Vec<_>>();
    let nested = lists.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>();
    let dtype_json = serde_json::to_string(&DType::List(
        DType::Primitive(PType::I16, Nullability::Nullable).into(),
        Nullability::Nullable,
    ))
    .unwrap();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(&numbers),
            },
            MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(&text),
            },
            MemoryColumn {
                name: "items",
                values: MemoryColumnValues::TypedJson {
                    dtype_json: &dtype_json,
                    values: &nested,
                },
            },
        ],
    )
}

fn expected_positions(
    keys: &[usize],
    req: &VortexQueryPrimitiveRequest,
    first: usize,
) -> Vec<(usize, bool)> {
    let keep = if req.kind == Kind::DistinctRows {
        Keep::First
    } else {
        req.duplicate_keep
    };
    let passing = (0..keys.len())
        .filter(|&position| keys[position] >= first)
        .collect::<Vec<_>>();
    passing
        .iter()
        .enumerate()
        .filter_map(|(index, &position)| {
            let previous = passing[..index]
                .iter()
                .any(|&other| keys[other] == keys[position]);
            let following = passing[index + 1..]
                .iter()
                .any(|&other| keys[other] == keys[position]);
            let duplicate = match keep {
                Keep::First => previous,
                Keep::Last => following,
                Keep::AllDuplicates => previous || following,
            };
            (req.kind == Kind::DuplicateMaskRows || !duplicate).then_some((position, duplicate))
        })
        .take(req.source_order_limit.unwrap_or(usize::MAX))
        .collect()
}

#[allow(clippy::too_many_arguments)] // One full stream oracle is reused across policy and pressure controls.
fn check_stream(
    name: &str,
    req: &VortexQueryPrimitiveRequest,
    keys: &[usize],
    width: usize,
    first: usize,
    batch_rows: usize,
    grant: u64,
) {
    let expected = expected_positions(keys, req, first);
    let (_fixture, directory) = workspace();
    let mut bound = req.clone();
    let upstream_filter = req.kind == Kind::DuplicateMaskRows && first != 0;
    let input = if upstream_filter {
        // Masks require a preceding filter; their existing canonical request
        // deliberately rejects an attached unary predicate.
        use shardloom_core::{ExprId, Expression, ExpressionKind, ScalarValue};
        bound.predicate = None;
        VortexRelationalPlan::Filter(Box::new(crate::relational_query::VortexRelationalFilter {
            input: scan(),
            predicate: Expression::new(
                ExprId::new("eligible-key").unwrap(),
                ExpressionKind::Compare {
                    left: Box::new(Expression::column(
                        ExprId::new("s").unwrap(),
                        ColumnRef::new("s").unwrap(),
                    )),
                    op: shardloom_core::ComparisonOp::GtEq,
                    right: Box::new(Expression::literal(
                        ExprId::new("minimum").unwrap(),
                        ScalarValue::Utf8(key_text(first, width)),
                    )),
                },
            ),
        }))
    } else {
        scan()
    };
    let prepared = with_spill(
        prepare_unary(&unary(input, bound), grant)
            .unwrap_or_else(|error| panic!("{name}: {error}")),
        &directory,
    );
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut position = 0;
    let mut calls = 0;
    let mut ended = false;
    let mut prior: Option<Weak<MemoryLease>> = None;
    let mut delivered = 0;
    let report = prepared.with_batch_inputs(&mut |_, ordinal, session| {
        assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
        assert_eq!(ordinal, calls);
        calls += 1;
        assert!(!ended);
        if position == keys.len() {
            ended = true;
            return Ok(None);
        }
        let end = (position + batch_rows).min(keys.len());
        let batch = ordinal_source(session, position, &keys[position..end], width)?;
        position = end;
        prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }).unwrap().for_each_batch(&CancellationToken::default(), |array, context| {
        for actual in values(&array, context)? {
            let (ordinal, duplicate) = expected[delivered];
            let expected = match req.kind {
                Kind::DuplicateMaskRows => json!({"duplicated":duplicate}),
                Kind::DistinctRows => json!({"s":key_text(keys[ordinal], width)}),
                _ => json!({"n":ordinal,"s":key_text(keys[ordinal], width),"items":[ordinal,null]}),
            };
            assert_eq!(actual, expected, "{name}/{delivered}");
            delivered += 1;
        }
        Ok(())
    }).unwrap_or_else(|error| panic!("{name}: {error}"));
    assert!(ended && prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
    assert_eq!(delivered, expected.len(), "{name}");
    assert_eq!(report.spillable_unary_membership_stages, 1);
    let input_rows = if upstream_filter {
        keys.iter().filter(|&&key| key >= first).count()
    } else {
        keys.len()
    };
    assert_eq!(report.spillable_unary_input_rows, input_rows as u64);
    assert_eq!(report.spillable_unary_output_rows, delivered as u64);
    assert!(report.spill.as_ref().unwrap().runs_written > 0, "{name}");
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    assert!(report.runtime.memory.peak_reserved_bytes <= grant);
    assert!(report.native_io_certificate.is_certified());
    assert!(report.input.as_ref().unwrap().end_of_input_observed);
    assert_eq!(report.input.as_ref().unwrap().replay_sources, 0);
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    clean(&directory);
}

#[test]
fn unary_membership_spilled_replacements_preserve_payload_ordinals_predicates_and_limits() {
    // Even keys reappear in reverse order with different scalar/nested payloads;
    // odd keys survive remove-all. A hot key then crosses another batch boundary.
    let keys = (0..2049)
        .chain((0..2049).rev().filter(|key| key % 2 == 0))
        .chain(std::iter::repeat_n(0, 257))
        .collect::<Vec<_>>();
    for (name, req) in membership_cases() {
        for limit in [None, Some(777)] {
            for first in [0, 5] {
                let mut req = req.clone();
                req.source_order_limit = limit;
                if first != 0 {
                    req.predicate = Some(shardloom_core::PredicateExpr::Compare {
                        column: ColumnRef::new("s").unwrap(),
                        op: shardloom_core::ComparisonOp::GtEq,
                        value: shardloom_core::StatValue::Utf8(key_text(first, 2048)),
                    });
                }
                check_stream(
                    &format!("{name}/{limit:?}/{first}"),
                    &req,
                    &keys,
                    2048,
                    first,
                    251,
                    16 << 20,
                );
            }
        }
    }
}

#[test]
fn unary_membership_single_keys_larger_than_the_buffer_follow_the_memory_grant() {
    for (name, req) in membership_cases() {
        check_stream(&name, &req, &[0, 1, 0], 2 << 20, 0, 1, 128 << 20);
    }
}

// Raw native arrays deliberately bypass finite public input construction so
// observation tests can include values that must never reach public output.
// Memory-pressure acceptance above uses the admitted finite producer boundary.
fn raw_membership(
    array: &ArrayRef,
    req: &VortexQueryPrimitiveRequest,
    spill: bool,
    runs: bool,
) -> Result<Vec<Value>> {
    use crate::local_primitives::{native_relational_spill::State, prepared_unary::BoundUnary};
    use crate::relational_query::VortexRelationalSpillPolicy;
    let (_fixture, directory) = workspace();
    let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
    let bound = BoundUnary::for_relation(req, array.dtype(), session.memory()).unwrap();
    let baseline = session.memory().snapshot().reserved_bytes;
    let mut ended = false;
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let mut output = Vec::new();
        let produce = |consume: &mut dyn FnMut(ArrayRef) -> Result<()>| {
            for start in (0..array.len()).step_by(37) {
                consume(array.slice(start..(start + 37).min(array.len())).unwrap())?;
            }
            ended = true;
            Ok(())
        };
        let mut consume = |array: ArrayRef| {
            assert_eq!(
                array.dtype(),
                &DType::struct_(bound.fields().to_vec(), Nullability::NonNullable)
            );
            output.extend(values(&array, context)?);
            Ok(())
        };
        if spill {
            let policy = VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20).unwrap();
            let state = State::new(&policy, context)?;
            bound.consume_relation_membership(context, &state, 37, produce, &mut consume)?;
            let report = state.finish()?;
            assert_eq!(report.runs_written > 0, runs);
            assert!(report.owned_cleanup_completed);
        } else {
            bound.consume_relation(context, Some(array.len() as u64), 37, produce, &mut consume)?;
        }
        Ok(output)
    });
    if result.is_ok() {
        assert!(ended);
    }
    assert_eq!(session.memory().snapshot().reserved_bytes, baseline);
    clean(&directory);
    result
}

fn floating_payload_source(mode: &str, total: usize, groups: usize) -> ArrayRef {
    use vortex::array::arrays::VarBinViewArray;
    let strings = (0..total)
        .map(|row| key_text(row % groups, 8192))
        .collect::<Vec<_>>();
    let invalid = |row: usize| match mode {
        "replaced" => row < groups,
        "ignored-duplicate" => row >= groups,
        _ => row == groups - 1,
    };
    StructArray::new(
        FieldNames::from(["s", "n", "small", "wide"]),
        vec![
            VarBinViewArray::from_iter_str(strings.iter().map(String::as_str)).into_array(),
            PrimitiveArray::from_iter(0..total as u64).into_array(),
            PrimitiveArray::from_iter((0..total).map(|row| {
                if invalid(row) {
                    f32::NAN
                } else {
                    f32::from(u16::try_from(row).unwrap())
                }
            }))
            .into_array(),
            PrimitiveArray::from_iter((0..total).map(|row| {
                if invalid(row) {
                    f64::INFINITY
                } else {
                    f64::from(u32::try_from(row).unwrap())
                }
            }))
            .into_array(),
        ],
        total,
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn unary_membership_private_payload_movement_preserves_unobserved_float_values() {
    const GROUPS: usize = 257;
    for mode in ["replaced", "ignored-duplicate", "limited", "selected"] {
        let total = if matches!(mode, "limited" | "selected") {
            GROUPS
        } else {
            GROUPS * 2
        };
        let array = floating_payload_source(mode, total, GROUPS);
        for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
            if mode == "replaced" && keep == Keep::First {
                continue;
            }
            let mut req = request(Kind::DropDuplicateRows, &["s", "n", "small", "wide"]);
            req.deduplicate_key_projection = Some(columns(&["s"]));
            req.duplicate_keep = keep;
            if mode == "limited" {
                req.source_order_limit = Some(1);
            }
            let selected_invalid =
                mode == "selected" || (mode == "ignored-duplicate" && keep == Keep::Last);
            let mut expected = None;
            for spill in [false, true] {
                let result = raw_membership(
                    &array,
                    &req,
                    spill,
                    mode != "limited" || keep != Keep::First,
                );
                if selected_invalid {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("finite declared dtype"),
                        "{mode}/{keep:?}/{spill}"
                    );
                } else {
                    let actual =
                        result.unwrap_or_else(|error| panic!("{mode}/{keep:?}/{spill}: {error}"));
                    let ordinals = if mode == "limited" {
                        vec![0]
                    } else if keep == Keep::AllDuplicates {
                        vec![]
                    } else if keep == Keep::Last {
                        (GROUPS..GROUPS * 2).collect()
                    } else {
                        (0..GROUPS).collect()
                    };
                    assert_eq!(
                        actual
                            .iter()
                            .map(|row| usize::try_from(row["n"].as_u64().unwrap()).unwrap())
                            .collect::<Vec<_>>(),
                        ordinals
                    );
                    for row in &actual {
                        assert_eq!(row["small"].as_f64(), row["n"].as_f64());
                        assert_eq!(row["wide"].as_f64(), row["n"].as_f64());
                    }
                    if let Some(expected) = &expected {
                        assert_eq!(&actual, expected);
                    }
                    expected = Some(actual);
                }
            }
        }
    }
}

#[test]
fn unary_membership_prefix_limit_skips_invalid_keys_but_future_policies_observe_them() {
    use vortex::array::{arrays::DecimalArray, dtype::DecimalDType};
    let array = StructArray::new(
        FieldNames::from(["key"]),
        vec![DecimalArray::from_iter([1_i128, 999, 2], DecimalDType::new(2, 0)).into_array()],
        3,
        Validity::NonNullable,
    )
    .into_array();
    for kind in [
        Kind::DistinctRows,
        Kind::DropDuplicateRows,
        Kind::DuplicateMaskRows,
    ] {
        for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
            if kind == Kind::DistinctRows && keep != Keep::First {
                continue;
            }
            let mut req = request(kind, &["key"]);
            req.duplicate_keep = keep;
            req.source_order_limit = Some(1);
            if kind == Kind::DropDuplicateRows {
                req.deduplicate_key_projection = Some(columns(&["key"]));
            }
            for spill in [false, true] {
                let result = raw_membership(&array, &req, spill, false);
                if keep == Keep::First {
                    assert_eq!(
                        result.unwrap(),
                        vec![if kind == Kind::DuplicateMaskRows {
                            json!({"duplicated":false})
                        } else {
                            json!({"key":"decimal128(2,0):1"})
                        }]
                    );
                } else {
                    let error = result.unwrap_err().to_string();
                    assert!(
                        error.contains("precision"),
                        "{kind:?}/{keep:?}/{spill}: {error}"
                    );
                }
            }
        }
    }
}

#[test]
fn unary_membership_ignores_unselected_nested_duplicate_payloads() {
    use vortex::array::arrays::{ListArray, VarBinViewArray};
    const GROUPS: usize = 257;
    let strings = (0..GROUPS * 2)
        .map(|row| key_text(row % GROUPS, 8192))
        .collect::<Vec<_>>();
    let array = StructArray::new(
        FieldNames::from(["s", "items"]),
        vec![
            VarBinViewArray::from_iter_str(strings.iter().map(String::as_str)).into_array(),
            ListArray::new(
                PrimitiveArray::from_iter((0..GROUPS * 2).map(|row| {
                    if row < GROUPS {
                        f64::from(u32::try_from(row).unwrap())
                    } else {
                        f64::NAN
                    }
                }))
                .into_array(),
                PrimitiveArray::from_iter(0..=GROUPS as u64 * 2).into_array(),
                Validity::NonNullable,
                &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                    &vortex::array::array_session(),
                ),
            )
            .into_array(),
        ],
        GROUPS * 2,
        Validity::NonNullable,
    )
    .into_array();
    for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
        let mut req = request(Kind::DropDuplicateRows, &["s", "items"]);
        req.deduplicate_key_projection = Some(columns(&["s"]));
        req.duplicate_keep = keep;
        for spill in [false, true] {
            let result = raw_membership(&array, &req, spill, true);
            if keep == Keep::Last {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("finite declared dtype")
                );
            } else {
                let expected = if keep == Keep::First {
                    (0..GROUPS)
                        .map(|row| json!({"s":key_text(row,8192),"items":[f64::from(u32::try_from(row).unwrap())]}))
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                };
                assert_eq!(result.unwrap(), expected);
            }
        }
    }
}

fn signed_zero_source(zeros: [f64; 5], pad: &str) -> ArrayRef {
    use vortex::array::arrays::VarBinViewArray;
    let nested = StructArray::new(
        FieldNames::from(["value"]),
        vec![PrimitiveArray::from_iter(zeros).into_array()],
        zeros.len(),
        Validity::NonNullable,
    )
    .into_array();
    StructArray::new(
        FieldNames::from(["id", "small", "wide", "nested", "pad"]),
        vec![
            PrimitiveArray::from_iter(0..zeros.len() as u64).into_array(),
            PrimitiveArray::from_iter([-0.0_f32, 0.0, -0.0, 1.0, 0.0]).into_array(),
            PrimitiveArray::from_iter(zeros).into_array(),
            nested,
            VarBinViewArray::from_iter_str(std::iter::repeat_n(pad, zeros.len())).into_array(),
        ],
        zeros.len(),
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn unary_membership_spilled_flat_nested_and_composite_keys_keep_signed_zero_distinct() {
    let zeros = [-0.0_f64, 0.0, -0.0, 1.0, 0.0];
    let pad = "z".repeat(600 << 10);
    let array = signed_zero_source(zeros, &pad);
    for keys in [
        vec!["small", "pad"],
        vec!["wide", "pad"],
        vec!["nested", "pad"],
        vec!["small", "wide", "nested", "pad"],
    ] {
        for (keep, ordinals, masks) in [
            (
                Keep::First,
                vec![0, 1, 3],
                vec![false, false, true, false, true],
            ),
            (
                Keep::Last,
                vec![2, 3, 4],
                vec![true, true, false, false, false],
            ),
            (
                Keep::AllDuplicates,
                vec![3],
                vec![true, true, true, false, true],
            ),
        ] {
            for spill in [false, true] {
                let mut req = request(
                    Kind::DropDuplicateRows,
                    &["id", "small", "wide", "nested", "pad"],
                );
                req.deduplicate_key_projection = Some(columns(&keys));
                req.duplicate_keep = keep;
                let actual = raw_membership(&array, &req, spill, true).unwrap();
                assert_eq!(
                    actual
                        .iter()
                        .map(|row| usize::try_from(row["id"].as_u64().unwrap()).unwrap())
                        .collect::<Vec<_>>(),
                    ordinals
                );
                for (row, &ordinal) in actual.iter().zip(&ordinals) {
                    assert_eq!(
                        row["small"].as_f64().unwrap().to_bits(),
                        zeros[ordinal].to_bits()
                    );
                    assert_eq!(
                        row["wide"].as_f64().unwrap().to_bits(),
                        zeros[ordinal].to_bits()
                    );
                    assert_eq!(
                        row["nested"]["value"].as_f64().unwrap().to_bits(),
                        zeros[ordinal].to_bits()
                    );
                    assert_eq!(row["pad"].as_str().unwrap(), pad);
                }
                let mut req = request(Kind::DuplicateMaskRows, &keys);
                req.duplicate_keep = keep;
                assert_eq!(
                    raw_membership(&array, &req, spill, true).unwrap(),
                    masks
                        .iter()
                        .map(|v| json!({"duplicated":v}))
                        .collect::<Vec<_>>()
                );
                if keep == Keep::First {
                    let req = request(Kind::DistinctRows, &keys);
                    let actual = raw_membership(&array, &req, spill, true).unwrap();
                    assert_eq!(actual.len(), 3);
                    for (row, value) in actual.iter().zip([-0.0_f64, 0.0, 1.0]) {
                        let selected = if keys[0] == "nested" {
                            &row["nested"]["value"]
                        } else {
                            &row[keys[0]]
                        };
                        assert_eq!(selected.as_f64().unwrap().to_bits(), value.to_bits());
                    }
                }
            }
        }
    }
}
