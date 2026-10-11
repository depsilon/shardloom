//! Persisted compression and complete-result pruning for exact timestamps.

use super::*;
use crate::{
    physical_encoding_inventory::{PhysicalEncodingInspectionLimits, inspect_physical_encodings},
    resident_session::{ResidentVortexSession, file_pruning_tests::check_native_predicate},
};
use std::collections::BTreeSet;
use vortex::{
    array::{
        expr::{
            Expression, eq, get_item, gt, gt_eq, is_not_null, is_null, lit, lt, lt_eq, not_eq, root,
        },
        scalar::{PValue, ScalarValue},
    },
    file::WriteOptionsSessionExt as _,
    layout::layouts::flat::writer::FlatLayoutStrategy,
};

fn flat_control(context: &LocalVortexWriteContext, path: &Path, array: &ArrayRef, stats: bool) {
    let mut options = context
        .session
        .write_options()
        .with_strategy(Arc::new(FlatLayoutStrategy::default()));
    if !stats {
        options = options.with_file_statistics(Vec::new());
    }
    options
        .blocking(&context.runtime)
        .write(fs::File::create(path).unwrap(), array.to_array_iterator())
        .unwrap();
}

fn physical_encodings(context: &LocalVortexWriteContext, path: &Path) -> BTreeSet<String> {
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(path))
        .unwrap();
    let inventory = context
        .runtime
        .block_on(inspect_physical_encodings(
            &file,
            PhysicalEncodingInspectionLimits {
                max_layout_nodes: 1024,
                max_flat_references: 256,
                max_segment_bytes: 1 << 20,
                max_total_segment_bytes: 4 << 20,
                max_array_nodes: 4096,
            },
        ))
        .unwrap();
    assert_eq!(inventory["complete_flat_inspection"], true);
    inventory["flat_references"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["auxiliary_path"].as_array().unwrap().is_empty())
        .flat_map(|entry| entry["stored_array_nodes"].as_array().unwrap())
        .map(|node| node["encoding_id"].as_str().unwrap().to_owned())
        .collect()
}

fn wrapped_timestamp(values: &[Option<i64>], shape: &str) -> ArrayRef {
    let timestamp = timestamp(values);
    if shape == "root" {
        return timestamp;
    }
    let array = StructArray::new(
        FieldNames::from(["renamed_timestamp"]),
        vec![timestamp],
        values.len(),
        Validity::NonNullable,
    )
    .into_array();
    if shape == "flat" {
        return array;
    }
    StructArray::new(
        FieldNames::from(["renamed_nested"]),
        vec![array],
        values.len(),
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn full_domain_timestamp_files_store_useful_native_compression_at_both_endpoints() {
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        let _drivers =
            crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
        for (profile, base, narrow) in [
            ("constant-min", i64::MIN, false),
            ("constant-max", i64::MAX, false),
            ("narrow-min", i64::MIN, true),
            ("narrow-max", i64::MAX - 255, true),
        ] {
            let original = (0..4096)
                .map(|row| {
                    if narrow && row % 17 == 0 {
                        None
                    } else {
                        Some(base + if narrow { i64::from(row % 256) } else { 0 })
                    }
                })
                .collect::<Vec<_>>();
            for shape in ["root", "flat", "nested"] {
                let array = wrapped_timestamp(&original, shape);
                let control = directory.0.join(format!("{profile}-{shape}-flat.vortex"));
                flat_control(&context, &control, &array, true);
                let control_bytes = fs::metadata(&control).unwrap().len();
                let control_encodings = physical_encodings(&context, &control);
                for composition in [
                    Composition::Default,
                    Composition::FastLoad,
                    Composition::Balanced,
                    Composition::SourceText,
                ] {
                    for route in [Route::Array, Route::Stream, Route::Retained, Route::ColumnCandidate] {
                        let name = format!("{profile}-{shape}-{composition:?}-{route:?}");
                        let path = directory.0.join(format!("{name}.vortex"));
                        write_and_verify(&context, &path, composition, route, &array);
                        let bytes = fs::metadata(&path).unwrap().len();
                        let encodings = physical_encodings(&context, &path);
                        assert!(bytes * 2 < control_bytes, "{name}: {bytes} vs {control_bytes}");
                        assert!(
                            encodings.difference(&control_encodings).next().is_some(),
                            "{name}: persisted arrays must use a compressed representation: {encodings:?}"
                        );
                        eprintln!("timestamp-physical-proof {}", json!({
                            "case": name, "rows": original.len(), "file_bytes": bytes,
                            "flat_control_bytes": control_bytes, "encodings": encodings,
                        }));
                    }
                }
            }
        }
    });
}

fn timestamp_literal(dtype: &DType, value: i64) -> Expression {
    lit(Scalar::try_new(
        dtype.clone(),
        Some(ScalarValue::Primitive(PValue::I64(value))),
    )
    .unwrap())
}

#[test]
#[allow(clippy::too_many_lines)] // One cross-product shares actual writer files and integer oracles.
fn full_domain_timestamp_written_statistics_prune_conservatively_without_reading_payloads() {
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        for (profile, original) in [
            (
                "endpoints",
                vec![
                    Some(i64::MIN),
                    Some(i64::MIN + 1),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(i64::MAX - 1),
                    Some(i64::MAX),
                    None,
                ],
            ),
            (
                "narrow-min",
                (0..64)
                    .map(|row| Some(i64::MIN + row))
                    .chain([None])
                    .collect(),
            ),
            (
                "narrow-max",
                (0..64)
                    .map(|row| Some(i64::MAX - 63 + row))
                    .chain([None])
                    .collect(),
            ),
            ("constant", vec![Some(i64::MAX); 64]),
            ("all-null", vec![None; 64]),
            ("empty", Vec::new()),
        ] {
            let stamp = timestamp(&original);
            let dtype = stamp.dtype().clone();
            let array = StructArray::new(
                FieldNames::from(["row_id", "renamed_timestamp"]),
                vec![
                    PrimitiveArray::from_iter(0..u64::try_from(original.len()).unwrap())
                        .into_array(),
                    stamp,
                ],
                original.len(),
                Validity::NonNullable,
            )
            .into_array();
            let control = directory.0.join(format!("{profile}-no-stats.vortex"));
            flat_control(&context, &control, &array, false);
            let session = ResidentVortexSession::new(16 << 20, 2).unwrap();
            let memory = session.memory().clone();
            let without_stats = session.prepare_file(control).unwrap();
            for composition in [
                Composition::Default,
                Composition::FastLoad,
                Composition::Balanced,
                Composition::SourceText,
            ] {
                let path = directory
                    .0
                    .join(format!("{profile}-{composition:?}.vortex"));
                write_and_verify(&context, &path, composition, Route::Array, &array);
                eprintln!(
                    "timestamp-pruning-source {profile}-{composition:?}: {:?}",
                    physical_encodings(&context, &path)
                );
                let with_stats = session.prepare_file(&path).unwrap();
                let mut predicates = Vec::new();
                for threshold in [i64::MIN, i64::MIN + 1, -1, 0, 1, 2, i64::MAX - 1, i64::MAX] {
                    for operator in ["lt", "le", "eq", "ne", "gt", "ge"] {
                        let column = get_item("renamed_timestamp", root());
                        let literal = timestamp_literal(&dtype, threshold);
                        let expression = match operator {
                            "lt" => lt(column, literal),
                            "le" => lt_eq(column, literal),
                            "eq" => eq(column, literal),
                            "ne" => not_eq(column, literal),
                            "gt" => gt(column, literal),
                            "ge" => gt_eq(column, literal),
                            _ => unreachable!(),
                        };
                        let selected = original
                            .iter()
                            .enumerate()
                            .filter_map(|(row, value)| {
                                value
                                    .is_some_and(|value| match operator {
                                        "lt" => value < threshold,
                                        "le" => value <= threshold,
                                        "eq" => value == threshold,
                                        "ne" => value != threshold,
                                        "gt" => value > threshold,
                                        "ge" => value >= threshold,
                                        _ => unreachable!(),
                                    })
                                    .then_some(row)
                            })
                            .collect::<Vec<_>>();
                        predicates.push((format!("{operator}-{threshold}"), expression, selected));
                    }
                }
                for null in [false, true] {
                    let column = get_item("renamed_timestamp", root());
                    let expression = if null {
                        is_null(column)
                    } else {
                        is_not_null(column)
                    };
                    let selected = original
                        .iter()
                        .enumerate()
                        .filter_map(|(row, value)| (value.is_none() == null).then_some(row))
                        .collect();
                    predicates.push((format!("is-null-{null}"), expression, selected));
                }
                let mut pruned = 0;
                for (predicate, expression, selected) in predicates {
                    let label = format!("{profile}-{composition:?}-{predicate}");
                    let (candidate_pruned, _) = check_native_predicate(
                        &with_stats,
                        &array,
                        &["row_id", "renamed_timestamp"],
                        &expression,
                        &selected,
                        &format!("{label}-with-stats"),
                    );
                    let (control_pruned, control_reads) = check_native_predicate(
                        &without_stats,
                        &array,
                        &["row_id", "renamed_timestamp"],
                        &expression,
                        &selected,
                        &format!("{label}-without-stats"),
                    );
                    assert!(
                        !control_pruned,
                        "{label}: missing statistics cannot prove absence"
                    );
                    if profile == "endpoints" && predicate == "eq-2" {
                        assert!(
                            !candidate_pruned,
                            "absent value inside bounds must read through"
                        );
                        assert!(control_reads > 0);
                    }
                    pruned += usize::from(candidate_pruned);
                }
                if !original.is_empty() {
                    assert!(
                        pruned > 0,
                        "{profile}-{composition:?}: exercise real file pruning"
                    );
                }
            }
            assert!(memory.snapshot().peak_reserved_bytes <= memory.snapshot().limit_bytes);
            drop((without_stats, session));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    });
}
