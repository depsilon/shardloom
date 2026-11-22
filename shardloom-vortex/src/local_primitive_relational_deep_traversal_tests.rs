// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::Arc;
use vortex::array::{
    ArrayRef, Canonical, ExecutionCtx,
    arrays::{FixedSizeListArray, ListArray, ListViewArray, MapArray, PrimitiveArray, StructArray},
    builtins::ArrayBuiltins as _,
    dtype::{DType, FieldNames, MapDType, Nullability, PType, StructFields},
    scalar::Scalar,
    validity::Validity,
};
use vortex::mask::Mask;

fn build_deep_struct_chain(
    depth: usize,
    ptype: PType,
    nullability: Nullability,
) -> (ArrayRef, DType) {
    let mut array = match ptype {
        PType::I32 => PrimitiveArray::from_iter([10_i32, 20, 30, 40]).into_array(),
        PType::I64 => PrimitiveArray::from_iter([10_i64, 20, 30, 40]).into_array(),
        other => panic!("unsupported test ptype: {other:?}"),
    };
    let mut dtype = DType::Primitive(ptype, Nullability::NonNullable);
    for _ in 0..depth {
        array = StructArray::try_new(
            FieldNames::from(["inner"]),
            vec![array],
            4,
            if matches!(nullability, Nullability::Nullable) {
                Validity::AllValid
            } else {
                Validity::NonNullable
            },
        )
        .unwrap()
        .into_array();
        dtype = DType::Struct(
            StructFields::new(FieldNames::from(["inner"]), vec![dtype]),
            nullability,
        );
    }
    (array, dtype)
}

fn build_deep_list_chain(depth: usize, ctx: &mut ExecutionCtx) -> (ArrayRef, DType) {
    let mut array = PrimitiveArray::from_iter([10_i64, 20, 30, 40]).into_array();
    let mut cast_target = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..depth {
        array = ListArray::try_new(
            array,
            PrimitiveArray::from_iter([0_u64, 1, 2, 3, 4]).into_array(),
            Validity::NonNullable,
            ctx,
        )
        .unwrap()
        .into_array();
        cast_target = DType::List(Arc::new(cast_target), Nullability::Nullable);
    }
    (array, cast_target)
}

fn build_deep_listview_chain(depth: usize, ctx: &mut ExecutionCtx) -> (ArrayRef, DType) {
    let mut array = PrimitiveArray::from_iter([10_i64, 20, 30, 40]).into_array();
    let mut cast_target = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..depth {
        array = ListViewArray::try_new(
            array,
            PrimitiveArray::from_iter([0_u64, 1, 2, 3]).into_array(),
            PrimitiveArray::from_iter([1_u64, 1, 1, 1]).into_array(),
            Validity::NonNullable,
            ctx,
        )
        .unwrap()
        .into_array();
        cast_target = DType::List(Arc::new(cast_target), Nullability::Nullable);
    }
    (array, cast_target)
}

fn build_deep_fsl_chain(depth: usize) -> (ArrayRef, DType) {
    let mut array = PrimitiveArray::from_iter([10_i64, 20, 30, 40]).into_array();
    let mut cast_target = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..depth {
        array = FixedSizeListArray::new(array, 1, Validity::NonNullable, 4).into_array();
        cast_target = DType::FixedSizeList(Arc::new(cast_target), 1, Nullability::Nullable);
    }
    (array, cast_target)
}

fn build_deep_map_chain(depth: usize, ctx: &mut ExecutionCtx) -> (ArrayRef, DType) {
    let mut value = PrimitiveArray::from_iter([10_i64, 20, 30, 40]).into_array();
    let mut cast_val_dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..depth {
        let val_dtype = value.dtype().clone();
        let keys = PrimitiveArray::from_iter([1_i32, 2, 3, 4]).into_array();
        let entries_struct = StructArray::try_new(
            FieldNames::from(["key", "value"]),
            vec![keys, value],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let entries = ListViewArray::try_new(
            entries_struct,
            PrimitiveArray::from_iter([0_u64, 1, 2, 3]).into_array(),
            PrimitiveArray::from_iter([1_u64, 1, 1, 1]).into_array(),
            Validity::NonNullable,
            ctx,
        )
        .unwrap();
        let map_dtype = MapDType::try_new(
            DType::Primitive(PType::I32, Nullability::NonNullable),
            val_dtype,
            false,
        )
        .unwrap();
        value = MapArray::try_new(map_dtype, entries).unwrap().into_array();
        let cast_map_dtype = MapDType::try_new(
            DType::Primitive(PType::I32, Nullability::NonNullable),
            cast_val_dtype,
            false,
        )
        .unwrap();
        cast_val_dtype = DType::Map(cast_map_dtype, Nullability::Nullable);
    }
    (value, cast_val_dtype)
}

fn check_ops(
    label: &str,
    depth: usize,
    array: ArrayRef,
    cast_target: Option<DType>,
    ctx: &mut ExecutionCtx,
) {
    eprintln!("[audit] {label} depth={depth}: start dtype eq/clone");
    let dtype_clone: DType = array.dtype().clone();
    assert_eq!(&dtype_clone, array.dtype());

    eprintln!("[audit] {label} depth={depth}: scalar_at + Scalar clone/eq/drop");
    let s0: Scalar = array.execute_scalar(0, ctx).unwrap();
    let s0_clone = s0.clone();
    assert_eq!(s0, s0_clone);
    drop((s0, s0_clone));

    eprintln!("[audit] {label} depth={depth}: slice");
    let sliced = array.slice(1..3).unwrap();
    assert_eq!(sliced.len(), 2);

    eprintln!("[audit] {label} depth={depth}: filter");
    let mask = Mask::from_iter([true, false, true, false]);
    let filtered = array.filter(mask).unwrap();
    assert_eq!(filtered.len(), 2);

    eprintln!("[audit] {label} depth={depth}: take");
    let indices = PrimitiveArray::from_iter([3_u64, 1]).into_array();
    let taken = array.take(indices).unwrap();
    assert_eq!(taken.len(), 2);

    if let Some(target) = cast_target {
        eprintln!("[audit] {label} depth={depth}: cast + Canonical::empty");
        let empty = Canonical::empty(&target);
        assert!(empty.is_empty());
        drop(empty);
        let casted = array.cast(target).unwrap();
        assert_eq!(casted.len(), 4);
        drop(casted);
    } else {
        let empty = Canonical::empty(array.dtype());
        assert!(empty.is_empty());
        drop(empty);
    }

    eprintln!("[audit] {label} depth={depth}: execute Canonical + drop");
    let canonical = array.clone().execute::<Canonical>(ctx).unwrap();
    assert_eq!(canonical.len(), 4);
    drop((canonical, taken, filtered, sliced, array, dtype_clone));
}

fn run_vortex_array_small_stack_audit(depth: usize) {
    use vortex::array::extension::datetime::{TimeUnit, Timestamp};
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let (struct_arr, _) = build_deep_struct_chain(depth, PType::I32, Nullability::NonNullable);
    let (_, struct_cast_target) = build_deep_struct_chain(depth, PType::I64, Nullability::Nullable);
    check_ops(
        "Struct",
        depth,
        struct_arr,
        Some(struct_cast_target),
        &mut ctx,
    );

    let (list_arr, list_cast_target) = build_deep_list_chain(depth, &mut ctx);
    check_ops("List", depth, list_arr, Some(list_cast_target), &mut ctx);

    let (listview_arr, listview_cast_target) = build_deep_listview_chain(depth, &mut ctx);
    check_ops(
        "ListView",
        depth,
        listview_arr,
        Some(listview_cast_target),
        &mut ctx,
    );

    let (fsl_arr, fsl_cast_target) = build_deep_fsl_chain(depth);
    check_ops(
        "FixedSizeList",
        depth,
        fsl_arr,
        Some(fsl_cast_target),
        &mut ctx,
    );

    let (map_arr, map_cast_target) = build_deep_map_chain(depth, &mut ctx);
    check_ops("Map", depth, map_arr, Some(map_cast_target), &mut ctx);

    let mut ext_dtype =
        DType::Extension(Timestamp::new(TimeUnit::Microseconds, Nullability::NonNullable).erased());
    for level in 0..depth {
        ext_dtype = if level % 2 == 0 {
            DType::Struct(
                StructFields::new(FieldNames::from(["ext"]), vec![ext_dtype]),
                Nullability::Nullable,
            )
        } else {
            DType::List(Arc::new(ext_dtype), Nullability::Nullable)
        };
    }
    let ext_empty = Canonical::empty(&ext_dtype);
    assert!(ext_empty.is_empty());
    drop((ext_empty, ext_dtype));
}

#[cfg(feature = "universal-format-io")]
fn run_arrow_record_batch_slice_small_stack_audit(depth: usize) {
    use arrow_array::{Array as _, Int64Array, RecordBatch, make_array};
    use arrow_buffer::OffsetBuffer;
    use arrow_data::ArrayData;
    use arrow_schema::{DataType as ArrowType, Field as ArrowField, Fields as ArrowFields, Schema};
    use std::sync::Arc;

    for shape in ["mixed_list_struct", "pure_struct", "pure_fsl"] {
        eprintln!("[audit] Arrow RecordBatch::slice {shape} depth={depth}: build");
        let leaf: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40]));
        let mut data: ArrayData = leaf.to_data();
        let mut field = ArrowField::new("leaf", ArrowType::Int64, false);

        for level in 0..depth {
            let kind = match shape {
                "pure_struct" => 1,
                "pure_fsl" => 2,
                _ => level % 2,
            };
            match kind {
                0 => {
                    let offsets = OffsetBuffer::<i32>::from_lengths([1, 1, 1, 1]);
                    let dt = ArrowType::List(Arc::new(field));
                    data = ArrayData::builder(dt.clone())
                        .len(4)
                        .add_buffer(offsets.into_inner().into_inner())
                        .add_child_data(data)
                        .build()
                        .unwrap();
                    field = ArrowField::new("l", dt, false);
                }
                1 => {
                    let fields = ArrowFields::from(vec![field]);
                    let dt = ArrowType::Struct(fields);
                    data = ArrayData::builder(dt.clone())
                        .len(4)
                        .add_child_data(data)
                        .build()
                        .unwrap();
                    field = ArrowField::new("s", dt, false);
                }
                _ => {
                    let dt = ArrowType::FixedSizeList(Arc::new(field), 1);
                    data = ArrayData::builder(dt.clone())
                        .len(4)
                        .add_child_data(data)
                        .build()
                        .unwrap();
                    field = ArrowField::new("fsl", dt, false);
                }
            }
        }

        eprintln!("[audit] Arrow RecordBatch::slice {shape} depth={depth}: make_array");
        let root_arr = make_array(data);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![root_arr]).unwrap();

        eprintln!("[audit] Arrow RecordBatch::slice {shape} depth={depth}: slice + drop");
        let sliced = batch.slice(1, 2);
        assert_eq!(sliced.num_rows(), 2);
        let sliced_rebuilt = make_array(sliced.column(0).to_data());
        assert_eq!(sliced_rebuilt.len(), 2);
        drop((sliced_rebuilt, sliced, batch));
        eprintln!("[audit] Arrow RecordBatch::slice {shape} depth={depth}: done");
    }
}

fn spawn_on_128k_stack(name: &'static str, f: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .name(name.to_owned())
        .stack_size(128 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn deep_traversal_provider_128k_stack_audit() {
    const CHILD: &str = "SHARDLOOM_DEEP_TRAVERSAL_PROVIDER_AUDIT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        for depth in [64_usize, 512, 2_048] {
            spawn_on_128k_stack("vortex-array-audit", move || {
                run_vortex_array_small_stack_audit(depth);
            });
            #[cfg(feature = "universal-format-io")]
            spawn_on_128k_stack("arrow-record-batch-audit", move || {
                run_arrow_record_batch_slice_small_stack_audit(depth);
            });
        }
        return;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            concat!(module_path!(), "::deep_traversal_provider_128k_stack_audit")
                .strip_prefix("shardloom_vortex::")
                .unwrap(),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "status={:?}\nstdout={}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
mod deep_workflow_and_probe_tests {
    use super::super::*;
    use crate::relational_query::{VortexRelationalFilter, VortexRelationalSpillPolicy};
    use arrow_array::{
        Array as _, ArrayRef as ArrowArrayRef, Int64Array, ListArray as ArrowListArray,
        RecordBatch, StringArray, StructArray as ArrowStructArray,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use shardloom_core::{
        ComparisonOp, ExecutionResourceOrigin, ExecutionResources, ExprId, Expression,
        ExpressionKind, ScalarValue,
    };
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(prefix: &str) -> Self {
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let id = SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("shardloom-{prefix}-{}-{id}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn build_nested_arrow_column(
        depth: usize,
        mixed: bool,
        rows: usize,
        seed: i64,
    ) -> (Field, ArrowArrayRef) {
        let mut array: ArrowArrayRef = Arc::new(Int64Array::from_iter((0..rows).map(|row| {
            if rows >= 2 && row % 17 == 5 {
                None
            } else {
                Some(seed + i64::try_from(row).unwrap())
            }
        })));
        let mut field = Field::new("leaf", DataType::Int64, true);
        for level in 0..depth {
            let nulls = if rows >= 2 {
                Some(NullBuffer::from_iter(
                    (0..rows).map(|row| (row + level) % 19 != 11),
                ))
            } else {
                None
            };
            if mixed && level % 2 == 1 {
                let item_field = Arc::new(Field::new("item", field.data_type().clone(), true));
                let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1_usize, rows));
                array = Arc::new(ArrowListArray::new(
                    Arc::clone(&item_field),
                    offsets,
                    array,
                    nulls,
                ));
                field = Field::new(format!("l{level}"), DataType::List(item_field), true);
            } else {
                let child_field = Arc::new(Field::new(
                    format!("f{level}"),
                    field.data_type().clone(),
                    true,
                ));
                let fields: Fields = vec![Arc::clone(&child_field)].into();
                array = Arc::new(ArrowStructArray::new(fields.clone(), vec![array], nulls));
                field = Field::new(format!("s{level}"), DataType::Struct(fields), true);
            }
        }
        (field, array)
    }

    fn build_nested_record_batch(
        depth: usize,
        mixed: bool,
        rows: usize,
        with_payload: bool,
        seed: i64,
    ) -> RecordBatch {
        let ids: ArrowArrayRef = Arc::new(Int64Array::from_iter(
            (0..rows).map(|row| i64::try_from(row).unwrap()),
        ));
        let (nested_field, nested_array) = build_nested_arrow_column(depth, mixed, rows, seed);
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        let mut columns = vec![ids];
        if with_payload {
            let pad = "p".repeat(1200);
            let payloads: ArrowArrayRef = Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("{seed}-{row:04}-{pad}")),
            ));
            fields.push(Field::new("payload", DataType::Utf8, false));
            columns.push(payloads);
        }
        fields.push(Field::new("nested", nested_field.data_type().clone(), true));
        columns.push(nested_array);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn run_deep_24_filter_join_spill_write(
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        filter_min_id: i64,
        expected_rows: usize,
        out_vortex: &std::path::Path,
        spill_dir: &std::path::Path,
    ) {
        let token = CancellationToken::default();
        let session = ResidentVortexSession::new(128 << 20, 1).unwrap();
        let initial_memory = session.memory().clone();

        let left_source =
            ResidentMemorySource::from_arrow_record_batch(&session, left_batch.clone(), &token)
                .unwrap();
        let right_source =
            ResidentMemorySource::from_arrow_record_batch(&session, right_batch.clone(), &token)
                .unwrap();
        assert!(session.snapshot().memory.reserved_bytes > 0);

        let left_uri = DatasetUri::new("memory://deep24_left").unwrap();
        let right_uri = DatasetUri::new("memory://deep24_right").unwrap();

        let mut exec_policy = policy();
        exec_policy.resource_envelope.memory_budget_bytes = 128 << 20;

        let prepared = prepare_relational_with_schema_in_session(exec_policy, &session, |prep| {
            prep.register_memory_source(left_uri.clone(), |_| Ok(left_source))?;
            prep.register_memory_source(right_uri.clone(), |_| Ok(right_source))?;
            let filtered_left = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input: VortexRelationalPlan::Scan(VortexRelationalScan {
                    source_uri: left_uri,
                    projection: shardloom_plan::ProjectionRequest::All,
                    predicate: None,
                }),
                predicate: Expression::new(
                    ExprId::new("id_gt_39").unwrap(),
                    ExpressionKind::Compare {
                        left: Box::new(Expression::column(
                            ExprId::new("id").unwrap(),
                            ColumnRef::new("id").unwrap(),
                        )),
                        op: ComparisonOp::Gt,
                        right: Box::new(Expression::new(
                            ExprId::new("cutoff").unwrap(),
                            ExpressionKind::Literal(ScalarValue::Int64(filter_min_id)),
                        )),
                    },
                ),
            }));
            let right_scan = VortexRelationalPlan::Scan(VortexRelationalScan {
                source_uri: right_uri,
                projection: shardloom_plan::ProjectionRequest::All,
                predicate: None,
            });
            Ok(VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                left: filtered_left,
                right: right_scan,
                kind: JoinKind::Inner,
                condition: None,
                keys: vec![VortexRelationalJoinKey {
                    left: ColumnRef::new("id").unwrap(),
                    right: ColumnRef::new("id").unwrap(),
                }],
                columns: [
                    (Side::Left, "id", "id"),
                    (Side::Left, "payload", "left_payload"),
                    (Side::Right, "payload", "right_payload"),
                    (Side::Left, "nested", "left_nested"),
                    (Side::Right, "nested", "right_nested"),
                ]
                .into_iter()
                .map(|(side, col_name, out_name)| VortexRelationalJoinColumn {
                    side,
                    column: ColumnRef::new(col_name).unwrap(),
                    output_column: out_name.into(),
                })
                .collect(),
            })))
        })
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(spill_dir, 128 << 20, 1 << 20).unwrap())
        .unwrap();

        let written = prepared
            .write(
                out_vortex,
                crate::VortexLocalPrimitiveRowExportFormat::Vortex,
                true,
            )
            .unwrap();
        let spill_report = written.execution.spill.as_ref().expect("spill report");
        assert!(
            spill_report.runs_written > 0,
            "expected native spill runs > 0, got {}",
            spill_report.runs_written
        );
        assert!(spill_report.owned_cleanup_completed);
        assert_eq!(std::fs::read_dir(spill_dir).unwrap().count(), 0);
        assert_eq!(written.output.rows_written, expected_rows as u64);

        drop((written, prepared, session));
        assert_eq!(initial_memory.snapshot().reserved_bytes, 0);
    }

    fn verify_deep_24_reopen_cancellation_and_refusal(
        left_batch: RecordBatch,
        out_vortex: &std::path::Path,
        expected_nested_dt: &DataType,
        filter_min_id: i64,
        rows: usize,
        expected_rows: usize,
    ) {
        let token = CancellationToken::default();
        let mut exec_policy = policy();
        exec_policy.resource_envelope.memory_budget_bytes = 128 << 20;

        let reopen_session = ResidentVortexSession::new(128 << 20, 1).unwrap();
        let reopen_memory = reopen_session.memory().clone();
        let scan_plan = VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(out_vortex.display().to_string()).unwrap(),
            projection: shardloom_plan::ProjectionRequest::All,
            predicate: None,
        });
        let reopen_prepared =
            prepare_relational_in_session(&scan_plan, exec_policy, &reopen_session).unwrap();
        let mut retained = Vec::new();
        let mut delivered_rows = 0_usize;
        reopen_prepared
            .for_each_arrow_batch(&token, |batch| {
                delivered_rows += batch.num_rows();
                retained.push(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(delivered_rows, expected_rows);
        assert!(retained.len() >= 2, "expected multiple streamed batches");

        let mut seen_ids = Vec::with_capacity(expected_rows);
        for batch in &retained {
            for col_name in ["left_nested", "right_nested"] {
                assert_eq!(
                    batch
                        .schema()
                        .field_with_name(col_name)
                        .unwrap()
                        .data_type(),
                    expected_nested_dt
                );
            }
            let id_col = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for r in 0..batch.num_rows() {
                seen_ids.push(id_col.value(r));
            }
        }
        seen_ids.sort_unstable();
        let expected_ids: Vec<i64> = ((filter_min_id + 1)..i64::try_from(rows).unwrap()).collect();
        assert_eq!(seen_ids, expected_ids);

        let pre_cancel = CancellationToken::default();
        pre_cancel.cancel();
        assert!(
            ResidentMemorySource::from_arrow_record_batch(
                &reopen_session,
                left_batch.clone(),
                &pre_cancel
            )
            .is_err()
        );
        assert!(
            reopen_prepared
                .for_each_arrow_batch(&pre_cancel, |_| Ok(()))
                .is_err()
        );

        let mid_cancel = CancellationToken::default();
        let mut seen_batches = 0_usize;
        let mid_err = reopen_prepared.for_each_arrow_batch(&mid_cancel, |_| {
            seen_batches += 1;
            if seen_batches == 1 {
                mid_cancel.cancel();
            }
            Ok(())
        });
        assert!(mid_err.is_err(), "expected mid-stream cancellation to fail");

        drop((reopen_prepared, reopen_session));
        assert!(reopen_memory.snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(reopen_memory.snapshot().reserved_bytes, 0);

        let tiny_session = ResidentVortexSession::new(64 * 1024, 1).unwrap();
        let tiny_memory = tiny_session.memory().clone();
        assert!(
            ResidentMemorySource::from_arrow_record_batch(&tiny_session, left_batch, &token)
                .is_err()
        );
        let tiny_policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
            ExecutionResources::from_bytes(64 * 1024, 1, ExecutionResourceOrigin::Context).unwrap(),
        )
        .unwrap();
        assert!(prepare_relational_in_session(&scan_plan, tiny_policy, &tiny_session).is_err());
        drop(tiny_session);
        assert_eq!(tiny_memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn deep_24_end_to_end_nested_workflow_spill_vortex_arrow_and_cleanup() {
        // 23 wrappers around `Int64` inside column `"nested"` puts the leaf at
        // depth 24 relative to the root Struct (`0`), matching the current limit.
        let container_depth = 23_usize;
        let rows = 2_150_usize;
        let filter_min_id = 39_i64;
        let expected_rows = rows - (usize::try_from(filter_min_id).unwrap() + 1); // 2110 > BATCH_ROWS (2048)

        let left_batch = build_nested_record_batch(container_depth, true, rows, true, 10_000);
        let right_batch = build_nested_record_batch(container_depth, true, rows, true, 20_000);
        let expected_nested_dt = left_batch
            .schema()
            .field_with_name("nested")
            .unwrap()
            .data_type()
            .clone();

        let dir = TempDir::new("deep24-e2e");
        let spill_dir = dir.path().join("spill");
        std::fs::create_dir_all(&spill_dir).unwrap();
        let out_vortex = dir.path().join("joined.vortex");

        run_deep_24_filter_join_spill_write(
            &left_batch,
            &right_batch,
            filter_min_id,
            expected_rows,
            &out_vortex,
            &spill_dir,
        );
        verify_deep_24_reopen_cancellation_and_refusal(
            left_batch,
            &out_vortex,
            &expected_nested_dt,
            filter_min_id,
            rows,
            expected_rows,
        );
    }

    fn build_nested_json_value(depth: usize, mixed: bool, value: i64) -> serde_json::Value {
        let mut current = serde_json::json!(value);
        for level in 0..depth {
            if mixed && level % 2 == 1 {
                current = serde_json::Value::Array(vec![current]);
            } else {
                let mut map = serde_json::Map::new();
                map.insert(format!("f{level}"), current);
                current = serde_json::Value::Object(map);
            }
        }
        let mut root = serde_json::Map::new();
        root.insert("id".to_owned(), serde_json::json!(1_i64));
        root.insert("nested".to_owned(), current);
        serde_json::Value::Object(root)
    }

    fn probe_vortex_file(
        batch: &RecordBatch,
        path: &std::path::Path,
    ) -> std::result::Result<(), String> {
        use vortex::array::ArrayRef;
        use vortex::arrow::ArrowSessionExt as _;
        use vortex::file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _};
        use vortex::io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        };
        use vortex::session::VortexSession;

        let runtime = CurrentThreadRuntime::new();
        let vsession = VortexSession::default().with_handle(runtime.handle());
        crate::native_extensions::register(&vsession);
        let schema = batch.schema();
        let array = vsession
            .arrow()
            .from_arrow_record_batch(batch.clone(), schema.as_ref())
            .map_err(|e| format!("from_arrow: {e}"))?;
        let mut file = std::fs::File::create(path).map_err(|e| format!("create: {e}"))?;
        let mut writer = vsession
            .write_options()
            .blocking(&runtime)
            .writer(&mut file, array.dtype().clone());
        writer.push(array).map_err(|e| format!("push: {e}"))?;
        writer.finish().map_err(|e| format!("finish: {e}"))?;
        let reopened = runtime
            .block_on(vsession.open_options().open_path(path))
            .map_err(|e| format!("open_path: {e}"))?;
        let arrays = reopened
            .scan()
            .map_err(|e| format!("scan: {e}"))?
            .with_ordered(true)
            .into_array_iter(&runtime)
            .map_err(|e| format!("into_array_iter: {e}"))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("collect: {e}"))?;
        let total_rows: usize = arrays.iter().map(ArrayRef::len).sum();
        (total_rows == 4)
            .then_some(())
            .ok_or_else(|| format!("row mismatch: {total_rows}"))
    }

    fn probe_vortex_ipc(batch: &RecordBatch) -> std::result::Result<(), String> {
        use std::io::Cursor;
        use vortex::array::ArrayRef;
        use vortex::arrow::ArrowSessionExt as _;
        use vortex::io::{runtime::current::CurrentThreadRuntime, session::RuntimeSessionExt as _};
        use vortex::ipc::{
            iterator::SyncIPCReader,
            messages::{EncoderMessage, MessageEncoder},
        };
        use vortex::session::VortexSession;

        let runtime = CurrentThreadRuntime::new();
        let vsession = VortexSession::default().with_handle(runtime.handle());
        crate::native_extensions::register(&vsession);
        let schema = batch.schema();
        let array = vsession
            .arrow()
            .from_arrow_record_batch(batch.clone(), schema.as_ref())
            .map_err(|e| format!("from_arrow: {e}"))?;
        let mut encoder = MessageEncoder::new(vsession.clone());
        let dtype_parts = encoder
            .encode(EncoderMessage::DType(array.dtype()))
            .map_err(|e| format!("encode dtype: {e}"))?;
        let array_parts = encoder
            .encode(EncoderMessage::Array(&array))
            .map_err(|e| format!("encode array: {e}"))?;
        let mut stream = Vec::new();
        for part in dtype_parts.into_iter().chain(array_parts) {
            stream.extend_from_slice(&part);
        }
        let reader = SyncIPCReader::try_new(Cursor::new(stream), &vsession)
            .map_err(|e| format!("SyncIPCReader::try_new: {e}"))?;
        let decoded = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("IPC collect: {e}"))?;
        let total_rows: usize = decoded.iter().map(ArrayRef::len).sum();
        (total_rows == 4)
            .then_some(())
            .ok_or_else(|| format!("IPC row mismatch: {total_rows}"))
    }

    fn probe_arrow_ipc(batch: &RecordBatch) -> std::result::Result<(), String> {
        use arrow_ipc::{reader::FileReader, writer::FileWriter};
        use std::io::Cursor;

        let schema = batch.schema();
        let mut buf = Vec::new();
        {
            let mut w =
                FileWriter::try_new(&mut buf, &schema).map_err(|e| format!("writer_new: {e}"))?;
            w.write(batch).map_err(|e| format!("write: {e}"))?;
            w.finish().map_err(|e| format!("finish: {e}"))?;
        }
        let reader =
            FileReader::try_new(Cursor::new(buf), None).map_err(|e| format!("reader_new: {e}"))?;
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("read: {e}"))?;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        (total_rows == 4)
            .then_some(())
            .ok_or_else(|| format!("row mismatch: {total_rows}"))
    }

    fn probe_parquet(batch: &RecordBatch) -> std::result::Result<(), String> {
        use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

        let schema = batch.schema();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), None)
                .map_err(|e| format!("writer_new: {e}"))?;
            w.write(batch).map_err(|e| format!("write: {e}"))?;
            w.close().map_err(|e| format!("close: {e}"))?;
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf))
            .map_err(|e| format!("reader_builder: {e}"))?
            .build()
            .map_err(|e| format!("build: {e}"))?;
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("read: {e}"))?;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        (total_rows == 4)
            .then_some(())
            .ok_or_else(|| format!("row mismatch: {total_rows}"))
    }

    fn probe_avro(batch: &RecordBatch) -> std::result::Result<(), String> {
        use arrow_avro::{reader::ReaderBuilder, writer::AvroWriter};
        use std::io::Cursor;

        let schema = batch.schema();
        let mut buf = Vec::new();
        {
            let mut w = AvroWriter::new(&mut buf, schema.as_ref().clone())
                .map_err(|e| format!("writer_new: {e}"))?;
            w.write(batch).map_err(|e| format!("write: {e}"))?;
            w.finish().map_err(|e| format!("finish: {e}"))?;
        }
        let reader = ReaderBuilder::new()
            .build(Cursor::new(buf))
            .map_err(|e| format!("reader_build: {e}"))?;
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("read: {e}"))?;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        (total_rows == 4)
            .then_some(())
            .ok_or_else(|| format!("row mismatch: {total_rows}"))
    }

    fn probe_orc(batch: &RecordBatch, path: &std::path::Path) -> std::result::Result<(), String> {
        use orc_rust::{ArrowReaderBuilder, ArrowWriterBuilder};

        let translated = crate::universal_format_io::translate_orc_schema(&batch.schema())
            .map_err(|e| format!("translate: {e}"))?;
        {
            let file = std::fs::File::create(path).map_err(|e| format!("create: {e}"))?;
            let mut w = ArrowWriterBuilder::new(file, Arc::clone(&translated))
                .try_build()
                .map_err(|e| format!("writer_build: {e}"))?;
            w.write(batch).map_err(|e| format!("write: {e}"))?;
            w.close().map_err(|e| format!("close: {e}"))?;
        }
        let file = std::fs::File::open(path).map_err(|e| format!("open: {e}"))?;
        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| format!("reader_new: {e}"))?
            .with_schema(translated)
            .build();
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| format!("read: {e}"))?;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        (total_rows == 4)
            .then_some(())
            .ok_or_else(|| format!("row mismatch: {total_rows}"))
    }

    fn probe_json(mixed: bool, depth: usize) -> std::result::Result<(), String> {
        let v = build_nested_json_value(depth, mixed, 42);
        let mut line = serde_json::to_vec(&v).map_err(|e| format!("to_vec: {e}"))?;
        line.push(b'\n');
        let parsed: serde_json::Value = serde_json::from_slice(&line[..line.len() - 1])
            .map_err(|e| format!("from_slice: {e}"))?;
        (parsed == v)
            .then_some(())
            .ok_or_else(|| "JSON round-trip value mismatch".to_owned())
    }

    fn run_single_boundary_probe(
        shape: &str,
        mixed: bool,
        depth: usize,
        boundary: &str,
    ) -> std::result::Result<(), String> {
        let dir = TempDir::new("depth-probe-cell");
        let batch = build_nested_record_batch(depth, mixed, 4, false, 100);
        match boundary {
            "1_vortex_file" => {
                probe_vortex_file(&batch, &dir.path().join(format!("{shape}-{depth}.vortex")))
            }
            "2_vortex_ipc" => probe_vortex_ipc(&batch),
            "3_arrow_ipc" => probe_arrow_ipc(&batch),
            "4_parquet" => probe_parquet(&batch),
            "5_avro" => probe_avro(&batch),
            "6_orc" => probe_orc(&batch, &dir.path().join(format!("{shape}-{depth}.orc"))),
            "7_json" => probe_json(mixed, depth),
            other => Err(format!("unknown boundary {other}")),
        }
    }

    #[test]
    fn deep_24_all_seven_format_boundaries_and_32_limit_probe() {
        let boundaries = [
            "1_vortex_file",
            "2_vortex_ipc",
            "3_arrow_ipc",
            "4_parquet",
            "5_avro",
            "6_orc",
            "7_json",
        ];

        // Container depth 23 inside `"nested"` gives leaf depth 24 relative to
        // the root Struct, matching the admitted depth-24 public payload ceiling.
        for (shape, mixed) in [("pure_struct", false), ("mixed_struct_list", true)] {
            for boundary in boundaries {
                run_single_boundary_probe(shape, mixed, 23, boundary).unwrap_or_else(|err| {
                    panic!("expected {shape} depth=24 (23 nested) {boundary} to pass: {err}");
                });
            }
        }

        // Default verifier / schema recursion limits refuse deeper nesting by
        // container depth 32 without overriding VORTEX_MAX_LAYOUT_DEPTH or
        // FlatBuffers max_depth.
        for (shape, mixed) in [("pure_struct", false), ("mixed_struct_list", true)] {
            let file_err = run_single_boundary_probe(shape, mixed, 32, "1_vortex_file")
                .expect_err("expected 1_vortex_file to reject container depth 32");
            assert!(
                file_err.contains("Nested table depth limit reached"),
                "{shape} 1_vortex_file depth=32 unexpected error: {file_err}"
            );

            let ipc_err = run_single_boundary_probe(shape, mixed, 32, "2_vortex_ipc")
                .expect_err("expected 2_vortex_ipc to reject container depth 32");
            assert!(
                ipc_err.contains("Nested table depth limit reached"),
                "{shape} 2_vortex_ipc depth=32 unexpected error: {ipc_err}"
            );
        }

        let avro_err = run_single_boundary_probe("pure_struct", false, 32, "5_avro")
            .expect_err("expected pure_struct 5_avro to reject container depth 32");
        assert!(
            avro_err.contains("recursion limit exceeded"),
            "pure_struct 5_avro depth=32 unexpected error: {avro_err}"
        );
    }
}
