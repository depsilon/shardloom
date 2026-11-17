use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use vortex::array::dtype::{Nullability, PType, StructFields};

fn wide_nullable_i64(width: usize) -> DType {
    DType::Struct(
        StructFields::from_iter((0..width).map(|index| {
            (
                format!("field_{index}"),
                DType::Primitive(PType::I64, Nullability::Nullable),
            )
        })),
        Nullability::Nullable,
    )
}

#[test]
fn wide_nested_schema_uses_the_callers_memory_grant_and_retains_credit() {
    let dtype = DType::List(wide_nullable_i64(1025).into(), Nullability::Nullable);
    let larger = DType::List(wide_nullable_i64(8500).into(), Nullability::Nullable);
    let memory = LiveMemoryPool::new(32 * 1024 * 1024).unwrap();

    assert!(metadata_bytes(&dtype, &memory).unwrap() > 0);
    assert!(schema_bytes(&dtype, &memory).unwrap() > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let estimate = metadata_bytes(&larger, &memory).unwrap();
    assert!(estimate > 8 * 1024 * 1024);
    let lease = reserve_metadata(&larger, &memory).unwrap();
    assert_eq!(lease.bytes(), estimate);
    assert!(memory.snapshot().reserved_bytes >= lease.bytes());
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let constrained = LiveMemoryPool::new(1).unwrap();
    assert!(reserve_metadata(&larger, &constrained).is_err());
    assert!(constrained.snapshot().denied_reservations > 0);
    assert_eq!(constrained.snapshot().reserved_bytes, 0);
}

#[test]
fn repeated_shared_subtrees_are_denied_by_the_resource_budget() {
    let mut dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    for depth in 0..24 {
        dtype = DType::Struct(
            StructFields::from_iter([
                (format!("left_{depth}"), dtype.clone()),
                (format!("right_{depth}"), dtype),
            ]),
            Nullability::Nullable,
        );
    }
    let memory = LiveMemoryPool::new(256 * 1024).unwrap();
    assert!(reserve_metadata(&dtype, &memory).is_err());
    assert!(memory.snapshot().denied_reservations > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn schema_continuations_are_admitted_separately_and_refunded_before_retention() {
    let leaf = DType::Primitive(PType::I64, Nullability::Nullable);
    let mut dtype = leaf.clone();
    for _ in 0..20 {
        dtype = DType::Struct(
            StructFields::from_iter([("nested", dtype), ("sibling", leaf.clone())]),
            Nullability::Nullable,
        );
    }
    let ample = LiveMemoryPool::new(1 << 20).unwrap();
    let retained_bytes = metadata_bytes(&dtype, &ample).unwrap();
    assert_eq!(ample.snapshot().reserved_bytes, 0);

    // Retained schema storage alone is insufficient while continuation capacity
    // overlaps the traversal. Refusal must preserve unrelated live ownership.
    let memory = LiveMemoryPool::new(retained_bytes + 4096).unwrap();
    let existing = memory.reserve(4096).unwrap();
    assert!(reserve_metadata(&dtype, &memory).is_err());
    assert!(memory.snapshot().denied_reservations > 0);
    assert_eq!(memory.snapshot().reserved_bytes, existing.bytes());
    drop(existing);
    let retained = reserve_metadata(&dtype, &memory).unwrap();
    assert_eq!(retained.bytes(), retained_bytes);
    assert_eq!(memory.snapshot().reserved_bytes, retained.bytes());
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn schema_continuations_preserve_sibling_error_order() {
    let mut too_deep = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..25 {
        too_deep = DType::List(too_deep.into(), Nullability::Nullable);
    }
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let unsupported_first = DType::Struct(
        StructFields::from_iter([("first", DType::Null), ("second", too_deep.clone())]),
        Nullability::Nullable,
    );
    assert!(matches!(
        reserve_metadata(&unsupported_first, &memory),
        Err(ShardLoomError::NotImplemented(_))
    ));
    let depth_first = DType::Struct(
        StructFields::from_iter([("first", too_deep), ("second", DType::Null)]),
        Nullability::Nullable,
    );
    let error = reserve_metadata(&depth_first, &memory).unwrap_err();
    assert!(error.to_string().contains("depth 24"));
    assert_eq!(memory.snapshot().denied_reservations, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn schema_names_can_exceed_the_former_metadata_budget_without_exceeding_node_bounds() {
    let dtype = DType::Struct(
        StructFields::from_iter([(
            "n".repeat((4 << 20) + 1),
            DType::Primitive(PType::I64, Nullability::Nullable),
        )]),
        Nullability::Nullable,
    );
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    // One struct and one leaf isolate the old byte ceiling from width/nodes.
    let credit = reserve_metadata(&dtype, &memory).unwrap();
    assert!(credit.bytes() > 8 << 20);
    drop(credit);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn pre_cancelled_inspection_rejects_without_reserving_memory() {
    let memory = LiveMemoryPool::new(32 * 1024 * 1024).unwrap();
    let dtype = wide_nullable_i64(1025);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(metadata_bytes(&dtype, (&memory, &cancelled)).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn failed_schema_growth_preserves_other_owners() {
    let memory = LiveMemoryPool::new(32 << 10).unwrap();
    let existing = memory.reserve(8192).unwrap();
    assert!(reserve_metadata(&wide_nullable_i64(1025), &memory).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, existing.bytes());
    assert!(memory.snapshot().denied_reservations > 0);
    drop(existing);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn wide_result_serialization_admits_scratch_separately_from_wire_size() {
    let dtype = wide_nullable_i64(8500);
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    let scratch = reserve_serialization(&dtype, &memory).unwrap();
    assert!(scratch.bytes() > 8 << 20);
    drop(scratch);
    let output =
        crate::local_primitives::collect::serialize_result_schema(&dtype, &memory).unwrap();
    assert!(output.value().len() < 8 << 20);
    let wire: serde_json::Value = serde_json::from_str(output.value()).unwrap();
    assert_eq!(wire["Struct"][0]["names"].as_array().unwrap().len(), 8500);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(reserve_serialization(&dtype, (&memory, &cancellation)).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn result_schema_walk_accounts_all_native_children_and_refunds_scratch() {
    use vortex::array::dtype::{MapDType, UnionVariants};
    use vortex::array::extension::datetime::{TimeUnit, Timestamp};

    let leaf = DType::Primitive(PType::I64, Nullability::NonNullable);
    let map = DType::Map(
        MapDType::try_new(leaf.clone(), leaf.clone(), false).unwrap(),
        Nullability::Nullable,
    );
    let union = DType::Union(
        UnionVariants::new(["map", "leaf"].into(), vec![map, leaf.clone()]).unwrap(),
        Nullability::Nullable,
    );
    let dtype = DType::Struct(
        StructFields::from_iter([
            ("union", union),
            (
                "list",
                DType::List(leaf.clone().into(), Nullability::Nullable),
            ),
            (
                "fixed",
                DType::FixedSizeList(leaf.into(), 2, Nullability::Nullable),
            ),
            (
                "time",
                DType::Extension(
                    Timestamp::new(TimeUnit::Nanoseconds, Nullability::Nullable).erased(),
                ),
            ),
        ]),
        Nullability::Nullable,
    );
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let credit = reserve_serialization(&dtype, &memory).unwrap();
    // Struct + union/map/three leaves + list/leaf + fixed/leaf + extension/storage.
    assert_eq!(credit.bytes(), 12 * 1024);
    assert_eq!(memory.snapshot().reserved_bytes, credit.bytes());
    drop(credit);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let constrained = LiveMemoryPool::new(12 * 1024).unwrap();
    assert!(reserve_serialization(&dtype, &constrained).is_err());
    assert_eq!(constrained.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn iterative_result_schema_walk_keeps_its_distinct_depth_boundary() {
    let mut dtype = DType::Null;
    for _ in 0..64 {
        dtype = DType::List(dtype.into(), Nullability::Nullable);
    }
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let credit = reserve_serialization(&dtype, &memory).unwrap();
    assert_eq!(credit.bytes(), 65 * 1024);
    drop(credit);
    dtype = DType::List(dtype.into(), Nullability::Nullable);
    let error = reserve_serialization(&dtype, &memory).unwrap_err();
    assert!(error.to_string().contains("64 nested levels"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn wide_arrow_metadata_and_shared_declarations_preserve_resource_admission() {
    use arrow_schema::{DataType, Field};
    let wide = DataType::Struct(
        (0..8500)
            .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
            .collect(),
    );
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    assert!(arrow_metadata_bytes(&wide, &memory).unwrap() > 8 << 20);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    validate_arrow_declaration(&wide).unwrap();
    let constrained = LiveMemoryPool::new(1024).unwrap();
    assert!(arrow_metadata_bytes(&wide, &constrained).is_err());
    assert_eq!(constrained.snapshot().reserved_bytes, 0);

    let mut shared = DataType::Int64;
    for _ in 0..24 {
        shared = DataType::Struct(
            vec![
                Field::new("left", shared.clone(), true),
                Field::new("right", shared, true),
            ]
            .into(),
        );
    }
    validate_arrow_declaration(&shared).unwrap();
    let expanded = LiveMemoryPool::new(256 << 10).unwrap();
    assert!(arrow_metadata_bytes(&shared, &expanded).is_err());
    assert_eq!(expanded.snapshot().reserved_bytes, 0);
}

#[test]
fn cancellation_during_name_walk_stops_traversal_and_releases_budget() {
    let memory = LiveMemoryPool::new(32 * 1024 * 1024).unwrap();
    let names: Vec<String> = (0..1025).map(|index| format!("field_{index}")).collect();
    let cancellation = CancellationToken::default();
    let scope = Scope::from((&memory, &cancellation));
    let mut budget = Budget::new(scope).unwrap();
    let visits = AtomicUsize::new(0);
    let result = budget.names(1025, |index| {
        visits.fetch_add(1, Ordering::SeqCst);
        if index == 512 {
            cancellation.cancel();
        }
        names[index].as_str()
    });
    assert!(result.is_err());
    assert_eq!(visits.load(Ordering::SeqCst), 513);
    drop(budget);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[cfg(all(feature = "universal-format-io", feature = "vortex-write"))]
#[test]
fn native_uuid_shared_field_admission_retains_declarations_and_resource_limits() {
    use arrow_schema::{DataType, Field};
    let memory = LiveMemoryPool::new(32 << 10).unwrap();
    let field = Field::new("id", DataType::FixedSizeBinary(16), true)
        .with_metadata([("ARROW:extension:name".into(), "arrow.uuid".into())].into());
    for declared in [
        field.clone(),
        field.clone().with_data_type(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::FixedSizeBinary(16)),
        )),
    ] {
        assert!(arrow_field_metadata_bytes(&declared, &memory).unwrap() > 0);
        assert!(arrow_file_metadata_bytes(&declared, &memory).unwrap() > 0);
    }
    for invalid in [
        field
            .clone()
            .with_metadata(std::collections::HashMap::default()),
        field.clone().with_data_type(DataType::FixedSizeBinary(15)),
        field
            .clone()
            .with_metadata([("ARROW:extension:name".into(), "other.uuid".into())].into()),
    ] {
        assert!(arrow_field_metadata_bytes(&invalid, &memory).is_err());
        assert!(arrow_file_metadata_bytes(&invalid, &memory).is_err());
    }
    let denied = LiveMemoryPool::new(1).unwrap();
    assert!(arrow_file_metadata_bytes(&field, &denied).is_err());
    assert!(denied.snapshot().denied_reservations > 0);
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(arrow_file_metadata_bytes(&field, (&memory, &cancellation)).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn support_and_schema_validation_preserve_error_distinctions_and_release_credit() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    assert!(!supported(&DType::Null, &memory).unwrap());
    let tiny = LiveMemoryPool::new(1).unwrap();
    assert!(
        supported(
            &DType::Primitive(PType::I64, Nullability::NonNullable),
            &tiny
        )
        .is_err()
    );

    let duplicate = DType::Struct(
        StructFields::from_iter([
            (
                "duplicate",
                DType::Primitive(PType::I64, Nullability::Nullable),
            ),
            (
                "duplicate",
                DType::Primitive(PType::I64, Nullability::Nullable),
            ),
        ]),
        Nullability::Nullable,
    );
    assert!(reserve_schema(&duplicate, &memory).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let mut too_deep = DType::Primitive(PType::I64, Nullability::Nullable);
    for _ in 0..25 {
        too_deep = DType::List(too_deep.into(), Nullability::Nullable);
    }
    assert!(reserve_schema(&too_deep, &memory).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
