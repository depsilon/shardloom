// SPDX-License-Identifier: Apache-2.0

use super::*;
use arrow_schema::{DataType as ArrowType, Field as ArrowField, TimeUnit};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use vortex::array::dtype::{DType, DecimalDType, Nullability, PType, StructFields};

fn small_stack<F>(name: &str, f: F)
where
    F: FnOnce() + Send + 'static,
{
    let handle = std::thread::Builder::new()
        .name(name.into())
        .stack_size(128 * 1024)
        .spawn(f)
        .unwrap();
    handle.join().unwrap();
}

fn deep_mixed_dtype(depth: usize) -> DType {
    let mut current = DType::Primitive(PType::I64, Nullability::Nullable);
    for level in 0..depth {
        current = match level % 3 {
            0 => DType::List(Arc::new(current), Nullability::Nullable),
            1 => DType::FixedSizeList(Arc::new(current), 2, Nullability::NonNullable),
            _ => DType::Struct(
                StructFields::new(["inner"].into(), vec![current]),
                Nullability::Nullable,
            ),
        };
    }
    current
}

fn deep_avro_struct(depth: usize, with_avro_meta: bool) -> ArrowField {
    let mut current = ArrowField::new("amount", ArrowType::Decimal128(38, 6), true);
    if with_avro_meta {
        current = current.with_metadata(
            [
                ("precision".into(), "38".into()),
                ("scale".into(), "6".into()),
                ("avro.name".into(), "Money".into()),
            ]
            .into(),
        );
    }
    for level in 0..depth {
        let field_name = if level % 2 == 0 { "items" } else { "nested" };
        let dt = if level % 2 == 0 {
            ArrowType::List(Arc::new(current))
        } else {
            ArrowType::Struct(vec![current].into())
        };
        current = ArrowField::new(field_name, dt, true);
        if with_avro_meta && level % 2 == 1 {
            current =
                current.with_metadata([("avro.namespace".into(), "com.example".into())].into());
        }
    }
    current
}

#[test]
fn shallow_admitted_conversion_and_field_count_match_baseline() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let cancel = CancellationToken::default();

    let scalar = DType::Primitive(PType::I32, Nullability::NonNullable);
    assert_eq!(
        field("x", &scalar),
        field_admitted("x", &scalar, (&memory, &cancel)).unwrap()
    );
    assert_eq!(
        data_type(&scalar),
        data_type_admitted(&scalar, (&memory, &cancel)).unwrap()
    );
    assert_eq!(field_count(&scalar), 1);
    assert_eq!(
        field_count_admitted(&scalar, (&memory, &cancel)).unwrap(),
        1
    );

    let dec = DType::Decimal(DecimalDType::new(10, 2), Nullability::Nullable);
    assert_eq!(
        data_type_admitted(&dec, (&memory, &cancel)).unwrap(),
        Some(ArrowType::Decimal128(10, 2))
    );

    let nested = deep_mixed_dtype(5);
    let f = field("nested", &nested).unwrap();
    let fa = field_admitted("nested", &nested, (&memory, &cancel))
        .unwrap()
        .unwrap();
    assert_eq!(f, fa);
    assert_eq!(
        data_type(&nested),
        data_type_admitted(&nested, (&memory, &cancel)).unwrap()
    );
    assert_eq!(field_count(&nested), 6);
    assert_eq!(
        field_count_admitted(&nested, (&memory, &cancel)).unwrap(),
        6
    );

    let overflow = DType::FixedSizeList(
        Arc::new(DType::Primitive(PType::I8, Nullability::Nullable)),
        u32::MAX,
        Nullability::Nullable,
    );
    assert_eq!(field("huge", &overflow), None);
    assert_eq!(
        field_admitted("huge", &overflow, (&memory, &cancel)).unwrap(),
        None
    );

    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn deep_conversion_matching_and_refusal_on_small_stack() {
    small_stack(
        "native_arrow_conversion::tests::deep_conversion_matching_and_refusal_on_small_stack",
        || {
            let depth = 2_048;
            let deep = deep_mixed_dtype(depth);
            let memory = LiveMemoryPool::new(32 << 20).unwrap();
            let cancel = CancellationToken::default();

            let count = field_count(&deep);
            assert_eq!(count, depth + 1);
            assert_eq!(
                field_count_admitted(&deep, (&memory, &cancel)).unwrap(),
                count
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);

            let dt = data_type(&deep).unwrap();
            let dt_admitted = data_type_admitted(&deep, (&memory, &cancel))
                .unwrap()
                .unwrap();
            assert_eq!(dt, dt_admitted);
            assert_eq!(memory.snapshot().reserved_bytes, 0);

            let f = field("root", &deep).unwrap();
            let f_admitted = field_admitted("root", &deep, (&memory, &cancel))
                .unwrap()
                .unwrap();
            assert_eq!(f, f_admitted);
            assert_eq!(memory.snapshot().reserved_bytes, 0);

            // Credit refusal mid-traversal releases all partial frames cleanly.
            let tiny = LiveMemoryPool::new(2048).unwrap();
            assert!(field_admitted("root", &deep, (&tiny, &cancel)).is_err());
            assert_eq!(tiny.snapshot().reserved_bytes, 0);
            assert!(data_type_admitted(&deep, (&tiny, &cancel)).is_err());
            assert_eq!(tiny.snapshot().reserved_bytes, 0);

            // Linear chains hold only one frontier entry in `field_count_admitted`,
            // while tight budgets or wide/branching structs refuse stack growth cleanly.
            let micro = LiveMemoryPool::new(16).unwrap();
            assert!(field_count_admitted(&deep, (&micro, &cancel)).is_err());
            assert_eq!(micro.snapshot().reserved_bytes, 0);
            let wide_fields = (0..128).map(|idx| format!("f{idx}")).collect::<Vec<_>>();
            let wide_names = wide_fields.iter().map(String::as_str).collect::<Vec<_>>();
            let wide = DType::Struct(
                StructFields::new(
                    wide_names.into(),
                    vec![DType::Primitive(PType::I64, Nullability::Nullable); 128],
                ),
                Nullability::Nullable,
            );
            assert!(field_count_admitted(&wide, (&tiny, &cancel)).is_err());
            assert_eq!(tiny.snapshot().reserved_bytes, 0);

            // Cancellation check fires cleanly with zero retained bytes.
            cancel.cancel();
            assert!(field_admitted("root", &deep, (&memory, &cancel)).is_err());
            assert_eq!(memory.snapshot().reserved_bytes, 0);

            // Deep Avro schema matching handles 2,048 levels without recursion.
            let expected = deep_avro_struct(depth, false);
            let actual_ok = deep_avro_struct(depth, true);
            assert!(avro_field_matches(&actual_ok, &expected));

            let bad_leaf = ArrowField::new(
                "amount",
                ArrowType::Timestamp(TimeUnit::Microsecond, None),
                true,
            );
            let mut mismatch = bad_leaf;
            for level in 0..depth {
                let field_name = if level % 2 == 0 { "items" } else { "nested" };
                let dt = if level % 2 == 0 {
                    ArrowType::List(Arc::new(mismatch))
                } else {
                    ArrowType::Struct(vec![mismatch].into())
                };
                mismatch = ArrowField::new(field_name, dt, true);
            }
            assert!(!avro_field_matches(&actual_ok, &mismatch));
        },
    );
}
