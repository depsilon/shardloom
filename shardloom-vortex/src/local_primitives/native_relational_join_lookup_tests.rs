use super::*;
use crate::{
    local_primitives::{
        native_relational_join::{Kind, Side},
        native_relational_records as private,
        native_relational_spill::{Ordering, State},
        result_batch::{self, Value},
    },
    relational_query::VortexRelationalSpillPolicy,
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    IntoArray as _, VortexSessionExecute as _,
    arrays::{PrimitiveArray, StructArray},
    dtype::{DType, FieldNames, Nullability, PType},
    memory::MemorySessionExt as _,
    validity::Validity,
};

#[test]
#[allow(clippy::too_many_lines)] // Private record mutation and complete candidate oracle share one context.
fn ordered_join_hash_collisions_still_compare_exact_keys_and_pack_across_blocks() {
    let workspace = std::env::temp_dir().join(format!(
        "shardloom-join-collision-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&workspace).unwrap();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 1 << 20).unwrap();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let key = |row: u32| {
        (!row.is_multiple_of(7)).then_some(if row.is_multiple_of(3) { 42u64 } else { 7 })
    };
    let expected = (0..41u32)
        .filter(|row| key(*row) == Some(42))
        .collect::<Vec<_>>();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let spec = Spec {
                kind: Kind::Inner,
                condition: None,
                left_keys: vec!["key".into()],
                right_keys: vec!["key".into()],
                fields: vec![(
                    "id".into(),
                    DType::Primitive(PType::U32, Nullability::NonNullable),
                )],
                columns: vec![(Side::Right, "id".into())],
            };
            let right = StructArray::new(
                FieldNames::from(["key", "id"]),
                vec![
                    PrimitiveArray::from_option_iter((0..41u32).map(key)).into_array(),
                    PrimitiveArray::from_iter(0..41u32).into_array(),
                ],
                41,
                Validity::NonNullable,
            )
            .into_array();
            let fields = vec![
                (
                    "key".into(),
                    DType::Primitive(PType::U64, Nullability::Nullable),
                ),
                (
                    "id".into(),
                    DType::Primitive(PType::U32, Nullability::NonNullable),
                ),
            ];
            let layout = super::super::records::Layout::new(&spec, &fields, context)?;
            let left = Batch::new(
                StructArray::new(
                    FieldNames::from(["key"]),
                    vec![PrimitiveArray::from_option_iter([Some(42u64), None]).into_array()],
                    2,
                    Validity::NonNullable,
                )
                .into_array(),
                &spec.left_keys,
                context,
            )?;
            let hash = left.hash(0, false)?.unwrap();
            let source = Batch::new(right, &spec.right_keys, context)?;
            let record = layout.record(&source, 0..41, 0, context)?;
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(3)?;
            // Force distinct nonnull keys into one hash range. Hashes may select
            // candidates but must never establish equality, even across blocks.
            columns.values.push(result_batch::build_column(
                &private::u64_type().as_nullable(),
                41,
                &context.native_session().allocator(),
                |row| {
                    Ok(if key(u32::try_from(row).unwrap()).is_some() {
                        Value::UInt(hash)
                    } else {
                        Value::Null
                    })
                },
            )?);
            columns
                .values
                .push(logical_field_from_native_array(&record, private::ORDINAL)?);
            columns
                .values
                .push(logical_field_from_native_array(&record, DATA)?);
            let record = private::structure(&layout.build.fields, columns, 41)?;
            let state = State::new(&policy, context)?;
            let mut order = Ordering::new(&layout.build, &state, 3, context)?;
            order.build(record, context)?;
            let stored = order.retain(context)?;
            let mut search = Search::new(&stored, &spec);
            assert!(search.range(&left, 1, context)?.is_empty());
            let mut range = search.range(&left, 0, context)?;
            assert_eq!(
                range.end - range.start,
                (0..41u32).filter(|row| key(*row).is_some()).count() as u64
            );
            let mut observed = Vec::new();
            while range.start < range.end {
                let candidates = search.candidates(&left, 0, &mut range, 5, context)?;
                assert_eq!(
                    candidates.table.rows(),
                    (expected.len() - observed.len()).min(5)
                );
                let rows = (0..candidates.table.rows()).map(Some).collect::<Vec<_>>();
                let column = candidates.table.gather(&rows, false, context)?.column(
                    "id",
                    &spec.fields[0].1,
                    context,
                )?;
                let mut execution = context.native_session().create_execution_ctx();
                for row in 0..column.len() {
                    let Value::UInt(value) =
                        result_batch::scalar_value(&column, row, &mut execution)?
                    else {
                        panic!("candidate id changed type");
                    };
                    observed.push(u32::try_from(value).unwrap());
                }
            }
            assert_eq!(observed, expected);
            assert!(search.blocks > 3);
            drop(search);
            stored.finish(context)?;
            assert!(state.finish()?.owned_cleanup_completed);
            Ok(())
        })
        .unwrap();
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    std::fs::remove_dir(workspace).unwrap();
}
