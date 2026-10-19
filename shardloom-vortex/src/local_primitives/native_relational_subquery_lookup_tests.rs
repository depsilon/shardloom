//! Deliberate collisions exercise full-key and three-valued candidate reduction.

use super::*;
use crate::{
    local_primitives::{
        native_capacity::ReservedVec,
        native_relational_records as private,
        native_relational_spill::{Ordering as NativeOrder, State},
        result_batch::{self, Value},
    },
    relational_query::VortexRelationalSpillPolicy,
    resident_session::ResidentVortexSession,
};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{PrimitiveArray, StructArray},
    dtype::FieldNames,
    memory::MemorySessionExt as _,
    validity::Validity,
};

fn rows(groups: &[u64], first: &[Option<u64>], second: &[Option<u64>]) -> ArrayRef {
    StructArray::new(
        FieldNames::from(["g", "a", "b"]),
        vec![
            PrimitiveArray::from_iter(groups.iter().copied()).into_array(),
            PrimitiveArray::from_option_iter(first.iter().copied()).into_array(),
            PrimitiveArray::from_option_iter(second.iter().copied()).into_array(),
        ],
        groups.len(),
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn subquery_membership_hash_collisions_preserve_exact_groups_and_nullable_row_truth() {
    let workspace = std::env::temp_dir().join(format!(
        "shardloom-subquery-collision-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    std::fs::create_dir(&workspace).unwrap();
    let policy = VortexRelationalSpillPolicy::new(&workspace, 32 << 20, 1 << 20).unwrap();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    for (group, a, b, expected) in [
        (7, Some(42), Some(3), [Some(true), None]),
        (7, Some(42), None, [None, None]),
        (7, Some(43), None, [Some(false), None]),
        (7, Some(43), Some(4), [Some(false), Some(false)]),
        (9, Some(42), Some(3), [Some(false), Some(false)]),
    ] {
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                let spec = Spec {
                    kind: Kind::In,
                    guard: None,
                    fields: vec![],
                    left_keys: vec!["g".into(), "a".into(), "b".into()],
                    right_keys: vec!["g".into(), "a".into(), "b".into()],
                    correlation: 1,
                    negated: false,
                };
                let left = Batch::new(
                    rows(&[7, 7], &[Some(42), None], &[Some(3), Some(3)]),
                    &spec.left_keys,
                    context,
                )?;
                let mut groups = [9; 41];
                let mut first = [Some(42); 41];
                let mut second = [Some(3); 41];
                groups[40] = group;
                first[40] = a;
                second[40] = b;
                // Nullable false-positive candidates from another correlation group
                // must not contribute UNKNOWN to the selected group's result.
                second[0] = None;
                let right = rows(&groups, &first, &second);
                let fields = right.dtype().as_struct_fields_opt().unwrap();
                let fields = fields
                    .names()
                    .iter()
                    .zip(fields.fields())
                    .map(|(name, dtype)| (name.to_string(), dtype))
                    .collect::<Vec<_>>();
                let layout = super::super::records::Layout::new(&spec, &fields, context)?;
                let source = Batch::new(right, &spec.right_keys, context)?;
                let record = layout.record(&spec, &source, 0..41, 0, context)?;
                let group_hash = left.hash_prefix(0, 1, false)?.unwrap();
                let full_hash = left.hash(0, false)?.unwrap();
                let mut columns = ReservedVec::new(context.memory())?;
                columns.reserve(4)?;
                columns
                    .values
                    .push(private::unsigned(41, context, |_| Ok(group_hash))?);
                columns.values.push(result_batch::build_column(
                    &private::u64_type().as_nullable(),
                    41,
                    &context.native_session().allocator(),
                    |row| {
                        Ok(if source.hash(row, false)?.is_some() {
                            Value::UInt(full_hash)
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
                let forged = private::structure(&layout.order.fields, columns, 41)?;
                let state = State::new(&policy, context)?;
                let mut order = NativeOrder::new(&layout.order, &state, 3, context)?;
                order.build(forged, context)?;
                let mut search = Search::new(order.retain(context)?, &spec);
                for (row, expected) in expected.into_iter().enumerate() {
                    assert_eq!(search.evaluate(&left, row, context)?, expected);
                }
                assert!(search.blocks > 3);
                search.finish(context)?;
                assert!(state.finish()?.owned_cleanup_completed);
                Ok(())
            })
            .unwrap();
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
        assert_eq!(std::fs::read_dir(&workspace).unwrap().count(), 0);
    }
    std::fs::remove_dir(workspace).unwrap();
}
