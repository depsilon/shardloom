// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom regressions for resource-admitted lazy schema containers.

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use vortex_buffer::Alignment;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use super::StructFields;
use crate::dtype::DType;
use crate::dtype::Nullability;
use crate::dtype::PType;
use crate::flatbuffers::FlatBuffer;
use crate::flatbuffers::WriteFlatBufferExt;
use crate::memory::DefaultHostAllocator;
use crate::memory::HostAllocator;
use crate::memory::HostMemoryReservation;
use crate::memory::HostMemorySessionExt;
use crate::memory::WritableHostBuffer;

#[derive(Debug, Default)]
struct State {
    limit: usize,
    live: usize,
    reservations: usize,
    slow: bool,
}

#[derive(Debug, Clone, Default)]
struct Admission(Arc<Mutex<State>>);

struct Credit {
    admission: Admission,
    bytes: usize,
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.admission.0.lock().live -= self.bytes;
    }
}

impl HostAllocator for Admission {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        DefaultHostAllocator.allocate(len, alignment)
    }

    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        let mut state = self.0.lock();
        let live = state
            .live
            .checked_add(len)
            .ok_or_else(|| vortex_err!("test overflow"))?;
        if live > state.limit {
            return Err(vortex_err!("test schema grant denied"));
        }
        state.live = live;
        state.reservations += 1;
        let slow = state.slow;
        drop(state);
        let credit = HostMemoryReservation::new(Credit {
            admission: self.clone(),
            bytes: len,
        });
        if slow {
            // Give every racing caller time to reach first-touch construction.
            // Only one may hold an admitted child construction at a time.
            thread::sleep(Duration::from_millis(20));
        }
        Ok(credit)
    }
}

fn fixture() -> VortexResult<FlatBuffer> {
    let payload = DType::Struct(
        StructFields::from_iter((0..1025).map(|index| {
            (
                format!("field_{index}"),
                DType::Primitive(PType::I64, Nullability::Nullable),
            )
        })),
        Nullability::Nullable,
    );
    let dtype = DType::Struct(
        StructFields::from_iter([("payload", payload)]),
        Nullability::NonNullable,
    );
    Ok(FlatBuffer::from(dtype.write_flatbuffer_bytes()?))
}

#[test]
fn lazy_struct_admission_refuses_retries_and_follows_the_final_child_owner() -> VortexResult<()> {
    let admission = Admission::default();
    let session = VortexSession::empty().with_host_allocator(Arc::new(admission.clone()));
    let buffer = fixture()?;
    assert!(DType::from_flatbuffer(buffer.clone(), &session).is_err());
    assert_eq!(admission.0.lock().live, 0);

    admission.0.lock().limit = 32 << 20;
    let root = DType::from_flatbuffer(buffer, &session)?;
    let fields = root.as_struct_fields_opt().unwrap().clone();
    let root_bytes = admission.0.lock().live;
    assert!(root_bytes > 0);
    admission.0.lock().limit = root_bytes;
    assert!(fields.try_field_by_index(0).is_err());
    assert!(fields.try_field_dtypes().next().unwrap().is_err());
    assert_eq!(admission.0.lock().live, root_bytes);
    assert_eq!(admission.0.lock().reservations, 1);

    admission.0.lock().limit = 32 << 20;
    let child = fields.try_field_by_index(0)?.unwrap();
    let alias = fields.try_field_by_index(0)?.unwrap();
    let child_fields = child.as_struct_fields_opt().unwrap();
    assert!(Arc::ptr_eq(
        &child_fields.0,
        &alias.as_struct_fields_opt().unwrap().0
    ));
    assert_eq!(child_fields.nfields(), 1025);
    assert_eq!(child_fields.find("field_1024"), Some(1024));
    let all_bytes = admission.0.lock().live;
    assert!(all_bytes > root_bytes);
    assert_eq!(admission.0.lock().reservations, 2);
    drop((fields, root, session, child));
    assert_eq!(admission.0.lock().live, all_bytes - root_bytes);
    assert_eq!(
        alias.as_struct_fields_opt().unwrap().find("field_1024"),
        Some(1024)
    );
    drop(alias);
    assert_eq!(admission.0.lock().live, 0);
    Ok(())
}

#[test]
fn native_child_cursor_does_not_parse_skipped_lazy_children() -> VortexResult<()> {
    use crate::dtype::UnionVariants;

    let wide = DType::Struct(
        StructFields::from_iter((0..1025).map(|index| (format!("field_{index}"), DType::Null))),
        Nullability::Nullable,
    );
    let leaf = DType::Bool(Nullability::NonNullable);
    for dtype in [
        DType::Struct(
            StructFields::from_iter([("wide", wide.clone()), ("leaf", leaf.clone())]),
            Nullability::Nullable,
        ),
        DType::Union(
            UnionVariants::new(["wide", "leaf"].into(), vec![wide, leaf.clone()])?,
            Nullability::Nullable,
        ),
    ] {
        let admission = Admission::default();
        admission.0.lock().limit = 32 << 20;
        let session = VortexSession::empty().with_host_allocator(Arc::new(admission.clone()));
        let root =
            DType::from_flatbuffer(FlatBuffer::from(dtype.write_flatbuffer_bytes()?), &session)?;
        let live = admission.0.lock().live;
        admission.0.lock().limit = live;
        let children = root.try_child_dtypes();
        assert_eq!(children.len(), 2);
        assert_eq!(admission.0.lock().live, live);
        assert_eq!(children.clone().nth(1).unwrap()?, &leaf);
        assert_eq!(admission.0.lock().live, live);
        assert!(children.clone().next().unwrap().is_err());
        assert_eq!(admission.0.lock().live, live);
        admission.0.lock().limit = 32 << 20;
        assert_eq!(
            children
                .clone()
                .next()
                .unwrap()?
                .as_struct_fields_opt()
                .unwrap()
                .nfields(),
            1025
        );
        assert!(admission.0.lock().live > live);
        drop(root);
        assert_eq!(admission.0.lock().live, 0);
    }
    Ok(())
}

#[test]
fn borrowed_map_value_reports_lazy_admission_failure_and_can_retry() -> VortexResult<()> {
    let value = DType::Struct(
        StructFields::from_iter((0..1025).map(|index| (format!("field_{index}"), DType::Null))),
        Nullability::Nullable,
    );
    let dtype = DType::map(
        DType::Primitive(PType::I64, Nullability::NonNullable),
        value,
        false,
        Nullability::Nullable,
    )?;
    let admission = Admission::default();
    let session = VortexSession::empty().with_host_allocator(Arc::new(admission.clone()));
    let root = DType::from_flatbuffer(FlatBuffer::from(dtype.write_flatbuffer_bytes()?), &session)?;
    let mut children = root.try_child_dtypes();
    assert_eq!(
        children.next().unwrap()?,
        &DType::Primitive(PType::I64, Nullability::NonNullable)
    );
    assert!(children.next().unwrap().is_err());
    assert_eq!(admission.0.lock().live, 0);
    admission.0.lock().limit = 32 << 20;
    let value = root.try_child_dtypes().nth(1).unwrap()?;
    assert_eq!(value.as_struct_fields_opt().unwrap().nfields(), 1025);
    assert!(admission.0.lock().live > 0);
    drop(root);
    assert_eq!(admission.0.lock().live, 0);
    Ok(())
}

#[test]
fn concurrent_lazy_first_touch_constructs_one_admitted_child() -> VortexResult<()> {
    let admission = Admission::default();
    admission.0.lock().limit = 32 << 20;
    let session = VortexSession::empty().with_host_allocator(Arc::new(admission.clone()));
    let root = DType::from_flatbuffer(fixture()?, &session)?;
    let fields = root.as_struct_fields_opt().unwrap().clone();
    admission.0.lock().slow = true;
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let fields = fields.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                fields.try_field_by_index(0).unwrap().unwrap()
            })
        })
        .collect();
    let children: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    let first = children[0].as_struct_fields_opt().unwrap();
    for child in &children {
        assert!(Arc::ptr_eq(
            &first.0,
            &child.as_struct_fields_opt().unwrap().0
        ));
    }
    assert_eq!(admission.0.lock().reservations, 2);
    drop((root, fields, session, children));
    assert_eq!(admission.0.lock().live, 0);
    Ok(())
}

#[test]
fn lazy_schema_expression_binding_propagates_admission_errors() -> VortexResult<()> {
    use crate::dtype::FieldName;
    use crate::dtype::FieldNames;
    use crate::expr::root;
    use crate::expr::select;
    use crate::scalar_fn::ScalarFnVTable;
    use crate::scalar_fn::fns::get_item::GetItem;
    use crate::scalar_fn::fns::select::FieldSelection;
    use crate::scalar_fn::fns::select::Select;

    let admission = Admission::default();
    admission.0.lock().limit = 32 << 20;
    let session = VortexSession::empty().with_host_allocator(Arc::new(admission.clone()));
    let dtype = DType::from_flatbuffer(fixture()?, &session)?;
    let root_bytes = admission.0.lock().live;
    admission.0.lock().limit = root_bytes;
    let name = FieldName::from("payload");
    let included = FieldSelection::Include(FieldNames::from(["payload"]));
    let selected = select(vec![name.clone()], root()).bind(&dtype)?;
    for error in [
        dtype
            .as_struct_fields_opt()
            .unwrap()
            .try_field(&name)
            .unwrap_err(),
        GetItem
            .return_dtype(&name, std::slice::from_ref(&dtype))
            .unwrap_err(),
        Select
            .return_dtype(
                &FieldSelection::Exclude(FieldNames::empty()),
                std::slice::from_ref(&dtype),
            )
            .unwrap_err(),
        Select.simplify(&included, &selected).unwrap_err(),
    ] {
        assert!(error.to_string().contains("test schema grant denied"));
        assert_eq!(admission.0.lock().live, root_bytes);
    }
    assert!(
        dtype
            .as_struct_fields_opt()
            .unwrap()
            .try_field("missing")?
            .is_none()
    );
    admission.0.lock().limit = 32 << 20;
    let payload = GetItem.return_dtype(&name, std::slice::from_ref(&dtype))?;
    assert_eq!(payload.as_struct_fields_opt().unwrap().nfields(), 1025);
    Select.simplify(&included, &selected)?;
    drop((selected, payload, dtype, session));
    assert_eq!(admission.0.lock().live, 0);
    Ok(())
}
