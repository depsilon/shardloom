// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the ShardLoom contributors

//! Receiving metadata ownership through real native array escape paths.

use std::any::Any;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_buffer::buffer;

use crate::ArrayRef;
use crate::IntoArray;
use crate::VTable;
use crate::VortexSessionExecute;
use crate::array::typed::{ArrayData, ArrayInner};
use crate::array_session;
use crate::arrays::Constant;
use crate::arrays::ConstantArray;
use crate::arrays::PrimitiveArray;
use crate::arrays::StructArray;
use crate::arrays::{Bool, Chunked, Primitive, Struct, VarBin};
use crate::validity::Validity;

struct Owner(Arc<AtomicUsize>);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn owned_constant() -> (ArrayRef, Weak<Owner>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Owner(Arc::clone(&drops)));
    let weak = Arc::downgrade(&owner);
    let array = ConstantArray::new(17i32, 8)
        .into_array()
        .with_retained_owner(owner)
        .unwrap();
    (array, weak, drops)
}

#[test]
fn retained_owner_survives_clones_and_nested_slices() {
    let (array, weak, drops) = owned_constant();
    let clone = array.clone();
    let slice = clone.slice(2..7).unwrap().slice(1..3).unwrap();
    drop(array);
    drop(clone);
    assert!(weak.upgrade().is_some());
    let mut ctx = array_session().create_execution_ctx();
    assert_eq!(
        i32::try_from(&slice.execute_scalar(0, &mut ctx).unwrap()).unwrap(),
        17
    );
    assert_eq!(slice.len(), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(slice);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn retained_owner_survives_empty_slice() {
    let (array, weak, _) = owned_constant();
    let empty = array.slice(3..3).unwrap();
    let dtype = array.dtype().clone();
    drop(array);
    assert!(empty.is_empty());
    assert_eq!(empty.dtype(), &dtype);
    assert!(weak.upgrade().is_some());
    drop(empty);
    assert!(weak.upgrade().is_none());
}

#[test]
fn retained_slice_materializes_at_the_explicit_execution_boundary() {
    let owner: Arc<dyn Any + Send + Sync> = Arc::new(());
    let input = PrimitiveArray::from_iter(0i64..16)
        .into_array()
        .with_retained_owner(owner)
        .unwrap();
    let slice = input.slice(3..9).unwrap().slice(1..4).unwrap();
    let mut ctx = array_session().create_execution_ctx();
    let result = slice.execute::<PrimitiveArray>(&mut ctx).unwrap();
    assert_eq!(result.as_slice::<i64>(), &[4, 5, 6]);
}

#[test]
fn retained_child_owner_survives_parent_release() {
    let (child, weak, _) = owned_constant();
    let parent = StructArray::from_fields(&[("value", child)])
        .unwrap()
        .into_array();
    let escaped = parent.nth_child(0).unwrap();
    drop(parent);
    assert!(escaped.is::<Constant>());
    assert!(weak.upgrade().is_some());
    drop(escaped);
    assert!(weak.upgrade().is_none());
}

#[test]
fn retained_owner_survives_typed_parts_and_structural_rebuild() {
    let (array, weak, _) = owned_constant();
    let parts = array.downcast::<Constant>().try_into_parts().unwrap();
    assert!(weak.upgrade().is_some());
    let rebuilt = ConstantArray::try_from_parts(parts).unwrap().into_array();
    let replaced = rebuilt
        .dyn_array()
        .with_slots(&rebuilt, rebuilt.slots().iter().cloned().collect())
        .unwrap();
    drop(rebuilt);
    assert!(weak.upgrade().is_some());
    drop(replaced);
    assert!(weak.upgrade().is_none());
}

#[test]
fn retained_owner_survives_buffer_rebuild() {
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Owner(Arc::clone(&drops)));
    let weak = Arc::downgrade(&owner);
    let array = PrimitiveArray::new(buffer![1i32, 2, 3], Validity::NonNullable)
        .into_array()
        .with_retained_owner(owner)
        .unwrap();
    let replacement = array
        .dyn_array()
        .with_buffers(&array, array.buffer_handles())
        .unwrap();
    drop(array);
    assert!(weak.upgrade().is_some());
    drop(replacement);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn retained_owner_cannot_modify_aliases_or_replace_an_owner() {
    let (array, weak, _) = owned_constant();
    let error = array.clone().with_retained_owner(Arc::new(())).unwrap_err();
    assert!(error.to_string().contains("unique array"));
    assert!(weak.upgrade().is_some());
    let error = array.with_retained_owner(Arc::new(())).unwrap_err();
    assert!(error.to_string().contains("already has a lifetime owner"));
    assert!(weak.upgrade().is_none());
}

#[test]
fn rebuilt_parts_preserve_provider_owners_and_reject_conflicts() {
    let (array, weak, _) = owned_constant();
    let mut parts = array.downcast::<Constant>().try_into_parts().unwrap();
    parts.inherit_owner(&None).unwrap();
    let original = parts.retained_owner.clone();
    parts.inherit_owner(&original).unwrap();
    let different: Arc<dyn Any + Send + Sync> = Arc::new(());
    assert!(parts.inherit_owner(&Some(different)).is_err());
    assert!(Arc::ptr_eq(
        parts.retained_owner.as_ref().unwrap(),
        original.as_ref().unwrap()
    ));
    drop(original);
    assert!(weak.upgrade().is_some());
    drop(parts);
    assert!(weak.upgrade().is_none());
}

#[test]
fn ownerless_slices_keep_existing_reduction_and_measure_array_footprint() {
    let original = ConstantArray::new(7i64, 100).into_array();
    assert!(original.slice(3..7).unwrap().is::<Constant>());
    assert!(original.dyn_array().retained_owner().is_none());

    // Compare the common allocation with the identical pre-hook data fields.
    // ArrayRef itself remains one fat pointer. Record actual target layout.
    #[allow(dead_code)]
    struct WithoutOwner<V: VTable> {
        vtable: V,
        data: V::TypedArrayData,
    }
    fn measure<V: VTable>() {
        let before = size_of::<ArrayInner<WithoutOwner<V>>>();
        let after = size_of::<ArrayInner<ArrayData<V>>>();
        assert_eq!(
            after - before,
            size_of::<Option<Arc<dyn Any + Send + Sync>>>()
        );
        eprintln!(
            "{}: before={before}, after={after}, delta={}",
            std::any::type_name::<V>(),
            after - before
        );
    }
    measure::<Constant>();
    measure::<Primitive>();
    measure::<Bool>();
    measure::<Struct>();
    measure::<VarBin>();
    measure::<Chunked>();
    assert_eq!(size_of::<ArrayRef>(), 2 * size_of::<usize>());
}

struct BufferDropProbe {
    bytes: [u8; 4],
    owner: Weak<Owner>,
    observed: Arc<AtomicBool>,
}

impl AsRef<[u8]> for BufferDropProbe {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for BufferDropProbe {
    fn drop(&mut self) {
        self.observed
            .store(self.owner.upgrade().is_some(), Ordering::SeqCst);
    }
}

#[test]
fn encoding_data_drops_before_its_retained_owner() {
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Owner(Arc::clone(&drops)));
    let observed = Arc::new(AtomicBool::new(false));
    let buffer = ByteBuffer::from_bytes_aligned(
        Bytes::from_owner(BufferDropProbe {
            bytes: [1, 2, 3, 4],
            owner: Arc::downgrade(&owner),
            observed: Arc::clone(&observed),
        }),
        Alignment::none(),
    );
    let array = PrimitiveArray::new(buffer, Validity::NonNullable)
        .into_array()
        .with_retained_owner(owner)
        .unwrap();
    drop(array);
    assert!(observed.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
