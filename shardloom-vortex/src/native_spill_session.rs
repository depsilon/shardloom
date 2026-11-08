//! Independent memory configuration for native spill work on the query runtime.
//!
//! Vortex 0.90 session clones share a mutable configuration cell. Changing an
//! allocator on a clone also changes existing query contexts and sibling spill
//! sessions. Start with an independent cell and inherit the native registries,
//! writer policy and runtime handle explicitly. Most registries remain shared;
//! allocator-bound dtype plugins and memory configuration do not. This is the
//! finite local spill environment, not a general fork of arbitrary extension or
//! external-I/O session state.

use std::sync::Arc;

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{
        aggregate_fn::session::AggregateFnSession, dtype::session::DTypeSession,
        memory::HostMemorySessionExt as _, optimizer::kernels::KernelSession,
        scalar_fn::session::ScalarFnSession, session::ArraySession,
    },
    editions::{EditionSession, EnabledEditions},
    io::session::RuntimeSession,
    layout::session::LayoutSession,
    session::{SessionExt as _, VortexSession, VortexSessionVar},
};

pub(crate) fn with_memory(parent: &VortexSession, memory: LiveMemoryPool) -> VortexSession {
    use vortex::array::dtype::{extension::ExtVTable as _, session::DTypeSessionExt as _};
    let child = VortexSession::empty();
    // Dtype plugins may carry allocator admission. Copy the registry contents
    // into an independent cell before rebinding a receiving metadata owner.
    if let Some(parent) = parent.get_opt::<DTypeSession>() {
        let dtypes = DTypeSession::default();
        for (id, plugin) in parent.registry().snapshot().iter() {
            dtypes.registry().insert(*id, Arc::clone(plugin));
        }
        child.register(dtypes);
    }
    inherit::<ArraySession>(parent, &child);
    inherit::<KernelSession>(parent, &child);
    inherit::<LayoutSession>(parent, &child);
    inherit::<ScalarFnSession>(parent, &child);
    inherit::<AggregateFnSession>(parent, &child);
    inherit::<EditionSession>(parent, &child);
    inherit::<EnabledEditions>(parent, &child);
    inherit::<RuntimeSession>(parent, &child);
    #[cfg(all(feature = "native-binary-objects", unix))]
    inherit::<crate::binary_object::Arena>(parent, &child);
    inherit::<crate::native_provider_memory::ProviderMemory>(parent, &child);
    if parent.allows_unknown() {
        child.allow_unknown();
    }
    if parent
        .get_opt::<crate::native_provider_memory::ProviderMemory>()
        .is_some()
    {
        crate::native_provider_memory::install(&child, memory.clone());
    }
    let child = child.with_host_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory),
    ));
    if child
        .dtypes()
        .registry()
        .contains_key(&crate::native_embedding::Embedding::default().id())
    {
        crate::native_embedding::register_dtype(&child);
    }
    if child
        .dtypes()
        .registry()
        .contains_key(&crate::native_tensor::FixedShapeTensor::default().id())
    {
        crate::native_tensor::register_dtype(&child);
    }
    if child
        .dtypes()
        .registry()
        .contains_key(&crate::native_variable_tensor::VariableShapeTensor::default().id())
    {
        crate::native_variable_tensor::register_dtype(&child);
    }
    for (kind, id) in [
        (
            crate::native_vector::Kind::Sparse,
            crate::native_vector::SparseVector::default().id(),
        ),
        (
            crate::native_vector::Kind::Quantized,
            crate::native_vector::QuantizedVector::default().id(),
        ),
    ] {
        if child.dtypes().registry().contains_key(&id) {
            crate::native_vector::register_dtype(&child, kind);
        }
    }
    child
}

fn inherit<T: VortexSessionVar + Clone>(parent: &VortexSession, child: &VortexSession) {
    if let Some(value) = parent.get_opt::<T>() {
        child.register((*value).clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            ArrayVTable as _, VortexSessionExecute as _, arrays::Primitive,
            session::ArraySessionExt as _,
        },
        buffer::Alignment,
        editions::{ComponentKind, EditionSessionExt as _},
        io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        },
    };

    #[test]
    fn fixed_tensor_spill_metadata_uses_receiving_pool_and_preserves_identity() {
        use vortex::array::dtype::{
            DType, PType, extension::ExtVTable as _, session::DTypeSessionExt as _,
        };
        let parent_pool = LiveMemoryPool::new(64 << 10).unwrap();
        let parent = VortexSession::default().with_host_allocator(Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(parent_pool.clone()),
        ));
        crate::native_tensor::register(&parent);
        let child_pool = LiveMemoryPool::new(64 << 10).unwrap();
        let child = with_memory(&parent, child_pool.clone());
        let id = crate::native_tensor::FixedShapeTensor::default().id();
        let storage = DType::FixedSizeList(
            DType::Primitive(PType::F32, true.into()).into(),
            6,
            true.into(),
        );
        let raw = br#"{"permutation":[1,0],"shape":[2,3]}"#;
        let received = child
            .dtypes()
            .registry()
            .get(&id)
            .unwrap()
            .deserialize(raw, storage.clone())
            .unwrap();
        assert!(child_pool.snapshot().reserved_bytes > 0);
        assert_eq!(parent_pool.snapshot().reserved_bytes, 0);
        let original = parent
            .dtypes()
            .registry()
            .get(&id)
            .unwrap()
            .deserialize(raw, storage)
            .unwrap();
        assert_eq!(received, original);
        let clone = received.clone();
        drop((received, child, original, parent));
        assert_eq!(parent_pool.snapshot().reserved_bytes, 0);
        assert!(child_pool.snapshot().reserved_bytes > 0);
        drop(clone);
        assert_eq!(child_pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn variable_tensor_spill_metadata_uses_receiving_pool_and_preserves_identity() {
        use vortex::array::dtype::{
            DType, FieldNames, PType, StructFields, extension::ExtVTable as _,
            session::DTypeSessionExt as _,
        };
        let parent_pool = LiveMemoryPool::new(64 << 10).unwrap();
        let parent = VortexSession::default().with_host_allocator(Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(parent_pool.clone()),
        ));
        crate::native_variable_tensor::register(&parent);
        let child_pool = LiveMemoryPool::new(64 << 10).unwrap();
        let child = with_memory(&parent, child_pool.clone());
        let id = crate::native_variable_tensor::VariableShapeTensor::default().id();
        let storage = DType::Struct(
            StructFields::new(
                FieldNames::from(["data", "shape"].as_slice()),
                vec![
                    DType::List(
                        DType::Primitive(PType::F32, true.into()).into(),
                        false.into(),
                    ),
                    DType::FixedSizeList(
                        DType::Primitive(PType::I32, false.into()).into(),
                        3,
                        false.into(),
                    ),
                ],
            ),
            true.into(),
        );
        let raw = br#"{"permutation":[2,0,1],"uniform_shape":[null,2,null]}"#;
        let received = child
            .dtypes()
            .registry()
            .get(&id)
            .unwrap()
            .deserialize(raw, storage.clone())
            .unwrap();
        assert!(child_pool.snapshot().reserved_bytes > 0);
        assert_eq!(parent_pool.snapshot().reserved_bytes, 0);
        let original = parent
            .dtypes()
            .registry()
            .get(&id)
            .unwrap()
            .deserialize(raw, storage.clone())
            .unwrap();
        assert_eq!(received, original);
        let denied_pool = LiveMemoryPool::new(256).unwrap();
        let denied = with_memory(&parent, denied_pool.clone());
        assert!(
            denied
                .dtypes()
                .registry()
                .get(&id)
                .unwrap()
                .deserialize(raw, storage)
                .is_err()
        );
        assert_eq!(denied_pool.snapshot().reserved_bytes, 0);
        let clone = received.clone();
        drop((received, child, original, parent));
        assert_eq!(parent_pool.snapshot().reserved_bytes, 0);
        assert!(child_pool.snapshot().reserved_bytes > 0);
        drop(clone);
        assert_eq!(child_pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn dense_embedding_spill_metadata_reserves_only_the_receiving_pool() {
        use vortex::array::dtype::{
            DType, PType, extension::ExtVTable as _, session::DTypeSessionExt as _,
        };
        let parent_pool = LiveMemoryPool::new(64 << 10).unwrap();
        let parent = VortexSession::default().with_host_allocator(Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(parent_pool.clone()),
        ));
        crate::native_embedding::register(&parent);
        let child_pool = LiveMemoryPool::new(64 << 10).unwrap();
        let child = with_memory(&parent, child_pool.clone());
        let id = crate::native_embedding::Embedding::default().id();
        let storage = DType::FixedSizeList(
            DType::Primitive(PType::F32, false.into()).into(),
            3,
            true.into(),
        );
        let dtype = child
            .dtypes()
            .registry()
            .get(&id)
            .unwrap()
            .deserialize(br#"{"version":1}"#, storage.clone())
            .unwrap();
        assert!(child_pool.snapshot().reserved_bytes > 0);
        assert_eq!(parent_pool.snapshot().reserved_bytes, 0);
        let parent_dtype = parent
            .dtypes()
            .registry()
            .get(&id)
            .unwrap()
            .deserialize(br#"{"version":1}"#, storage.clone())
            .unwrap();
        assert!(parent_pool.snapshot().reserved_bytes > 0);
        assert_eq!(dtype, parent_dtype);
        let clone = dtype.clone();
        drop((dtype, child, parent_dtype, parent));
        assert_eq!(parent_pool.snapshot().reserved_bytes, 0);
        assert!(child_pool.snapshot().reserved_bytes > 0);
        drop(clone);
        assert_eq!(child_pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn native_spill_session_preserves_registries_and_writer_policy_without_memory_aliasing() {
        let runtime = CurrentThreadRuntime::new();
        let source_memory = LiveMemoryPool::new(4096).unwrap();
        let parent = VortexSession::default()
            .with_handle(runtime.handle())
            .with_host_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
                source_memory.clone(),
            )));
        parent.allow_unknown();
        parent
            .enable_edition(vortex::editions::CORE_2025_05_0)
            .unwrap();
        // Replace a registered plugin before the child is created, so checking
        // pointer identity proves inheritance rather than identical defaults.
        parent.arrays().register(Primitive);
        let source_allocator = parent.host_allocator();
        let before = parent.create_execution_ctx();
        let memory = LiveMemoryPool::new(1024).unwrap();
        let child = with_memory(&parent, memory.clone());
        let plugin = Primitive.id();
        assert!(Arc::ptr_eq(
            &parent.arrays().registry().get(&plugin).unwrap(),
            &child.arrays().registry().get(&plugin).unwrap(),
        ));
        for kind in [
            ComponentKind::Array,
            ComponentKind::Layout,
            ComponentKind::DType,
            ComponentKind::Aggregate,
        ] {
            let mut expected = parent.enabled_component_ids(kind);
            let mut actual = child.enabled_component_ids(kind);
            expected.sort();
            actual.sort();
            assert_eq!(actual, expected);
        }
        assert!(child.allows_unknown());
        assert!(Arc::ptr_eq(&source_allocator, &parent.host_allocator()));
        assert!(Arc::ptr_eq(&source_allocator, &before.host_allocator()));
        assert!(
            child
                .host_allocator()
                .allocate(1024, Alignment::none())
                .is_err()
        );
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
        let source = before
            .host_allocator()
            .allocate(1024, Alignment::none())
            .unwrap()
            .freeze();
        let run = child
            .host_allocator()
            .allocate(512, Alignment::none())
            .unwrap()
            .freeze();
        drop(child);
        assert_eq!(source_memory.snapshot().reserved_bytes, 1280);
        assert_eq!(memory.snapshot().reserved_bytes, 768);
        drop(run);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        drop(source);
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
        assert!(Arc::ptr_eq(&source_allocator, &parent.host_allocator()));
    }
}
