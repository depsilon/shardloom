//! Independent memory configuration for native spill work on the query runtime.
//!
//! Vortex 0.85 session clones share a mutable configuration cell. Changing an
//! allocator on a clone also changes existing query contexts and sibling spill
//! sessions. Start with an independent cell and inherit the native registries,
//! writer policy and runtime handle explicitly. Registry contents remain shared;
//! memory configuration does not. This is the finite local spill environment,
//! not a general fork of arbitrary extension or external-I/O session state.

use std::sync::Arc;

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{
        aggregate_fn::session::AggregateFnSession, dtype::session::DTypeSession,
        memory::MemorySessionExt as _, optimizer::kernels::KernelSession,
        scalar_fn::session::ScalarFnSession, session::ArraySession, stats::session::StatsSession,
    },
    editions::{EditionSession, EnabledEditions},
    io::session::RuntimeSession,
    layout::session::LayoutSession,
    session::{SessionExt as _, VortexSession, VortexSessionVar},
};

pub(crate) fn with_memory(parent: &VortexSession, memory: LiveMemoryPool) -> VortexSession {
    let child = VortexSession::empty();
    inherit::<DTypeSession>(parent, &child);
    inherit::<ArraySession>(parent, &child);
    inherit::<KernelSession>(parent, &child);
    inherit::<LayoutSession>(parent, &child);
    inherit::<ScalarFnSession>(parent, &child);
    inherit::<StatsSession>(parent, &child);
    inherit::<AggregateFnSession>(parent, &child);
    inherit::<EditionSession>(parent, &child);
    inherit::<EnabledEditions>(parent, &child);
    inherit::<RuntimeSession>(parent, &child);
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
    child.with_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
        memory,
    )))
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
    fn native_spill_session_preserves_registries_and_writer_policy_without_memory_aliasing() {
        let runtime = CurrentThreadRuntime::new();
        let source_memory = LiveMemoryPool::new(4096).unwrap();
        let parent = VortexSession::default()
            .with_handle(runtime.handle())
            .with_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
                source_memory.clone(),
            )));
        parent.allow_unknown();
        parent
            .enable_edition(vortex::editions::CORE_2025_05_0)
            .unwrap();
        // Replace a registered plugin before the child is created, so checking
        // pointer identity proves inheritance rather than identical defaults.
        parent.arrays().register(Primitive);
        let source_allocator = parent.allocator();
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
        assert!(Arc::ptr_eq(&source_allocator, &parent.allocator()));
        assert!(Arc::ptr_eq(&source_allocator, &before.allocator()));
        assert!(child.allocator().allocate(1024, Alignment::none()).is_err());
        assert_eq!(source_memory.snapshot().reserved_bytes, 0);
        let source = before
            .allocator()
            .allocate(1024, Alignment::none())
            .unwrap()
            .freeze();
        let run = child
            .allocator()
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
        assert!(Arc::ptr_eq(&source_allocator, &parent.allocator()));
    }
}
