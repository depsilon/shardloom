//! Terminal JSON traversal shared by bounded collection and streaming writers.
//! Flat native views retain arrays; no list/struct scalar tree is built.

use super::{
    native_capacity::ReservedVec, native_list, native_payload, native_relational_batch::failed,
    vortex_error,
};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::io::Write;
use vortex::array::{ArrayRef, ExecutionCtx, dtype::FieldNames, validity::Validity};

#[path = "native_json_build.rs"]
mod build;
#[path = "native_json_write.rs"]
mod write;

pub(super) struct Column {
    tree: Tree,
    // Retain schema admission through all native views and container releases.
    _metadata: MemoryLease,
}

#[derive(Clone, Copy, Default)]
pub(super) struct WriteCounts {
    pub(super) scalars: u64,
    pub(super) utf8_bytes: u64,
}

impl WriteCounts {
    fn add(&mut self, other: Self) -> Result<()> {
        self.scalars = self
            .scalars
            .checked_add(other.scalars)
            .ok_or_else(|| failed("JSON scalar count overflow"))?;
        self.utf8_bytes = self
            .utf8_bytes
            .checked_add(other.utf8_bytes)
            .ok_or_else(|| failed("JSON UTF-8 copy count overflow"))?;
        Ok(())
    }
}

// Nodes only refer to earlier arena entries. Vec destruction therefore releases
// child views before their parent views without recursively owning Nodes.
struct Tree {
    nodes: ReservedVec<Node>,
    // A scalar root keeps its existing inline representation and needs no arena
    // allocation. Child nodes still precede this root during destruction.
    root: Node,
    memory: LiveMemoryPool,
}

enum Node {
    Scalar(ArrayRef),
    Uuid(crate::native_uuid::Values),
    Struct {
        names: FieldNames,
        fields: ReservedVec<usize>,
        validity: Validity,
    },
    List {
        column: native_list::Column,
        child: usize,
    },
}

impl Column {
    pub(super) fn new(
        array: &ArrayRef,
        execution: &mut ExecutionCtx,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let metadata = if native_payload::is_nested(array.dtype()) {
            crate::native_payload_schema::reserve_metadata(array.dtype(), (memory, cancellation))?
        } else {
            memory.reserve(1024)?
        };
        Ok(Self {
            tree: build::build(array, execution, memory, cancellation)?,
            _metadata: metadata,
        })
    }

    /// Return scalar materializations and UTF-8 payload bytes copied at this sink.
    pub(super) fn write(
        &self,
        row: usize,
        writer: &mut impl Write,
        execution: &mut ExecutionCtx,
        cancellation: &CancellationToken,
    ) -> Result<WriteCounts> {
        write::write(&self.tree, row, writer, execution, cancellation)
    }
}

#[cfg(test)]
#[path = "native_json_tests.rs"]
mod tests;
