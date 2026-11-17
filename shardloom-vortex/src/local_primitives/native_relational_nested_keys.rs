//! Nested relational keys over the original native scalar and container owners.

use super::{KeyColumn, failed};
use crate::local_primitives::{
    native_capacity::{ReservedStack, ReservedVec},
    native_list,
};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use vortex::{array::dtype::DType, mask::Mask};

#[path = "native_relational_nested_key_binding.rs"]
mod binding;
#[path = "native_relational_nested_key_comparison.rs"]
mod comparison;
#[path = "native_relational_nested_key_walk.rs"]
mod walk;

#[cfg(test)]
#[path = "native_relational_nested_key_reference.rs"]
mod reference;
#[cfg(test)]
#[path = "native_relational_nested_key_traversal_tests.rs"]
mod tests;

pub(super) fn is_container(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Struct(..) | DType::List(..) | DType::FixedSizeList(..)
    )
}

pub(in crate::local_primitives) struct Column {
    // Postorder storage releases complete children before their parent. Links
    // are positions in this vector, so even the final owner drops iteratively.
    nodes: ReservedVec<Node>,
    root: usize,
    cancellation: CancellationToken,
    owner: MemoryLease,
    _schema: Option<MemoryLease>,
}

struct Node {
    value: Value,
    next_sibling: Option<usize>,
}

enum Value {
    Leaf(KeyColumn),
    Container(Container),
}

struct Container {
    children: Children,
    dtype: DType,
    rows: usize,
}

enum Children {
    Struct {
        valid: Mask,
        first: Option<usize>,
        count: usize,
    },
    List {
        coordinates: native_list::Coordinates,
        elements: usize,
    },
}

impl Column {
    fn root(&self) -> &Container {
        let Value::Container(root) = &self.nodes.values[self.root].value else {
            unreachable!("nested key construction finishes with a container");
        };
        root
    }

    pub(super) fn len(&self) -> usize {
        self.root().rows
    }

    pub(super) fn is_null(&self, row: usize) -> Result<bool> {
        self.root().is_null(row)
    }
}

impl Container {
    fn is_null(&self, row: usize) -> Result<bool> {
        if row >= self.rows {
            return Err(failed("nested key row exceeds native input"));
        }
        Ok(match &self.children {
            Children::Struct { valid, .. } => !valid.value(row),
            Children::List { coordinates, .. } => coordinates.at(row)?.is_none(),
        })
    }
}

type Frames<T> = ReservedStack<T, 4>;
