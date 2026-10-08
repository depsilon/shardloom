//! Positional access to the same native keys in resident or stored partitions.

use crate::{
    local_primitives::{
        native_relational_batch::{Table, failed},
        native_relational_keys::Cell,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use std::cmp::Ordering;

pub(in super::super) trait Input {
    fn key_is_null(
        &mut self,
        position: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool>;

    fn raw_cell(
        &mut self,
        position: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Cell>;

    fn hash_key(
        &mut self,
        position: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>>;

    fn compare_key(
        &mut self,
        left: usize,
        right: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Ordering>;
}

pub(in super::super) struct Resident<'a> {
    pub(in super::super) table: &'a Table,
    pub(in super::super) rows: &'a [usize],
}

impl Resident<'_> {
    fn row(&self, position: usize) -> Result<usize> {
        self.rows
            .get(position)
            .copied()
            .ok_or_else(|| failed("window position exceeds its partition"))
    }
}

impl Input for Resident<'_> {
    fn key_is_null(
        &mut self,
        position: usize,
        key: usize,
        _context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        self.table.key_is_null(self.row(position)?, key)
    }

    fn raw_cell(
        &mut self,
        position: usize,
        key: usize,
        _context: &NativeExecutionContext<'_>,
    ) -> Result<Cell> {
        self.table.raw_cell(self.row(position)?, key)
    }

    fn hash_key(
        &mut self,
        position: usize,
        key: usize,
        _context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        self.table.hash_key(self.row(position)?, key)
    }

    fn compare_key(
        &mut self,
        left: usize,
        right: usize,
        key: usize,
        _context: &NativeExecutionContext<'_>,
    ) -> Result<Ordering> {
        self.table
            .compare_key(self.row(left)?, self.row(right)?, key)
    }
}
