//! The exact grouping owner accepts either an ordinary batch or bounded
//! references to matched source values. Only new group payloads escape.

use super::{
    native_relational_batch::{Batch, take_batch},
    native_relational_keys::KeyLookupCache,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{ArrayRef, dtype::DType};

pub(super) trait Rows {
    fn len(&self) -> usize;
    fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>>;
    fn equal_batch(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        nulls_equal: bool,
    ) -> Result<bool>;
    fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool>;
    fn key_cache(&self, context: &NativeExecutionContext<'_>)
    -> Result<Option<KeyLookupCache<'_>>>;
    fn gather(
        &self,
        fields: &[(String, DType)],
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef>;
}

impl Rows for Batch {
    fn len(&self) -> usize {
        self.array.len()
    }
    fn hash(&self, row: usize, nulls_equal: bool) -> Result<Option<u64>> {
        self.hash(row, nulls_equal)
    }
    fn equal_batch(
        &self,
        row: usize,
        batch: &Batch,
        other: usize,
        nulls_equal: bool,
    ) -> Result<bool> {
        batch.equal(other, self, row, nulls_equal)
    }
    fn equal(&self, row: usize, other: usize, nulls_equal: bool) -> Result<bool> {
        self.equal(row, self, other, nulls_equal)
    }
    fn key_cache(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<KeyLookupCache<'_>>> {
        self.key_cache(context)
    }
    fn gather(
        &self,
        fields: &[(String, DType)],
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        take_batch(&self.array, fields, rows, context)
    }
}
