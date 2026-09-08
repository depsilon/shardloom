//! Optional exact integer DISTINCT spill accumulator. The parent query owns
//! the declared operator envelope through the final native result. Source
//! numeric execution retains the configured query context; bounded run buffers
//! use its registry/runtime with the child allocator. No second runtime exists.

use super::super::{
    NativeNumericOwner, logical_field_from_native_array,
    native_numeric_accessor::NativeNumericAccessorWork, vortex_error,
};
use super::{
    Pair,
    spill::{ExactDistinctSpill, Policy, SpilledDistinctResult},
};
use crate::VortexAggregateSpillPolicy;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, VortexSessionExecute as _,
        arrays::{Primitive, PrimitiveArray},
        dtype::{DType, Nullability},
        memory::MemorySessionExt as _,
    },
    io::runtime::BlockingRuntime,
    session::VortexSession,
};

pub(in super::super) struct SpillAccumulator {
    spill: ExactDistinctSpill,
    operator_memory: LiveMemoryPool,
    run_session: VortexSession,
    source_ctx: ExecutionCtx,
    columns: [String; 2],
    dtypes: [DType; 2],
    cancellation: Arc<std::sync::atomic::AtomicBool>,
    metadata: MemoryLease,
    source_rows: u64,
    failed: bool,
    // Drop the parent admission only after every child-owned field.
    envelope: MemoryLease,
}

pub(in super::super) struct OwnedSpillResult {
    pub(super) result: SpilledDistinctResult,
    // Result fields drop before the parent envelope. The conservative full
    // declared envelope remains admitted until the last result owner drops.
    metadata: MemoryLease,
    envelope: MemoryLease,
}

impl OwnedSpillResult {
    pub(in super::super) fn reserved_bytes(&self) -> u64 {
        self.envelope.bytes()
    }
    pub(in super::super) fn selected_reserved_bytes(&self) -> u64 {
        self.result.reserved_bytes() + self.metadata.bytes()
    }
}

impl SpillAccumulator {
    pub(in super::super) fn new(
        policy: &VortexAggregateSpillPolicy,
        columns: [String; 2],
        dtypes: [DType; 2],
        retained: usize,
        query_memory: &LiveMemoryPool,
        session: &VortexSession,
    ) -> Result<Self> {
        check_cancelled(&policy.cancellation)?;
        if retained == 0 || dtypes.iter().any(|dtype| {
            !matches!(dtype, DType::Primitive(ptype, Nullability::NonNullable) if ptype.is_int())
        }) {
            return Err(failed("requires two nonnullable identity integer columns and positive bounded output"));
        }
        let workspace = std::fs::symlink_metadata(&policy.workspace)
            .map_err(|error| failed(&format!("workspace admission failed: {error}")))?;
        if !workspace.is_dir() || workspace.file_type().is_symlink() {
            return Err(failed("workspace must be an existing real directory"));
        }
        let metadata_bytes = columns.iter().try_fold(
            size_of::<Self>() + size_of::<OwnedSpillResult>() + 2 * size_of::<usize>(),
            |bytes, column| {
                bytes
                    .checked_add(column.capacity())
                    .ok_or_else(|| failed("column metadata capacity overflow"))
            },
        )?;
        // This admission precedes the child pool and any workspace creation.
        let envelope = query_memory.reserve(policy.memory_bytes)?;
        let operator_memory = LiveMemoryPool::new(policy.memory_bytes)?;
        let metadata = operator_memory
            .reserve(u64::try_from(metadata_bytes).map_err(|_| failed("metadata exceeds u64"))?)?;
        let run_session = session.clone().with_allocator(Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(operator_memory.clone()),
        ));
        let signed =
            |dtype: &DType| matches!(dtype, DType::Primitive(ptype, _) if ptype.is_signed_int());
        let spill = ExactDistinctSpill::new(
            Policy {
                workspace: policy.workspace.clone(),
                quota_bytes: policy.quota_bytes,
                memory_bytes: policy.memory_bytes,
                cancellation: Arc::clone(&policy.cancellation),
            },
            operator_memory.clone(),
            signed(&dtypes[0]),
            signed(&dtypes[1]),
            retained,
        )?;
        Ok(Self {
            spill,
            operator_memory,
            run_session,
            source_ctx: session.create_execution_ctx(),
            columns,
            dtypes,
            cancellation: Arc::clone(&policy.cancellation),
            envelope,
            metadata,
            source_rows: 0,
            failed: false,
        })
    }

    pub(in super::super) fn push(
        &mut self,
        chunk: &ArrayRef,
        runtime: &impl BlockingRuntime,
    ) -> Result<NativeNumericAccessorWork> {
        let result = self.push_inner(chunk, runtime);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn push_inner(
        &mut self,
        chunk: &ArrayRef,
        runtime: &impl BlockingRuntime,
    ) -> Result<NativeNumericAccessorWork> {
        if self.failed {
            return Err(failed(
                "accumulator failed and cannot accept more source rows",
            ));
        }
        check_cancelled(&self.cancellation)?;
        let mut owners = [None, None];
        let mut work = NativeNumericAccessorWork::default();
        for (index, slot) in owners.iter_mut().enumerate() {
            let array = logical_field_from_native_array(chunk, &self.columns[index])?;
            if array.dtype() != &self.dtypes[index] || array.len() != chunk.len() {
                return Err(failed(
                    "source projection changed its admitted dtype or row count",
                ));
            }
            let started = Instant::now();
            let primitive = array
                .clone()
                .execute::<PrimitiveArray>(&mut self.source_ctx)
                .map_err(vortex_error)?;
            if primitive.dtype() != array.dtype() || primitive.len() != chunk.len() {
                return Err(failed("native source owner changed its dtype or row count"));
            }
            let canonical_bytes = primitive.nbytes();
            *slot = Some(NativeNumericOwner::new(primitive, &mut self.source_ctx)?);
            if !array.is::<Primitive>() {
                work.record_native_owner(
                    &self.columns[index],
                    u64::try_from(chunk.len())
                        .map_err(|_| failed("source row count exceeds u64"))?,
                    array.nbytes(),
                    canonical_bytes,
                    started.elapsed().as_nanos(),
                )?;
            }
        }
        let [Some(group), Some(value)] = &owners else {
            unreachable!("two source owners");
        };
        let groups = group
            .integer_key_slice()
            .ok_or_else(|| failed("group lost all-valid integer admission"))?;
        let values = value
            .integer_key_slice()
            .ok_or_else(|| failed("distinct value lost all-valid integer admission"))?;
        groups.for_each_pair(values, None, |row, key| {
            if row.is_multiple_of(4096) {
                check_cancelled(&self.cancellation)?;
            }
            self.spill.push(
                Pair {
                    group_bits: key.first_bits,
                    value_bits: key.second_bits,
                    signedness: key.key_kinds,
                },
                1,
                runtime,
                &self.run_session,
            )
        })?;
        self.source_rows = self
            .source_rows
            .checked_add(
                u64::try_from(chunk.len()).map_err(|_| failed("source row count exceeds u64"))?,
            )
            .ok_or_else(|| failed("source row count overflow"))?;
        Ok(work)
    }

    pub(in super::super) fn finish(
        self,
        runtime: &impl BlockingRuntime,
    ) -> Result<OwnedSpillResult> {
        if self.failed {
            return Err(failed("accumulator failed and cannot publish a result"));
        }
        check_cancelled(&self.cancellation)?;
        let result = self.spill.finish(runtime, &self.run_session)?;
        if result.evidence.rows != self.source_rows {
            return Err(failed(
                "complete pair weight differs from admitted source rows",
            ));
        }
        // Store cleanup and merge/source readers have drained before this owner
        // leaves the same-source execution boundary. Keep all remaining child
        // accounting covered even if a provider releases an owner later.
        if self.operator_memory.snapshot().reserved_bytes > self.envelope.bytes() {
            return Err(failed(
                "child reservations exceed the query-owned operator envelope",
            ));
        }
        Ok(OwnedSpillResult {
            result,
            metadata: self.metadata,
            envelope: self.envelope,
        })
    }
}

fn check_cancelled(cancellation: &std::sync::atomic::AtomicBool) -> Result<()> {
    if cancellation.load(Ordering::Acquire) {
        Err(failed("execution cancelled"))
    } else {
        Ok(())
    }
}
fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native exact integer COUNT DISTINCT spill {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "exact_distinct_spill_accumulator_tests.rs"]
mod tests;
