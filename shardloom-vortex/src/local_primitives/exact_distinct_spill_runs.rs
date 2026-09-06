//! Bounded native block codec and exact pair merge. All numeric fields remain
//! typed native primitives; no row-export StatValue/Arrow vectors are created.

use super::super::super::{
    logical_field_from_native_array,
    query_run_store::{QueryRunBlock, QueryRunReader},
    vortex_error,
};
use super::{
    LocalVortexRuntime, NativeQueryRun, Pair, Policy, QueryRunStore, failed, reserved_vec,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use std::{collections::BinaryHeap, sync::Arc};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::{PrimitiveArray, StructArray, primitive::PrimitiveArrayExt as _},
        dtype::{DType, PType},
        validity::Validity,
    },
    session::VortexSession,
};

pub(super) const BLOCK_ROWS: usize = 1024;
pub(super) const FAN_IN: usize = 4;

#[derive(Clone, Copy, Debug)]
pub(super) struct Record {
    pub pair: Pair,
    pub weight: u64,
}
impl Record {
    pub(super) fn key(&self) -> (u64, u64) {
        let group = self.pair.group_bits
            ^ if self.pair.signedness & 1 != 0 {
                1_u64 << 63
            } else {
                0
            };
        let value = self.pair.value_bits
            ^ if self.pair.signedness & 2 != 0 {
                1_u64 << 63
            } else {
                0
            };
        (group, value)
    }
}

pub(super) fn array(rows: &[Record]) -> ArrayRef {
    StructArray::new(
        ["group_bits", "value_bits", "signedness", "weight"].into(),
        [
            rows.iter()
                .map(|row| row.pair.group_bits)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.pair.value_bits)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.pair.signedness)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.weight)
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .into_array()
}
pub(super) fn run_dtype() -> DType {
    array(&[]).dtype().clone()
}

pub(super) fn blocks(
    mut rows: impl Iterator<Item = Result<Record>>,
) -> impl Iterator<Item = Result<ArrayRef>> {
    let mut failed = false;
    std::iter::from_fn(move || {
        if failed {
            return None;
        }
        let mut block = match reserved_vec(BLOCK_ROWS) {
            Ok(block) => block,
            Err(error) => {
                failed = true;
                return Some(Err(error));
            }
        };
        while block.len() < BLOCK_ROWS {
            match rows.next() {
                Some(Ok(row)) => block.push(row),
                Some(Err(error)) => {
                    failed = true;
                    return Some(Err(error));
                }
                None => break,
            }
        }
        (!block.is_empty()).then(|| Ok(array(&block)))
    })
}

struct ReadBlock {
    group: PrimitiveArray,
    value: PrimitiveArray,
    signature: PrimitiveArray,
    weight: PrimitiveArray,
    offset: usize,
    // Retains the exact native input plus metadata/work credits through every
    // typed view above, even if the source/store/descriptor owner disappears.
    _owner: QueryRunBlock,
}
impl ReadBlock {
    fn new(block: QueryRunBlock, ctx: &mut ExecutionCtx) -> Result<Self> {
        let rows = block.array().len();
        let mut field = |name, ptype| {
            let column = logical_field_from_native_array(block.array(), name)?;
            let primitive = column
                .execute::<PrimitiveArray>(ctx)
                .map_err(vortex_error)?;
            if primitive.len() != rows
                || primitive.ptype() != ptype
                || primitive.dtype().is_nullable()
            {
                return Err(failed("native run primitive field shape changed"));
            }
            Ok(primitive)
        };
        let group = field("group_bits", PType::U64)?;
        let value = field("value_bits", PType::U64)?;
        let signature = field("signedness", PType::U8)?;
        let weight = field("weight", PType::U64)?;
        Ok(Self {
            group,
            value,
            signature,
            weight,
            offset: 0,
            _owner: block,
        })
    }

    fn next(&mut self) -> Option<Record> {
        if self.offset == self.group.len() {
            return None;
        }
        let index = self.offset;
        self.offset += 1;
        Some(Record {
            pair: Pair {
                group_bits: self.group.as_slice::<u64>()[index],
                value_bits: self.value.as_slice::<u64>()[index],
                signedness: self.signature.as_slice::<u8>()[index],
            },
            weight: self.weight.as_slice::<u64>()[index],
        })
    }
}

struct RunReader {
    reader: QueryRunReader,
    block: Option<ReadBlock>,
    remaining: u64,
    previous: Option<(u64, u64)>,
    signature: u8,
}
impl RunReader {
    fn next(
        &mut self,
        runtime: &LocalVortexRuntime,
        ctx: &mut ExecutionCtx,
    ) -> Result<Option<Record>> {
        if self.remaining == 0 {
            self.reader.validate()?;
            return Ok(None);
        }
        let row = if let Some(row) = self.block.as_mut().and_then(ReadBlock::next) {
            row
        } else {
            // Release the prior native block before the next file read.
            self.block = None;
            let block = self
                .reader
                .next_block(runtime)?
                .ok_or_else(|| failed("run ended before declared pair count"))?;
            self.block = Some(ReadBlock::new(block, ctx)?);
            self.block
                .as_mut()
                .and_then(ReadBlock::next)
                .ok_or_else(|| failed("run yielded an empty block"))?
        };
        if row.pair.signedness != self.signature
            || row.weight == 0
            || self.previous.is_some_and(|previous| previous > row.key())
        {
            return Err(failed(
                "run pair signature, positive weight or monotonic order changed",
            ));
        }
        // next_block validates around actual I/O; EOF and the complete merge
        // boundary validate again. Do not turn every cached row into fstat I/O.
        self.previous = Some(row.key());
        self.remaining -= 1;
        Ok(Some(row))
    }
}

struct Head {
    row: Record,
    reader: usize,
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for a minimum complete-pair heap, with deterministic reader
        // index only breaking equal-pair ties. Weight never changes equality.
        (other.row.key(), other.reader).cmp(&(self.row.key(), self.reader))
    }
}

pub(super) struct RunMerge<'runtime> {
    readers: Vec<RunReader>,
    heads: BinaryHeap<Head>,
    // Keep the fixed merge/head reservation alive independently of whether any
    // run reader still owns a block or the merge has an empty input stream.
    _work: Arc<MemoryLease>,
    policy: Policy,
    runtime: &'runtime LocalVortexRuntime,
    ctx: ExecutionCtx,
    failed: bool,
}
impl<'runtime> RunMerge<'runtime> {
    pub(super) fn new<'run>(
        inputs: impl Iterator<Item = &'run NativeQueryRun>,
        store: &QueryRunStore,
        work: Arc<MemoryLease>,
        policy: Policy,
        signature: u8,
        runtime: &'runtime LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<Self> {
        policy.check()?;
        let mut readers = reserved_vec(FAN_IN)?;
        for run in inputs {
            if readers.len() == FAN_IN {
                return Err(failed("merge file-handle bound exceeded"));
            }
            readers.push(RunReader {
                reader: store.open(run, &run_dtype(), runtime, session, Arc::clone(&work))?,
                block: None,
                remaining: run.rows,
                previous: None,
                signature,
            });
        }
        let mut ctx = session.create_execution_ctx();
        let mut heads = BinaryHeap::new();
        heads
            .try_reserve_exact(FAN_IN)
            .map_err(|_| failed("merge head allocation failed"))?;
        if heads.capacity() != FAN_IN {
            return Err(failed("merge head capacity exceeded reservation"));
        }
        for (reader, input) in readers.iter_mut().enumerate() {
            if let Some(row) = input.next(runtime, &mut ctx)? {
                heads.push(Head { row, reader });
            }
        }
        Ok(Self {
            readers,
            heads,
            _work: work,
            policy,
            runtime,
            ctx,
            failed: false,
        })
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.policy.check()?;
        for reader in &self.readers {
            reader.reader.validate()?;
        }
        Ok(())
    }
}
impl Iterator for RunMerge<'_> {
    type Item = Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Err(error) = self.policy.check() {
            self.failed = true;
            return Some(Err(error));
        }
        let Head { row, reader } = self.heads.pop()?;
        match self.readers[reader].next(self.runtime, &mut self.ctx) {
            Ok(Some(next)) => self.heads.push(Head { row: next, reader }),
            Ok(None) => {}
            Err(error) => {
                self.failed = true;
                return Some(Err(error));
            }
        }
        Some(Ok(row))
    }
}
