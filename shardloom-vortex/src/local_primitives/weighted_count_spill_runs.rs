//! Native full-key block codec and bounded merge. UTF8 uses explicit offsets
//! and payload ownership; dictionary codes/hashes never enter persisted keys.

use super::super::{
    logical_field_from_native_array,
    query_run_store::{QueryRunBlock, QueryRunReader},
    vortex_error,
};
use super::{
    Copies, FAN_IN, KeyOrder, Policy, QueryRunStore, Run, as_u64, block_rows_for_key, copy_text,
    failed, reserved_vec,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};
use vortex::{
    array::{
        ArrayRef, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            PrimitiveArray, StructArray, VarBinArray, VarBinViewArray,
            primitive::PrimitiveArrayExt as _,
        },
        dtype::{DType, Nullability, PType},
        validity::Validity,
    },
    buffer::Buffer,
    io::runtime::BlockingRuntime,
    session::VortexSession,
};

#[derive(Clone, Copy)]
pub(super) struct Record {
    pub number: u64,
    pub offset: usize,
    pub len: usize,
    pub weight: u64,
}
impl Record {
    pub(super) fn text<'a>(&self, arena: &'a [u8]) -> &'a [u8] {
        &arena[self.offset..self.offset + self.len]
    }
}
pub(super) struct Row {
    pub number: u64,
    pub text: Vec<u8>,
    pub weight: u64,
}

pub(super) fn dtype() -> DType {
    DType::Struct(
        vortex::array::dtype::StructFields::new(
            ["integer_bits", "signature", "text", "weight"].into(),
            vec![
                DType::Primitive(PType::U64, Nullability::NonNullable),
                DType::Primitive(PType::U8, Nullability::NonNullable),
                DType::Utf8(Nullability::NonNullable),
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ],
        ),
        Nullability::NonNullable,
    )
}

pub(super) fn array<'a>(
    rows: impl ExactSizeIterator<Item = (u64, &'a [u8], u64)>,
    signature: u8,
    copies: &Copies,
) -> Result<ArrayRef> {
    let count = rows.len();
    let mut input = reserved_vec(count)?;
    for row in rows {
        input.push(row);
    }
    let rows = input;
    let payload_bytes = rows
        .iter()
        .try_fold(0_usize, |bytes, (_, text, _)| bytes.checked_add(text.len()))
        .ok_or_else(|| failed("native block byte count overflowed"))?;
    let mut offsets = reserved_vec(
        count
            .checked_add(1)
            .ok_or_else(|| failed("native offset capacity overflowed"))?,
    )?;
    let mut payload = reserved_vec(payload_bytes)?;
    let mut numbers = reserved_vec(count)?;
    let mut signatures = reserved_vec(count)?;
    let mut weights = reserved_vec(count)?;
    offsets.push(0_u64);
    for (number, text, weight) in rows {
        numbers.push(number);
        signatures.push(signature);
        weights.push(weight);
        payload.extend_from_slice(text);
        offsets.push(as_u64(payload.len())?);
    }
    Copies::add(&copies.encoded, payload.len())?;
    let text = VarBinArray::try_new(
        PrimitiveArray::new(Buffer::from(offsets), Validity::NonNullable).into_array(),
        Buffer::from(payload),
        DType::Utf8(Nullability::NonNullable),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    Ok(StructArray::new(
        ["integer_bits", "signature", "text", "weight"].into(),
        [
            PrimitiveArray::new(Buffer::from(numbers), Validity::NonNullable).into_array(),
            PrimitiveArray::new(Buffer::from(signatures), Validity::NonNullable).into_array(),
            text.into_array(),
            PrimitiveArray::new(Buffer::from(weights), Validity::NonNullable).into_array(),
        ],
        count,
        Validity::NonNullable,
    )
    .into_array())
}

pub(super) fn blocks<'a>(
    mut rows: impl Iterator<Item = Result<Row>> + 'a,
    block_rows: usize,
    signature: u8,
    copies: &'a Copies,
) -> impl Iterator<Item = Result<ArrayRef>> + 'a {
    let mut failed_once = false;
    std::iter::from_fn(move || {
        if failed_once {
            return None;
        }
        let mut block = match reserved_vec(block_rows) {
            Ok(block) => block,
            Err(error) => {
                failed_once = true;
                return Some(Err(error));
            }
        };
        while block.len() < block_rows {
            match rows.next() {
                Some(Ok(row)) => block.push(row),
                Some(Err(error)) => {
                    failed_once = true;
                    return Some(Err(error));
                }
                None => break,
            }
        }
        if block.is_empty() {
            return None;
        }
        let result = array(
            block
                .iter()
                .map(|row| (row.number, row.text.as_slice(), row.weight)),
            signature,
            copies,
        );
        if result.is_err() {
            failed_once = true;
        }
        Some(result)
    })
}

struct ReadBlock {
    number: PrimitiveArray,
    signature: PrimitiveArray,
    text: VarBinViewArray,
    weight: PrimitiveArray,
    offset: usize,
    // This owner outlives every native typed view, including provider-created
    // UTF8 views. It carries the store's read-path/run/work reservations.
    _owner: QueryRunBlock,
}
impl ReadBlock {
    fn new(block: QueryRunBlock, ctx: &mut ExecutionCtx) -> Result<Self> {
        let count = block.array().len();
        let mut field = |name, ptype| -> Result<PrimitiveArray> {
            let field = logical_field_from_native_array(block.array(), name)?
                .execute::<PrimitiveArray>(ctx)
                .map_err(vortex_error)?;
            if field.len() != count || field.ptype() != ptype || field.dtype().is_nullable() {
                return Err(failed("native primitive field shape changed"));
            }
            Ok(field)
        };
        let number = field("integer_bits", PType::U64)?;
        let signature = field("signature", PType::U8)?;
        let weight = field("weight", PType::U64)?;
        let text = logical_field_from_native_array(block.array(), "text")?
            .execute::<VarBinViewArray>(ctx)
            .map_err(vortex_error)?;
        if text.len() != count || text.dtype() != &DType::Utf8(Nullability::NonNullable) {
            return Err(failed("native UTF8 field shape changed"));
        }
        Ok(Self {
            number,
            signature,
            text,
            weight,
            offset: 0,
            _owner: block,
        })
    }
    fn next(
        &mut self,
        max_key_bytes: usize,
        order: KeyOrder,
        copies: &Copies,
    ) -> Result<Option<Row>> {
        if self.offset == self.number.len() {
            return Ok(None);
        }
        let index = self.offset;
        let number = self.number.as_slice::<u64>()[index];
        let weight = self.weight.as_slice::<u64>()[index];
        if self.signature.as_slice::<u8>()[index] != order.signature()
            || weight == 0
            || order == KeyOrder::Text && number != 0
        {
            return Err(failed(
                "native run key signature or positive weight changed",
            ));
        }
        let text = self.text.bytes_at(index);
        if text.len() > max_key_bytes {
            return Err(failed("native run key exceeds declared byte bound"));
        }
        std::str::from_utf8(&text).map_err(|_| failed("native run UTF8 invalid"))?;
        Copies::add(&copies.heads, text.len())?;
        let text = copy_text(&text)?;
        self.offset += 1;
        Ok(Some(Row {
            number,
            text,
            weight,
        }))
    }
}
struct RunReader {
    reader: QueryRunReader,
    block: Option<ReadBlock>,
    remaining: u64,
    previous: Option<Row>,
    max_key_bytes: usize,
    max_observed_key_bytes: usize,
}
impl RunReader {
    fn validate(&self) -> Result<()> {
        if self.remaining != 0 || self.max_observed_key_bytes != self.max_key_bytes {
            return Err(failed(
                "native run observed key geometry differs from descriptor",
            ));
        }
        self.reader.validate()
    }
    fn next(
        &mut self,
        runtime: &impl BlockingRuntime,
        ctx: &mut ExecutionCtx,
        policy: &Policy,
        order: KeyOrder,
        copies: &Copies,
    ) -> Result<Option<Row>> {
        policy.check()?;
        if self.remaining == 0 {
            self.validate()?;
            return Ok(None);
        }
        let cached = match self.block.as_mut() {
            Some(block) => block.next(self.max_key_bytes, order, copies)?,
            None => None,
        };
        let row = if let Some(row) = cached {
            row
        } else {
            self.block = None;
            let block = self
                .reader
                .next_block(runtime)?
                .ok_or_else(|| failed("run ended before declared row count"))?;
            self.block = Some(ReadBlock::new(block, ctx)?);
            self.block
                .as_mut()
                .expect("new block")
                .next(self.max_key_bytes, order, copies)?
                .ok_or_else(|| failed("run yielded empty block"))?
        };
        self.max_observed_key_bytes = self.max_observed_key_bytes.max(row.text.len());
        if self.previous.as_ref().is_some_and(|previous| {
            order
                .compare((previous.number, &previous.text), (row.number, &row.text))
                .is_gt()
        }) {
            return Err(failed("native run complete key order regressed"));
        }
        // A bounded independent previous key allows releasing the previous
        // native block before reading its successor. Count this copy too.
        self.previous = None;
        Copies::add(&copies.heads, row.text.len())?;
        self.previous = Some(Row {
            number: row.number,
            text: copy_text(&row.text)?,
            weight: row.weight,
        });
        self.remaining -= 1;
        Ok(Some(row))
    }
}
struct Head {
    row: Row,
    reader: usize,
    order: KeyOrder,
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order
            .compare(
                (other.row.number, &other.row.text),
                (self.row.number, &self.row.text),
            )
            .then_with(|| other.reader.cmp(&self.reader))
    }
}

pub(super) struct RunMerge<'runtime, R: BlockingRuntime> {
    readers: Vec<RunReader>,
    heads: BinaryHeap<Head>,
    _work: Arc<MemoryLease>,
    policy: Policy,
    order: KeyOrder,
    copies: Arc<Copies>,
    runtime: &'runtime R,
    ctx: ExecutionCtx,
    failed: bool,
}
impl<'runtime, R: BlockingRuntime> RunMerge<'runtime, R> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new<'run>(
        inputs: impl Iterator<Item = &'run Run>,
        store: &QueryRunStore,
        work: Arc<MemoryLease>,
        policy: Policy,
        order: KeyOrder,
        copies: Arc<Copies>,
        runtime: &'runtime R,
        session: &VortexSession,
    ) -> Result<Self> {
        policy.check()?;
        let mut readers = reserved_vec(FAN_IN)?;
        for run in inputs {
            if readers.len() == FAN_IN {
                return Err(failed("merge file-handle bound exceeded"));
            }
            if run.max_key_bytes > policy.max_key_bytes
                || run.native.block_rows != block_rows_for_key(run.max_key_bytes)
            {
                return Err(failed("native run block geometry differs from descriptor"));
            }
            readers.push(RunReader {
                reader: store.open(&run.native, &dtype(), runtime, session, Arc::clone(&work))?,
                block: None,
                remaining: run.native.rows,
                previous: None,
                max_key_bytes: run.max_key_bytes,
                max_observed_key_bytes: 0,
            });
        }
        let mut ctx = session.create_execution_ctx();
        let mut heads = BinaryHeap::new();
        heads
            .try_reserve_exact(FAN_IN)
            .map_err(|_| failed("merge head allocation failed"))?;
        if heads.capacity() != FAN_IN {
            return Err(failed("merge head allocation exceeded reservation"));
        }
        for (reader, input) in readers.iter_mut().enumerate() {
            if let Some(row) = input.next(runtime, &mut ctx, &policy, order, &copies)? {
                heads.push(Head { row, reader, order });
            }
        }
        Ok(Self {
            readers,
            heads,
            _work: work,
            policy,
            order,
            copies,
            runtime,
            ctx,
            failed: false,
        })
    }
    pub(super) fn validate(&self) -> Result<()> {
        self.policy.check()?;
        for reader in &self.readers {
            reader.validate()?;
        }
        Ok(())
    }
}
impl<R: BlockingRuntime> Iterator for RunMerge<'_, R> {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Err(error) = self.policy.check() {
            self.failed = true;
            return Some(Err(error));
        }
        let Head { row, reader, order } = self.heads.pop()?;
        match self.readers[reader].next(
            self.runtime,
            &mut self.ctx,
            &self.policy,
            self.order,
            &self.copies,
        ) {
            Ok(Some(next)) => self.heads.push(Head {
                row: next,
                reader,
                order,
            }),
            Ok(None) => {}
            Err(error) => {
                self.failed = true;
                return Some(Err(error));
            }
        }
        Some(Ok(row))
    }
}
