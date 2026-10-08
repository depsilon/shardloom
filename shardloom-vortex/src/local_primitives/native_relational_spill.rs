//! Query-local relational order runs over the shared native run store.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, failed, take_batch},
    native_relational_sort::{self, Sort, Spec},
    query_run_store::{
        self, NativeQueryRun, QueryRunBlock, QueryRunReader, QueryRunSpec, QueryRunStore,
        QueryRunStorePolicy,
    },
};
use crate::{
    relational_query::{VortexRelationalSpillPolicy, VortexRelationalSpillReport},
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::MemoryLease;
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering as KeyOrdering,
    path::Path,
    sync::Arc,
};
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
};

const BLOCK_ROWS: usize = 1024;
const READER_WORK_BYTES: u64 = 2 * 64 * 1024;

#[cfg(test)]
type BeforeRunOpen = Box<dyn FnOnce(&Path)>;
#[cfg(test)]
type AfterMergeBlock = Box<dyn FnOnce(usize)>;
#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_RUN_OPEN: RefCell<Option<BeforeRunOpen>> = const { RefCell::new(None) };
    pub(crate) static AFTER_MERGE_BLOCK: RefCell<Option<AfterMergeBlock>> = const { RefCell::new(None) };
}

/// One execution owns one quota/store even when several order nodes overlap.
/// The caller's resident pool owns all credits; no child execution grant exists.
pub(super) struct State {
    policy: VortexRelationalSpillPolicy,
    store: RefCell<Option<QueryRunStore>>,
    merge_passes: Cell<u64>,
    open_runs: Cell<usize>,
    max_open_runs: Cell<usize>,
    max_block_rows: Cell<usize>,
    _metadata: MemoryLease,
}

impl State {
    pub(super) fn new(
        policy: &VortexRelationalSpillPolicy,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let metadata = context.memory().reserve(
            (policy.workspace.as_os_str().len() as u64)
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(1024))
                .ok_or_else(|| failed("spill configuration capacity overflow"))?,
        )?;
        Ok(Self {
            policy: policy.clone(),
            store: RefCell::new(None),
            merge_passes: Cell::new(0),
            open_runs: Cell::new(0),
            max_open_runs: Cell::new(0),
            max_block_rows: Cell::new(0),
            _metadata: metadata,
        })
    }

    fn with_store<T>(
        &self,
        context: &NativeExecutionContext<'_>,
        operation: impl FnOnce(&mut QueryRunStore) -> Result<T>,
    ) -> Result<T> {
        context.check_cancelled()?;
        let mut store = self.store.borrow_mut();
        if store.is_none() {
            let scratch = context.memory().reserve(128 * 1024)?;
            *store = Some(QueryRunStore::new(
                QueryRunStorePolicy::relational_order(&self.policy, context.cancellation().clone()),
                context.memory().clone(),
                scratch,
            )?);
        }
        operation(
            store
                .as_mut()
                .ok_or_else(|| failed("spill store was not admitted"))?,
        )
    }

    fn open<'a>(
        &'a self,
        run: &NativeQueryRun,
        spec: &Spec,
        context: &NativeExecutionContext<'_>,
        work: &Arc<MemoryLease>,
    ) -> Result<Reader<'a>> {
        #[cfg(test)]
        if let Some(hook) = BEFORE_RUN_OPEN.with(|hook| hook.borrow_mut().take()) {
            hook(&run.path);
        }
        let opened = self
            .open_runs
            .get()
            .checked_add(1)
            .ok_or_else(|| failed("spill reader count overflow"))?;
        let inner = self.with_store(context, |store| {
            store.open(
                run,
                &dtype(spec),
                context.runtime(),
                context.native_session(),
                Arc::clone(work),
            )
        })?;
        self.open_runs.set(opened);
        self.max_open_runs.set(self.max_open_runs.get().max(opened));
        Ok(Reader { inner, state: self })
    }

    pub(super) fn finish(&self) -> Result<VortexRelationalSpillReport> {
        if self.open_runs.get() != 0 {
            return Err(failed(
                "spill cleanup cannot complete with retained run readers",
            ));
        }
        let mut store = self.store.borrow_mut().take();
        let snapshot = store
            .as_ref()
            .map(QueryRunStore::snapshot)
            .unwrap_or_default();
        if let Some(store) = &mut store {
            store.cleanup()?;
        }
        Ok(VortexRelationalSpillReport {
            workspace: self.policy.workspace.clone(),
            quota_bytes: self.policy.quota_bytes,
            buffer_bytes: self.policy.buffer_bytes,
            peak_disk_bytes: snapshot.peak_disk_bytes,
            runs_written: snapshot.runs_written,
            runs_validated: snapshot.runs_validated,
            merge_passes: self.merge_passes.get(),
            max_open_runs: self.max_open_runs.get(),
            run_block_rows: self.max_block_rows.get(),
            owned_cleanup_completed: true,
        })
    }
}

struct Reader<'a> {
    inner: QueryRunReader,
    state: &'a State,
}

impl std::ops::Deref for Reader<'_> {
    type Target = QueryRunReader;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for Reader<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        self.state.open_runs.set(self.state.open_runs.get() - 1);
    }
}

struct Run {
    native: NativeQueryRun,
    level: u32,
}

pub(super) struct Ordering<'a> {
    spec: &'a Spec,
    state: &'a State,
    sort: Sort<'a>,
    runs: ReservedVec<Run>,
    block_rows: usize,
    work: Option<Arc<MemoryLease>>,
    retained_bytes: u64,
}

impl<'a> Ordering<'a> {
    pub(super) fn new(
        spec: &'a Spec,
        state: &'a State,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            spec,
            state,
            sort: Sort::new(spec, context.memory())?,
            runs: ReservedVec::new(context.memory())?,
            block_rows: batch_rows.clamp(1, BLOCK_ROWS),
            work: None,
            retained_bytes: 0,
        })
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        let incoming = self.sort.incoming_bytes(&array)?;
        if incoming > self.state.policy.buffer_bytes {
            // A lazy take/field view may retain a much larger input domain. Only
            // when retention crosses the threshold, compact selected native rows
            // instead of rejecting an otherwise small logical batch.
            self.flush(context)?;
            let mut rows = ReservedVec::new(context.memory())?;
            rows.reserve(self.block_rows)?;
            let mut start = 0;
            while start < array.len() {
                let mut end = (start + self.block_rows).min(array.len());
                loop {
                    context.check_cancelled()?;
                    rows.values.clear();
                    rows.values.extend(start..end);
                    let compact = take_batch(&array, &self.spec.fields, &rows.values, context)?;
                    let bytes = self.sort.incoming_bytes(&compact)?;
                    if bytes <= self.state.policy.buffer_bytes {
                        self.build_admitted(compact, bytes, context)?;
                        break;
                    }
                    if end - start == 1 {
                        return Err(failed(
                            "one native ordering row exceeds the configured spill buffer threshold",
                        ));
                    }
                    end = start + (end - start) / 2;
                }
                start = end;
            }
            return Ok(());
        }
        self.build_admitted(array, incoming, context)
    }

    fn build_admitted(
        &mut self,
        array: ArrayRef,
        incoming: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if incoming
            > self
                .state
                .policy
                .buffer_bytes
                .saturating_sub(self.retained_bytes)
        {
            self.flush(context)?;
        }
        self.sort.build(array, context)?;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(incoming)
            .ok_or_else(|| failed("sort retained input estimate overflow"))?;
        Ok(())
    }

    fn work(&mut self, context: &NativeExecutionContext<'_>) -> Result<Arc<MemoryLease>> {
        if self.work.is_none() {
            self.work = Some(Arc::new(context.memory().reserve(READER_WORK_BYTES)?));
        }
        self.work
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| failed("spill work was not admitted"))
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.sort.rows() == 0 {
            return Ok(());
        }
        let work = self.work(context)?;
        let rows = self.sort.ordered_rows(context)?;
        let spec = run_spec(self.spec, rows.values.len() as u64, self.block_rows)?;
        self.runs.reserve_one()?;
        let blocks = rows.values.chunks(self.block_rows).map(|rows| {
            context.check_cancelled()?;
            self.sort.gather_rows(rows, context)
        });
        let native = self.state.with_store(context, |store| {
            store.write_arrays(
                &spec,
                blocks,
                context.runtime(),
                context.native_session(),
                &work,
            )
        })?;
        self.state
            .max_block_rows
            .set(self.state.max_block_rows.get().max(self.block_rows));
        self.runs.values.push(Run { native, level: 0 });
        drop(rows);
        self.sort = Sort::new(self.spec, context.memory())?;
        self.retained_bytes = 0;
        while self.runs.values.len() >= 2 {
            let length = self.runs.values.len();
            if self.runs.values[length - 1].level != self.runs.values[length - 2].level {
                break;
            }
            self.merge_last(context)?;
        }
        Ok(())
    }

    fn merge_last(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        let work = self.work(context)?;
        let right = self
            .runs
            .values
            .pop()
            .ok_or_else(|| failed("right sort run is absent"))?;
        let left = self
            .runs
            .values
            .pop()
            .ok_or_else(|| failed("left sort run is absent"))?;
        let rows = left
            .native
            .rows
            .checked_add(right.native.rows)
            .ok_or_else(|| failed("merged sort run row count overflow"))?;
        let level = left
            .level
            .max(right.level)
            .checked_add(1)
            .ok_or_else(|| failed("sort merge level overflow"))?;
        let mut merger = Merge::new(
            self.spec,
            self.state.open(&left.native, self.spec, context, &work)?,
            self.state.open(&right.native, self.spec, context, &work)?,
            context,
        )?;
        let spec = run_spec(self.spec, rows, self.block_rows)?;
        let blocks = std::iter::from_fn(|| merger.next(self.block_rows, context).transpose());
        let native = self.state.with_store(context, |store| {
            store.write_arrays(
                &spec,
                blocks,
                context.runtime(),
                context.native_session(),
                &work,
            )
        })?;
        merger.validate()?;
        drop(merger);
        self.state.with_store(context, |store| {
            store.remove(&left.native)?;
            store.remove(&right.native)
        })?;
        self.runs.values.push(Run { native, level });
        self.state.merge_passes.set(
            self.state
                .merge_passes
                .get()
                .checked_add(1)
                .ok_or_else(|| failed("sort merge count overflow"))?,
        );
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if self.runs.values.is_empty() {
            return self.sort.finish(context, batch_rows, consume);
        }
        self.flush(context)?;
        while self.runs.values.len() > 1 {
            self.merge_last(context)?;
        }
        let work = self.work(context)?;
        let run = self
            .runs
            .values
            .pop()
            .ok_or_else(|| failed("completed sort run is absent"))?;
        let mut reader = self.state.open(&run.native, self.spec, context, &work)?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(self.block_rows)?;
        while let Some(block) = reader.next_block(context.runtime())? {
            context.check_cancelled()?;
            rows.values.clear();
            rows.values.extend(0..block.array().len());
            // Do not detach the store's borrowed array from its work/path credits.
            // Compact into the shared allocator before a downstream node retains it.
            consume(take_batch(
                block.array(),
                &self.spec.fields,
                &rows.values,
                context,
            )?)?;
            context.check_cancelled()?;
        }
        reader.validate()?;
        drop(reader);
        self.state
            .with_store(context, |store| store.remove(&run.native))
    }
}

fn dtype(spec: &Spec) -> DType {
    DType::struct_(spec.fields.clone(), Nullability::NonNullable)
}

fn run_spec(spec: &Spec, rows: u64, block_rows: usize) -> Result<QueryRunSpec> {
    let metadata_bytes = rows
        .div_ceil(block_rows as u64)
        .checked_mul(spec.fields.len() as u64 + 1)
        .and_then(|blocks| blocks.checked_mul(1024))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(|| failed("relational run metadata capacity overflow"))?;
    Ok(QueryRunSpec {
        dtype: dtype(spec),
        rows,
        block_rows,
        metadata_bytes,
    })
}

struct HeldBlock {
    batch: Batch,
    _block: QueryRunBlock,
}

struct Cursor<'a> {
    reader: Reader<'a>,
    block: Option<Arc<HeldBlock>>,
    row: usize,
}

impl Cursor<'_> {
    fn load(&mut self, spec: &Spec, context: &NativeExecutionContext<'_>) -> Result<()> {
        context.check_cancelled()?;
        self.row = 0;
        self.block = self
            .reader
            .next_block(context.runtime())?
            .map(|block| -> Result<_> {
                let batch = Batch::new(block.array().clone(), &spec.names, context)?;
                Ok(Arc::new(HeldBlock {
                    batch,
                    _block: block,
                }))
            })
            .transpose()?;
        Ok(())
    }

    fn advance(&mut self, spec: &Spec, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.row += 1;
        if self
            .block
            .as_ref()
            .is_some_and(|block| self.row == block.batch.array.len())
        {
            self.load(spec, context)?;
        }
        Ok(())
    }
}

struct Merge<'a> {
    spec: &'a Spec,
    cursors: [Cursor<'a>; 2],
}

impl<'a> Merge<'a> {
    fn new(
        spec: &'a Spec,
        left: Reader<'a>,
        right: Reader<'a>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut this = Self {
            spec,
            cursors: [left, right].map(|reader| Cursor {
                reader,
                block: None,
                row: 0,
            }),
        };
        for cursor in &mut this.cursors {
            cursor.load(spec, context)?;
        }
        Ok(this)
    }

    fn validate(&self) -> Result<()> {
        for cursor in &self.cursors {
            cursor.reader.validate()?;
        }
        Ok(())
    }

    fn winner(&self) -> Result<Option<usize>> {
        let [left, right] = &self.cursors;
        let (Some(a), Some(b)) = (&left.block, &right.block) else {
            return Ok(if left.block.is_some() {
                Some(0)
            } else if right.block.is_some() {
                Some(1)
            } else {
                None
            });
        };
        for (index, key) in self.spec.keys.iter().enumerate() {
            let order = native_relational_sort::compare_key(
                key,
                a.batch.key_is_null(left.row, index)?,
                b.batch.key_is_null(right.row, index)?,
                || a.batch.compare_key(left.row, &b.batch, right.row, index),
            )?;
            if order != KeyOrdering::Equal {
                return Ok(Some(usize::from(order.is_gt())));
            }
        }
        // Runs represent adjacent input ranges. Earlier input wins every tie.
        Ok(Some(0))
    }

    fn next(
        &mut self,
        block_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        context.check_cancelled()?;
        let mut table = Table::new(context.memory())?;
        let mut owners = ReservedVec::<(Arc<HeldBlock>, usize)>::new(context.memory())?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(block_rows)?;
        while rows.values.len() < block_rows {
            let Some(winner) = self.winner()? else {
                break;
            };
            let cursor = &mut self.cursors[winner];
            let block = cursor
                .block
                .as_ref()
                .ok_or_else(|| failed("merge head is absent"))?;
            let offset = if let Some((_, offset)) = owners
                .values
                .iter()
                .find(|(held, _)| Arc::ptr_eq(held, block))
            {
                *offset
            } else {
                let offset = table.rows();
                owners.push((Arc::clone(block), offset))?;
                table.push(Batch::new(block.batch.array.clone(), &[], context)?)?;
                offset
            };
            rows.values.push(Some(
                offset
                    .checked_add(cursor.row)
                    .ok_or_else(|| failed("merge selection overflow"))?,
            ));
            cursor.advance(self.spec, context)?;
        }
        if rows.values.is_empty() {
            self.validate()?;
            return Ok(None);
        }
        let output =
            native_relational_sort::gather(&table, &rows.values, &self.spec.fields, context)?;
        self.validate()?;
        #[cfg(test)]
        if let Some(hook) = AFTER_MERGE_BLOCK.with(|hook| hook.borrow_mut().take()) {
            hook(output.len());
        }
        context.check_cancelled()?;
        Ok(Some(output))
    }
}

pub(crate) fn recover(policy: &VortexRelationalSpillPolicy, directory: &Path) -> Result<()> {
    let validated = VortexRelationalSpillPolicy::new(
        policy.workspace.clone(),
        policy.quota_bytes,
        policy.buffer_bytes,
    )?;
    query_run_store::recover(
        &QueryRunStorePolicy::relational_order(&validated, CancellationToken::default()),
        directory,
    )
}
