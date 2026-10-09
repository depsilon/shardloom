//! Immutable, chronological replacement runs for sparse native pivot state.
//! This is private operator state, not a general key/value store or checkpoint.

use super::{
    Arc, ArrayRef, MemoryLease, NativeExecutionContext, NativeQueryRun, QueryRunBlock,
    READER_WORK_BYTES, Reader, ReservedVec, Result, Spec, State, failed, run_spec,
};
use crate::local_primitives::{
    native_relational_batch::Batch, native_relational_keys::Cell as KeyCell,
};
use std::cmp::Ordering;

#[path = "native_relational_pivot_merge.rs"]
mod merge;

pub(in crate::local_primitives) const BLOCK_ROWS: usize = 1024;
pub(in crate::local_primitives) const INDEX: &str = "index_key";
pub(in crate::local_primitives) const KIND: &str = "record_kind";
pub(in crate::local_primitives) const DOMAIN: &str = "domain_key";

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::local_primitives) struct Key {
    pub(in crate::local_primitives) index: String,
    pub(in crate::local_primitives) kind: u64,
    pub(in crate::local_primitives) domain: String,
}

impl Key {
    pub(in crate::local_primitives) fn view(&self) -> KeyRef<'_> {
        KeyRef {
            index: self.index.as_bytes(),
            kind: self.kind,
            domain: self.domain.as_bytes(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::local_primitives) struct KeyRef<'a> {
    pub(in crate::local_primitives) index: &'a [u8],
    pub(in crate::local_primitives) kind: u64,
    pub(in crate::local_primitives) domain: &'a [u8],
}

struct Bounds {
    first: Key,
    last: Key,
    _metadata: MemoryLease,
}

impl Bounds {
    fn new(first: &Key, last: &Key, context: &NativeExecutionContext<'_>) -> Result<Self> {
        Self::from_refs(first.view(), last.view(), context)
    }

    fn from_refs(
        first: KeyRef<'_>,
        last: KeyRef<'_>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let bytes = [first, last].iter().try_fold(1024_u64, |bytes, key| {
            (key.index.len() as u64)
                .checked_add(key.domain.len() as u64)
                .and_then(|length| length.checked_mul(2))
                .and_then(|length| bytes.checked_add(length))
                .ok_or_else(|| failed("pivot run bounds capacity overflow"))
        })?;
        let metadata = context.memory().reserve(bytes)?;
        let owned = |key: KeyRef<'_>| -> Result<Key> {
            Ok(Key {
                index: std::str::from_utf8(key.index)
                    .map_err(|_| failed("pivot index key is not UTF8"))?
                    .to_owned(),
                kind: key.kind,
                domain: std::str::from_utf8(key.domain)
                    .map_err(|_| failed("pivot domain key is not UTF8"))?
                    .to_owned(),
            })
        };
        Ok(Self {
            first: owned(first)?,
            last: owned(last)?,
            _metadata: metadata,
        })
    }

    fn missing_before(
        first: KeyRef<'_>,
        next: &Row,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let index = next.index()?;
        let KeyCell::Utf8(domain) = next.held.batch.raw_cell(next.row, 2)? else {
            return Err(failed("pivot domain key changed its native type"));
        };
        Self::from_refs(
            first,
            KeyRef {
                index: index.as_ref(),
                kind: next.kind()?,
                domain: domain.as_ref(),
            },
            context,
        )
    }
}

struct Held {
    batch: Batch,
    _block: QueryRunBlock,
    _metadata: MemoryLease,
    start: u64,
}

pub(in crate::local_primitives) struct Row {
    held: Arc<Held>,
    row: usize,
}

impl Row {
    pub(in crate::local_primitives) fn array(&self) -> &ArrayRef {
        &self.held.batch.array
    }

    pub(in crate::local_primitives) fn offset(&self) -> usize {
        self.row
    }

    pub(in crate::local_primitives) fn kind(&self) -> Result<u64> {
        let KeyCell::NonnegativeInteger(kind) = self.held.batch.raw_cell(self.row, 1)? else {
            return Err(failed("pivot record kind changed its native type"));
        };
        if kind > 1 {
            return Err(failed("pivot record kind is outside its private domain"));
        }
        Ok(kind)
    }

    pub(in crate::local_primitives) fn index(&self) -> Result<vortex::buffer::ByteBuffer> {
        let KeyCell::Utf8(index) = self.held.batch.raw_cell(self.row, 0)? else {
            return Err(failed("pivot index key changed its native type"));
        };
        Ok(index)
    }

    fn compare(&self, key: KeyRef<'_>) -> Result<Ordering> {
        let order = self.index()?.as_ref().cmp(key.index);
        if order != Ordering::Equal {
            return Ok(order);
        }
        let order = self.kind()?.cmp(&key.kind);
        if order != Ordering::Equal {
            return Ok(order);
        }
        let KeyCell::Utf8(domain) = self.held.batch.raw_cell(self.row, 2)? else {
            return Err(failed("pivot domain key changed its native type"));
        };
        Ok(domain.as_ref().cmp(key.domain))
    }

    fn compare_row(&self, other: &Self) -> Result<Ordering> {
        for column in 0..3 {
            let order =
                self.held
                    .batch
                    .compare_key(self.row, &other.held.batch, other.row, column)?;
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(Ordering::Equal)
    }
}

fn load(
    reader: &Reader<'_>,
    start: u64,
    spec: &Spec,
    context: &NativeExecutionContext<'_>,
) -> Result<Arc<Held>> {
    context.check_cancelled()?;
    let metadata = context
        .memory()
        .reserve((std::mem::size_of::<Held>() + 128) as u64)?;
    let block = reader
        .read_block_at(start, context.runtime())?
        .ok_or_else(|| failed("pivot run block is absent"))?;
    let batch = Batch::new(block.array().clone(), &spec.names, context)?;
    Ok(Arc::new(Held {
        batch,
        _block: block,
        _metadata: metadata,
        start,
    }))
}

#[cfg(test)]
type CacheHook = Box<dyn FnOnce(&std::path::Path)>;
#[cfg(test)]
thread_local! {
    pub(crate) static BEFORE_CACHE_HIT: std::cell::RefCell<Option<CacheHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static BEFORE_GAP_HIT: std::cell::RefCell<Option<CacheHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static AFTER_COUNT_PASS: std::cell::RefCell<Option<CacheHook>> = const { std::cell::RefCell::new(None) };
}

struct Cache {
    held: [Option<(u64, Arc<Held>)>; 2],
    next: usize,
    blocks: u64,
    _metadata: MemoryLease,
}

impl Cache {
    fn new(context: &NativeExecutionContext<'_>) -> Result<Self> {
        Ok(Self {
            held: [None, None],
            next: 0,
            blocks: 0,
            _metadata: context
                .memory()
                .reserve((std::mem::size_of::<Self>() + 128) as u64)?,
        })
    }

    fn clear(&mut self) {
        self.held = [None, None];
    }

    fn row(
        &mut self,
        id: u64,
        native: &NativeQueryRun,
        reader: &Reader<'_>,
        position: u64,
        spec: &Spec,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Row> {
        context.check_cancelled()?;
        let start = position / native.block_rows as u64 * native.block_rows as u64;
        let slot = self.held.iter().position(|held| {
            held.as_ref()
                .is_some_and(|(run, held)| *run == id && held.start == start)
        });
        #[cfg(test)]
        if slot.is_some()
            && let Some(hook) = BEFORE_CACHE_HIT.with(|slot| slot.borrow_mut().take())
        {
            hook(&native.path);
        }
        // A cached payload is never permission to reuse a changed file generation.
        reader.validate()?;
        if position >= native.rows {
            return Err(failed("pivot lookup exceeds its run"));
        }
        let slot = if let Some(slot) = slot {
            slot
        } else {
            let slot = self.next;
            self.held[slot] = None;
            let held = load(reader, start, spec, context)?;
            self.held[slot] = Some((id, held));
            self.blocks = self
                .blocks
                .checked_add(1)
                .ok_or_else(|| failed("pivot lookup block count overflow"))?;
            slot
        };
        self.next = (slot + 1) % self.held.len();
        let held = &self.held[slot]
            .as_ref()
            .ok_or_else(|| failed("pivot cache slot is absent"))?
            .1;
        Ok(Row {
            held: Arc::clone(held),
            row: usize::try_from(position - start)
                .map_err(|_| failed("pivot lookup row offset exceeds addressable memory"))?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn find(
        &mut self,
        id: u64,
        native: &NativeQueryRun,
        reader: &Reader<'_>,
        key: KeyRef<'_>,
        spec: &Spec,
        context: &NativeExecutionContext<'_>,
        missing: Option<&mut Option<Bounds>>,
    ) -> Result<Option<Row>> {
        let mut low = 0;
        let mut high = native.rows;
        // Adjacent output cells and online updates often remain in a held
        // block. A whole-run binary search would evict that block with its
        // upper levels before reaching the same key range again. Exact bounds
        // narrow the search without another directory or retained payload.
        for slot in 0..self.held.len() {
            let Some((run, held)) = &self.held[slot] else {
                continue;
            };
            if *run != id {
                continue;
            }
            let start = held.start;
            let end = start
                .checked_add(held.batch.array.len() as u64)
                .filter(|end| *end > start && *end <= native.rows)
                .ok_or_else(|| failed("pivot cached block has invalid bounds"))?;
            let first = self.row(id, native, reader, start, spec, context)?;
            if first.compare(key)? == Ordering::Greater {
                continue;
            }
            let last = self.row(id, native, reader, end - 1, spec, context)?;
            if last.compare(key)? != Ordering::Less {
                low = start;
                high = end;
                break;
            }
        }
        while low < high {
            let middle = low + (high - low) / 2;
            let row = self.row(id, native, reader, middle, spec, context)?;
            match row.compare(key)? {
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
                Ordering::Equal => return Ok(Some(row)),
            }
        }
        reader.validate()?;
        if let Some(missing) = missing {
            // Runs are immutable. Binary search proves every key from this
            // absent target up to its successor absent too. Retain only one
            // credited gap per run, never another payload block or directory.
            *missing = None;
            if low < native.rows {
                let next = self.row(id, native, reader, low, spec, context)?;
                *missing = Some(Bounds::missing_before(key, &next, context)?);
            }
        }
        Ok(None)
    }
}

struct Run<'s> {
    native: NativeQueryRun,
    reader: Reader<'s>,
    bounds: Bounds,
    index_bounds: Option<Bounds>,
    missing: Option<Bounds>,
    id: u64,
    level: u32,
}

pub(in crate::local_primitives) struct Store<'p, 's> {
    spec: &'p Spec,
    state: &'s State,
    runs: ReservedVec<Run<'s>>,
    cache: Cache,
    work: Arc<MemoryLease>,
    opens: u64,
}

impl<'p, 's> Store<'p, 's> {
    pub(in crate::local_primitives) fn new(
        spec: &'p Spec,
        state: &'s State,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        if spec.names != [INDEX, KIND, DOMAIN] {
            return Err(failed("pivot run keys differ from their private schema"));
        }
        Ok(Self {
            spec,
            state,
            runs: ReservedVec::new(context.memory())?,
            cache: Cache::new(context)?,
            work: Arc::new(context.memory().reserve(READER_WORK_BYTES)?),
            opens: 0,
        })
    }

    pub(in crate::local_primitives) fn buffer_bytes(&self) -> u64 {
        self.state.policy.buffer_bytes
    }

    pub(in crate::local_primitives) fn find(
        &mut self,
        key: &Key,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Row>> {
        for run in self.runs.values.iter_mut().rev() {
            context.check_cancelled()?;
            let missing = run
                .missing
                .as_ref()
                .is_some_and(|bounds| key >= &bounds.first && key < &bounds.last);
            #[cfg(test)]
            if missing && let Some(hook) = BEFORE_GAP_HIT.with(|slot| slot.borrow_mut().take()) {
                hook(&run.native.path);
            }
            run.reader.validate()?;
            if (key.kind == 0
                && run
                    .index_bounds
                    .as_ref()
                    .is_none_or(|bounds| key < &bounds.first || key > &bounds.last))
                || key < &run.bounds.first
                || key > &run.bounds.last
                || missing
            {
                continue;
            }
            if let Some(row) = self.cache.find(
                run.id,
                &run.native,
                &run.reader,
                key.view(),
                self.spec,
                context,
                Some(&mut run.missing),
            )? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    pub(in crate::local_primitives) fn append(
        &mut self,
        rows: usize,
        first: &Key,
        last: &Key,
        indices: Option<(&Key, &Key)>,
        blocks: impl Iterator<Item = Result<ArrayRef>>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if rows == 0 || first > last {
            return Err(failed("pivot flush has invalid key bounds"));
        }
        let bounds = Bounds::new(first, last, context)?;
        let index_bounds = indices
            .map(|(first, last)| Bounds::new(first, last, context))
            .transpose()?;
        self.runs.reserve_one()?;
        let spec = run_spec(self.spec, rows as u64, BLOCK_ROWS)?;
        let native = self.state.with_store(context, |store| {
            store.write_arrays(
                &spec,
                blocks,
                context.runtime(),
                context.native_session(),
                &self.work,
            )
        })?;
        self.state
            .max_block_rows
            .set(self.state.max_block_rows.get().max(BLOCK_ROWS));
        self.push(native, bounds, index_bounds, 0, context)
    }

    fn push(
        &mut self,
        native: NativeQueryRun,
        bounds: Bounds,
        index_bounds: Option<Bounds>,
        level: u32,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let reader = self.state.open(&native, self.spec, context, &self.work)?;
        self.opens = self
            .opens
            .checked_add(1)
            .ok_or_else(|| failed("pivot reader count overflow"))?;
        self.runs.push(Run {
            native,
            reader,
            bounds,
            index_bounds,
            missing: None,
            id: self.opens,
            level,
        })
    }

    pub(in crate::local_primitives) fn compact(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        while self.runs.values.len() >= 2 {
            let n = self.runs.values.len();
            if self.runs.values[n - 1].level != self.runs.values[n - 2].level {
                break;
            }
            self.merge_last(context)?;
        }
        Ok(())
    }

    fn merge_last(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.cache.clear();
        let right = self
            .runs
            .values
            .pop()
            .ok_or_else(|| failed("newer pivot run is absent"))?;
        let left = self
            .runs
            .values
            .pop()
            .ok_or_else(|| failed("older pivot run is absent"))?;
        let bounds = Bounds::new(
            std::cmp::min(&left.bounds.first, &right.bounds.first),
            std::cmp::max(&left.bounds.last, &right.bounds.last),
            context,
        )?;
        // Replacement never deletes index markers. Their exact range remains
        // useful when newer cells extend a merged run beyond its older markers.
        let index_bounds = match (&left.index_bounds, &right.index_bounds) {
            (None, None) => None,
            (Some(bounds), None) | (None, Some(bounds)) => {
                Some(Bounds::new(&bounds.first, &bounds.last, context)?)
            }
            (Some(left), Some(right)) => Some(Bounds::new(
                std::cmp::min(&left.first, &right.first),
                std::cmp::max(&left.last, &right.last),
                context,
            )?),
        };
        let level = left
            .level
            .max(right.level)
            .checked_add(1)
            .ok_or_else(|| failed("pivot merge level overflow"))?;
        let rows = merge::count(self.spec, &left, &right, context)?;
        #[cfg(test)]
        if let Some(hook) = AFTER_COUNT_PASS.with(|slot| slot.borrow_mut().take()) {
            hook(&left.native.path);
        }
        left.reader.validate()?;
        right.reader.validate()?;
        let mut merge = merge::Merge::new(self.spec, &left, &right);
        let spec = run_spec(self.spec, rows, BLOCK_ROWS)?;
        let blocks = std::iter::from_fn(|| merge.next(context).transpose());
        let native = self.state.with_store(context, |store| {
            store.write_arrays(
                &spec,
                blocks,
                context.runtime(),
                context.native_session(),
                &self.work,
            )
        })?;
        merge.validate()?;
        drop(merge);
        let Run {
            native: left_native,
            reader: left_reader,
            ..
        } = left;
        let Run {
            native: right_native,
            reader: right_reader,
            ..
        } = right;
        drop((left_reader, right_reader));
        self.state.with_store(context, |store| {
            store.remove(&left_native)?;
            store.remove(&right_native)
        })?;
        self.state.merge_passes.set(
            self.state
                .merge_passes
                .get()
                .checked_add(1)
                .ok_or_else(|| failed("pivot merge count overflow"))?,
        );
        self.push(native, bounds, index_bounds, level, context)
    }

    pub(in crate::local_primitives) fn complete(
        mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<(Option<Owned>, u64, u64)> {
        while self.runs.values.len() > 1 {
            self.merge_last(context)?;
        }
        self.cache.clear();
        let run = self.runs.values.pop().map(|run| {
            let Run { native, reader, .. } = run;
            drop(reader);
            Owned { native }
        });
        Ok((run, self.cache.blocks, self.opens))
    }
}

/// No borrowed spill reader escapes dynamic binding. The completed owner can
/// reopen only under that execution's shared store and cleanup policy.
pub(in crate::local_primitives) struct Owned {
    native: NativeQueryRun,
}

pub(in crate::local_primitives) struct Access<'a, 's> {
    owner: &'a Owned,
    spec: &'a Spec,
    reader: Reader<'s>,
    cache: Cache,
}

impl Owned {
    pub(in crate::local_primitives) fn rows(&self) -> u64 {
        self.native.rows
    }

    pub(in crate::local_primitives) fn open<'a, 's>(
        &'a self,
        state: &'s State,
        spec: &'a Spec,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Access<'a, 's>> {
        let work = Arc::new(context.memory().reserve(READER_WORK_BYTES)?);
        Ok(Access {
            owner: self,
            spec,
            reader: state.open(&self.native, spec, context, &work)?,
            cache: Cache::new(context)?,
        })
    }

    pub(in crate::local_primitives) fn finish(
        self,
        state: &State,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        state.with_store(context, |store| store.remove(&self.native))
    }
}

impl Access<'_, '_> {
    pub(in crate::local_primitives) fn validate(&self) -> Result<()> {
        self.reader.validate()
    }
    pub(in crate::local_primitives) fn rows(&self) -> u64 {
        self.owner.native.rows
    }
    pub(in crate::local_primitives) fn blocks(&self) -> u64 {
        self.cache.blocks
    }
    pub(in crate::local_primitives) fn row(
        &mut self,
        position: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Row> {
        self.cache.row(
            0,
            &self.owner.native,
            &self.reader,
            position,
            self.spec,
            context,
        )
    }
    pub(in crate::local_primitives) fn find(
        &mut self,
        key: KeyRef<'_>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Row>> {
        self.cache.find(
            0,
            &self.owner.native,
            &self.reader,
            key,
            self.spec,
            context,
            None,
        )
    }
    pub(in crate::local_primitives) fn next_index(
        &mut self,
        position: &mut u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        while *position < self.rows() {
            let current = *position;
            let row = self.row(current, context)?;
            *position += 1;
            if row.kind()? == 0 {
                return Ok(Some(current));
            }
        }
        self.validate()?;
        Ok(None)
    }
}
