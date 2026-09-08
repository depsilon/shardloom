//! Candidate-only footer transposition over unchanged bounded child writes.
//! No production dispatch selects this strategy until paired acceptance.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use futures::{StreamExt as _, future::BoxFuture, stream};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::{
    array::{ArrayRef, arrays::Struct as StructArray, dtype::DType},
    error::{VortexResult, vortex_err},
    layout::{
        LayoutChildren, LayoutParts, LayoutRef, LayoutStrategy, LayoutWriterContext,
        layouts::{chunked::ChunkedLayout, struct_::Struct},
        segments::SegmentSinkRef,
        sequence::{
            SendableSequentialStream, SequencePointer, SequentialStreamAdapter,
            SequentialStreamExt as _,
        },
    },
    session::VortexSession,
};

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_field_names)] // These are explicit independent admission bounds.
pub(crate) struct ColumnLayoutBounds {
    pub(crate) max_columns: usize,
    pub(crate) max_row_groups: usize,
    pub(crate) max_rows_per_group: usize,
}

impl Default for ColumnLayoutBounds {
    fn default() -> Self {
        Self {
            max_columns: 256,
            max_row_groups: 65_536,
            max_rows_per_group: 1_048_576,
        }
    }
}

#[derive(Default)]
pub(crate) struct ColumnLayoutCounters {
    input_groups: AtomicU64,
    empty_groups: AtomicU64,
    child_writer_calls: AtomicU64,
    transposed_references: AtomicU64,
    peak_reference_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColumnLayoutSnapshot {
    pub(crate) input_groups: u64,
    pub(crate) empty_groups: u64,
    pub(crate) child_writer_calls: u64,
    pub(crate) transposed_references: u64,
    pub(crate) peak_reference_bytes: u64,
}

impl ColumnLayoutCounters {
    pub(crate) fn snapshot(&self) -> ColumnLayoutSnapshot {
        ColumnLayoutSnapshot {
            input_groups: self.input_groups.load(Ordering::Relaxed),
            empty_groups: self.empty_groups.load(Ordering::Relaxed),
            child_writer_calls: self.child_writer_calls.load(Ordering::Relaxed),
            transposed_references: self.transposed_references.load(Ordering::Relaxed),
            peak_reference_bytes: self.peak_reference_bytes.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct ColumnAddressableLayout {
    child: Arc<dyn LayoutStrategy>,
    memory: LiveMemoryPool,
    bounds: ColumnLayoutBounds,
    counters: Arc<ColumnLayoutCounters>,
    started: AtomicBool,
}

impl ColumnAddressableLayout {
    pub(crate) fn new(
        child: Arc<dyn LayoutStrategy>,
        memory: LiveMemoryPool,
        bounds: ColumnLayoutBounds,
    ) -> VortexResult<Self> {
        if bounds.max_columns == 0
            || bounds.max_columns > 256
            || bounds.max_row_groups == 0
            || bounds.max_row_groups > 65_536
            || bounds.max_rows_per_group == 0
            || bounds.max_rows_per_group > 1_048_576
        {
            return Err(vortex_err!(
                "column-addressable writer requires bounded positive column/group/row limits"
            ));
        }
        Ok(Self {
            child,
            memory,
            bounds,
            counters: Arc::new(ColumnLayoutCounters::default()),
            started: AtomicBool::new(false),
        })
    }

    pub(crate) fn counters(&self) -> Arc<ColumnLayoutCounters> {
        Arc::clone(&self.counters)
    }

    fn admit_dtype(&self, dtype: &DType) -> VortexResult<usize> {
        let fields = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| vortex_err!("column-addressable writer requires Struct input"))?;
        if dtype.is_nullable() || fields.nfields() > self.bounds.max_columns {
            return Err(vortex_err!(
                "column-addressable writer requires a nonnullable root within the column bound"
            ));
        }
        if fields.fields().any(|dtype| {
            !matches!(
                dtype,
                DType::Null
                    | DType::Bool(_)
                    | DType::Primitive(..)
                    | DType::Utf8(_)
                    | DType::Binary(_)
            )
        }) {
            return Err(vortex_err!(
                "column-addressable writer admits scalar fields only"
            ));
        }
        for (index, name) in fields.names().iter().enumerate() {
            if fields.names().iter().take(index).any(|prior| prior == name) {
                return Err(vortex_err!(
                    "column-addressable writer requires distinct field names"
                ));
            }
        }
        Ok(fields.nfields())
    }

    fn admit_array(&self, array: &ArrayRef, dtype: &DType, groups: usize) -> VortexResult<()> {
        if array.dtype() != dtype || array.as_opt::<StructArray>().is_none() {
            return Err(vortex_err!(
                "column-addressable writer requires stable native Struct batches; no implicit canonicalization"
            ));
        }
        if array.len() > self.bounds.max_rows_per_group
            || (!array.is_empty() && groups == self.bounds.max_row_groups)
        {
            return Err(vortex_err!(
                "column-addressable writer exceeded actual row-group/row admission"
            ));
        }
        Ok(())
    }
}

impl LayoutStrategy for ColumnAddressableLayout {
    fn write_stream<'a, 'b, 'future>(
        &'a self,
        ctx: LayoutWriterContext,
        sink: SegmentSinkRef,
        mut input: SendableSequentialStream,
        eof: SequencePointer,
        session: &'b VortexSession,
    ) -> BoxFuture<'future, VortexResult<LayoutRef>>
    where
        'a: 'future,
        'b: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            if self.started.swap(true, Ordering::AcqRel) {
                return Err(vortex_err!(
                    "column-addressable strategy is owned by one writer"
                ));
            }
            let dtype = input.dtype().clone();
            let columns = self.admit_dtype(&dtype)?;
            let mut matrix =
                ReferenceMatrix::new(columns, &self.memory, Arc::clone(&self.counters))?;
            let mut rows = 0_u64;
            let mut groups = 0_usize;
            while let Some(item) = input.next().await {
                let (sequence, array) = item?;
                self.admit_array(&array, &dtype, groups)?;
                if array.is_empty() {
                    self.counters.empty_groups.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                matrix.admit_next_group()?;
                let group_rows = u64::try_from(array.len())
                    .map_err(|_| vortex_err!("column-addressable row count overflow"))?;
                rows = rows
                    .checked_add(group_rows)
                    .ok_or_else(|| vortex_err!("column-addressable row count overflow"))?;
                self.counters.input_groups.fetch_add(1, Ordering::Relaxed);
                let (start, child_eof) = sequence.descend().split();
                self.counters
                    .child_writer_calls
                    .fetch_add(1, Ordering::Relaxed);
                let child = self
                    .child
                    .write_stream(
                        ctx.clone(),
                        Arc::clone(&sink),
                        SequentialStreamAdapter::new(
                            dtype.clone(),
                            stream::iter([Ok((start.downgrade(), array))]),
                        )
                        .sendable(),
                        child_eof,
                        session,
                    )
                    .await?;
                matrix.accept(child, &dtype, group_rows)?;
                groups += 1;
            }
            drop(eof);
            Ok(matrix.into_layout(dtype, rows))
        })
    }
}

struct ReferenceMatrix {
    columns: Vec<Vec<LayoutRef>>,
    references: Arc<Mutex<MemoryLease>>,
    counters: Arc<ColumnLayoutCounters>,
}

impl ReferenceMatrix {
    fn new(
        columns: usize,
        memory: &LiveMemoryPool,
        counters: Arc<ColumnLayoutCounters>,
    ) -> VortexResult<Self> {
        // Adapter vector/Arc capacity; provider layout objects and serializer
        // scratch keep their existing separately scoped allocation contract.
        let per_column = 2 * std::mem::size_of::<Vec<LayoutRef>>()
            + std::mem::size_of::<LayoutRef>()
            + std::mem::size_of::<ReservedChildren>()
            + 4 * std::mem::size_of::<usize>();
        let root = std::mem::size_of::<ReservedChildren>()
            + std::mem::size_of::<Vec<LayoutRef>>()
            + std::mem::size_of::<Mutex<MemoryLease>>()
            + 6 * std::mem::size_of::<usize>();
        let base = columns
            .checked_mul(per_column)
            .and_then(|bytes| bytes.checked_add(root))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| vortex_err!("column-addressable reference capacity overflow"))?;
        let lease = memory
            .reserve(base)
            .map_err(|error| vortex_err!("column-addressable reference admission: {error}"))?;
        counters
            .peak_reference_bytes
            .fetch_max(base, Ordering::Relaxed);
        Ok(Self {
            columns: (0..columns).map(|_| Vec::new()).collect(),
            references: Arc::new(Mutex::new(lease)),
            counters,
        })
    }

    fn admit_next_group(&mut self) -> VortexResult<()> {
        for column in &mut self.columns {
            if column.len() < column.capacity() {
                continue;
            }
            let old = column.capacity();
            let new = old
                .checked_mul(2)
                .ok_or_else(|| vortex_err!("column-addressable reference capacity overflow"))?
                .max(1);
            let unit = std::mem::size_of::<LayoutRef>() as u64;
            let old_bytes = u64::try_from(old)
                .map_err(|_| vortex_err!("column-addressable reference capacity overflow"))?
                * unit;
            let new_bytes = u64::try_from(new)
                .map_err(|_| vortex_err!("column-addressable reference capacity overflow"))?
                .checked_mul(unit)
                .ok_or_else(|| vortex_err!("column-addressable reference capacity overflow"))?;
            let mut reservation = self
                .references
                .lock()
                .map_err(|_| vortex_err!("column-addressable reference owner poisoned"))?;
            let current = reservation.bytes();
            let transient = current
                .checked_add(new_bytes)
                .ok_or_else(|| vortex_err!("column-addressable reference capacity overflow"))?;
            reservation
                .resize(transient)
                .map_err(|error| vortex_err!("column-addressable reference admission: {error}"))?;
            self.counters
                .peak_reference_bytes
                .fetch_max(transient, Ordering::Relaxed);
            if let Err(error) = column.try_reserve_exact(new - column.len()) {
                reservation.resize(current).map_err(|error| {
                    vortex_err!("column-addressable reference release: {error}")
                })?;
                return Err(vortex_err!(
                    "column-addressable reference allocation: {error}"
                ));
            }
            if column.capacity() != new {
                return Err(vortex_err!(
                    "column-addressable allocator exceeded requested reference capacity"
                ));
            }
            reservation
                .resize(current - old_bytes + new_bytes)
                .map_err(|error| vortex_err!("column-addressable reference release: {error}"))?;
        }
        Ok(())
    }

    fn accept(&mut self, child: LayoutRef, dtype: &DType, rows: u64) -> VortexResult<()> {
        if child.as_opt::<Struct>().is_none()
            || child.dtype() != dtype
            || child.row_count() != rows
            || child.nchildren() != self.columns.len()
        {
            return Err(vortex_err!(
                "column-addressable child returned an incompatible Struct root"
            ));
        }
        let fields = dtype.as_struct_fields_opt().expect("admitted Struct");
        for (index, column) in self.columns.iter_mut().enumerate() {
            let field = child
                .slot(index + 1)?
                .ok_or_else(|| vortex_err!("column-addressable child omitted a field"))?;
            if Some(field.dtype().clone()) != fields.field_by_index(index)
                || field.row_count() != rows
            {
                return Err(vortex_err!(
                    "column-addressable child field dtype/row count mismatch"
                ));
            }
            column.push(field);
        }
        self.counters
            .transposed_references
            .fetch_add(self.columns.len() as u64, Ordering::Relaxed);
        drop(child);
        Ok(())
    }

    fn into_layout(self, dtype: DType, rows: u64) -> LayoutRef {
        let Self {
            columns,
            references,
            counters: _,
        } = self;
        let fields = dtype.as_struct_fields_opt().expect("admitted Struct");
        let children = columns
            .into_iter()
            .enumerate()
            .map(|(index, children)| {
                ChunkedLayout::new(
                    rows,
                    fields.field_by_index(index).expect("admitted field"),
                    Arc::new(ReservedChildren {
                        children: Arc::new(children),
                        references: Arc::clone(&references),
                    }),
                )
                .into_layout()
            })
            .collect();
        LayoutParts::new(
            Struct,
            dtype,
            rows,
            Vec::new(),
            Arc::new(ReservedChildren {
                children: Arc::new(children),
                references,
            }),
            (),
        )
        .into_typed()
        .into_layout()
    }
}

struct ReservedChildren {
    children: Arc<Vec<LayoutRef>>,
    references: Arc<Mutex<MemoryLease>>,
}

impl LayoutChildren for ReservedChildren {
    fn to_arc(&self) -> Arc<dyn LayoutChildren> {
        Arc::new(Self {
            children: Arc::clone(&self.children),
            references: Arc::clone(&self.references),
        })
    }
    fn child(&self, index: usize, dtype: &DType) -> VortexResult<LayoutRef> {
        let child = self
            .children
            .get(index)
            .ok_or_else(|| vortex_err!("column-addressable child index out of bounds"))?;
        if child.dtype() != dtype {
            return Err(vortex_err!("column-addressable child dtype mismatch"));
        }
        Ok(Arc::clone(child))
    }
    fn child_row_count(&self, index: usize) -> u64 {
        self.children[index].row_count()
    }
    fn nchildren(&self) -> usize {
        self.children.len()
    }
    fn child_is_indivisible(&self, index: usize) -> bool {
        self.children[index].dyn_is_indivisible()
    }
}

#[cfg(test)]
#[path = "column_addressable_layout_tests.rs"]
mod tests;
