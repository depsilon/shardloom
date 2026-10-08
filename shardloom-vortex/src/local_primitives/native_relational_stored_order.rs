//! Completed native ordering with bounded positional access for relational state.

use super::{
    Arc, ArrayRef, MemoryLease, NativeExecutionContext, NativeQueryRun, Ordering, QueryRunBlock,
    Reader, Result, State, dtype, failed,
};
use crate::local_primitives::{native_payload, vortex_error};

#[allow(clippy::large_enum_variant)] // One bounded stack owner avoids additional heap allocation.
enum Source<'a> {
    Resident {
        array: ArrayRef,
        metadata: Arc<MemoryLease>,
    },
    Run {
        native: NativeQueryRun,
        reader: Reader<'a>,
    },
}

/// Internal consumers borrow blocks and compact retained selections before
/// dropping those blocks. The public result path never receives borrowed arrays.
pub(in crate::local_primitives) struct StoredOrder<'a> {
    source: Source<'a>,
    state: &'a State,
    rows: u64,
    block_rows: usize,
}

pub(in crate::local_primitives) struct StoredBlock {
    array: ArrayRef,
    _run: Option<QueryRunBlock>,
    _resident_metadata: Option<Arc<MemoryLease>>,
}

impl StoredBlock {
    pub(in crate::local_primitives) fn array(&self) -> &ArrayRef {
        &self.array
    }
}

impl<'a> Ordering<'a> {
    pub(in crate::local_primitives) fn retain(
        mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<StoredOrder<'a>> {
        context.check_cancelled()?;
        let (source, rows) = if self.runs.values.is_empty() {
            // This result is bounded by the admitted resident sort threshold.
            // Charge its parent metadata before gathering; old and new payload
            // owners overlap in the same pool until the sorter is dropped.
            let metadata = Arc::new(
                context
                    .memory()
                    .reserve(native_payload::metadata_bytes(&dtype(self.spec))?)?,
            );
            let rows = self.sort.ordered_rows(context)?;
            let array = self.sort.gather_rows(&rows.values, context)?;
            let length = array.len() as u64;
            (Source::Resident { array, metadata }, length)
        } else {
            let native = self.complete_run(context)?;
            let rows = native.rows;
            let work = self.work(context)?;
            let reader = self.state.open(&native, self.spec, context, &work)?;
            (Source::Run { native, reader }, rows)
        };
        context.check_cancelled()?;
        Ok(StoredOrder {
            source,
            state: self.state,
            rows,
            block_rows: self.block_rows,
        })
    }
}

impl StoredOrder<'_> {
    pub(in crate::local_primitives) fn rows(&self) -> u64 {
        self.rows
    }

    pub(in crate::local_primitives) fn block_rows(&self) -> usize {
        self.block_rows
    }

    pub(in crate::local_primitives) fn validate(&self) -> Result<()> {
        match &self.source {
            Source::Resident { .. } => Ok(()),
            Source::Run { reader, .. } => reader.validate(),
        }
    }

    pub(in crate::local_primitives) fn read_block_at(
        &self,
        start: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<StoredBlock>> {
        context.check_cancelled()?;
        self.validate()?;
        if start > self.rows
            || (start != self.rows && !start.is_multiple_of(self.block_rows as u64))
        {
            return Err(failed(
                "stored native order requires an aligned in-range block",
            ));
        }
        if start == self.rows {
            return Ok(None);
        }
        match &self.source {
            Source::Resident { array, metadata } => {
                let end = start.saturating_add(self.block_rows as u64).min(self.rows);
                let start = usize::try_from(start)
                    .map_err(|_| failed("stored resident order offset overflow"))?;
                let end = usize::try_from(end)
                    .map_err(|_| failed("stored resident order offset overflow"))?;
                Ok(Some(StoredBlock {
                    array: array.slice(start..end).map_err(vortex_error)?,
                    _run: None,
                    _resident_metadata: Some(Arc::clone(metadata)),
                }))
            }
            Source::Run { reader, .. } => reader
                .read_block_at(start, context.runtime())?
                .map(|block| StoredBlock {
                    array: block.array().clone(),
                    _run: Some(block),
                    _resident_metadata: None,
                })
                .map(Some)
                .ok_or_else(|| failed("stored native run returned no in-range block")),
        }
    }

    pub(in crate::local_primitives) fn finish(
        self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        self.validate()?;
        if let Source::Run { native, reader } = self.source {
            drop(reader);
            self.state
                .with_store(context, |store| store.remove(&native))?;
        }
        Ok(())
    }
}
