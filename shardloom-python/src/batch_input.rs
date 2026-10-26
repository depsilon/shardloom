//! Lazy Python producer demand inside the existing native allocation. Object
//! batches use the direct construction adapter; columnar batches use capsules.

use super::{native_error, object_input, session::failed};
use arrow_array::{RecordBatch, RecordBatchReader as _, ffi_stream::ArrowArrayStreamReader};
use arrow_pyarrow::FromPyArrow as _;
use arrow_schema::SchemaRef;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyIterator, PySequence},
};
use shardloom_core::{DatasetUri, Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, MemoryLease},
};
use shardloom_vortex::{
    resident_memory_source::{MemoryBatchSourceBuilder, ResidentMemorySource},
    resident_session::ResidentVortexSession,
};
use std::sync::{Arc, Mutex};

pub(super) type InputErrors = Arc<Mutex<Option<PyErr>>>;
pub(super) type BatchArgument = (String, Py<PyAny>, Vec<(String, String)>, bool);

pub(super) struct BatchDeclaration {
    pub(super) reference: String,
    open: Py<PyAny>,
    fields: Vec<(String, String)>,
    pub(super) streaming: bool,
}

impl BatchDeclaration {
    pub(super) fn same_owner(&self, other: &Self) -> bool {
        self.open.is(&other.open)
            && self.fields == other.fields
            && self.streaming == other.streaming
    }
}

impl From<BatchArgument> for BatchDeclaration {
    fn from((reference, open, fields, streaming): BatchArgument) -> Self {
        Self {
            reference,
            open,
            fields,
            streaming,
        }
    }
}

struct ReaderEntry<'a> {
    reader: BatchReader<'a>,
    next_batch: u64,
}

/// Source routing retains lazy readers, never a collection of input batches.
/// Reader slots and each reader's schema/builder share the execution allocation.
pub(super) struct BatchReaders<'a> {
    entries: Vec<ReaderEntry<'a>>,
    _metadata: MemoryLease,
    capacity: usize,
    previous: Option<usize>,
}

impl<'a> BatchReaders<'a> {
    pub(super) fn reserve(
        declarations: &[&BatchDeclaration],
        session: &ResidentVortexSession,
    ) -> Result<Option<Self>> {
        let capacity = declarations
            .iter()
            .filter(|declaration| declaration.streaming)
            .count();
        if capacity == 0 {
            return Ok(None);
        }
        let bytes = |count: usize| {
            count
                .checked_mul(std::mem::size_of::<ReaderEntry<'a>>())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(|| failed("batch reader metadata size overflow"))
        };
        let mut metadata = session.reserve_input_scratch(bytes(capacity)?)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(capacity)
            .map_err(|_| failed("batch reader metadata allocation failed"))?;
        metadata.resize(bytes(entries.capacity())?)?;
        Ok(Some(Self {
            entries,
            _metadata: metadata,
            capacity,
            previous: None,
        }))
    }

    pub(super) fn push(&mut self, reader: BatchReader<'a>) -> Result<()> {
        if self.entries.len() == self.capacity {
            return Err(failed(
                "batch reader count exceeds its admitted declaration",
            ));
        }
        if self
            .entries
            .iter()
            .any(|entry| entry.reader.declaration.reference == reader.declaration.reference)
        {
            return Err(failed("streaming source references must be distinct"));
        }
        self.entries.push(ReaderEntry {
            reader,
            next_batch: 0,
        });
        Ok(())
    }

    pub(super) fn next(&mut self, uri: &DatasetUri, ordinal: u64) -> Result<Option<RecordBatch>> {
        let index = self
            .entries
            .iter()
            .position(|entry| entry.reader.declaration.reference == uri.as_str())
            .ok_or_else(|| failed("native demand names an undeclared streaming source"))?;
        let entry = &self.entries[index];
        if entry.next_batch != ordinal || entry.reader.closed {
            return Err(failed(
                "native demand has an invalid streaming source ordinal",
            ));
        }
        // The engine consumed the previous batch before asking for this one.
        // Release its conversion credit even when demand switches source URI.
        if let Some(previous) = self.previous.take() {
            self.entries[previous].reader.conversion = None;
        }
        let entry = &mut self.entries[index];
        let batch = entry.reader.next()?;
        if batch.is_some() {
            entry.next_batch = entry
                .next_batch
                .checked_add(1)
                .ok_or_else(|| failed("streaming source batch ordinal overflow"))?;
            self.previous = Some(index);
        }
        Ok(batch)
    }
}

struct ObjectBatch {
    rows: Py<PySequence>,
    length: usize,
    offset: usize,
}

/// A reader does not invoke even the producer factory until native execution
/// requests its first batch. Its construction and schema already hold credits.
pub(super) struct BatchReader<'a> {
    declaration: &'a BatchDeclaration,
    builder: Option<MemoryBatchSourceBuilder>,
    schema: Budgeted<SchemaRef>,
    cancellation: CancellationToken,
    errors: InputErrors,
    iterator: Option<Py<PyIterator>>,
    objects: Option<ObjectBatch>,
    arrow: Option<ArrowArrayStreamReader>,
    conversion: Option<MemoryLease>,
    closed: bool,
}

impl<'a> BatchReader<'a> {
    pub(super) fn new(
        declaration: &'a BatchDeclaration,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
        errors: &InputErrors,
    ) -> Result<Self> {
        let builder = MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
        let schema = builder.object_input_schema(&declaration.fields)?;
        Ok(Self {
            declaration,
            builder: Some(builder),
            schema,
            cancellation: cancellation.clone(),
            errors: Arc::clone(errors),
            iterator: None,
            objects: None,
            arrow: None,
            conversion: None,
            closed: false,
        })
    }

    pub(super) fn schema(&self) -> SchemaRef {
        Arc::clone(self.schema.value())
    }

    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.cancellation.check()?;
        // The previous input has been consumed before the next native demand.
        self.conversion = None;
        let result = self.next_python();
        result.map_err(|error| {
            if let Ok(mut first) = self.errors.lock()
                && first.is_none()
            {
                *first = Some(error);
            }
            failed("Python batch producer failed")
        })
    }

    fn next_python(&mut self) -> PyResult<Option<RecordBatch>> {
        if self.closed {
            return Ok(None);
        }
        loop {
            self.cancellation.check().map_err(native_error)?;
            if let Some(rows) = &mut self.objects {
                let end = rows
                    .offset
                    .saturating_add(object_input::CONSTRUCTION_ROWS)
                    .min(rows.length);
                let batch = Python::attach(|py| {
                    if rows.rows.bind(py).len()? != rows.length {
                        return Err(PyValueError::new_err(
                            "batch row sequence length changed during native intake",
                        ));
                    }
                    object_input::convert_batch(
                        py,
                        rows.rows.bind(py),
                        rows.offset..end,
                        self.schema.value(),
                        self.builder.as_ref().expect("reader owns its builder"),
                        &self.cancellation,
                    )
                })?;
                rows.offset = end;
                if end == rows.length {
                    self.objects = None;
                }
                let (batch, credit) = batch.into_parts();
                self.conversion = Some(credit);
                return Ok(Some(batch));
            }
            if let Some(reader) = &mut self.arrow {
                let next = Python::attach(|_| reader.next());
                if let Some(batch) = next {
                    return batch
                        .map(Some)
                        .map_err(|error| PyValueError::new_err(error.to_string()));
                }
                Python::attach(|_| drop(self.arrow.take()));
            }
            let next = Python::attach(|py| {
                if self.iterator.is_none() {
                    self.iterator =
                        Some(self.declaration.open.bind(py).call0()?.try_iter()?.unbind());
                }
                self.iterator
                    .as_ref()
                    .expect("opened input iterator")
                    .bind(py)
                    .clone()
                    .next()
                    .transpose()
                    .map(|item| item.map(Bound::unbind))
            })?;
            let Some(next) = next else {
                self.close()?;
                return Ok(None);
            };
            Python::attach(|py| {
                let next = next.bind(py);
                if next.hasattr("__arrow_c_stream__")? {
                    let reader = ArrowArrayStreamReader::from_pyarrow_bound(next)?;
                    if reader.schema().fields() != self.schema.value().fields() {
                        return Err(PyValueError::new_err(
                            "columnar batch changed its declared schema",
                        ));
                    }
                    self.arrow = Some(reader);
                } else {
                    let rows = object_input::sequence(next)?;
                    self.objects = Some(ObjectBatch {
                        length: rows.len()?,
                        rows: rows.unbind(),
                        offset: 0,
                    });
                }
                Ok::<_, PyErr>(())
            })?;
        }
    }

    pub(super) fn collect_resident(mut self) -> Result<ResidentMemorySource> {
        let mut any = false;
        while let Some(batch) = self.next()? {
            self.builder
                .as_mut()
                .expect("reader owns its builder")
                .push_arrow(batch)?;
            any = true;
        }
        let mut builder = self.builder.take().expect("reader owns its builder");
        if !any {
            builder.push_arrow(RecordBatch::new_empty(self.schema()))?;
        }
        builder.finish()
    }

    fn close(&mut self) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.objects = None;
        self.conversion = None;
        Python::attach(|py| {
            drop(self.arrow.take());
            if let Some(iterator) = self.iterator.take() {
                let iterator = iterator.bind(py);
                if iterator.hasattr("close")? {
                    iterator.call_method0("close")?;
                }
            }
            Ok(())
        })
    }
}

impl Drop for BatchReader<'_> {
    fn drop(&mut self) {
        // End-of-input closes before commit. On failure/cancellation, cleanup
        // must preserve the producer, native or consumer error already returned.
        let _ = self.close();
    }
}

pub(super) fn map_failure(errors: &InputErrors, error: ShardLoomError) -> PyErr {
    errors
        .lock()
        .ok()
        .and_then(|mut first| first.take())
        .unwrap_or_else(|| native_error(error))
}
