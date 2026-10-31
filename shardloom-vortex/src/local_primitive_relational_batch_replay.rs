//! Query-local replay uses credited native payloads and the existing run store.

use super::batch_input::failed;
use super::{ArrayRef, Metrics, NativeExecutionContext, ReservedVec, Result};
use crate::local_primitives::{native_payload, vortex_error};
#[cfg(feature = "vortex-write")]
use crate::local_primitives::{native_relational_sort::Spec, native_relational_spill};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::dtype::DType;

const COPY_ROWS: usize = 1024;

#[path = "local_primitive_relational_replay_capture.rs"]
mod capture;

#[cfg(test)]
#[path = "local_primitive_relational_replay_capture_tests.rs"]
mod capture_tests;

#[derive(Default)]
pub(super) struct CopyStats {
    pub(super) batches: u64,
    pub(super) rows: u64,
    pub(super) buffer_bytes: u64,
    pub(super) max_batch_rows: usize,
}

fn checked_add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| failed("native replay counter overflow"))?;
    Ok(())
}

pub(super) struct Schema {
    #[cfg(feature = "vortex-write")]
    spec: Spec,
    _metadata: MemoryLease,
}

impl Schema {
    pub(super) fn new(dtype: &DType, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let metadata = crate::native_payload_schema::reserve_schema(dtype, context.memory())?;
        let fields = dtype
            .as_struct_fields_opt()
            .ok_or_else(|| failed("streaming replay requires a native record schema"))?;
        #[cfg(not(feature = "vortex-write"))]
        let _ = fields;
        Ok(Self {
            #[cfg(feature = "vortex-write")]
            spec: Spec {
                fields: fields
                    .names()
                    .iter()
                    .zip(fields.fields())
                    .map(|(name, dtype)| (name.to_string(), dtype))
                    .collect(),
                keys: Vec::new(),
                names: Vec::new(),
                copy_policy: native_payload::CopyPolicy::PreserveUnobserved,
            },
            _metadata: metadata,
        })
    }
}

pub(super) struct Recorder<'p, 's> {
    resident: ReservedVec<ArrayRef>,
    copy_rows: usize,
    capture: Option<capture::Capture>,
    #[cfg(feature = "vortex-write")]
    ordering: Option<native_relational_spill::Ordering<'p, 's>>,
    _scope: std::marker::PhantomData<(&'p (), &'s ())>,
}

pub(super) enum Completed {
    Resident(ReservedVec<ArrayRef>),
    #[cfg(feature = "vortex-write")]
    Stored(native_relational_spill::Replay),
}

impl<'p, 's> Recorder<'p, 's> {
    pub(super) fn new(
        schema: &'p Schema,
        metrics: &'s Metrics<'_>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        // Source replay is recorded before its consumer runs. Its flush and
        // merge payloads therefore overlap the consumer's retained state under
        // the same grant. Keep these blocks small when native spill is enabled;
        // output fanout supplies its own bound through `with_batch_rows`.
        #[cfg(feature = "vortex-write")]
        let copy_rows = if metrics.spill.is_some() {
            64
        } else {
            COPY_ROWS
        };
        #[cfg(not(feature = "vortex-write"))]
        let copy_rows = COPY_ROWS;
        let recorder = Self::with_batch_rows(schema, metrics, context, copy_rows)?;
        #[cfg(feature = "vortex-write")]
        let recorder = {
            let mut recorder = recorder;
            if let Some(ordering) = &mut recorder.ordering {
                // Leave room for the original input, gathered writer blocks,
                // merge readers and downstream retained state in the same pool.
                ordering.limit_buffer(context.memory().snapshot().limit_bytes / 8)?;
            }
            recorder
        };
        Ok(recorder)
    }

    pub(super) fn with_batch_rows(
        schema: &'p Schema,
        metrics: &'s Metrics<'_>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<Self> {
        if batch_rows == 0 {
            return Err(failed("native replay requires a positive batch bound"));
        }
        let copy_rows = batch_rows.min(COPY_ROWS);
        #[cfg(not(feature = "vortex-write"))]
        let _ = (schema, metrics);
        Ok(Self {
            resident: ReservedVec::new(context.memory())?,
            copy_rows,
            capture: None,
            #[cfg(feature = "vortex-write")]
            ordering: metrics
                .spill
                .as_ref()
                .map(|state| {
                    native_relational_spill::Ordering::new(&schema.spec, state, copy_rows, context)
                })
                .transpose()?,
            _scope: std::marker::PhantomData,
        })
    }

    pub(super) fn build(
        &mut self,
        array: &ArrayRef,
        report: &mut CopyStats,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if array.len() < self.copy_rows / 4 && capture::admitted(array) {
            let mut start = 0;
            while start < array.len() {
                if self.capture.is_none() {
                    self.capture = capture::Capture::new(array.dtype(), self.copy_rows, context)?;
                }
                let Some(capture) = &mut self.capture else {
                    break;
                };
                let count = capture.remaining().min(array.len() - start);
                capture.append(array, start, count, report, context)?;
                start += count;
                if capture.remaining() == 0 {
                    self.flush_capture(context)?;
                }
            }
            if start == array.len() {
                return Ok(());
            }
            if start != 0 {
                self.flush_capture(context)?;
                return self.copy(
                    &array.slice(start..array.len()).map_err(vortex_error)?,
                    report,
                    context,
                );
            }
        }
        self.flush_capture(context)?;
        self.copy(array, report, context)
    }

    fn copy(
        &mut self,
        array: &ArrayRef,
        report: &mut CopyStats,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        // Retain only the supplied native relation. Input replay supplies its
        // full source schema; output replay supplies the completed query schema.
        // Bounded slices limit transient gather work for either producer.
        for start in (0..array.len()).step_by(self.copy_rows) {
            context.check_cancelled()?;
            let end = start.saturating_add(self.copy_rows).min(array.len());
            let compact = native_payload::detach_with_policy(
                &array.slice(start..end).map_err(vortex_error)?,
                native_payload::CopyPolicy::PreserveUnobserved,
                context,
            )?;
            checked_add(&mut report.batches, 1)?;
            checked_add(&mut report.rows, compact.len() as u64)?;
            checked_add(&mut report.buffer_bytes, compact.nbytes())?;
            report.max_batch_rows = report.max_batch_rows.max(compact.len());
            self.retain(compact, context)?;
        }
        Ok(())
    }

    fn retain(&mut self, array: ArrayRef, context: &NativeExecutionContext<'_>) -> Result<()> {
        #[cfg(feature = "vortex-write")]
        if let Some(ordering) = &mut self.ordering {
            return ordering.build(array, context);
        }
        context.check_cancelled()?;
        self.resident.push(array)
    }

    fn flush_capture(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if let Some(capture) = self.capture.take() {
            self.retain(capture.finish(context)?, context)?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self, context: &NativeExecutionContext<'_>) -> Result<Completed> {
        context.check_cancelled()?;
        self.flush_capture(context)?;
        #[cfg(feature = "vortex-write")]
        if let Some(ordering) = self.ordering {
            return Ok(Completed::Stored(
                ordering.retain(context)?.into_replay(context)?,
            ));
        }
        Ok(Completed::Resident(self.resident))
    }
}

impl Completed {
    /// Deliver the retained relation to an admitted sink, including typed empties.
    #[cfg(feature = "vortex-write")]
    pub(super) fn produce(
        &self,
        dtype: &DType,
        metrics: &Metrics<'_>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        rows: u64,
        consume: &mut dyn FnMut(ArrayRef) -> Result<bool>,
    ) -> Result<()> {
        if batch_rows == 0 {
            return Err(failed("invalid output replay batch bound"));
        }
        let mut submitted = 0_u64;
        self.run(dtype, metrics, context, &mut |array| {
            for start in (0..array.len()).step_by(batch_rows) {
                let end = start.saturating_add(batch_rows).min(array.len());
                let batch = array.slice(start..end).map_err(vortex_error)?;
                checked_add(&mut submitted, batch.len() as u64)?;
                if !consume(batch)? {
                    return Err(failed("output replay consumer stopped before completion"));
                }
            }
            Ok(())
        })?;
        if submitted != rows {
            return Err(failed("output replay row count changed"));
        }
        if submitted == 0 && !consume(native_payload::empty_record(dtype, context)?)? {
            return Err(failed("output replay consumer stopped before completion"));
        }
        context.check_cancelled()
    }

    pub(super) fn is_spilled(&self) -> bool {
        match self {
            Self::Resident(_) => false,
            #[cfg(feature = "vortex-write")]
            Self::Stored(stored) => stored.is_spilled(),
        }
    }

    pub(super) fn run(
        &self,
        dtype: &DType,
        metrics: &Metrics<'_>,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(&ArrayRef) -> Result<()>,
    ) -> Result<()> {
        #[cfg(not(feature = "vortex-write"))]
        let _ = (dtype, metrics);
        match self {
            Self::Resident(arrays) => {
                for array in &arrays.values {
                    context.check_cancelled()?;
                    consume(array)?;
                }
            }
            #[cfg(feature = "vortex-write")]
            Self::Stored(stored) => {
                let schema = Schema::new(dtype, context)?;
                let state = metrics
                    .spill
                    .as_ref()
                    .ok_or_else(|| failed("native replay store is absent"))?;
                stored.for_each(state, &schema.spec, context, consume)?;
            }
        }
        context.check_cancelled()
    }
}
