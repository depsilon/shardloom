//! Query-local replay uses credited native payloads and the existing run store.

use super::{
    ArrayRef, ExecutedVortexBatchInput, Metrics, NativeExecutionContext, ReservedVec, Result,
    checked_add, failed,
};
use crate::local_primitives::{native_payload, vortex_error};
#[cfg(feature = "vortex-write")]
use crate::local_primitives::{native_relational_sort::Spec, native_relational_spill};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::dtype::DType;

const COPY_ROWS: usize = 1024;

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
        #[cfg(not(feature = "vortex-write"))]
        let _ = (schema, metrics);
        Ok(Self {
            resident: ReservedVec::new(context.memory())?,
            #[cfg(feature = "vortex-write")]
            ordering: metrics
                .spill
                .as_ref()
                .map(|state| {
                    native_relational_spill::Ordering::new(&schema.spec, state, COPY_ROWS, context)
                })
                .transpose()?,
            _scope: std::marker::PhantomData,
        })
    }

    pub(super) fn build(
        &mut self,
        array: &ArrayRef,
        report: &mut ExecutedVortexBatchInput,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        // Copy the complete schema before any scan projection/filter. Bounded
        // slices limit transient gather work even for large Arrow input batches.
        for start in (0..array.len()).step_by(COPY_ROWS) {
            context.check_cancelled()?;
            let end = start.saturating_add(COPY_ROWS).min(array.len());
            let compact = native_payload::detach_with_policy(
                &array.slice(start..end).map_err(vortex_error)?,
                native_payload::CopyPolicy::PreserveUnobserved,
                context,
            )?;
            checked_add(&mut report.replay_batches_detached, 1)?;
            checked_add(&mut report.replay_rows_detached, compact.len() as u64)?;
            checked_add(&mut report.replay_buffer_bytes_copied, compact.nbytes())?;
            #[cfg(feature = "vortex-write")]
            if let Some(ordering) = &mut self.ordering {
                ordering.build(compact, context)?;
                continue;
            }
            self.resident.push(compact)?;
        }
        Ok(())
    }

    pub(super) fn finish(self, context: &NativeExecutionContext<'_>) -> Result<Completed> {
        context.check_cancelled()?;
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
