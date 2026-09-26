//! Immutable owned-array input to the existing native aggregate scanner.
//! No file bytes, footer, aggregate state or query answers are constructed here.

use crate::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::prepared_aggregate::PreparedVortexAggregate,
    memory_file_generation::{MemoryFileCompositionBounds, composition_array},
    resident_session::{NativeExecutionContext, OwnedVortexResultBatch, ResidentVortexSession},
};
use shardloom_core::{DatasetUri, NativeIoCertificate, Result, ShardLoomError};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::Budgeted};
use std::{
    any::Any,
    fmt::Write as _,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use vortex::{
    array::{
        ArrayRef, MaskFuture, VortexSessionExecute as _,
        dtype::{DType, FieldMask},
    },
    error::{VortexResult, vortex_err},
    expr::BoundExpression,
    layout::{ArrayFuture, LayoutReader, RowSplits, SplitRange, scan::scan_builder::ScanBuilder},
    mask::Mask,
    session::VortexSession,
};

/// Intake bounds for retained arrays. Logical bytes are not total process RSS;
/// original buffer reservations remain attached to their native owners.
#[derive(Clone, Copy, Debug)]
pub struct OwnedArraySourceBounds {
    pub max_rows: u64,
    pub max_columns: usize,
    pub max_batches: usize,
    pub max_logical_bytes: u64,
    pub max_metadata_bytes: u64,
}

impl Default for OwnedArraySourceBounds {
    fn default() -> Self {
        Self {
            max_rows: 1_048_576,
            max_columns: 1024,
            max_batches: 4096,
            max_logical_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 1024 * 1024,
        }
    }
}

struct Owner {
    array: Budgeted<ArrayRef>,
    result: OwnedVortexResultBatch,
    uri: DatasetUri,
}

/// A source retaining completed native arrays and their session. Clones and
/// prepared consumers share immutable buffers, never aggregate state.
#[derive(Clone)]
pub struct OwnedArraySource(Arc<Owner>);

static NEXT_SOURCE: AtomicU64 = AtomicU64::new(0);
const SPLIT_ROWS: u64 = 8192;

impl OwnedArraySource {
    /// Retain native arrays without serializing an intermediate file.
    /// # Errors
    /// Rejects inconsistent schema/rows, bounds, memory pressure or cancellation.
    pub fn from_owned(
        result: OwnedVortexResultBatch,
        bounds: OwnedArraySourceBounds,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let session = result.retained_session();
        session.with_native_execution_context(cancellation, |context| {
            Self::from_owned_in_context(result, bounds, context)
        })
    }

    pub(crate) fn from_owned_in_context(
        result: OwnedVortexResultBatch,
        bounds: OwnedArraySourceBounds,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        result
            .retained_session()
            .validate_execution_context(context)?;
        context.check_general_execution()?;
        result.validate_schema_and_rows()?;
        if bounds.max_logical_bytes == 0
            || bounds.max_metadata_bytes == 0
            || result.logical_buffer_bytes() > bounds.max_logical_bytes
        {
            return Err(failed(
                "owned result exceeds logical-byte or metadata bounds",
            ));
        }
        let mut composition = MemoryFileCompositionBounds {
            max_rows: bounds.max_rows,
            max_columns: bounds.max_columns,
            max_batches: bounds.max_batches,
            ..MemoryFileCompositionBounds::default()
        };
        composition.storage.max_metadata_bytes = bounds.max_metadata_bytes;
        let array = composition_array(&result, composition, context)?;
        let id = NEXT_SOURCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| failed("source identity exhausted"))?;
        let uri = DatasetUri::new(format!(
            "memory://shardloom/{}/owned-array/{id}.vortex",
            std::process::id()
        ))?;
        context.check_cancelled()?;
        Ok(Self(Arc::new(Owner { array, result, uri })))
    }

    #[must_use]
    pub fn source_uri(&self) -> &DatasetUri {
        &self.0.uri
    }
    #[must_use]
    pub fn dtype(&self) -> &DType {
        self.0.array.value().dtype()
    }
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.0.result.row_count()
    }
    pub(crate) fn session(&self) -> ResidentVortexSession {
        self.0.result.retained_session()
    }

    /// Prepare the same native aggregate family used by file-backed inputs.
    /// # Errors
    /// Rejects mismatched source identity, unsupported schemas/policies, and
    /// explicit spill (which requires the existing file-backed source contract).
    pub fn prepare_aggregate(
        &self,
        request: &VortexQueryPrimitiveRequest,
        policy: VortexLocalPrimitiveExecutionPolicy,
    ) -> Result<PreparedVortexAggregate> {
        crate::local_primitives::prepared_aggregate::prepare_owned_aggregate(request, policy, self)
    }

    pub(crate) fn scan(&self, session: &VortexSession) -> ScanBuilder<ArrayRef> {
        ScanBuilder::new(
            session.clone(),
            Arc::new(Reader {
                owner: self.clone(),
                session: session.clone(),
                name: Arc::from("owned_array"),
            }),
        )
    }

    pub(crate) fn annotate(&self, certificate: &mut NativeIoCertificate) -> Result<()> {
        let source = &mut certificate.source_capability_report;
        source.source_kind = "owned_vortex_arrays".into();
        source.adapter_id = "shardloom.resident_vortex.owned_array.v1".into();
        source.schema_discovery_status = "validated_owned_native_arrays".into();
        source.statistics_availability = "exact_owned_row_count;file_statistics_absent".into();
        write!(certificate.source_pushdown_report.proof_basis,
            ";owned_array_source_uri={};immutable_array_owner_retained=true;source_specific_file_opens=0;construction_array_serializer_calls=0;construction_segment_assembly_bytes_copied=0;construction_footer_serializer_calls=0;no_query_answer_cache=true",
            self.source_uri().as_str()).map_err(|error| failed(&error.to_string()))?;
        Ok(())
    }
}

struct Reader {
    owner: OwnedArraySource,
    session: VortexSession,
    name: Arc<str>,
}

impl Reader {
    fn slice(&self, range: &Range<u64>) -> VortexResult<ArrayRef> {
        if range.start > range.end || range.end > self.row_count() {
            return Err(vortex_err!("owned array scan range exceeds source"));
        }
        self.owner.0.array.value().slice(
            usize::try_from(range.start).map_err(|error| vortex_err!("{error}"))?
                ..usize::try_from(range.end).map_err(|error| vortex_err!("{error}"))?,
        )
    }
}

impl LayoutReader for Reader {
    fn name(&self) -> &Arc<str> {
        &self.name
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn dtype(&self) -> &DType {
        self.owner.dtype()
    }
    fn row_count(&self) -> u64 {
        self.owner.row_count()
    }
    fn register_splits(
        &self,
        _fields: &[FieldMask],
        range: &SplitRange,
        splits: &mut RowSplits,
    ) -> VortexResult<()> {
        range.check_bounds(self.row_count())?;
        let mut row = range.row_range().start;
        while row < range.row_range().end {
            row = row.saturating_add(SPLIT_ROWS).min(range.row_range().end);
            splits.push(range.row_offset() + row);
        }
        Ok(())
    }
    fn pruning_evaluation(
        &self,
        _range: &Range<u64>,
        _expr: &BoundExpression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        Ok(MaskFuture::ready(mask))
    }

    fn filter_evaluation(
        &self,
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        let array = self.slice(range)?;
        let expr = expr.clone();
        let session = self.session.clone();
        // The future retains metadata and producer leases as well as buffer Arcs.
        let owner = self.owner.clone();
        Ok(MaskFuture::new(mask.len(), async move {
            let mask = mask.await?;
            let selected = array.filter(mask.clone())?.apply_bound(&expr)?;
            let evaluated = selected
                .null_as_false()
                .execute(&mut session.create_execution_ctx())?;
            let result = mask.intersect_by_rank(&evaluated);
            drop(owner);
            Ok(result)
        }))
    }

    fn projection_evaluation(
        &self,
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<ArrayFuture> {
        let array = self.slice(range)?;
        let expr = expr.clone();
        let owner = self.owner.clone();
        Ok(Box::pin(async move {
            let mask = mask.await?;
            let selected = if mask.all_true() {
                array
            } else {
                array.filter(mask)?
            };
            let projected = selected.apply_bound(&expr)?;
            drop(owner);
            Ok(projected)
        }))
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("SL-VORTEX-OWNED-ARRAY: {reason}"))
}

#[cfg(test)]
#[path = "owned_array_source_tests.rs"]
mod tests;
