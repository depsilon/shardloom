//! Immutable owned-array input to the existing native aggregate scanner.
//! No file bytes, footer, aggregate state or query answers are constructed here.

use crate::{
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    local_primitives::prepared_aggregate::PreparedVortexAggregate,
    memory_file_generation::{MemoryFileCompositionBounds, composition_array},
    resident_session::{NativeExecutionContext, OwnedVortexResultBatch, ResidentVortexSession},
};
use shardloom_core::{DatasetUri, NativeIoCertificate, Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, MemoryLease},
};
use std::{
    any::Any,
    collections::HashMap,
    fmt::Write as _,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use vortex::{
    array::{
        ArrayRef, MaskFuture, VortexSessionExecute as _,
        aggregate_fn::{
            AggregateFnVTableExt as _, EmptyOptions, NumericalAggregateOpts,
            fns::{max::Max, min::Min, nan_count::NanCount, null_count::NullCount},
        },
        arrays::{StructArray, struct_::StructArrayExt as _},
        builtins::ArrayBuiltins as _,
        dtype::{DType, FieldMask, FieldName, Nullability, PType},
        scalar::Scalar,
        scalar_fn::fns::{
            binary::Binary, get_item::GetItem, is_not_null::IsNotNull, is_null::IsNull,
            literal::Literal, not::Not, operators::Operator,
        },
    },
    buffer::BitBufferMut,
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

#[derive(Clone, Debug)]
struct SplitZoneStats {
    row_count: u64,
    null_count: u64,
    nan_count: u64,
    min: Option<Scalar>,
    max: Option<Scalar>,
}

#[derive(Default)]
struct ZoneIndex {
    columns: HashMap<FieldName, Vec<SplitZoneStats>>,
    split_count: usize,
}

impl ZoneIndex {
    fn build(
        array: &ArrayRef,
        row_count: u64,
        max_metadata_bytes: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<(Self, MemoryLease)> {
        if row_count == 0 {
            return Ok((Self::default(), context.memory().reserve(0)?));
        }
        let DType::Struct(fields, Nullability::NonNullable) = array.dtype() else {
            return Ok((Self::default(), context.memory().reserve(0)?));
        };
        let total_rows =
            usize::try_from(row_count).map_err(|_| failed("owned row count overflow"))?;
        let split_rows = usize::try_from(SPLIT_ROWS).map_err(|_| failed("split size overflow"))?;
        let split_count = total_rows.div_ceil(split_rows);
        let mut columns: HashMap<FieldName, Vec<SplitZoneStats>> = HashMap::new();
        for (name, dtype) in fields.names().iter().zip(fields.fields()) {
            if is_prunable_zone_dtype(&dtype) {
                columns.insert(name.clone(), Vec::with_capacity(split_count));
            }
        }
        if columns.is_empty() {
            return Ok((Self::default(), context.memory().reserve(0)?));
        }
        let estimated_bytes = u64::try_from(split_count)
            .ok()
            .and_then(|s| s.checked_mul(u64::try_from(columns.len()).ok()?))
            .and_then(|n| n.checked_mul(128))
            .and_then(|b| b.checked_add(256))
            .ok_or_else(|| failed("zone index metadata size overflow"))?;
        if estimated_bytes > max_metadata_bytes {
            return Err(failed("owned array zone-map metadata bound exceeded"));
        }
        let mut lease = context.memory().reserve(estimated_bytes)?;
        populate_zone_splits(
            array,
            fields,
            total_rows,
            split_rows,
            split_count,
            max_metadata_bytes,
            &mut columns,
            &mut lease,
            context,
        )?;
        Ok((
            Self {
                columns,
                split_count,
            },
            lease,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn populate_zone_splits(
    array: &ArrayRef,
    fields: &vortex::array::dtype::StructFields,
    total_rows: usize,
    split_rows: usize,
    split_count: usize,
    max_metadata_bytes: u64,
    columns: &mut HashMap<FieldName, Vec<SplitZoneStats>>,
    lease: &mut MemoryLease,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let null_fn = NullCount.bind(EmptyOptions);
    let nan_fn = NanCount.bind(EmptyOptions);
    let min_fn = Min.bind(NumericalAggregateOpts::skip_nans());
    let max_fn = Max.bind(NumericalAggregateOpts::skip_nans());
    let mut exec_ctx = context.native_session().create_execution_ctx();
    let mut scalar_bytes = lease.bytes();

    for split_idx in 0..split_count {
        context.check_cancelled()?;
        let start = split_idx * split_rows;
        let end = (start + split_rows).min(total_rows);
        let slice_len = u64::try_from(end - start).map_err(|_| failed("split length overflow"))?;
        let chunk = array
            .slice(start..end)
            .map_err(|err| failed(&err.to_string()))?
            .execute::<StructArray>(&mut exec_ctx)
            .map_err(|err| failed(&err.to_string()))?;

        for ((name, dtype), col) in fields
            .names()
            .iter()
            .zip(fields.fields())
            .zip(chunk.iter_unmasked_fields())
        {
            let Some(entry) = columns.get_mut(name) else {
                continue;
            };
            let null_count = aggregate_u64(col, &null_fn, &mut exec_ctx).unwrap_or(0);
            let nan_count = if matches!(
                dtype,
                DType::Primitive(PType::F16 | PType::F32 | PType::F64, _)
            ) {
                aggregate_u64(col, &nan_fn, &mut exec_ctx).unwrap_or(0)
            } else {
                0
            };
            let (min, min_bytes) = if null_count == slice_len {
                (None, 0)
            } else {
                bounded_stat_scalar(col, &min_fn, &mut exec_ctx)
            };
            let (max, max_bytes) = if null_count == slice_len {
                (None, 0)
            } else {
                bounded_stat_scalar(col, &max_fn, &mut exec_ctx)
            };
            let extra = min_bytes
                .checked_add(max_bytes)
                .ok_or_else(|| failed("zone index metadata size overflow"))?;
            let next_bytes = scalar_bytes
                .checked_add(extra)
                .ok_or_else(|| failed("zone index metadata size overflow"))?;
            if next_bytes > max_metadata_bytes {
                return Err(failed("owned array zone-map metadata bound exceeded"));
            }
            if extra > 0 {
                lease.resize(next_bytes)?;
                scalar_bytes = next_bytes;
            }
            entry.push(SplitZoneStats {
                row_count: slice_len,
                null_count,
                nan_count,
                min,
                max,
            });
        }
    }
    Ok(())
}

fn is_prunable_zone_dtype(dtype: &DType) -> bool {
    !matches!(
        dtype,
        DType::Struct(..)
            | DType::List(..)
            | DType::FixedSizeList(..)
            | DType::Variant(..)
            | DType::Null
    )
}

fn aggregate_u64(
    array: &ArrayRef,
    aggregate_fn: &vortex::array::aggregate_fn::AggregateFnRef,
    ctx: &mut vortex::array::ExecutionCtx,
) -> Option<u64> {
    let mut acc = aggregate_fn.accumulator(array.dtype()).ok()?;
    acc.accumulate(array, ctx).ok()?;
    let scalar = acc.partial_scalar().ok()?;
    scalar.as_primitive_opt().and_then(|p| p.as_::<u64>())
}

fn bounded_stat_scalar(
    array: &ArrayRef,
    aggregate_fn: &vortex::array::aggregate_fn::AggregateFnRef,
    ctx: &mut vortex::array::ExecutionCtx,
) -> (Option<Scalar>, u64) {
    let Ok(mut acc) = aggregate_fn.accumulator(array.dtype()) else {
        return (None, 0);
    };
    if acc.accumulate(array, ctx).is_err() {
        return (None, 0);
    }
    let Ok(scalar) = acc.partial_scalar() else {
        return (None, 0);
    };
    if scalar.is_null() {
        return (None, 0);
    }
    if let Some(len) = scalar.as_utf8_opt().and_then(|s| s.len()) {
        if len > 256 {
            return (None, 0);
        }
        return (Some(scalar), len as u64);
    }
    if let Some(len) = scalar.as_binary_opt().and_then(|b| b.len()) {
        if len > 256 {
            return (None, 0);
        }
        return (Some(scalar), len as u64);
    }
    (Some(scalar), 0)
}

struct Owner {
    array: Budgeted<ArrayRef>,
    result: OwnedVortexResultBatch,
    uri: DatasetUri,
    zone_index: ZoneIndex,
    _zone_lease: MemoryLease,
    pruned_splits: AtomicU64,
    pruned_rows: AtomicU64,
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
        let remaining_metadata_bytes = bounds
            .max_metadata_bytes
            .saturating_sub(array.reserved_bytes());
        let id = NEXT_SOURCE
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| failed("source identity exhausted"))?;
        let uri = DatasetUri::new(format!(
            "memory://shardloom/{}/owned-array/{id}.vortex",
            std::process::id()
        ))?;
        let (zone_index, zone_lease) = ZoneIndex::build(
            array.value(),
            result.row_count(),
            remaining_metadata_bytes,
            context,
        )?;
        context.check_cancelled()?;
        Ok(Self(Arc::new(Owner {
            array,
            result,
            uri,
            zone_index,
            _zone_lease: zone_lease,
            pruned_splits: AtomicU64::new(0),
            pruned_rows: AtomicU64::new(0),
        })))
    }

    #[must_use]
    pub fn pruned_splits(&self) -> u64 {
        self.0.pruned_splits.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn pruned_rows(&self) -> u64 {
        self.0.pruned_rows.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn has_zone_maps(&self) -> bool {
        !self.0.zone_index.columns.is_empty() && self.0.zone_index.split_count > 0
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
        self.scan_with_stats(session).0
    }

    pub(crate) fn scan_with_stats(
        &self,
        session: &VortexSession,
    ) -> (ScanBuilder<ArrayRef>, Arc<OwnedScanPruningStats>) {
        let scan_stats = Arc::new(OwnedScanPruningStats::new(self.row_count()));
        let scan = ScanBuilder::new(
            session.clone(),
            Arc::new(Reader {
                owner: self.clone(),
                session: session.clone(),
                name: Arc::from("owned_array"),
                scan_stats: Arc::clone(&scan_stats),
            }),
        );
        (scan, scan_stats)
    }

    pub(crate) fn annotate(&self, certificate: &mut NativeIoCertificate) -> Result<()> {
        let source = &mut certificate.source_capability_report;
        source.source_kind = "owned_vortex_arrays".into();
        source.adapter_id = "shardloom.resident_vortex.owned_array.v1".into();
        source.schema_discovery_status = "validated_owned_native_arrays".into();
        source.statistics_availability = if self.has_zone_maps() {
            "exact_owned_row_count;owned_zone_maps_per_8192_row_split".into()
        } else {
            "exact_owned_row_count;file_statistics_absent".into()
        };
        write!(
            certificate.source_pushdown_report.proof_basis,
            ";owned_array_source_uri={};immutable_array_owner_retained=true;source_specific_file_opens=0;construction_array_serializer_calls=0;construction_segment_assembly_bytes_copied=0;construction_footer_serializer_calls=0;no_query_answer_cache=true;owned_zone_maps_built={}",
            self.source_uri().as_str(),
            self.has_zone_maps(),
        )
        .map_err(|error| failed(&error.to_string()))?;
        Ok(())
    }
}

/// Per-scan unique 8,192-row zone exclusion tracker so sub-zone split sizes
/// (`SplitBy::RowCount(<8192)`) and concurrent scans never overcount or mix certificates.
pub(crate) struct OwnedScanPruningStats {
    row_count: u64,
    tracker: Mutex<ScanZoneTracker>,
}

struct ScanZoneTracker {
    pruned_ranges: Vec<Vec<(u64, u64)>>,
    excluded_zones: Vec<bool>,
    excluded_zone_count: u64,
}

impl OwnedScanPruningStats {
    fn new(row_count: u64) -> Self {
        let split_count = usize::try_from(row_count.div_ceil(SPLIT_ROWS)).unwrap_or(0);
        Self {
            row_count,
            tracker: Mutex::new(ScanZoneTracker {
                pruned_ranges: vec![Vec::new(); split_count],
                excluded_zones: vec![false; split_count],
                excluded_zone_count: 0,
            }),
        }
    }

    pub(crate) fn excluded_zones(&self) -> u64 {
        self.tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .excluded_zone_count
    }

    fn record_pruned_zone_slice(&self, zone_idx: usize, z_start: u64, z_end: u64) -> (bool, u64) {
        let zone_lo = (zone_idx as u64) * SPLIT_ROWS;
        let zone_hi = ((zone_idx as u64 + 1) * SPLIT_ROWS).min(self.row_count);
        let start = z_start.max(zone_lo);
        let end = z_end.min(zone_hi);
        if start >= end {
            return (false, 0);
        }
        let mut tracker = self
            .tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(already) = tracker.excluded_zones.get(zone_idx).copied() else {
            return (false, 0);
        };
        if already {
            return (false, 0);
        }
        let ranges = &mut tracker.pruned_ranges[zone_idx];
        let prev_covered: u64 = ranges.iter().map(|(lo, hi)| hi - lo).sum();
        ranges.push((start, end));
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for &(lo, hi) in ranges.iter() {
            if let Some(last) = merged.last_mut()
                && lo <= last.1
            {
                last.1 = last.1.max(hi);
                continue;
            }
            merged.push((lo, hi));
        }
        *ranges = merged;
        let new_covered: u64 = ranges.iter().map(|(lo, hi)| hi - lo).sum();
        let delta_rows = new_covered.saturating_sub(prev_covered);
        let newly_excluded = ranges.len() == 1 && ranges[0] == (zone_lo, zone_hi);
        if newly_excluded {
            tracker.excluded_zones[zone_idx] = true;
            tracker.excluded_zone_count += 1;
        }
        (newly_excluded, delta_rows)
    }
}

struct Reader {
    owner: OwnedArraySource,
    session: VortexSession,
    name: Arc<str>,
    scan_stats: Arc<OwnedScanPruningStats>,
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
        range: &Range<u64>,
        expr: &BoundExpression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        if self.owner.dtype().is_nullable()
            || range.is_empty()
            || mask.all_false()
            || !self.owner.has_zone_maps()
        {
            return Ok(MaskFuture::ready(mask));
        }
        let zones = &self.owner.0.zone_index;
        let zone_start =
            usize::try_from(range.start / SPLIT_ROWS).map_err(|e| vortex_err!("{e}"))?;
        let zone_end =
            usize::try_from(range.end.div_ceil(SPLIT_ROWS)).map_err(|e| vortex_err!("{e}"))?;
        if zone_end > zones.split_count {
            return Ok(MaskFuture::ready(mask));
        }
        let mut builder = BitBufferMut::with_capacity(mask.len());
        for zone_idx in zone_start..zone_end {
            let z_start = ((zone_idx as u64) * SPLIT_ROWS).max(range.start);
            let z_end = (((zone_idx as u64) + 1) * SPLIT_ROWS).min(range.end);
            let z_len = z_end.saturating_sub(z_start);
            let z_len_usize = usize::try_from(z_len).map_err(|e| vortex_err!("{e}"))?;
            let pruned = can_prune_split(expr, zone_idx, zones);
            builder.append_n(!pruned, z_len_usize);
            if pruned {
                let (newly_excluded, delta_rows) = self
                    .scan_stats
                    .record_pruned_zone_slice(zone_idx, z_start, z_end);
                if newly_excluded {
                    self.owner.0.pruned_splits.fetch_add(1, Ordering::Relaxed);
                }
                if delta_rows > 0 {
                    self.owner
                        .0
                        .pruned_rows
                        .fetch_add(delta_rows, Ordering::Relaxed);
                }
            }
        }
        let zone_mask = Mask::from(builder.freeze());
        Ok(MaskFuture::ready(std::ops::BitAnd::bitand(
            mask, &zone_mask,
        )))
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
                .fill_null(false)?
                .execute::<Mask>(&mut session.create_execution_ctx())?;
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

fn root_item_name(expr: &BoundExpression) -> Option<&FieldName> {
    let field = expr.as_opt::<GetItem>()?;
    expr.children()
        .first()
        .is_some_and(BoundExpression::is_root)
        .then_some(field)
}

fn flip_cmp(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::Gte => Operator::Lte,
        Operator::Lt => Operator::Gt,
        Operator::Lte => Operator::Gte,
        other => other,
    }
}

fn can_prune_split(expr: &BoundExpression, split_idx: usize, zones: &ZoneIndex) -> bool {
    if let Some(scalar) = expr.as_opt::<Literal>() {
        return !scalar
            .as_bool_opt()
            .and_then(|value| value.value())
            .unwrap_or(false);
    }
    if let Some(&operator) = expr.as_opt::<Binary>() {
        let children = expr.children();
        if children.len() != 2 {
            return false;
        }
        match operator {
            Operator::And => {
                return can_prune_split(&children[0], split_idx, zones)
                    || can_prune_split(&children[1], split_idx, zones);
            }
            Operator::Or => {
                return can_prune_split(&children[0], split_idx, zones)
                    && can_prune_split(&children[1], split_idx, zones);
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::Gte
            | Operator::Lt
            | Operator::Lte => {
                return can_prune_comparison(
                    operator,
                    &children[0],
                    &children[1],
                    split_idx,
                    zones,
                );
            }
            _ => return false,
        }
    }
    if expr.as_opt::<IsNull>().is_some()
        && let Some(col) = expr.children().first().and_then(root_item_name)
    {
        return zones
            .columns
            .get(col)
            .and_then(|splits| splits.get(split_idx))
            .is_some_and(|stats| stats.null_count == 0);
    }
    if expr.as_opt::<IsNotNull>().is_some()
        && let Some(col) = expr.children().first().and_then(root_item_name)
    {
        return zones
            .columns
            .get(col)
            .and_then(|splits| splits.get(split_idx))
            .is_some_and(|stats| stats.null_count == stats.row_count);
    }
    if expr.as_opt::<Not>().is_some()
        && let Some(inner) = expr.children().first()
        && inner.as_opt::<IsNull>().is_some()
        && let Some(col) = inner.children().first().and_then(root_item_name)
    {
        return zones
            .columns
            .get(col)
            .and_then(|splits| splits.get(split_idx))
            .is_some_and(|stats| stats.null_count == stats.row_count);
    }
    false
}

fn can_prune_comparison(
    operator: Operator,
    left: &BoundExpression,
    right: &BoundExpression,
    split_idx: usize,
    zones: &ZoneIndex,
) -> bool {
    let (col, cmp, lit) =
        if let (Some(col), Some(lit)) = (root_item_name(left), right.as_opt::<Literal>()) {
            (col, operator, lit)
        } else if let (Some(lit), Some(col)) = (left.as_opt::<Literal>(), root_item_name(right)) {
            (col, flip_cmp(operator), lit)
        } else {
            return false;
        };
    if lit.is_null() {
        return true;
    }
    let Some(stats) = zones
        .columns
        .get(col)
        .and_then(|splits| splits.get(split_idx))
    else {
        return false;
    };
    if stats.null_count == stats.row_count {
        return true;
    }
    if lit.as_primitive_opt().is_some_and(|p| p.is_nan()) || stats.nan_count > 0 {
        return false;
    }
    let (Some(min), Some(max)) = (&stats.min, &stats.max) else {
        return false;
    };
    let (Some(min_cmp), Some(max_cmp)) = (min.partial_cmp(lit), max.partial_cmp(lit)) else {
        return false;
    };
    match cmp {
        Operator::Eq => min_cmp.is_gt() || max_cmp.is_lt(),
        Operator::NotEq => min_cmp.is_eq() && max_cmp.is_eq(),
        Operator::Gt => max_cmp.is_le(),
        Operator::Gte => max_cmp.is_lt(),
        Operator::Lt => min_cmp.is_ge(),
        Operator::Lte => min_cmp.is_gt(),
        _ => false,
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("SL-VORTEX-OWNED-ARRAY: {reason}"))
}

#[cfg(test)]
#[path = "owned_array_source_tests.rs"]
mod tests;
