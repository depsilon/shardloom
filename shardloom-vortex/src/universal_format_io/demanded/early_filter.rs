//! Two-stage key-first Parquet filtering for runtime join filters.
//!
//! Stage 1 decodes only key/filter columns for a row group, validates their
//! native intake, and evaluates runtime key predicates plus Bloom filters.
//! Zero-survivor row groups skip payload column chunks completely; positive
//! survivors use `RowSelection` on infallible payload-only columns and compose
//! filtered native keys with surviving payload batches under ordinary credit.

use super::{
    ArrowReaderMetadata, BatchFilterSatisfaction, ColumnarReader, NativeExecutionContext,
    PreparedColumnarSource, ResidentMemorySource, ResidentVortexSession, Result, SourceGroup,
    batch_work, chunks, failed, predicate_columns, source::PhysicalParquetFile,
};
use crate::{
    local_primitives::{
        native_relational_batch::{index_array, take_column},
        native_relational_probe_filter::{
            ProbeRuntimeFilter, apply_bloom_mask_selected, is_infallible_key_dtype,
        },
        predicate_to_vortex_expr,
    },
    query_primitive::VortexQueryPrimitiveKind,
    vortex_ingest::arrow_ownership::ArrowOwnershipPolicy,
};
use arrow_schema::DataType;
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::metadata::RowGroupMetaData,
};
use shardloom_core::{FloatPolicy, PredicateExpr};
use std::{
    collections::{BTreeSet, VecDeque},
    ops::{BitAnd as _, Range},
    sync::Arc,
};
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            BoolArray, ChunkedArray, ConstantArray, Struct, bool::BoolArrayExt as _,
            struct_::StructArrayExt as _,
        },
        dtype::{DType, Nullability, PType},
        scalar::Scalar,
    },
    mask::Mask,
};

#[derive(Clone)]
pub(super) enum ColumnSlot {
    Key(usize),
    Payload(usize),
    PartitionValue(Option<String>),
    MissingNull(DType),
}

pub(super) struct EarlyFilterPlan {
    pub(super) key_indices: Vec<usize>,
    pub(super) payload_indices: Vec<usize>,
    key_count: usize,
    slots: Vec<ColumnSlot>,
    projected_names: Vec<String>,
    mandatory_predicate_evaluated: bool,
    all_probe_filters_evaluated: bool,
}

pub(super) struct StagedKeyStream {
    chunks: VecDeque<(usize, Vec<ArrayRef>)>,
    offset: usize,
    key_count: usize,
    slots: Vec<ColumnSlot>,
    projected_names: Vec<String>,
    pub(super) filter_satisfaction: BatchFilterSatisfaction,
}

pub(super) struct Stage1Result {
    pub(super) survivors: u64,
    pub(super) surviving_ranges: Arc<[Range<usize>]>,
    pub(super) staged: Option<StagedKeyStream>,
}

fn is_infallible_arrow_key(field: &arrow_schema::Field) -> bool {
    field.metadata().is_empty()
        && matches!(
            field.data_type(),
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        )
        || (field.metadata().is_empty()
            && matches!(
                field.data_type(),
                DataType::Dictionary(key, val)
                    if matches!(key.as_ref(), DataType::Int32)
                        && matches!(val.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
            ))
}

fn arrow_key_matches_infallible_vortex(dtype: &DType) -> bool {
    is_infallible_key_dtype(dtype)
}

fn is_infallible_payload_field(field: &arrow_schema::Field, float_policy: FloatPolicy) -> bool {
    if !field.metadata().is_empty()
        || !crate::native_payload_schema::arrow_field_metadata_admitted(field)
    {
        return false;
    }
    match field.data_type() {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        DataType::Dictionary(key, val)
            if matches!(key.as_ref(), DataType::Int32)
                && matches!(val.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            true
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            float_policy == FloatPolicy::Extended
        }
        _ => false,
    }
}

fn missing_null_vortex_dtype(dtype: &DataType) -> Option<DType> {
    let n = Nullability::Nullable;
    Some(match dtype {
        DataType::Boolean => DType::Bool(n),
        DataType::Int8 => DType::Primitive(PType::I8, n),
        DataType::Int16 => DType::Primitive(PType::I16, n),
        DataType::Int32 => DType::Primitive(PType::I32, n),
        DataType::Int64 => DType::Primitive(PType::I64, n),
        DataType::UInt8 => DType::Primitive(PType::U8, n),
        DataType::UInt16 => DType::Primitive(PType::U16, n),
        DataType::UInt32 => DType::Primitive(PType::U32, n),
        DataType::UInt64 => DType::Primitive(PType::U64, n),
        DataType::Float16 => DType::Primitive(PType::F16, n),
        DataType::Float32 => DType::Primitive(PType::F32, n),
        DataType::Float64 => DType::Primitive(PType::F64, n),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DType::Utf8(n),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DType::Binary(n),
        _ => return None,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn plan(
    prepared: &PreparedColumnarSource,
    file: PhysicalParquetFile<'_>,
    group_rows: u64,
    mandatory_predicate: Option<(&PredicateExpr, bool)>,
    runtime_predicate: Option<&PredicateExpr>,
    probe_filters: &[ProbeRuntimeFilter],
    residual_present: bool,
    float_policy: FloatPolicy,
) -> Option<EarlyFilterPlan> {
    enum RawSlot {
        KeyPhys(usize),
        PayloadPhys(usize),
        PartitionValue(Option<String>),
        MissingNull(DType),
    }

    if !prepared.is_parquet()
        || prepared.max_rows.is_some()
        || (mandatory_predicate.is_none()
            && runtime_predicate.is_none()
            && probe_filters.is_empty())
        || (probe_filters.is_empty() && group_rows < 4_096)
    {
        return None;
    }
    let mut key_names = BTreeSet::new();
    if let Some((predicate, _)) = mandatory_predicate {
        predicate_columns(predicate, &mut key_names);
    }
    if let Some(predicate) = runtime_predicate {
        predicate_columns(predicate, &mut key_names);
    }
    for filter in probe_filters {
        if filter.probe_columns.is_empty() {
            return None;
        }
        if filter.empty_build && residual_present {
            return None;
        }
        for column in &filter.probe_columns {
            key_names.insert(column.as_str());
        }
    }
    if key_names.is_empty() {
        return None;
    }
    let logical_schema = prepared.source.schema();
    let physical_schema = file.metadata.schema();
    let decode_schema = file.encoded_metadata.unwrap_or(file.metadata).schema();

    // Wrapped collections can evaluate predicates over virtual/missing columns;
    // require every active Stage-1 key column to exist physically with matching
    // logical type/nullability on this file.
    let mut key_phys_set = BTreeSet::new();
    for name in &key_names {
        let Ok(logical_field) = logical_schema.field_with_name(name) else {
            return None;
        };
        let Ok(phys_idx) = physical_schema.index_of(name) else {
            return None;
        };
        let physical_field = physical_schema.field(phys_idx);
        let decode_field = decode_schema.field(phys_idx);
        if physical_field.data_type() != logical_field.data_type()
            || physical_field.is_nullable() != logical_field.is_nullable()
            || !is_infallible_arrow_key(physical_field)
            || !is_infallible_arrow_key(decode_field)
        {
            return None;
        }
        key_phys_set.insert(phys_idx);
    }

    let mut payload_phys_set = BTreeSet::new();
    let mut raw_slots = Vec::with_capacity(prepared.projection.len());
    let mut projected_names = Vec::with_capacity(prepared.projection.len());
    let mut key_name_set = BTreeSet::new();

    for &col_idx in &prepared.projection {
        let logical_field = logical_schema.field(col_idx);
        let name = logical_field.name();
        if key_names.contains(name.as_str()) {
            let phys_idx = physical_schema.index_of(name).ok()?;
            raw_slots.push(RawSlot::KeyPhys(phys_idx));
            key_name_set.insert(name.as_str());
        } else if let Ok(phys_idx) = physical_schema.index_of(name) {
            let physical_field = physical_schema.field(phys_idx);
            let decode_field = decode_schema.field(phys_idx);
            if physical_field.data_type() != logical_field.data_type()
                || physical_field.is_nullable() != logical_field.is_nullable()
                || !is_infallible_payload_field(physical_field, float_policy)
                || !is_infallible_payload_field(decode_field, float_policy)
            {
                return None;
            }
            payload_phys_set.insert(phys_idx);
            raw_slots.push(RawSlot::PayloadPhys(phys_idx));
        } else if let Some((_, value)) = file.partitions.iter().find(|(part, _)| part == name) {
            raw_slots.push(RawSlot::PartitionValue(value.clone()));
        } else {
            if !logical_field.is_nullable() {
                return None;
            }
            let null_dtype = missing_null_vortex_dtype(logical_field.data_type())?;
            raw_slots.push(RawSlot::MissingNull(null_dtype));
        }
        projected_names.push(name.clone());
    }

    let key_indices: Vec<usize> = key_phys_set.into_iter().collect();
    let payload_indices: Vec<usize> = payload_phys_set.into_iter().collect();
    if key_indices.is_empty() || payload_indices.is_empty() {
        return None;
    }
    let slots = raw_slots
        .into_iter()
        .map(|slot| match slot {
            RawSlot::KeyPhys(phys_idx) => ColumnSlot::Key(
                key_indices
                    .binary_search(&phys_idx)
                    .expect("key phys_idx must exist"),
            ),
            RawSlot::PayloadPhys(phys_idx) => ColumnSlot::Payload(
                payload_indices
                    .binary_search(&phys_idx)
                    .expect("payload phys_idx must exist"),
            ),
            RawSlot::PartitionValue(value) => ColumnSlot::PartitionValue(value),
            RawSlot::MissingNull(dtype) => ColumnSlot::MissingNull(dtype),
        })
        .collect();
    let key_count = key_indices.len();
    let mandatory_predicate_evaluated = mandatory_predicate.is_some_and(|(pred, complete)| {
        let mut referenced = BTreeSet::new();
        predicate_columns(pred, &mut referenced);
        complete && referenced.iter().all(|col| key_name_set.contains(col))
    });
    let mut all_probe_filters_evaluated = true;
    for filter in probe_filters {
        if !filter
            .probe_columns
            .iter()
            .all(|col| key_name_set.contains(col.as_str()))
        {
            return None;
        }
        if (!filter.empty_build || residual_present) && filter.bloom.is_none() {
            all_probe_filters_evaluated = false;
        }
    }
    Some(EarlyFilterPlan {
        key_indices,
        payload_indices,
        key_count,
        slots,
        projected_names,
        mandatory_predicate_evaluated,
        all_probe_filters_evaluated,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn run_stage1(
    reader: &ColumnarReader,
    plan: &EarlyFilterPlan,
    source_group: SourceGroup,
    group_meta: &RowGroupMetaData,
    metadata: &ArrowReaderMetadata,
    selected_chunk_bytes_read: &mut u64,
    session: &ResidentVortexSession,
    context: &NativeExecutionContext<'_>,
) -> Result<Stage1Result> {
    let input = &reader
        .input
        .as_ref()
        .ok_or_else(|| failed("selected file reader is absent during Stage 1 filter"))?
        .1;
    let key_mask =
        ProjectionMask::roots(metadata.parquet_schema(), plan.key_indices.iter().copied());
    let key_workspace = batch_work::parquet_workspace(group_meta, &key_mask)?;
    let _key_credit = context.memory().reserve(key_workspace)?;
    let key_chunks = chunks::SelectedChunks::read(input, group_meta, &key_mask, context.memory())?;
    *selected_chunk_bytes_read = selected_chunk_bytes_read
        .checked_add(key_chunks.total_bytes())
        .ok_or_else(|| failed("selected chunk bytes overflow"))?;

    let key_reader =
        ParquetRecordBatchReaderBuilder::new_with_metadata(key_chunks, metadata.clone())
            .with_projection(key_mask)
            .with_row_groups(vec![source_group.index])
            .with_batch_size(batch_work::MAX_ROWS)
            .build()
            .map_err(failed)?;

    let mut bound_mandatory = None;
    let mut bound_runtime = None;
    let mut exec_ctx = context.native_session().create_execution_ctx();
    let mut survivors: u64 = 0;
    let mut batch_first: usize = 0;
    let mut surviving_ranges: Vec<Range<usize>> = Vec::new();
    let mut key_chunks_deque: VecDeque<(usize, Vec<ArrayRef>)> = VecDeque::new();
    let mut all_filters_satisfied = plan.all_probe_filters_evaluated;

    for maybe_batch in key_reader {
        context.check_cancelled()?;
        let key_batch = maybe_batch.map_err(failed)?;
        let n = key_batch.num_rows();
        if n == 0 {
            continue;
        }
        let key_source = ResidentMemorySource::from_local_arrow_with_ownership(
            session,
            key_batch,
            context,
            ArrowOwnershipPolicy::ControlledParquet,
        )?;
        let key_array = key_source.batch_array(context)?;
        let key_struct = key_array
            .as_opt::<Struct>()
            .ok_or_else(|| failed("Stage 1 key batch is not a native struct array"))?;

        if let Some((predicate, _)) = &reader.mandatory_predicate
            && bound_mandatory.is_none()
        {
            let expr = predicate_to_vortex_expr(
                predicate,
                key_array.dtype(),
                VortexQueryPrimitiveKind::FilterAndProject,
            )?;
            let bound = expr
                .bind(key_array.dtype())
                .and_then(|bound| bound.optimize_recursive())
                .map_err(failed)?;
            bound_mandatory = Some(bound);
        }
        if let Some(predicate) = &reader.runtime_predicate
            && bound_runtime.is_none()
        {
            let expr = predicate_to_vortex_expr(
                predicate,
                key_array.dtype(),
                VortexQueryPrimitiveKind::FilterAndProject,
            )?;
            let bound = expr
                .bind(key_array.dtype())
                .and_then(|bound| bound.optimize_recursive())
                .map_err(failed)?;
            bound_runtime = Some(bound);
        }
        if let DType::Struct(fields, _) = key_array.dtype() {
            for filter in reader.probe_filters.iter() {
                if !filter.probe_columns.iter().all(|name| {
                    fields
                        .field(name.as_str())
                        .is_some_and(|d| arrow_key_matches_infallible_vortex(&d))
                }) {
                    all_filters_satisfied = false;
                }
            }
        }
        let mandatory_mask: Option<Mask> = if let Some(bound) = &bound_mandatory {
            let evaluated = key_array
                .clone()
                .apply_bound(bound)
                .map_err(failed)?
                .execute::<BoolArray>(&mut exec_ctx)
                .map_err(failed)?;
            Some(evaluated.to_mask_fill_null_false(&mut exec_ctx))
        } else {
            None
        };
        let runtime_mask: Option<Mask> = if mandatory_mask.as_ref().is_some_and(Mask::all_false) {
            None
        } else if let Some(bound) = &bound_runtime {
            let evaluated = key_array
                .clone()
                .apply_bound(bound)
                .map_err(failed)?
                .execute::<BoolArray>(&mut exec_ctx)
                .map_err(failed)?;
            Some(evaluated.to_mask_fill_null_false(&mut exec_ctx))
        } else {
            None
        };
        let pred_mask: Option<Mask> = match (mandatory_mask, runtime_mask) {
            (Some(a), Some(b)) => Some(a.bitand(&b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };

        let snap = context.memory().snapshot();
        let avail = snap.limit_bytes.saturating_sub(snap.reserved_bytes);
        if (n as u64).saturating_mul(64) > avail / 4 {
            all_filters_satisfied = false;
        }
        let bloom_mask = apply_bloom_mask_selected(
            key_array,
            &reader.probe_filters,
            pred_mask.as_ref(),
            context,
        )?;

        let mut local_rows = Vec::new();
        if !pred_mask.as_ref().is_some_and(Mask::all_false)
            && !bloom_mask.as_ref().is_some_and(Mask::all_false)
        {
            for row in 0..n {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if pred_mask.as_ref().is_some_and(|mask| !mask.value(row))
                    || bloom_mask.as_ref().is_some_and(|mask| !mask.value(row))
                {
                    continue;
                }
                local_rows.push(row);
                let rg_row = batch_first
                    .checked_add(row)
                    .ok_or_else(|| failed("Stage 1 row offset overflow"))?;
                if let Some(last) = surviving_ranges.last_mut()
                    && last.end == rg_row
                {
                    last.end += 1;
                } else {
                    surviving_ranges.push(rg_row..rg_row + 1);
                }
            }
        }

        if !local_rows.is_empty() {
            survivors = survivors
                .checked_add(local_rows.len() as u64)
                .ok_or_else(|| failed("Stage 1 survivor count overflow"))?;
            let key_count = plan.key_indices.len();
            let mut filtered_cols = Vec::with_capacity(key_count);
            let is_contiguous = local_rows
                .last()
                .is_some_and(|&last| last - local_rows[0] + 1 == local_rows.len());
            if local_rows.len() == n {
                for k in 0..key_count {
                    filtered_cols.push(key_struct.unmasked_field(k).clone());
                }
            } else if is_contiguous {
                let start = local_rows[0];
                let end = start + local_rows.len();
                for k in 0..key_count {
                    filtered_cols.push(
                        key_struct
                            .unmasked_field(k)
                            .slice(start..end)
                            .map_err(failed)?,
                    );
                }
            } else {
                let indices = index_array(local_rows.len(), false, context, |i| {
                    Ok(Some(local_rows[i]))
                })?;
                for k in 0..key_count {
                    let col = key_struct.unmasked_field(k);
                    filtered_cols.push(take_column(col, &indices, col.dtype(), context)?);
                }
            }
            key_chunks_deque.push_back((local_rows.len(), filtered_cols));
        }

        batch_first = batch_first
            .checked_add(n)
            .ok_or_else(|| failed("Stage 1 batch row offset overflow"))?;
        drop(key_source);
    }

    if batch_first as u64 != source_group.rows {
        return Err(failed(
            "Stage 1 key decoder did not complete its admitted row group extent",
        ));
    }

    let staged = (survivors > 0).then(|| StagedKeyStream {
        chunks: key_chunks_deque,
        offset: 0,
        key_count: plan.key_count,
        slots: plan.slots.clone(),
        projected_names: plan.projected_names.clone(),
        filter_satisfaction: BatchFilterSatisfaction {
            mandatory_predicate_satisfied: plan.mandatory_predicate_evaluated,
            probe_filters_satisfied: all_filters_satisfied,
        },
    });

    Ok(Stage1Result {
        survivors,
        surviving_ranges: Arc::from(surviving_ranges),
        staged,
    })
}

impl StagedKeyStream {
    #[allow(clippy::too_many_lines)]
    pub(super) fn compose_batch(
        &mut self,
        payload_source: ResidentMemorySource,
        rows: usize,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ResidentMemorySource> {
        let mut key_slices: Vec<Vec<ArrayRef>> = vec![Vec::new(); self.key_count];
        let mut remaining = rows;
        while remaining > 0 {
            let front = self
                .chunks
                .front()
                .ok_or_else(|| failed("Stage 1 key stream ended before Stage 2 payload"))?;
            let available = front.0.saturating_sub(self.offset);
            let take = remaining.min(available);
            let start = self.offset;
            let end = start + take;
            for (k, slices) in key_slices.iter_mut().enumerate() {
                let sliced = if start == 0 && end == front.0 {
                    front.1[k].clone()
                } else {
                    front.1[k].slice(start..end).map_err(failed)?
                };
                slices.push(sliced);
            }
            remaining -= take;
            if end == front.0 {
                self.chunks.pop_front();
                self.offset = 0;
            } else {
                self.offset = end;
            }
        }

        let max_slice_chunks = key_slices.iter().map(Vec::len).max().unwrap_or(1);
        let partition_scalar_bytes = self
            .slots
            .iter()
            .map(|slot| match slot {
                ColumnSlot::PartitionValue(Some(text)) => text.len() as u64,
                _ => 0,
            })
            .sum::<u64>();
        let extra_metadata = payload_source
            .columnar_metadata_bytes()
            .saturating_mul(4)
            .saturating_add(4096)
            .saturating_add((self.slots.len() as u64).saturating_mul(4096))
            .saturating_add((max_slice_chunks as u64).saturating_mul(4096))
            .saturating_add(partition_scalar_bytes);
        let names: Vec<&str> = self.projected_names.iter().map(String::as_str).collect();

        let composed = ResidentMemorySource::from_columnar_coverage(
            session,
            context,
            &names,
            rows,
            extra_metadata,
            Some(&payload_source),
            |metadata| {
                let payload_array = payload_source.batch_array(context)?;
                let payload_struct = payload_array
                    .as_opt::<Struct>()
                    .ok_or_else(|| failed("Stage 2 payload batch is not a native struct array"))?;
                let mut columns = Vec::with_capacity(self.slots.len());
                for slot in &self.slots {
                    match slot {
                        ColumnSlot::Key(k) => {
                            let chunks = std::mem::take(&mut key_slices[*k]);
                            if chunks.len() == 1 {
                                columns.push(
                                    crate::resident_memory_source::retain_columnar_metadata(
                                        &chunks[0], metadata,
                                    )?,
                                );
                            } else {
                                let dtype = chunks
                                    .first()
                                    .ok_or_else(|| failed("staged key column had zero slices"))?
                                    .dtype()
                                    .clone();
                                let chunked = ChunkedArray::try_new(chunks, dtype.clone())
                                    .map_err(failed)?
                                    .into_array();
                                let indices = index_array(rows, false, context, |r| Ok(Some(r)))?;
                                let flat = take_column(&chunked, &indices, &dtype, context)?;
                                columns.push(
                                    crate::resident_memory_source::retain_columnar_metadata(
                                        &flat, metadata,
                                    )?,
                                );
                            }
                        }
                        ColumnSlot::Payload(p) => {
                            columns.push(crate::resident_memory_source::retain_columnar_metadata(
                                payload_struct.unmasked_field(*p),
                                metadata,
                            )?);
                        }
                        ColumnSlot::PartitionValue(value) => {
                            let scalar = match value {
                                Some(text) => Scalar::from(text.as_str())
                                    .cast(&DType::Utf8(Nullability::Nullable))
                                    .map_err(failed)?,
                                None => Scalar::null(DType::Utf8(Nullability::Nullable)),
                            };
                            let col = ConstantArray::new(scalar, rows).into_array();
                            columns.push(crate::resident_memory_source::retain_columnar_metadata(
                                &col, metadata,
                            )?);
                        }
                        ColumnSlot::MissingNull(dtype) => {
                            let col =
                                ConstantArray::new(Scalar::null(dtype.clone()), rows).into_array();
                            columns.push(crate::resident_memory_source::retain_columnar_metadata(
                                &col, metadata,
                            )?);
                        }
                    }
                }
                Ok(columns)
            },
        )?;
        drop(payload_source);
        Ok(composed)
    }
}
