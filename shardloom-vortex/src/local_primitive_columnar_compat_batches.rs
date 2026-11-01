//! Width-aware conversion and pre-admitted file-unit accumulation.

use super::*;
use crate::resident_session::NativeExecutionContext;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::dtype::PType;

fn orc_encoder_overhead(dtype: &DType) -> Result<u64> {
    // The pinned provider reserves 512 literals per variable integer-RLE run.
    // A node also needs its encoder object, presence/byte staging and pointers.
    // Timestamp columns own two integer encoders; list containers own lengths;
    // structs own only presence and children. Growing payload is charged by
    // the separate eight-times converted-input envelope. Provider memory_size
    // omits pending integer literals, so do not substitute that observation.
    let (base, children) = match dtype {
        DType::Struct(fields, _) => (
            1024,
            fields.fields().try_fold(0_u64, |bytes, child| {
                bytes
                    .checked_add(orc_encoder_overhead(&child)?)
                    .ok_or_else(|| error("ORC encoder admission overflow"))
            })?,
        ),
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
            (5120, orc_encoder_overhead(child)?)
        }
        DType::Bool(_) | DType::Primitive(PType::F32 | PType::F64 | PType::I8, _) => (1024, 0),
        DType::Extension(_) => (9216, 0),
        _ => (5120, 0),
    };
    children
        .checked_add(base)
        .ok_or_else(|| error("ORC encoder admission overflow"))
}

impl PreparedCompatibilityExport {
    fn parquet_columns(&self) -> usize {
        fn leaves(dtype: &DType) -> usize {
            match dtype {
                DType::Struct(fields, _) => fields.fields().map(|child| leaves(&child)).sum(),
                DType::List(child, _) | DType::FixedSizeList(child, _, _) => leaves(child),
                _ => 1,
            }
        }
        // Parquet creates one page/dictionary writer per physical leaf. Lists
        // and structs add levels and schema nodes, not another value writer.
        // Keep every node in the independent schema/footer admission below.
        leaves(&self.plan.dtype)
    }

    pub(super) fn parquet_dictionary_enabled(&self) -> bool {
        // The pinned provider's dictionary interner allocates 4096 entries at
        // construction, independent of the dictionary-page target. Admit that
        // fixed hash-table workspace before enabling it for every column. Wide
        // schemas retain Snappy and statistics without thousands of mostly
        // empty dictionary tables.
        u64::try_from(self.parquet_columns())
            .ok()
            .and_then(|fields| fields.checked_mul(96 * 1024))
            .is_some_and(|bytes| bytes <= self.plan.session.memory().snapshot().limit_bytes / 8)
    }

    pub(super) fn parquet_page_bytes(&self) -> u64 {
        // The per-column page/dictionary target must shrink for wide schemas.
        // Otherwise a tiny wide result reserves the full narrow-file policy
        // thousands of times before seeing its first value.
        (self.plan.session.memory().snapshot().limit_bytes
            / 64
            / u64::try_from(self.parquet_columns().max(1)).unwrap_or(u64::MAX))
        .clamp(1024, 64 * 1024)
    }

    pub(super) fn writer_overhead(&self) -> Result<u64> {
        // IPC/Avro construct schema/encoder objects and size payload buffers from
        // the current batch. ORC retains small RLE staging arrays per stream.
        // Parquet additionally has bounded data and dictionary page targets.
        // Payload growth and compression overlap are charged separately below.
        let per_field = match self.format {
            VortexLocalPrimitiveRowExportFormat::Parquet => {
                8192 + self.parquet_page_bytes() * 2
                    + if self.parquet_dictionary_enabled() {
                        96 * 1024
                    } else {
                        0
                    }
            }
            _ => 4096,
        };
        let columns = if self.format == VortexLocalPrimitiveRowExportFormat::Parquet {
            self.parquet_columns()
        } else {
            self.schema_fields
        };
        let field_bytes = if self.format == VortexLocalPrimitiveRowExportFormat::Orc {
            orc_encoder_overhead(&self.plan.dtype)?
        } else {
            usize_to_u64(columns)?
                .checked_mul(per_field)
                .ok_or_else(|| error("writer overhead admission overflow"))?
        };
        field_bytes
            .checked_add(64 * 1024)
            .ok_or_else(|| error("writer overhead admission overflow"))
    }

    fn footer_metadata(&self, units: usize) -> Result<u64> {
        let base = CompatibilityLimits::metadata_for_batches(self.schema_fields, units)?;
        // Names and nested paths are resource-admitted, not capped at 256 bytes.
        // Retained file-unit descriptors can repeat those paths.
        let schema = self.schema_metadata.as_ref().map_or(0, MemoryLease::bytes);
        base.checked_add(
            schema
                .checked_mul(usize_to_u64(units)?)
                .ok_or_else(|| error("footer schema admission overflow"))?,
        )
        .ok_or_else(|| error("footer schema admission overflow"))
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) fn write_native_batch(
        &self,
        array: &ArrayRef,
        execution: &NativeExecutionContext<'_>,
        ctx: &mut ExecutionCtx,
        writer: &mut Writer<'_>,
        work: &mut CompatibilityWork,
        owner: &mut MemoryLease,
        metadata: &mut u64,
        initial: u64,
        rows: &mut u64,
        after_batch: &mut impl FnMut(u64) -> Result<()>,
    ) -> Result<()> {
        let mut limits = self.limits.clone();
        if limits.streaming {
            // First measure the native expansion. Arrow allocation is admitted
            // only after selecting a fitting slice below. Native decoding still
            // uses the same allocator, validation and cancellation contract.
            limits.arrow_batch_bytes = u64::MAX;
        }
        let nested_values = array.dtype().as_struct_fields_opt().is_some_and(|fields| {
            fields
                .fields()
                .any(|dtype| super::super::native_payload::is_nested(&dtype))
        });
        let overhead = self.writer_overhead()?;
        let mut start = 0;
        let mut next_rows = array.len().min(self.limits.batch_rows);
        while start < array.len() {
            self.limits.check()?;
            execution.check_cancelled()?;
            let mut count = next_rows.min(array.len() - start);
            loop {
                self.limits.check()?;
                let subset = array.slice(start..start + count).map_err(vortex_error)?;
                // List views may otherwise retain a complete producer child
                // domain even for a one-row slice. Compact only selected native
                // values, with their original logical types and validity.
                let subset = if nested_values && count != array.len() {
                    super::super::native_payload::detach_with_policy(
                        &subset,
                        super::super::native_payload::CopyPolicy::PreserveUnobserved,
                        execution,
                    )?
                } else {
                    subset
                };
                let (canonical, expanded) =
                    admitted_canonical(&subset, &self.plan.columns, ctx, &limits)?;
                if self.limits.streaming && expanded > self.limits.arrow_batch_bytes && count > 1 {
                    count = count.div_ceil(2);
                    add(&mut work.conversion_splits, 1)?;
                    continue;
                }
                let arrow_bound = if self.limits.streaming {
                    expanded.max(self.limits.arrow_batch_bytes)
                } else {
                    self.limits.arrow_batch_bytes
                };
                let view_target = if self.format == VortexLocalPrimitiveRowExportFormat::Parquet {
                    nested::immediate_parquet_target(
                        &canonical,
                        &self.schema,
                        expanded,
                        arrow_bound,
                    )?
                } else {
                    None
                };
                let conversion_bytes = view_target.as_ref().map_or(expanded, |(_, bytes)| *bytes);
                if writer.buffered_input_bytes > 0
                    && writer.buffered_input_bytes.saturating_add(conversion_bytes)
                        > writer.target_bytes
                {
                    writer.flush(work, false)?;
                }
                if self.limits.streaming {
                    *metadata =
                        self.footer_metadata(writer.pending_units(work.arrow_batches, count)?)?;
                }
                let required = |buffered: u64, metadata: u64| {
                    buffered
                        .checked_add(conversion_bytes)
                        .and_then(|n| n.checked_mul(8))
                        .and_then(|n| n.checked_add(metadata))
                        .and_then(|n| n.checked_add(overhead))
                        .map(|n| n.max(initial))
                        .ok_or_else(|| error("writer admission overflow"))
                };
                let snapshot = execution.memory().snapshot();
                let capacity = snapshot
                    .limit_bytes
                    .saturating_sub(snapshot.reserved_bytes)
                    .saturating_add(owner.bytes());
                let mut admitted = required(writer.buffered_input_bytes, *metadata)?;
                if admitted > capacity && writer.buffered_input_bytes > 0 {
                    writer.flush(work, true)?;
                    if self.limits.streaming {
                        *metadata =
                            self.footer_metadata(writer.pending_units(work.arrow_batches, count)?)?;
                    }
                    admitted = required(0, *metadata)?;
                }
                if self.limits.streaming && admitted > capacity && count > 1 {
                    count = count.div_ceil(2);
                    add(&mut work.conversion_splits, 1)?;
                    continue;
                }
                if work.arrow_batches >= usize_to_u64(self.limits.batches)? {
                    return Err(error("writer batch count exceeds admission"));
                }
                // One large value takes one fully admitted conversion. A real
                // grant/format failure propagates; no truncated value or second
                // execution route is substituted.
                if admitted > owner.bytes() {
                    owner.resize(admitted)?;
                }
                work.writer_reserved_bytes = work.writer_reserved_bytes.max(owner.bytes());
                let batch_schema = view_target
                    .as_ref()
                    .map_or(&self.schema, |(schema, _)| schema);
                let target = Field::new("", DataType::Struct(batch_schema.fields().clone()), false);
                let export_options =
                    ArrowExportOptions::default().with(CompactBuffers(view_target.is_none()));
                let arrow = execution
                    .native_session()
                    .arrow()
                    .exporter(&export_options)
                    .execute_arrow(canonical, Some(&target), ctx)
                    .map_err(vortex_error)?;
                let structure = arrow
                    .as_any()
                    .downcast_ref::<ArrowStructArray>()
                    .ok_or_else(|| error("Arrow provider did not return the admitted struct"))?;
                let batch =
                    RecordBatch::try_new(Arc::clone(batch_schema), structure.columns().to_vec())
                        .map_err(vortex_error)?;
                if batch.num_rows() != count {
                    return Err(error("Arrow provider changed row count"));
                }
                let actual = usize_to_u64(batch.get_array_memory_size())?;
                if actual > arrow_bound.max(conversion_bytes) {
                    return Err(error("Arrow retained buffers exceed batch admission"));
                }
                add(&mut work.admitted_arrow_expansion_bytes, expanded)?;
                work.max_arrow_batch_bytes = work.max_arrow_batch_bytes.max(actual);
                if count == 1 && actual > self.limits.arrow_batch_bytes {
                    add(&mut work.large_value_batches, 1)?;
                }
                writer.write(&batch, conversion_bytes, work)?;
                add(&mut work.arrow_batches, 1)?;
                add(rows, usize_to_u64(count)?)?;
                after_batch(*rows)?;
                start += count;
                next_rows = count;
                break;
            }
        }
        Ok(())
    }
}
