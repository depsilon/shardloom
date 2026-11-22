//! Join completed native fields to newly decoded fields by original coordinates.
//! The selected-column decoder and native intake remain the only realization path.

use super::{
    Arc, ColumnarReader, NativeExecutionContext, PreparedColumnarSource, ResidentMemorySource,
    ResidentVortexSession, Result, SourceGroup, batch_work, failed,
};
use crate::resident_session::columnar_coverage::{Acquisition, Region, Ticket};
use vortex::array::{ArrayRef, IntoArray as _, arrays::ChunkedArray};

impl super::LocalColumnarSource {
    /// Realize and validate selected columns through the ordinary native reader.
    /// Complete eligible regions may remain in this session's bounded optional
    /// coverage. Retention can decline or be reclaimed; this writes no artifact
    /// and does not promise reuse in another session. The report counts actual
    /// reads, decodes and prior completed coverage consumed by this call.
    ///
    /// # Errors
    /// Requires one local Parquet file and nonempty valid column demand. Rejects
    /// foreign memory ownership, source changes, cancellation, decode failures
    /// and mandatory memory refusal using the ordinary reader diagnostics.
    pub fn realize_native_columns(
        self: &Arc<Self>,
        session: &ResidentVortexSession,
        columns: &[shardloom_core::ColumnRef],
        cancellation: &shardloom_exec::compute_pool::CancellationToken,
    ) -> Result<super::ColumnarReadReport> {
        if self.other.is_some() || self.files.len() != 1 || !self.collection.is_single_file() {
            return Err(failed(
                "resident column realization requires one local Parquet file",
            ));
        }
        session.with_native_execution_context(cancellation, |context| {
            let _demand_credit = crate::native_payload_schema::reserve_names(
                context.memory(),
                columns.len(),
                |index| columns[index].as_str(),
            )?;
            let demand = super::SourceDemand {
                projection: super::ProjectionRequest::Columns(columns.to_vec()),
                ..super::SourceDemand::default()
            };
            let prepared = self.prepare(&demand, session, cancellation)?;
            let mut reader = prepared.open(context)?;
            while let Some(source) = reader.next_native(session, context)? {
                drop(source);
            }
            self.validate_generation_controlled(cancellation)?;
            reader.report()
        })
    }
}

pub(super) struct ReaderCoverage {
    acquired: Acquisition,
    ticket: Option<Ticket>,
    first_row: u64,
    expected_rows: u64,
    emitted_rows: u64,
}

impl ReaderCoverage {
    fn begin(
        prepared: &PreparedColumnarSource,
        group: SourceGroup,
        remaining: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Option<Self> {
        if !prepared.source.collection.is_single_file()
            || prepared.source.files.len() != 1
            || !eligible_fields(prepared)
        {
            return None;
        }
        let (store, operation) = context.columnar_coverage()?;
        let identity = &prepared.source.files[group.file].identity;
        let region = Region {
            index: group.index,
            first_row: group.first_row,
            rows: group.rows,
        };
        let acquired = store.acquire(identity, region, &prepared.projection, operation)?;
        let expected_rows = group.rows.min(remaining);
        let ticket = (expected_rows == group.rows)
            .then(|| store.begin(identity, region, &acquired.missing, operation))
            .flatten();
        Some(Self {
            acquired,
            ticket,
            first_row: group.first_row,
            expected_rows,
            emitted_rows: 0,
        })
    }

    fn hit_fields(&self) -> usize {
        self.acquired
            .hits
            .iter()
            .filter(|hit| hit.is_some())
            .count()
    }

    fn compose(
        &self,
        prepared: &PreparedColumnarSource,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
        first_row: u64,
        rows: usize,
        newly_read: Option<&ResidentMemorySource>,
    ) -> Result<ResidentMemorySource> {
        let end = first_row
            .checked_add(rows as u64)
            .ok_or_else(|| failed("coverage row extent overflow"))?;
        let metadata_bytes = self.composition_metadata(first_row, end, newly_read)?;
        let schema = prepared.source.schema();
        let names = prepared
            .projection
            .iter()
            .map(|&index| schema.field(index).name().as_str())
            .collect::<Vec<_>>();
        ResidentMemorySource::from_columnar_coverage(
            session,
            context,
            &names,
            rows,
            metadata_bytes,
            newly_read,
            |metadata| {
                let mut columns = Vec::with_capacity(names.len());
                let mut missing_position = 0;
                for hit in &self.acquired.hits {
                    context.check_cancelled()?;
                    let chunks = if let Some(hit) = hit {
                        covered_chunks(hit, first_row, end)?
                    } else {
                        let source = newly_read.ok_or_else(|| {
                            failed("native column coverage requires a missing field")
                        })?;
                        let field = source
                            .batch_array(context)?
                            .slots()
                            .get(missing_position + 1)
                            .and_then(Option::as_ref)
                            .ok_or_else(|| failed("native intake omitted a missing field"))?;
                        missing_position += 1;
                        vec![field.clone()]
                    };
                    columns.push(owned_chunks(chunks, metadata)?);
                }
                Ok(columns)
            },
        )
    }

    fn composition_metadata(
        &self,
        first_row: u64,
        end: u64,
        newly_read: Option<&ResidentMemorySource>,
    ) -> Result<u64> {
        let mut bytes = 0_u64;
        let mut add = |source_metadata: u64| -> Result<()> {
            bytes = source_metadata
                .checked_mul(4)
                .and_then(|value| value.checked_add(4096))
                .and_then(|value| bytes.checked_add(value))
                .ok_or_else(|| failed("coverage composition metadata overflow"))?;
            Ok(())
        };
        for hit in &self.acquired.hits {
            if let Some(hit) = hit {
                for fragment in &hit.coverage.fragments {
                    if fragment.first_row < end
                        && fragment.first_row + fragment.batch.rows() as u64 > first_row
                    {
                        add(fragment.batch.metadata_bytes())?;
                    }
                }
            } else {
                add(newly_read
                    .ok_or_else(|| failed("missing coverage intake"))?
                    .columnar_metadata_bytes())?;
            }
        }
        Ok(bytes)
    }
}

fn covered_chunks(
    hit: &crate::resident_session::columnar_coverage::Hit,
    first_row: u64,
    end: u64,
) -> Result<Vec<ArrayRef>> {
    let mut chunks = Vec::new();
    let count = hit
        .coverage
        .fragments
        .iter()
        .filter(|fragment| {
            fragment.first_row < end
                && fragment
                    .first_row
                    .saturating_add(fragment.batch.rows() as u64)
                    > first_row
        })
        .count();
    chunks.try_reserve_exact(count).map_err(failed)?;
    if chunks.capacity() > count {
        return Err(failed(
            "native coverage fragment vector exceeded its credit",
        ));
    }
    let mut covered = first_row;
    for fragment in &hit.coverage.fragments {
        let fragment_end = fragment
            .first_row
            .checked_add(fragment.batch.rows() as u64)
            .ok_or_else(|| failed("coverage fragment extent overflow"))?;
        let start = first_row.max(fragment.first_row);
        let stop = end.min(fragment_end);
        if start >= stop {
            continue;
        }
        if start != covered {
            return Err(failed("native column coverage has a row gap"));
        }
        let range = usize::try_from(start - fragment.first_row).map_err(failed)?
            ..usize::try_from(stop - fragment.first_row).map_err(failed)?;
        chunks.push(
            fragment
                .batch
                .column(hit.field)?
                .slice(range)
                .map_err(failed)?,
        );
        covered = stop;
    }
    if covered != end {
        return Err(failed("native column coverage omitted demanded rows"));
    }
    Ok(chunks)
}

fn owned_chunks(
    mut chunks: Vec<ArrayRef>,
    metadata: &Arc<shardloom_exec::live_memory::MemoryLease>,
) -> Result<ArrayRef> {
    if chunks.len() == 1 {
        return crate::resident_memory_source::retain_columnar_metadata(
            &chunks.remove(0),
            metadata,
        );
    }
    for chunk in &mut chunks {
        *chunk = crate::resident_memory_source::retain_columnar_metadata(chunk, metadata)?;
    }
    let dtype = chunks
        .first()
        .ok_or_else(|| failed("native coverage field has no fragments"))?
        .dtype()
        .clone();
    ChunkedArray::try_new(chunks, dtype)
        .map_err(failed)?
        .into_array()
        .with_retained_owner(metadata.clone())
        .map_err(failed)
}

fn eligible_fields(prepared: &PreparedColumnarSource) -> bool {
    use arrow_schema::DataType;
    let schema = prepared.source.schema();
    prepared.projection.iter().all(|&index| {
        let field = schema.field(index);
        field.metadata().is_empty()
            && matches!(
                field.data_type(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
            )
    })
}

impl ColumnarReader {
    pub(super) fn next_parquet_native(
        &mut self,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<(ResidentMemorySource, u64)>> {
        loop {
            context.check_cancelled()?;
            if let Some((index, _)) = &self.input {
                let identity = &self.prepared.source.files[*index].identity;
                if let Err(error) = identity.validate() {
                    if let Some((store, _)) = context.columnar_coverage() {
                        store.invalidate(identity);
                    }
                    return Err(error);
                }
            }
            if let Some(covered) = self.next_covered(session, context)? {
                return Ok(Some(covered));
            }
            if let Some(active) = &mut self.active {
                if let Some((batch, first_row)) = active.next(&mut self.sizing, context)? {
                    let rows = batch.num_rows();
                    self.rows = self
                        .rows
                        .checked_add(rows as u64)
                        .ok_or_else(|| failed("source row count overflow"))?;
                    let source = self.accept_decoded(batch, first_row, session, context)?;
                    return Ok(Some((source, first_row)));
                }
                self.active.take();
            }
            if let Some(coverage) = self.coverage.take() {
                if coverage.emitted_rows != coverage.expected_rows {
                    return Err(failed(
                        "native coverage did not complete its admitted extent",
                    ));
                }
                if let Some(ticket) = coverage.ticket
                    && let Some((store, _)) = context.columnar_coverage()
                {
                    store.validate(ticket);
                }
            }
            if let Some((store, _)) = context.columnar_coverage() {
                self.coverage_retained_bytes = self.coverage_retained_bytes.max(
                    store
                        .retained_bytes()
                        .ok_or_else(|| failed("native coverage accounting lock is poisoned"))?,
                );
            }
            let Some(&group) = self.prepared.row_groups.get(self.next_group) else {
                self.input.take();
                self.ended = true;
                if !self.decisions_recorded {
                    if let Some(selection) = self.prepared.region_selection {
                        selection.complete(context)?;
                    }
                    self.decisions_recorded = true;
                }
                return Ok(None);
            };
            let file = &self.prepared.source.files[group.file];
            if self
                .input
                .as_ref()
                .is_none_or(|(index, _)| *index != group.file)
            {
                self.input.take();
                self.input = Some((
                    group.file,
                    context.local_source_reader(
                        Arc::clone(&file.identity),
                        Arc::clone(&self.metrics),
                    )?,
                ));
            }
            let remaining = self
                .prepared
                .max_rows
                .map_or(u64::MAX, |limit| limit.saturating_sub(self.served_rows));
            let coverage = ReaderCoverage::begin(&self.prepared, group, remaining, context);
            if coverage
                .as_ref()
                .is_some_and(|coverage| coverage.hit_fields() > 0)
            {
                self.coverage_regions = self
                    .coverage_regions
                    .checked_add(1)
                    .ok_or_else(|| failed("coverage region counter overflow"))?;
            }
            if let Some(coverage) = &coverage {
                if !coverage.acquired.missing.is_empty() {
                    self.open_decoder(group, &coverage.acquired.missing, context)?;
                }
            } else {
                // Clone is covered by the prepared source-demand metadata lease.
                let projection = self.prepared.projection.clone();
                self.open_decoder(group, &projection, context)?;
            }
            self.coverage = coverage;
            self.next_group += 1;
        }
    }

    fn next_covered(
        &mut self,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<(ResidentMemorySource, u64)>> {
        let Some(coverage) = &mut self.coverage else {
            return Ok(None);
        };
        if !coverage.acquired.missing.is_empty() || coverage.emitted_rows == coverage.expected_rows
        {
            return Ok(None);
        }
        let rows = usize::try_from(
            (coverage.expected_rows - coverage.emitted_rows).min(batch_work::MAX_ROWS as u64),
        )
        .map_err(failed)?;
        let first_row = coverage
            .first_row
            .checked_add(coverage.emitted_rows)
            .ok_or_else(|| failed("native coverage source position overflow"))?;
        let source = coverage.compose(&self.prepared, session, context, first_row, rows, None)?;
        coverage.emitted_rows += rows as u64;
        let hit_fields = coverage.hit_fields();
        self.record_served(rows, hit_fields)?;
        Ok(Some((source, first_row)))
    }

    fn accept_decoded(
        &mut self,
        batch: arrow_array::RecordBatch,
        first_row: u64,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ResidentMemorySource> {
        let rows = batch.num_rows();
        let (source, detached) =
            ResidentMemorySource::from_local_arrow_with_coverage(session, batch, context)?;
        let hit_fields = self.coverage.as_ref().map_or(0, ReaderCoverage::hit_fields);
        let source = if let Some(coverage) = &mut self.coverage {
            if let Some(ticket) = coverage.ticket
                && let Some((store, _)) = context.columnar_coverage()
            {
                if let Some(detached) = detached {
                    store.append(ticket, first_row, detached);
                } else {
                    store.discard(ticket);
                    coverage.ticket = None;
                }
            }
            coverage.emitted_rows = coverage
                .emitted_rows
                .checked_add(rows as u64)
                .filter(|rows| *rows <= coverage.expected_rows)
                .ok_or_else(|| failed("coverage exceeded its admitted extent"))?;
            // Delivery metadata must be separate from optional cache metadata.
            // This preserves the existing per-batch consumer-release witness.
            if hit_fields > 0 || coverage.ticket.is_some() {
                coverage.compose(
                    &self.prepared,
                    session,
                    context,
                    first_row,
                    rows,
                    Some(&source),
                )?
            } else {
                source
            }
        } else {
            source
        };
        self.record_served(rows, hit_fields)?;
        Ok(source)
    }

    fn record_served(&mut self, rows: usize, hit_fields: usize) -> Result<()> {
        self.served_rows = self
            .served_rows
            .checked_add(rows as u64)
            .ok_or_else(|| failed("served source rows overflow"))?;
        self.coverage_field_rows = (rows as u64)
            .checked_mul(hit_fields as u64)
            .and_then(|count| self.coverage_field_rows.checked_add(count))
            .ok_or_else(|| failed("native coverage work counter overflow"))?;
        Ok(())
    }
}
