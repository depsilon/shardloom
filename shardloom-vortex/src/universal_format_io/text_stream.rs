//! Incremental text framing, exact schema discovery, and generation-held replay.
//!
//! Syntax and coercion remain the caller's existing adapter. This owner retains
//! one framed record during discovery and one credited output batch on replay.
//! A schema-free document is scanned twice; the first pass is reported explicitly.

use super::owned_batch::{BatchResult, OwnedBatch};
use super::{Field, RecordBatch, ScalarValue, Schema, SchemaRef, TextRecordBatchBuilder};
use crate::source_identity::SourceIdentity;
use arrow_array::{ArrayRef, RecordBatchReader, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_schema::{ArrowError, DataType};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    adaptive_work::{AdaptiveWorkReport, AdaptiveWorkSizer, WorkCost, WorkLimits, WorkShape},
    compute_pool::CancellationToken,
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// Framing only; record syntax is validated by the supplied text adapter.
#[derive(Clone, Copy, Debug)]
pub enum TextFraming {
    JsonDocument,
    JsonLines,
    Csv,
}

/// The decoder must preserve the adapter's existing duplicate/coercion rules.
pub type TextRecordDecoder = Arc<dyn Fn(&str) -> Result<Vec<(String, ScalarValue)>> + Send + Sync>;

/// Existing adapter inference/coercion rules, kept outside source ownership.
#[derive(Clone, Copy)]
pub struct TextTypePolicy {
    pub merge: fn(&DataType, &DataType) -> Result<DataType>,
    pub coerce: fn(ScalarValue, &DataType, &str) -> Result<ScalarValue>,
}

#[derive(Default)]
pub struct TextReadMetrics {
    pub bytes: AtomicU64,
    pub requests: AtomicU64,
}

/// Read one framed header under the same resource and generation checks.
/// The count includes actual buffer reads, which may include source read-ahead.
/// # Errors
/// Rejects absent/malformed headers, cancellation, changes and denied memory.
pub fn read_header(
    path: &Path,
    framing: TextFraming,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<(Budgeted<Vec<u8>>, u64)> {
    cancellation.check()?;
    let identity = Arc::new(SourceIdentity::capture(path)?);
    let metrics = Arc::new(TextReadMetrics::default());
    let mut reader = Frames::new(
        Arc::clone(&identity),
        framing,
        false,
        memory,
        cancellation,
        Arc::clone(&metrics),
    )?;
    let record = reader
        .next_record()?
        .ok_or_else(|| failed("text source has no header"))?;
    identity.validate()?;
    Ok((
        Budgeted::new(record.bytes, record.credit),
        metrics.bytes.load(Ordering::Relaxed),
    ))
}

/// A held text generation with a stable schema and an explicit discovery pass.
pub struct TextSource {
    identity: Arc<SourceIdentity>,
    schema: SchemaRef,
    decoder: TextRecordDecoder,
    framing: TextFraming,
    skip_first: bool,
    max_rows: Option<usize>,
    rows: usize,
    largest_record_bytes: usize,
    discovery_bytes: u64,
    memory: LiveMemoryPool,
    type_policy: Option<TextTypePolicy>,
    metadata: MemoryLease,
}

impl TextSource {
    /// Discover all fields/types without retaining records or typed columns.
    /// Supplied field types are authoritative; the decoder performs declared
    /// coercion. Additional fields retain ordinary adapter inference.
    /// # Errors
    /// Rejects malformed records, incompatible types, source changes, denied
    /// memory, cancellation and row-budget exhaustion.
    #[allow(clippy::too_many_arguments)]
    pub fn admit(
        path: &Path,
        framing: TextFraming,
        skip_first: bool,
        decoder: TextRecordDecoder,
        declared: Option<SchemaRef>,
        max_rows: Option<usize>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        Self::admit_adapted(
            path,
            framing,
            skip_first,
            decoder,
            declared,
            max_rows,
            &[],
            None,
            memory,
            cancellation,
        )
    }

    /// As `admit`, with the adapter's existing type-promotion/coercion policy.
    /// # Errors
    /// Propagates schema, adapter, resource and generation errors.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::needless_pass_by_value
    )] // Public schema ownership matches admit; retained field types are copied into discovery.
    pub fn admit_adapted(
        path: &Path,
        framing: TextFraming,
        skip_first: bool,
        decoder: TextRecordDecoder,
        declared: Option<SchemaRef>,
        max_rows: Option<usize>,
        seed_columns: &[String],
        type_policy: Option<TextTypePolicy>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        cancellation.check()?;
        let mut metadata = memory.reserve(8192)?;
        let identity = Arc::new(SourceIdentity::capture(path)?);
        let metrics = Arc::new(TextReadMetrics::default());
        let mut reader = Frames::new(
            Arc::clone(&identity),
            framing,
            skip_first,
            memory,
            cancellation,
            Arc::clone(&metrics),
        )?;
        let mut inferred = BTreeMap::<String, usize>::new();
        let mut fields = Vec::<ObservedField>::new();
        for name in seed_columns {
            metadata.resize(
                metadata
                    .bytes()
                    .checked_add(record_charge(name.len(), 16)? + 1024)
                    .ok_or_else(|| failed("text schema capacity overflow"))?,
            )?;
            if inferred.insert(name.clone(), fields.len()).is_some() {
                return Err(failed("text source has duplicate column names"));
            }
            let dtype = declared
                .as_ref()
                .and_then(|schema| schema.fields().iter().find(|field| field.name() == name))
                .map(|field| field.data_type().clone());
            fields.push(ObservedField {
                name: name.clone(),
                dtype,
                present: 0,
                null: false,
            });
        }
        let mut rows = 0_usize;
        let mut largest_record_bytes = 0;
        while let Some(record) = reader.next_record()? {
            cancellation.check()?;
            largest_record_bytes = largest_record_bytes.max(record.value().len());
            let _scratch = memory.reserve(record_charge(record.value().len(), 32)?)?;
            let raw = std::str::from_utf8(record.value()).map_err(failed)?;
            let decoded = resolve_record_fields(decoder(raw)?);
            rows = rows
                .checked_add(1)
                .ok_or_else(|| failed("text row count overflow"))?;
            enforce_rows(rows, max_rows)?;
            // Resolve duplicate fields before observing types, preserving the
            // previous adapter's last-value-wins semantics and first-name order.
            for (name, value) in &decoded {
                let name = name.as_str();
                let index = if let Some(&index) = inferred.get(name) {
                    index
                } else {
                    metadata.resize(
                        metadata
                            .bytes()
                            .checked_add(record_charge(name.len(), 16)? + 1024)
                            .ok_or_else(|| failed("text schema capacity overflow"))?,
                    )?;
                    super::validate_flat_columns(&[name.to_owned()], "text source")?;
                    let index = fields.len();
                    fields.push(ObservedField {
                        name: name.to_owned(),
                        dtype: None,
                        present: 0,
                        null: false,
                    });
                    inferred.insert(name.to_owned(), index);
                    index
                };
                let field = &mut fields[index];
                field.present += 1;
                if matches!(value, ScalarValue::Null) {
                    field.null = true;
                } else {
                    let dtype = declared
                        .as_ref()
                        .and_then(|schema| {
                            schema.fields().iter().find(|field| field.name() == name)
                        })
                        .map_or_else(
                            || super::primitive_arrow_dtype(name, value, "text source"),
                            |field| Ok(field.data_type().clone()),
                        )?;
                    field.dtype = Some(match field.dtype.as_ref() {
                        Some(previous) if previous != &dtype => {
                            if let Some(policy) = type_policy {
                                (policy.merge)(previous, &dtype)?
                            } else {
                                return Err(failed(format!(
                                    "column '{name}' changes type during schema discovery"
                                )));
                            }
                        }
                        _ => dtype,
                    });
                }
            }
        }
        if fields.is_empty() || (rows == 0 && seed_columns.is_empty()) {
            return Err(failed(
                "text source must include at least one object row and field",
            ));
        }
        if let Some(declared) = &declared {
            for hint in declared.fields() {
                let index = inferred.get(hint.name()).ok_or_else(|| {
                    failed(format!("declared column '{}' is absent", hint.name()))
                })?;
                fields[*index].dtype = Some(hint.data_type().clone());
            }
        }
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|field| {
                    Field::new(
                        field.name,
                        field.dtype.unwrap_or(DataType::Utf8),
                        rows == 0 || field.null || field.present != rows,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        identity.validate()?;
        cancellation.check()?;
        Ok(Arc::new(Self {
            identity,
            schema,
            decoder,
            framing,
            skip_first,
            max_rows,
            rows,
            largest_record_bytes,
            discovery_bytes: metrics.bytes.load(Ordering::Relaxed),
            memory: memory.clone(),
            type_policy,
            metadata,
        }))
    }

    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }
    #[must_use]
    pub fn discovery_bytes(&self) -> u64 {
        self.discovery_bytes
    }

    /// Add measured adapter header reads before sharing the descriptor.
    /// # Errors
    /// Rejects a shared descriptor or counter overflow.
    pub fn add_header_read_bytes(source: &mut Arc<Self>, bytes: u64) -> Result<()> {
        let source =
            Arc::get_mut(source).ok_or_else(|| failed("text descriptor is already shared"))?;
        source.discovery_bytes = source
            .discovery_bytes
            .checked_add(bytes)
            .ok_or_else(|| failed("text discovery byte count overflow"))?;
        Ok(())
    }
    #[must_use]
    pub fn identity(&self) -> Arc<SourceIdentity> {
        Arc::clone(&self.identity)
    }

    #[must_use]
    pub(crate) fn is_owned_by(&self, memory: &LiveMemoryPool) -> bool {
        memory.owns(&self.metadata)
    }

    /// Reopen the held generation. Projection avoids output construction, not
    /// record parsing or the explicit complete schema-discovery pass.
    /// # Errors
    /// Rejects invalid projection, source changes and denied resource admission.
    pub fn open(
        self: &Arc<Self>,
        projection: &[usize],
        batch_rows: usize,
        cancellation: &CancellationToken,
        metrics: Arc<TextReadMetrics>,
    ) -> Result<TextReader> {
        let schema = Arc::new(self.schema.project(projection).map_err(failed)?);
        let frames = Frames::new(
            Arc::clone(&self.identity),
            self.framing,
            self.skip_first,
            &self.memory,
            cancellation,
            metrics,
        )?;
        TextReader::new(Arc::clone(self), schema, frames, batch_rows)
    }

    /// The demanded source supplies its already generation-checked, I/O-admitted
    /// cursor. This private boundary never accepts an external producer.
    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    pub(crate) fn open_with_reader(
        self: &Arc<Self>,
        projection: &[usize],
        batch_rows: usize,
        cancellation: &CancellationToken,
        reader: Box<dyn Read + Send>,
    ) -> Result<TextReader> {
        self.identity.validate()?;
        let schema = Arc::new(self.schema.project(projection).map_err(failed)?);
        let frames = Frames::from_reader(
            self.identity.path(),
            reader,
            self.framing,
            self.skip_first,
            &self.memory,
            cancellation,
        )?;
        TextReader::new(Arc::clone(self), schema, frames, batch_rows)
    }
}

struct ObservedField {
    name: String,
    dtype: Option<DataType>,
    present: usize,
    null: bool,
}

/// Resolve a completely parsed record before schema observation or coercion.
/// Later duplicate values replace earlier values while first-name order stays
/// stable. Parsing/validation of discarded syntax remains the decoder's duty.
#[must_use]
pub fn resolve_record_fields(mut fields: Vec<(String, ScalarValue)>) -> Vec<(String, ScalarValue)> {
    let mut index = 0;
    while index < fields.len() {
        if let Some(previous) = fields[..index]
            .iter()
            .position(|(name, _)| name == &fields[index].0)
        {
            let (_, value) = fields.remove(index);
            fields[previous].1 = value;
        } else {
            index += 1;
        }
    }
    fields
}

pub struct TextReader {
    source: Arc<TextSource>,
    schema: SchemaRef,
    frames: Frames,
    batch_rows: usize,
    sizing: AdaptiveWorkSizer,
    cost: WorkCost,
    downstream_cost: WorkCost,
    record_workspace: u64,
    rows: usize,
    ended: bool,
}

impl TextReader {
    fn new(
        source: Arc<TextSource>,
        schema: SchemaRef,
        frames: Frames,
        batch_rows: usize,
    ) -> Result<Self> {
        // Include initial column builders, copied names and one framed record.
        // The largest discovered record guides safe continuation without making
        // an undemanded large record a hard minimum for an earlier prefix.
        let mut cost = super::work_sizing::schema_cost(&schema, 8192 + 4096, 2)?;
        cost.bytes_per_unit = cost.bytes_per_unit.max(builder_row_bytes(&schema)?);
        let record_workspace = record_charge(source.largest_record_bytes, 34)?;
        Ok(Self {
            source,
            schema,
            frames,
            batch_rows: batch_rows.clamp(1, 8192),
            sizing: AdaptiveWorkSizer::default(),
            cost,
            downstream_cost: WorkCost {
                fixed_bytes: 0,
                bytes_per_unit: 0,
            },
            record_workspace,
            rows: 0,
            ended: false,
        })
    }

    /// Applied batch targets under this reader's existing memory owner. Targets
    /// are capacities; EOF, byte limits or a complete-record boundary can yield
    /// a shorter batch. This is estimated workspace, not process memory.
    #[must_use]
    pub fn adaptive_work_report(&self) -> AdaptiveWorkReport {
        self.sizing.report()
    }

    /// The private shared-source adapter includes schema alignment and native
    /// intake overlap in the next row target, under the same pool. It does not
    /// acquire a second grant or change this reader's actual buffer ownership.
    pub(crate) fn with_downstream_cost(mut self, cost: WorkCost) -> Self {
        self.downstream_cost = cost;
        self
    }

    pub(crate) fn limit_next_batch(&mut self, rows: usize) -> Result<()> {
        if rows == 0 {
            return Err(failed("text next-batch limit must be positive"));
        }
        self.batch_rows = self.batch_rows.min(rows);
        Ok(())
    }

    fn batch_geometry(&self) -> Result<(WorkShape, usize, u64)> {
        let snapshot = self.source.memory.snapshot();
        let cost = WorkCost {
            fixed_bytes: self
                .cost
                .fixed_bytes
                .checked_add(self.downstream_cost.fixed_bytes)
                .ok_or_else(|| failed("text downstream workspace overflow"))?,
            bytes_per_unit: self
                .cost
                .bytes_per_unit
                .checked_add(self.downstream_cost.bytes_per_unit)
                .ok_or_else(|| failed("text downstream row capacity overflow"))?,
        };
        let selected = self.sizing.select(
            "text_batch",
            snapshot,
            cost,
            WorkLimits {
                min_units: 1,
                max_units: (self.source.rows - self.rows).min(self.batch_rows) as u64,
                max_in_flight: 1,
            },
        )?;
        let target_rows = usize::try_from(selected.units()).map_err(failed)?;
        let builder_row_bytes = builder_row_bytes(&self.schema)?;
        let capacity = usize::try_from(
            snapshot
                .limit_bytes
                .saturating_sub(snapshot.reserved_bytes)
                .saturating_sub(cost.fixed_bytes)
                .saturating_sub(self.record_workspace)
                / builder_row_bytes,
        )
        .unwrap_or(usize::MAX)
        .clamp(1, target_rows.min(1024));
        let initial = builder_row_bytes
            .checked_mul(capacity as u64)
            .and_then(|bytes| self.cost.fixed_bytes.checked_sub(4096)?.checked_add(bytes))
            .ok_or_else(|| failed("text batch capacity overflow"))?;
        Ok((selected, capacity, initial))
    }

    fn decode_row(&self, record: &[u8], names: &[String]) -> Result<Vec<ScalarValue>> {
        let decoded = resolve_record_fields((self.source.decoder)(
            std::str::from_utf8(record).map_err(failed)?,
        )?);
        let mut values = vec![ScalarValue::Null; names.len()];
        for (name, value) in decoded {
            if !self
                .source
                .schema
                .fields()
                .iter()
                .any(|field| field.name() == &name)
            {
                return Err(failed("text replay introduced an undiscovered field"));
            }
            if let Some(index) = names.iter().position(|column| column == &name) {
                values[index] = if let Some(policy) = self.source.type_policy {
                    (policy.coerce)(value, self.schema.field(index).data_type(), &name)?
                } else {
                    value
                };
            }
        }
        Ok(values)
    }

    fn read_batch(&mut self) -> Result<Option<OwnedBatch>> {
        if self.ended {
            return Ok(None);
        }
        self.source.identity.validate()?;
        self.frames.cancellation.check()?;
        if self.rows == self.source.rows {
            if self.frames.next_record()?.is_some() {
                return Err(failed("text replay row count changed"));
            }
            self.source.identity.validate()?;
            self.frames.cancellation.check()?;
            self.ended = true;
            return Ok(None);
        }
        let (selected, capacity, initial) = self.batch_geometry()?;
        let target_rows = usize::try_from(selected.units()).map_err(failed)?;
        let mut credit = self.source.memory.reserve(initial)?;
        let names = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        let mut builder = TextRecordBatchBuilder::new(
            Arc::clone(&self.schema),
            &names,
            capacity,
            "streaming text source",
        )?;
        let mut count = 0;
        let mut raw_bytes = 0_usize;
        while count < target_rows && (count == 0 || raw_bytes < 1 << 20) {
            // End at a record boundary while enough accounted headroom remains
            // for a complete next record. No decoder, callback or input read is
            // replayed after pressure. An irreducible record still fails at its
            // ordinary authoritative reservation if it cannot fit on its own.
            let current = self.source.memory.snapshot();
            if count > 0
                && current.limit_bytes.saturating_sub(current.reserved_bytes)
                    < self.record_workspace
            {
                break;
            }
            let Some(record) = self.frames.next_record()? else {
                self.ended = true;
                if self.rows != self.source.rows {
                    return Err(failed("text replay row count changed"));
                }
                break;
            };
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or_else(|| failed("text replay count overflow"))?;
            enforce_rows(self.rows, self.source.max_rows)?;
            // Admit raw parsing and growing Arrow builders before the decoder.
            // Large individual records may exceed the soft batch target with
            // complete credit; there is no 64 KiB / 8 MiB value truncation.
            credit.resize(
                credit
                    .bytes()
                    .checked_add(
                        (record.value().len() as u64)
                            .checked_mul(32)
                            .ok_or_else(|| failed("text batch capacity overflow"))?,
                    )
                    .ok_or_else(|| failed("text batch capacity overflow"))?,
            )?;
            let values = self.decode_row(record.value(), &names)?;
            builder.append_row(&values)?;
            raw_bytes = raw_bytes
                .checked_add(record.value().len())
                .ok_or_else(|| failed("text record size overflow"))?;
            count += 1;
        }
        self.source.identity.validate()?;
        self.frames.cancellation.check()?;
        if count == 0 {
            return Ok(None);
        }
        let batch = builder.finish()?;
        let actual = batch.get_array_memory_size() as u64;
        if actual > credit.bytes() {
            return Err(failed(
                "text builder exceeded its admitted allocation envelope",
            ));
        }
        credit.resize(actual)?;
        let observed = (raw_bytes as u64)
            .saturating_mul(32)
            .saturating_add(actual.saturating_mul(2))
            .div_ceil(count as u64);
        let batch = credited_batch(batch, credit)?;
        self.sizing.complete(selected, Some(observed));
        Ok(Some(batch))
    }

    pub(crate) fn next_owned(&mut self) -> Option<BatchResult> {
        match self.read_batch() {
            Ok(batch) => batch.map(Ok),
            Err(error) => {
                self.ended = true;
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

fn builder_row_bytes(schema: &arrow_schema::Schema) -> Result<u64> {
    (schema.fields().len() as u64)
        .checked_mul(16)
        .map(|bytes| bytes.max(1))
        .ok_or_else(|| failed("text batch capacity overflow"))
}

impl Iterator for TextReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_owned()
            .map(|batch| batch.map(|batch| batch.into_parts().0))
    }
}
impl RecordBatchReader for TextReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct HeldRead {
    file: File,
    identity: Arc<SourceIdentity>,
    cancellation: CancellationToken,
    metrics: Arc<TextReadMetrics>,
}
impl Read for HeldRead {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.cancellation
            .check()
            .and_then(|()| self.identity.validate())
            .map_err(std::io::Error::other)?;
        let count = self.file.read(bytes)?;
        self.metrics
            .bytes
            .fetch_add(count as u64, Ordering::Relaxed);
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.cancellation
            .check()
            .and_then(|()| self.identity.validate())
            .map_err(std::io::Error::other)?;
        Ok(count)
    }
}

#[derive(Clone, Copy)]
enum State {
    Start,
    ArrayFirst,
    ArrayNext,
    SingleEnd,
    Done,
}

struct Frames {
    reader: BufReader<Box<dyn Read + Send>>,
    framing: TextFraming,
    state: State,
    skip_first: bool,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    _buffer: MemoryLease,
}

impl Frames {
    #[allow(clippy::needless_pass_by_value)] // The held reader owns an Arc; the caller releases its input reference.
    fn new(
        identity: Arc<SourceIdentity>,
        framing: TextFraming,
        skip_first: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        metrics: Arc<TextReadMetrics>,
    ) -> Result<Self> {
        let file = identity.open_checked()?;
        let input = Box::new(HeldRead {
            file,
            identity: Arc::clone(&identity),
            cancellation: cancellation.clone(),
            metrics,
        });
        Self::from_reader(
            identity.path(),
            input,
            framing,
            skip_first,
            memory,
            cancellation,
        )
    }

    fn from_reader(
        path: &Path,
        input: Box<dyn Read + Send>,
        framing: TextFraming,
        skip_first: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        let mut buffer = memory.reserve(65_536)?;
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        let input: Box<dyn Read + Send> = if extension.eq_ignore_ascii_case("gz") {
            buffer.resize(65_536 + (1 << 20))?;
            Box::new(flate2::read::MultiGzDecoder::new(input))
        } else if extension.eq_ignore_ascii_case("zst") || extension.eq_ignore_ascii_case("zstd") {
            // Limit the provider's window before the first decoded byte. The
            // credited envelope also covers codec state and its input buffer.
            let window = (memory.snapshot().limit_bytes / 8)
                .max(1024)
                .ilog2()
                .min(23);
            buffer.resize(65_536 + (2_u64 << window) + (512 << 10))?;
            let mut decoder = zstd::stream::read::Decoder::new(input).map_err(failed)?;
            decoder.window_log_max(window).map_err(failed)?;
            Box::new(decoder)
        } else {
            input
        };
        Ok(Self {
            reader: BufReader::with_capacity(65_536, input),
            framing,
            state: State::Start,
            skip_first,
            memory: memory.clone(),
            cancellation: cancellation.clone(),
            _buffer: buffer,
        })
    }

    fn byte(&mut self) -> Result<Option<u8>> {
        let bytes = self.reader.fill_buf().map_err(failed)?;
        let next = bytes.first().copied();
        if next.is_some() {
            self.reader.consume(1);
        }
        Ok(next)
    }

    fn nonspace(&mut self) -> Result<Option<u8>> {
        loop {
            self.cancellation.check()?;
            match self.byte()? {
                Some(byte) if byte.is_ascii_whitespace() => {}
                other => return Ok(other),
            }
        }
    }

    fn next_record(&mut self) -> Result<Option<Record>> {
        loop {
            self.cancellation.check()?;
            if matches!(self.state, State::Done) {
                return Ok(None);
            }
            let record = match self.framing {
                TextFraming::JsonDocument => self.json_record()?,
                TextFraming::JsonLines | TextFraming::Csv => self.line_record()?,
            };
            if self.skip_first {
                self.skip_first = false;
                continue;
            }
            return Ok(record);
        }
    }

    fn json_record(&mut self) -> Result<Option<Record>> {
        let first = if matches!(self.state, State::Start) {
            let mut byte = self.nonspace()?;
            if byte == Some(0xef) {
                if self.byte()? != Some(0xbb) || self.byte()? != Some(0xbf) {
                    return Err(failed("invalid JSON byte-order mark"));
                }
                byte = self.nonspace()?;
            }
            match byte {
                Some(b'[') => {
                    self.state = State::ArrayFirst;
                    self.nonspace()?
                }
                Some(b'{') => {
                    self.state = State::SingleEnd;
                    Some(b'{')
                }
                _ => {
                    return Err(failed(
                        "JSON source must be an object or an array of objects",
                    ));
                }
            }
        } else if matches!(self.state, State::SingleEnd) {
            if self.nonspace()?.is_some() {
                return Err(failed("JSON source has trailing content"));
            }
            self.state = State::Done;
            return Ok(None);
        } else {
            let next = self.nonspace()?;
            if matches!(self.state, State::ArrayNext) {
                match next {
                    // Preserve the existing local JSON adapter's explicit
                    // trailing-comma compatibility at document-array level.
                    Some(b',') => self.nonspace()?,
                    Some(b']') => Some(b']'),
                    _ => return Err(failed("JSON array rows must be separated by ','")),
                }
            } else {
                next
            }
        };
        if first == Some(b']') && !matches!(self.state, State::SingleEnd) {
            if self.nonspace()?.is_some() {
                return Err(failed("JSON source has trailing content"));
            }
            self.state = State::Done;
            return Ok(None);
        }
        if first != Some(b'{') {
            return Err(failed("JSON rows must be objects"));
        }
        if !matches!(self.state, State::SingleEnd) {
            self.state = State::ArrayNext;
        }
        let mut record = self.new_record()?;
        append(&mut record, b'{')?;
        let mut depth = 1_usize;
        let mut quoted = false;
        let mut escaped = false;
        while depth != 0 {
            if record.value().len().is_multiple_of(4096) {
                self.cancellation.check()?;
            }
            let byte = self
                .byte()?
                .ok_or_else(|| failed("JSON object is not closed"))?;
            append(&mut record, byte)?;
            if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                }
            } else {
                match byte {
                    b'"' => quoted = true,
                    b'{' | b'[' => {
                        depth = depth
                            .checked_add(1)
                            .ok_or_else(|| failed("JSON nesting overflow"))?;
                    }
                    b'}' | b']' => depth -= 1,
                    _ => {}
                }
            }
        }
        Ok(Some(record))
    }

    fn line_record(&mut self) -> Result<Option<Record>> {
        loop {
            let mut record = self.new_record()?;
            let mut quoted = false;
            loop {
                if record.value().len().is_multiple_of(4096) {
                    self.cancellation.check()?;
                }
                let Some(byte) = self.byte()? else {
                    self.state = State::Done;
                    break;
                };
                if matches!(self.framing, TextFraming::Csv) && byte == b'"' {
                    quoted = !quoted;
                }
                if byte == b'\n' && !quoted {
                    break;
                }
                append(&mut record, byte)?;
            }
            if quoted {
                return Err(failed("CSV quoted record is not closed"));
            }
            // Delimiters and whitespace fields are data in CSV/TSV. Only an
            // empty physical line (including CRLF) is blank in this framing.
            // JSON Lines retains its whitespace-only-line behavior.
            let has_record = match self.framing {
                TextFraming::Csv => record.value().iter().any(|b| *b != b'\r'),
                TextFraming::JsonLines | TextFraming::JsonDocument => {
                    record.value().iter().any(|b| !b.is_ascii_whitespace())
                }
            };
            if has_record {
                return Ok(Some(record));
            }
            if matches!(self.state, State::Done) {
                return Ok(None);
            }
        }
    }

    fn new_record(&self) -> Result<Record> {
        Ok(Record {
            bytes: Vec::new(),
            credit: self.memory.reserve(0)?,
        })
    }
}

struct Record {
    bytes: Vec<u8>,
    credit: MemoryLease,
}
impl Record {
    fn value(&self) -> &[u8] {
        &self.bytes
    }
}

fn append(record: &mut Record, byte: u8) -> Result<()> {
    // Split ownership permits admission before fallible capacity growth.
    let (values, credit) = (&mut record.bytes, &mut record.credit);
    if values.len() == values.capacity() {
        let target = values.capacity().saturating_mul(2).max(4096);
        credit.resize(target as u64)?;
        values
            .try_reserve_exact(target - values.len())
            .map_err(failed)?;
        if values.capacity() > target {
            return Err(failed("text record allocation exceeded admission"));
        }
    }
    values.push(byte);
    Ok(())
}

fn record_charge(bytes: usize, multiplier: u64) -> Result<u64> {
    (bytes as u64)
        .checked_mul(multiplier)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| failed("text record capacity overflow"))
}
fn enforce_rows(rows: usize, limit: Option<usize>) -> Result<()> {
    if let Some(limit) = limit
        && rows > limit
    {
        return Err(failed(format!(
            "local source runtime profile supports at most {limit} text data rows"
        )));
    }
    Ok(())
}

struct CreditedBuffer {
    buffer: Buffer,
    _credit: Arc<MemoryLease>,
}
impl AsRef<[u8]> for CreditedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

// Bytes::from_owner preserves the original aligned allocation safely. Each
// Arrow buffer retains the shared lease even after the reader/source is dropped.
#[allow(clippy::needless_pass_by_value)] // Consume the uncredited batch so no parallel buffer owner escapes this boundary.
pub(crate) fn credited_batch(batch: RecordBatch, lease: MemoryLease) -> Result<OwnedBatch> {
    let credit = Arc::new(lease);
    let batch = OwnedBatch::with_owner(batch, credit.clone());
    let arrays = batch
        .columns()
        .iter()
        .map(|array| credited_array(array, &credit))
        .collect::<Result<Vec<_>>>()?;
    let output = RecordBatch::try_new_with_options(
        batch.schema(),
        arrays,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(failed)?;
    Ok(OwnedBatch::with_owner(output, credit))
}
fn credited_array(array: &ArrayRef, credit: &Arc<MemoryLease>) -> Result<ArrayRef> {
    let data = array.to_data();
    let wrap = |buffer: &Buffer| {
        Buffer::from(bytes::Bytes::from_owner(CreditedBuffer {
            buffer: buffer.clone(),
            _credit: Arc::clone(credit),
        }))
    };
    let nulls = data.nulls().map(|nulls| {
        NullBuffer::new(BooleanBuffer::new(
            wrap(nulls.buffer()),
            nulls.offset(),
            nulls.len(),
        ))
    });
    let buffers = data.buffers().iter().map(wrap).collect();
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            credited_array(&make_array(child.clone()), credit).map(|array| array.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(make_array(
        data.into_builder()
            .buffers(buffers)
            .nulls(nulls)
            .child_data(children)
            .build()
            .map_err(failed)?,
    ))
}

fn failed(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "streaming text source: {error}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests;
