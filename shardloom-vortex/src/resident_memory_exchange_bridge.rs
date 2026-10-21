// SPDX-License-Identifier: Apache-2.0
//! Finite experiment access to existing native source, encoder and consumer APIs.
//! This is not a public reader or a general Arrow decompression boundary.

use super::{ResidentMemorySource, memory_error, native_error};
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::{
        PreparedVortexRelational, prepare_relational_with_schema_in_session,
    },
    native_exchange_protocol::{
        authority::{Authority, Case, SQL_QUERY, unhex},
        hash,
        wire::{Mode, Operations, Projection, ProjectionField},
    },
    relational_query::{VortexRelationalPlan, VortexRelationalProject, VortexRelationalScan},
    resident_session::{NativeExecutionContext, ResidentVortexSession},
};
use arrow_array::{Array as _, RecordBatch};
use arrow_flight::{
    FlightData,
    decode::FlightRecordBatchStream,
    encode::{DictionaryHandling, FlightDataEncoder, FlightDataEncoderBuilder},
};
use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::{
    ColumnRef, DatasetUri, ExecutionResourceOrigin, ExecutionResources, ExprId, Expression,
    ExpressionKind, Result,
};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use shardloom_plan::ProjectionRequest;
use std::{io::Read as _, path::Path, sync::Arc, time::Instant};
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        arrays::{
            Chunked, Struct, chunked::ChunkedArrayExt as _, struct_::StructArraySlotsExt as _,
        },
        dtype::DType,
    },
    arrow::ArrowSessionExt as _,
    encodings::runend::{RunEnd, RunEndArraySlotsExt as _},
    ipc::messages::{EncoderMessage, MessageEncoder},
};

pub(crate) const GRANT: u64 = 256 << 20;
const MANIFEST_BYTES: u64 = 1 << 20;
const MANIFEST_SHA: &str = "58a1556806cfc708a0f4fddaef6e18a81442609b2ae031fba786ca82fb43eb7f";
// Declared temporary admission, not a measurement of every provider allocation.
// Provider-owned scratch and transport copies are reported as unknown separately.
pub(crate) const PROVIDER_ALLOWANCE: u64 = 64 << 20;

pub(crate) struct Fixture {
    pub(crate) authority: Case,
    pub(crate) sql: String,
    record: FixtureRecord,
    _metadata: MemoryLease,
}

#[derive(Deserialize)]
struct Manifest {
    version: u32,
    cases: Vec<FixtureRecord>,
}

#[derive(Deserialize)]
struct FixtureRecord {
    id: String,
    rows: u64,
    dtype: Value,
    native_preserving_fields: Vec<NativeField>,
    arrow_preserving_fields: Vec<ArrowField>,
    dtype_message: FileRecord,
    array_message: FileRecord,
    queries: Vec<QueryReference>,
}

#[derive(Deserialize)]
struct NativeField {
    path: Vec<String>,
    encoding: String,
}

#[derive(Deserialize)]
struct ArrowField {
    path: Vec<String>,
    arrow: String,
    keys: Option<String>,
    ends: Option<String>,
}

#[derive(Deserialize)]
struct FileRecord {
    file: String,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
struct QueryReference {
    id: String,
    projection: Vec<QueryField>,
    dtype: Value,
    rows: u64,
    jsonl_bytes: u64,
    jsonl_sha256: String,
}

#[derive(Deserialize)]
struct QueryField {
    source: String,
    alias: String,
}

fn bounded_read(path: &Path, max: u64) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path).map_err(native_error)?;
    let metadata = file.metadata().map_err(native_error)?;
    if !metadata.is_file() || metadata.len() > max {
        return Err(memory_error(
            "SLX_FIXTURE: input exceeds its admitted file bound",
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).map_err(native_error)?);
    file.take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(native_error)?;
    if bytes.len() as u64 != metadata.len() {
        return Err(memory_error("SLX_FIXTURE: file changed while reading"));
    }
    Ok(bytes)
}

fn verify_hash(bytes: &[u8], expected: &str) -> Result<()> {
    let expected = unhex(expected).map_err(|_| memory_error("SLX_FIXTURE: invalid frozen hash"))?;
    if hash(bytes) != expected {
        return Err(memory_error(
            "SLX_FIXTURE: input differs from frozen identity",
        ));
    }
    Ok(())
}

impl Fixture {
    /// Reads only the checked metadata; the receiver never needs source payload files.
    pub(crate) fn load(
        manifest: &Path,
        source_id: &str,
        authority: &Authority,
        session: &ResidentVortexSession,
    ) -> Result<Self> {
        let metadata = session.reserve_input_scratch(MANIFEST_BYTES)?;
        let bytes = bounded_read(manifest, MANIFEST_BYTES)?;
        verify_hash(&bytes, MANIFEST_SHA)?;
        let manifest: Manifest = serde_json::from_slice(&bytes).map_err(native_error)?;
        if manifest.version != 1 || manifest.cases.len() != 13 {
            return Err(memory_error("SLX_FIXTURE: invalid frozen fixture manifest"));
        }
        let record = manifest
            .cases
            .into_iter()
            .find(|case| case.id == source_id)
            .ok_or_else(|| memory_error("SLX_FIXTURE: source is absent from frozen manifest"))?;
        let sql = authority.sql.clone();
        let authority = authority
            .case(source_id)
            .map_err(|_| memory_error("SLX_FIXTURE: source is absent from authority"))?
            .clone();
        if record.rows != authority.rows
            || record.queries.len() != 3
            || record.dtype_message.file != format!("{source_id}.dtype.vxipc")
            || record.array_message.file != format!("{source_id}.array.vxipc")
        {
            return Err(memory_error("SLX_FIXTURE: manifest coordinates differ"));
        }
        for (query, projection) in record.queries.iter().zip(&authority.operations.projections) {
            if query.id != projection.id
                || query.rows != record.rows
                || query.projection.len() != projection.fields.len()
                || !query
                    .projection
                    .iter()
                    .zip(&projection.fields)
                    .all(|(left, right)| left.source == right.source && left.alias == right.alias)
            {
                return Err(memory_error(
                    "SLX_FIXTURE: frozen query declaration differs",
                ));
            }
        }
        Ok(Self {
            authority,
            sql,
            record,
            _metadata: metadata,
        })
    }

    pub(crate) fn original_source(
        &self,
        root: &Path,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<ResidentMemorySource> {
        let bytes = self
            .record
            .dtype_message
            .bytes
            .checked_add(self.record.array_message.bytes)
            .filter(|bytes| *bytes <= PROVIDER_ALLOWANCE)
            .ok_or_else(|| memory_error("SLX_FIXTURE: source exceeds native input bound"))?;
        let _input = session.reserve_input_scratch(bytes)?;
        cancellation.check()?;
        let mut messages = Vec::with_capacity(2);
        for record in [&self.record.dtype_message, &self.record.array_message] {
            let data = bounded_read(&root.join(&record.file), record.bytes)?;
            if data.len() as u64 != record.bytes {
                return Err(memory_error("SLX_FIXTURE: source length differs"));
            }
            verify_hash(&data, &record.sha256)?;
            messages.push(data);
        }
        let mut digest = Sha256::new();
        for message in &messages {
            digest.update((message.len() as u64).to_le_bytes());
            digest.update(message);
        }
        if &digest.finalize()[..] != self.authority.source_sha256.as_ref()
            || hash(&messages[0]) != self.authority.dtype_sha256
        {
            return Err(memory_error(
                "SLX_FIXTURE: original source authority differs",
            ));
        }
        let source = ResidentMemorySource::from_vortex_ipc(
            session,
            &messages[0],
            &messages[1],
            cancellation,
        )?;
        session.with_native_execution_context(cancellation, |context| {
            prepare_source_statistics(&source.0.array, context)
        })?;
        self.check_source(&source, Mode::NativeFragments)?;
        Ok(source)
    }

    pub(crate) fn check_source(&self, source: &ResidentMemorySource, mode: Mode) -> Result<Value> {
        if source.row_count() as u64 != self.record.rows
            || serde_json::to_value(source.dtype()).map_err(native_error)? != self.record.dtype
        {
            return Err(memory_error(
                "SLX_FIXTURE: received schema or row count differs",
            ));
        }
        let fields = if matches!(mode, Mode::NativeFragments | Mode::NativeAssembled) {
            self.record
                .native_preserving_fields
                .iter()
                .map(|field| (field.path.as_slice(), field.encoding.as_str()))
                .collect::<Vec<_>>()
        } else {
            self.record
                .arrow_preserving_fields
                .iter()
                .map(|field| {
                    let expected = if field.arrow == "Dictionary" {
                        "vortex.dict"
                    } else {
                        "vortex.runend"
                    };
                    (field.path.as_slice(), expected)
                })
                .collect()
        };
        let mut preserved = Vec::new();
        for (path, expected) in fields {
            let ids = encodings_at_path(&source.0.array, path)?;
            if ids.is_empty() || ids.iter().any(|id| id != expected) {
                return Err(memory_error(
                    "SLX_FIXTURE: required physical encoding was lost",
                ));
            }
            preserved.push(json!({"path": path, "native_encodings_below_chunk_containers": ids}));
        }
        Ok(json!(preserved))
    }

    pub(crate) fn consume(
        &self,
        source: &ResidentMemorySource,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let started = Instant::now();
        let prepared = prepare(source, &self.authority.operations)?;
        let preparation_seconds = started.elapsed().as_secs_f64();
        let before = source.0.session.snapshot().completed_executions;
        let mut queries = Vec::new();
        for &index in &self.authority.operations.execution_order {
            let index = usize::try_from(index).map_err(native_error)?;
            let reference = &self.record.queries[index];
            let result = prepared[index].collect_jsonl(cancellation)?;
            let schema: Value =
                serde_json::from_str(result.result_schema_json.value()).map_err(native_error)?;
            let bytes = result.result_jsonl.value().as_bytes();
            let certificate = &result.execution.native_io_certificate;
            if result.execution.output_rows != reference.rows
                || schema != reference.dtype
                || bytes.len() as u64 != reference.jsonl_bytes
                || !certificate.is_certified()
                || certificate.side_effects.fallback_attempted
            {
                return Err(memory_error(
                    "SLX_CONSUMER: complete result, schema or certificate differs",
                ));
            }
            verify_hash(bytes, &reference.jsonl_sha256)?;
            queries.push(
                json!({"query": reference.id, "rows": result.execution.output_rows,
                "jsonl_bytes": bytes.len(), "complete_jsonl_sha256": reference.jsonl_sha256,
                "dtype": schema, "certified": true, "fallback_attempted": false,
                "certificate_id": certificate.certificate_id, "certificate_path_id": certificate.path_id,
                "engine_execution_seconds": result.timing.execution.as_secs_f64(),
                "result_serialization_seconds": result.timing.result_serialization.as_secs_f64()}),
            );
        }
        if source.0.session.snapshot().completed_executions != before + 4 {
            return Err(memory_error(
                "SLX_CONSUMER: repeated projection did not execute again",
            ));
        }
        Ok(
            json!({"preparation_seconds": preparation_seconds, "queries": queries,
            "completed_executions": 4}),
        )
    }

    pub(crate) fn sql_schema(&self, source: &ResidentMemorySource) -> Result<Schema> {
        let operation = self
            .authority
            .operation(SQL_QUERY)
            .map_err(|_| memory_error("SLX_UNSUPPORTED: SQL source is not resident"))?;
        let prepared = prepare(source, &operation)?;
        let dtype = prepared
            .first()
            .and_then(PreparedVortexRelational::output_dtype)
            .ok_or_else(|| memory_error("SLX_SQL: projection has no native schema"))?;
        if serde_json::to_value(&dtype).map_err(native_error)? != self.record.queries[2].dtype {
            return Err(memory_error(
                "SLX_SQL: prepared projection schema differs from reference",
            ));
        }
        let DataType::Struct(fields) = target_type(&dtype, &[], &[])? else {
            return Err(memory_error("SLX_SQL: expected a record result"));
        };
        Ok(Schema::new(fields))
    }

    pub(crate) fn execute_sql_source(
        &self,
        source: &ResidentMemorySource,
        cancellation: &CancellationToken,
    ) -> Result<(ResidentMemorySource, Value)> {
        let operation = self
            .authority
            .operation(SQL_QUERY)
            .map_err(|_| memory_error("SLX_UNSUPPORTED: SQL source is not resident"))?;
        let before = source.0.session.snapshot().completed_executions;
        let mut prepared = prepare(source, &operation)?;
        let prepared = prepared
            .pop()
            .ok_or_else(|| memory_error("SLX_SQL: missing native projection"))?;
        if !prepared.output_dtype().is_some_and(|dtype| {
            serde_json::to_value(dtype).ok().as_ref() == Some(&self.record.queries[2].dtype)
        }) {
            return Err(memory_error("SLX_SQL: projected native schema differs"));
        }
        let execution = prepared.execute_native(cancellation)?;
        let certificate = &execution.execution.native_io_certificate;
        if !certificate.is_certified()
            || certificate.side_effects.fallback_attempted
            || execution.execution.output_rows != self.record.rows
            || source.0.session.snapshot().completed_executions != before + 1
        {
            return Err(memory_error(
                "SLX_SQL: native projection did not execute and certify",
            ));
        }
        let report = json!({"completed_executions": 1, "rows": execution.execution.output_rows,
            "certified": true, "fallback_attempted": false, "certificate_id": certificate.certificate_id,
            "certificate_path_id": certificate.path_id, "native_certificate": format!("{certificate:?}")});
        let projected =
            ResidentMemorySource::from_completed_result(&execution.result, cancellation)?;
        Ok((projected, report))
    }

    pub(crate) fn consume_sql_result(
        &self,
        source: &ResidentMemorySource,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let reference = &self.record.queries[2];
        let operation = Operations {
            projections: vec![Projection {
                id: "received_sql_result".to_owned(),
                fields: reference
                    .projection
                    .iter()
                    .map(|field| ProjectionField {
                        source: field.alias.clone(),
                        alias: field.alias.clone(),
                    })
                    .collect(),
            }],
            execution_order: vec![0],
        };
        let mut prepared = prepare(source, &operation)?;
        let result = prepared
            .pop()
            .ok_or_else(|| memory_error("SLX_SQL: missing received projection"))?
            .collect_jsonl(cancellation)?;
        let schema: Value =
            serde_json::from_str(result.result_schema_json.value()).map_err(native_error)?;
        let bytes = result.result_jsonl.value().as_bytes();
        let certificate = &result.execution.native_io_certificate;
        if result.execution.output_rows != reference.rows
            || schema != reference.dtype
            || bytes.len() as u64 != reference.jsonl_bytes
            || !certificate.is_certified()
            || certificate.side_effects.fallback_attempted
        {
            return Err(memory_error(
                "SLX_SQL: complete received values, schema or certificate differ",
            ));
        }
        verify_hash(bytes, &reference.jsonl_sha256)?;
        Ok(
            json!({"query": reference.id, "rows": result.execution.output_rows,
            "jsonl_bytes": bytes.len(), "complete_jsonl_sha256": reference.jsonl_sha256,
            "dtype": schema, "certified": true, "fallback_attempted": false,
            "certificate_id": certificate.certificate_id, "certificate_path_id": certificate.path_id,
            "engine_execution_seconds": result.timing.execution.as_secs_f64(),
            "result_serialization_seconds": result.timing.result_serialization.as_secs_f64()}),
        )
    }
}

fn prepare_source_statistics(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<()> {
    context.check_cancelled()?;
    for child in array.children_iter() {
        prepare_source_statistics(child, context)?;
    }
    if let Some(run_end) = array.as_opt::<RunEnd>().filter(|array| !array.is_empty()) {
        // Vortex 0.85 computes these statistics only during debug construction;
        // its subsequent `inherit` does not clear newly cached statistics. Make
        // the common, checked source preparation identical in both profiles so
        // the frozen native message remains exact. This derives statistics from
        // validated coordinates; it never installs or trusts wire statistics.
        // Every mode pays this preparation under the same native grant.
        let ends = run_end.ends();
        let bytes = (ends.len() as u64)
            .checked_mul(32)
            .ok_or_else(|| memory_error("SLX_FIXTURE: run-end statistics workspace overflow"))?;
        let _workspace = context.memory().reserve(bytes)?;
        let mut execution = context.native_session().create_execution_ctx();
        if ends.statistics().compute_is_strict_sorted(&mut execution) != Some(true) {
            return Err(memory_error(
                "SLX_FIXTURE: run-end coordinates are not strictly sorted",
            ));
        }
        context.check_cancelled()?;
    }
    Ok(())
}

fn encodings_at_path(array: &ArrayRef, path: &[String]) -> Result<Vec<String>> {
    if let Some(chunked) = array.as_opt::<Chunked>() {
        return chunked
            .chunks()
            .iter()
            .try_fold(Vec::new(), |mut ids, chunk| {
                ids.extend(encodings_at_path(chunk, path)?);
                Ok(ids)
            });
    }
    let Some((name, tail)) = path.split_first() else {
        return Ok(vec![array.encoding_id().as_str().to_owned()]);
    };
    let structure = array
        .as_opt::<Struct>()
        .ok_or_else(|| memory_error("SLX_FIXTURE: expected native record"))?;
    let index = array
        .dtype()
        .as_struct_fields()
        .names()
        .iter()
        .position(|candidate| candidate.as_ref() == name)
        .ok_or_else(|| memory_error("SLX_FIXTURE: preserved field is absent"))?;
    encodings_at_path(&structure.fields()[index], tail)
}

fn project(input: VortexRelationalPlan, projection: &Projection) -> Result<VortexRelationalPlan> {
    let expressions = projection
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Ok((
                field.alias.clone(),
                Expression::new(
                    ExprId::new(format!("exchange-field-{index}"))?,
                    ExpressionKind::Column(ColumnRef::new(&field.source)?),
                ),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(VortexRelationalPlan::Project(Box::new(
        VortexRelationalProject { input, expressions },
    )))
}

pub(crate) fn prepare(
    source: &ResidentMemorySource,
    operations: &Operations,
) -> Result<Vec<PreparedVortexRelational>> {
    let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(GRANT, 1, ExecutionResourceOrigin::Context)?,
    )?;
    let uri = DatasetUri::new("memory://independent-exchange-fixture")?;
    operations
        .projections
        .iter()
        .map(|projection| {
            prepare_relational_with_schema_in_session(policy, &source.0.session, |schemas| {
                schemas.register_memory_source(uri.clone(), |_| Ok(source.clone()))?;
                project(
                    VortexRelationalPlan::Scan(VortexRelationalScan {
                        source_uri: uri.clone(),
                        projection: ProjectionRequest::All,
                        predicate: None,
                    }),
                    projection,
                )
            })
        })
        .collect()
}

struct ProviderOwner {
    _source: ResidentMemorySource,
    _allowance: MemoryLease,
}

struct CreditedBytes {
    bytes: Bytes,
    _owner: Arc<ProviderOwner>,
}

impl AsRef<[u8]> for CreditedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn retain(bytes: Bytes, owner: &Arc<ProviderOwner>) -> Bytes {
    if bytes.is_empty() {
        return bytes;
    }
    Bytes::from_owner(CreditedBytes {
        bytes,
        _owner: owner.clone(),
    })
}

pub(crate) struct NativeEncoding {
    pub(crate) messages: [Vec<Bytes>; 2],
    pub(crate) assembly_bytes: u64,
}

pub(crate) fn encode_native(
    source: &ResidentMemorySource,
    mode: Mode,
    cancellation: &CancellationToken,
) -> Result<NativeEncoding> {
    if !matches!(mode, Mode::NativeFragments | Mode::NativeAssembled) {
        return Err(memory_error(
            "SLX_MODE: native encoder requires explicit native mode",
        ));
    }
    let allowance = source.0.session.reserve_input_scratch(PROVIDER_ALLOWANCE)?;
    let messages = source
        .0
        .session
        .with_native_execution_context(cancellation, |context| {
            let mut encoder = MessageEncoder::new(context.native_session().clone());
            Ok([
                encoder
                    .encode(EncoderMessage::DType(source.dtype()))
                    .map_err(native_error)?,
                encoder
                    .encode(EncoderMessage::Array(&source.0.array))
                    .map_err(native_error)?,
            ])
        })?;
    let total = messages.iter().flatten().try_fold(0_u64, |total, bytes| {
        total
            .checked_add(bytes.len() as u64)
            .filter(|total| *total <= PROVIDER_ALLOWANCE)
            .ok_or_else(|| {
                memory_error("SLX_RESOURCE_LIMIT: native encoding exceeds payload limit")
            })
    })?;
    let owner = Arc::new(ProviderOwner {
        _source: source.clone(),
        _allowance: allowance,
    });
    let messages = if mode == Mode::NativeAssembled {
        let copy = source.0.session.reserve_input_scratch(total)?;
        let copy_owner = Arc::new(ProviderOwner {
            _source: source.clone(),
            _allowance: copy,
        });
        messages.map(|parts| vec![retain(Bytes::from(parts.concat()), &copy_owner)])
    } else {
        messages.map(|parts| {
            parts
                .into_iter()
                .map(|bytes| retain(bytes, &owner))
                .collect()
        })
    };
    Ok(NativeEncoding {
        messages,
        assembly_bytes: if mode == Mode::NativeAssembled {
            total
        } else {
            0
        },
    })
}

fn target_type(dtype: &DType, path: &[String], preserving: &[ArrowField]) -> Result<DataType> {
    if let Some(field) = preserving.iter().find(|field| field.path == path) {
        let value = crate::native_arrow_conversion::data_type(dtype)
            .ok_or_else(|| memory_error("SLX_FIXTURE: native type has no admitted Arrow target"))?;
        return match (
            field.arrow.as_str(),
            field.keys.as_deref(),
            field.ends.as_deref(),
        ) {
            ("Dictionary", Some("UInt16"), None) => Ok(DataType::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(value),
            )),
            ("RunEndEncoded", None, Some("Int32")) => Ok(DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", value, dtype.is_nullable())),
            )),
            _ => Err(memory_error(
                "SLX_FIXTURE: unregistered preserving Arrow type",
            )),
        };
    }
    if let DType::Struct(fields, _) = dtype {
        return Ok(DataType::Struct(
            fields
                .names()
                .iter()
                .zip(fields.fields())
                .map(|(name, dtype)| {
                    let mut path = path.to_vec();
                    path.push(name.to_string());
                    Ok(Field::new(
                        name.to_string(),
                        target_type(&dtype, &path, preserving)?,
                        dtype.is_nullable(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .into(),
        ));
    }
    crate::native_arrow_conversion::data_type(dtype)
        .ok_or_else(|| memory_error("SLX_FIXTURE: native type has no admitted Arrow target"))
}

pub(crate) struct ArrowEncoding {
    encoder: FlightDataEncoder,
    owner: Arc<ProviderOwner>,
}

impl ArrowEncoding {
    pub(crate) async fn next(&mut self) -> Result<Option<FlightData>> {
        let Some(mut data) = self.encoder.try_next().await.map_err(native_error)? else {
            return Ok(None);
        };
        data.data_header = retain(data.data_header, &self.owner);
        data.data_body = retain(data.data_body, &self.owner);
        Ok(Some(data))
    }
}

pub(crate) fn encode_arrow(
    source: &ResidentMemorySource,
    fixture: &Fixture,
    mode: Mode,
    cancellation: &CancellationToken,
) -> Result<ArrowEncoding> {
    encode_arrow_with_fields(
        source,
        &fixture.record.arrow_preserving_fields,
        mode,
        cancellation,
    )
}

pub(crate) fn encode_sql(
    source: &ResidentMemorySource,
    cancellation: &CancellationToken,
) -> Result<ArrowEncoding> {
    encode_arrow_with_fields(source, &[], Mode::ArrowNone, cancellation)
}

fn encode_arrow_with_fields(
    source: &ResidentMemorySource,
    preserving: &[ArrowField],
    mode: Mode,
    cancellation: &CancellationToken,
) -> Result<ArrowEncoding> {
    let compression = match mode {
        Mode::ArrowNone => None,
        Mode::ArrowLz4 => Some(CompressionType::LZ4_FRAME),
        Mode::ArrowZstd => Some(CompressionType::ZSTD),
        _ => {
            return Err(memory_error(
                "SLX_MODE: Arrow encoder requires explicit Arrow mode",
            ));
        }
    };
    let allowance = source.0.session.reserve_input_scratch(PROVIDER_ALLOWANCE)?;
    let target = target_type(source.dtype(), &[], preserving)?;
    let DataType::Struct(fields) = target.clone() else {
        return Err(memory_error("SLX_FIXTURE: expected Arrow record schema"));
    };
    let schema = Arc::new(Schema::new(fields));
    let exported = source
        .0
        .session
        .with_native_execution_context(cancellation, |context| {
            let mut execution = context.native_session().create_execution_ctx();
            let target = Field::new("", target, false);
            let array = context
                .native_session()
                .arrow()
                .execute_arrow(source.0.array.clone(), Some(&target), &mut execution)
                .map_err(native_error)?;
            array.to_data().validate_full().map_err(native_error)?;
            let structure = array
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .ok_or_else(|| memory_error("SLX_FIXTURE: expected Arrow record result"))?;
            RecordBatch::try_new(schema, structure.columns().to_vec()).map_err(native_error)
        })?;
    let options = IpcWriteOptions::default()
        .try_with_compression(compression)
        .map_err(native_error)?;
    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(exported.schema())
        .with_dictionary_handling(DictionaryHandling::Resend)
        .with_max_flight_data_size(1 << 20)
        .with_options(options)
        .build(futures::stream::iter(vec![Ok(exported)]));
    Ok(ArrowEncoding {
        encoder,
        owner: Arc::new(ProviderOwner {
            _source: source.clone(),
            _allowance: allowance,
        }),
    })
}

/// Only call after every frame passed the frozen identity validator and clean RPC EOF.
pub(crate) async fn decode_arrow(
    session: &ResidentVortexSession,
    frames: Vec<FlightData>,
    cancellation: &CancellationToken,
) -> Result<ResidentMemorySource> {
    let _decode = session.reserve_input_scratch(PROVIDER_ALLOWANCE)?;
    let mut builder = super::MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
    let mut decoder = FlightRecordBatchStream::new_from_flight_data(futures::stream::iter(
        frames.into_iter().map(Ok),
    ));
    let mut batches = 0_u64;
    while let Some(batch) = decoder.try_next().await.map_err(native_error)? {
        cancellation.check()?;
        builder.push_arrow(batch)?;
        batches += 1;
    }
    if batches == 0 {
        let schema = decoder
            .schema()
            .ok_or_else(|| memory_error("SLX_INCOMPLETE: missing Arrow schema"))?;
        builder.push_arrow(RecordBatch::new_empty(schema.clone()))?;
    }
    builder.finish()
}

#[cfg(test)]
#[path = "resident_memory_exchange_bridge_tests.rs"]
mod tests;
