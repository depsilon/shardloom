//! Ignored, bounded `ClickBench` source-dictionary admission screen.
//!
//! This measures the real Parquet reader, Arrow-to-Vortex conversion, and
//! native writer for the fixed R1.b sample. It is deliberately a test-only
//! experiment: it does not select a production dictionary policy or claim
//! complete-ingest CPU or speedup.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow_schema::{DataType as ArrowDataType, Schema};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
};
use serde_json::{Value, json};
use sha2::Digest as _;
use vortex::{
    array::{
        ArrayRef as VortexArrayRef, IntoArray as _, VTable as _, VortexSessionExecute as _,
        arrays::{
            Dict, DictArray, Struct, StructArray, VarBinViewArray, dict::DictArraySlotsExt as _,
            struct_::StructArrayExt as _, varbinview::VarBinViewArrayExt as _,
        },
        dtype::{FieldNames, FieldPath},
        iter::ArrayIteratorAdapter,
    },
    editions::EditionSessionExt as _,
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

const SOURCE_ENV: &str = "SHARDLOOM_R1B_SOURCE";
const ROW_GROUPS: [usize; 3] = [0, 113, 225];
const INPUT_BATCH_ROWS: usize = 131_072;
const WRITER_ROW_BLOCK_SIZE: usize = 262_144;
const TOTAL_ROW_LIMIT: usize = INPUT_BATCH_ROWS * ROW_GROUPS.len();
const COLUMN_NAMES: [&str; 5] = ["URL", "Referer", "SearchPhrase", "Title", "OriginalURL"];
const MEMORY_LIMIT_BYTES: u64 = 512 << 20;
const COLUMN_FILE_LIMIT_BYTES: usize = 64 << 20;
const MAX_STDOUT_BYTES: usize = 2 << 20;
const WRITER_BLOCK_TARGET_BYTES: u64 = 8 << 20;

#[derive(Clone, Copy)]
enum InputRole {
    Plain,
    DictionaryHint,
}

impl InputRole {
    const fn name(self) -> &'static str {
        match self {
            Self::Plain => "plain_arrow_schema",
            Self::DictionaryHint => "selected_fields_arrow_dictionary_int32_utf8",
        }
    }

    const fn dictionary_hint(self) -> bool {
        matches!(self, Self::DictionaryHint)
    }
}

#[derive(Clone, Copy)]
enum WriterCase {
    Retained,
    PreserveRawDomain,
    PreserveZstdDomain,
    PreserveFsst,
}

impl WriterCase {
    const fn name(self) -> &'static str {
        match self {
            Self::Retained => "retained_zstd",
            Self::PreserveRawDomain => "preserve_raw_domain",
            Self::PreserveZstdDomain => "preserve_zstd_domain",
            Self::PreserveFsst => "preserve_fsst",
        }
    }

    const fn preserve(self) -> bool {
        !matches!(self, Self::Retained)
    }
}

struct PreparedReader {
    role: InputRole,
    preparation_elapsed_micros: u64,
    metadata: ArrowReaderMetadata,
    projection_roots: Vec<usize>,
    columns: Vec<String>,
}

struct LimitedWrite<'a> {
    inner: &'a mut Vec<u8>,
    written: usize,
    limit: usize,
}

impl Write for LimitedWrite<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("bounded native artifact byte count overflow"))?;
        if next > self.limit {
            return Err(io::Error::other(format!(
                "native per-column artifact exceeded its {} byte limit",
                self.limit
            )));
        }
        let written = self.inner.write(bytes)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[test]
#[ignore = "manual bounded R1.b source-dictionary admission screen; requires SHARDLOOM_R1B_SOURCE"]
#[allow(clippy::too_many_lines)]
fn clickbench_source_dictionary_admission_screen() {
    let source = std::env::var_os(SOURCE_ENV).map_or_else(
        || panic!("{SOURCE_ENV} must name the official hits.parquet source"),
        PathBuf::from,
    );
    assert!(
        source.is_file(),
        "source is not a regular file: {}",
        source.display()
    );

    let runtime = CurrentThreadRuntime::new();
    let mut memory = super::super::NativeIngestMemory::new(MEMORY_LIMIT_BYTES).unwrap();
    memory.session = memory.session.with_handle(runtime.handle());
    let plain = prepare_reader(&source, InputRole::Plain, &COLUMN_NAMES);
    let hinted = prepare_reader(&source, InputRole::DictionaryHint, &COLUMN_NAMES);

    let roles = [plain, hinted];
    let mut samples = Vec::with_capacity(ROW_GROUPS.len());
    let mut total_rows = 0_usize;

    for (region_index, row_group) in ROW_GROUPS.into_iter().enumerate() {
        let order = if region_index.is_multiple_of(2) {
            [0, 1]
        } else {
            [1, 0]
        };
        let mut role_samples = Vec::with_capacity(2);
        let mut reference_columns = Vec::<VarBinViewArray>::new();
        for role_index in order {
            let prepared = &roles[role_index];
            let (native, input_sample) = read_and_convert(&source, row_group, prepared, &memory);
            total_rows = total_rows
                .checked_add(input_sample.rows)
                .expect("selected row count overflow");
            assert_eq!(input_sample.rows, INPUT_BATCH_ROWS);

            let mut columns = Vec::with_capacity(COLUMN_NAMES.len());
            for (field_index, column_name) in prepared.columns.iter().enumerate() {
                let source_column = native.as_::<Struct>().unmasked_field(field_index).clone();
                let input_column = input_column_evidence(
                    column_name,
                    &source_column,
                    prepared.role,
                    &memory.session,
                );
                let expected = source_column
                    .clone()
                    .execute::<VarBinViewArray>(&mut memory.session.create_execution_ctx())
                    .unwrap();
                if let Some(reference) = reference_columns.get(field_index) {
                    assert_eq!(reference.len(), expected.len());
                    assert_column_range_equal(reference, &expected, 0, &memory);
                } else {
                    reference_columns.push(expected.clone());
                }
                let admitted = input_column["dictionary_admission_inputs"]
                    .as_object()
                    .unwrap()
                    .values()
                    .all(|value| value.as_bool() == Some(true));
                let writer_cases = if prepared.role.dictionary_hint() {
                    if region_index.is_multiple_of(2) {
                        vec![
                            WriterCase::Retained,
                            WriterCase::PreserveRawDomain,
                            WriterCase::PreserveZstdDomain,
                        ]
                    } else {
                        vec![
                            WriterCase::PreserveZstdDomain,
                            WriterCase::PreserveRawDomain,
                            WriterCase::Retained,
                        ]
                    }
                } else {
                    vec![WriterCase::Retained]
                };
                let mut cases = Vec::new();
                for writer_case in writer_cases {
                    let preparation_started = Instant::now();
                    let writer_input = if matches!(writer_case, WriterCase::PreserveZstdDomain) {
                        compress_dictionary_domain(&source_column, &memory)
                    } else {
                        source_column.clone()
                    };
                    let preparation_micros =
                        u64::try_from(preparation_started.elapsed().as_micros()).unwrap();
                    let writer_input_evidence = input_column_evidence(
                        column_name,
                        &writer_input,
                        prepared.role,
                        &memory.session,
                    );
                    let writer_admitted = writer_input_evidence["dictionary_admission_inputs"]
                        .as_object()
                        .unwrap()
                        .values()
                        .all(|value| value.as_bool() == Some(true));
                    let (artifact, writer_sample, _bytes) = write_and_verify_column(
                        column_name,
                        &writer_input,
                        &expected,
                        &memory,
                        &runtime,
                        writer_case,
                    );
                    let preserve_calls = writer_sample["writer_evidence_fields"]
                        ["vortex_ingest_text_dictionary_preserve_calls"]
                        .as_str().unwrap().parse::<u64>().unwrap();
                    assert_eq!(
                        preserve_calls > 0,
                        writer_case.preserve() && writer_admitted
                    );
                    if preserve_calls > 0 {
                        assert_eq!(artifact["physical_dictionary_retained"], true);
                    }
                    cases.push(json!({
                        "case": writer_case.name(),
                        "preserve_economical_source_dictionary_before_retained_zstd": writer_case.preserve(),
                        "dictionary_domain_preparation_micros": preparation_micros,
                        "writer_input": writer_input_evidence,
                        "writer_input_admitted_by_existing_bound": writer_admitted,
                        "native_artifact": artifact,
                        "writer": writer_sample,
                    }));
                }
                columns.push(json!({
                    "input": input_column,
                    "admitted_by_existing_bound": admitted,
                    "writer_cases": cases,
                }));
            }
            role_samples.push(json!({
                "role": prepared.role.name(),
                "reader_arrow_to_vortex_elapsed_micros": input_sample.elapsed_micros,
                "rows": input_sample.rows,
                "reader_batch_columns": input_sample.columns,
                "native_struct_nbytes": native.nbytes(),
                "columns": columns,
            }));
            drop(native);
        }
        drop(reference_columns);
        let final_reserved_bytes = memory.pool.snapshot().reserved_bytes;
        assert_eq!(final_reserved_bytes, 0);
        samples.push(json!({
            "row_group": row_group,
            "sample_rows_per_role": INPUT_BATCH_ROWS,
            "role_order": role_samples.iter().map(|sample| sample["role"].clone()).collect::<Vec<_>>(),
            "role_samples": role_samples,
            "both_reader_roles_exactly_equal": true,
            "reserved_bytes_after_all_region_owners_released": final_reserved_bytes,
        }));
    }

    assert_eq!(total_rows, TOTAL_ROW_LIMIT * 2);
    let result = json!({
        "schema_version": "shardloom.r1b_source_dictionary_admission_screen.v2",
        "source": source.display().to_string(),
        "source_role_policy": "same source and row groups; no datatype/name-based production admission rule",
        "roles": [InputRole::Plain.name(), InputRole::DictionaryHint.name()],
        "selected_columns": COLUMN_NAMES,
        "dictionary_hint": "Arrow Dictionary<Int32, Utf8> for the five selected columns only",
        "reader": {
            "role_metadata_preparation_micros": roles.iter().map(|prepared| json!({
                "role": prepared.role.name(),
                "elapsed_micros": prepared.preparation_elapsed_micros,
            })).collect::<Vec<_>>(),
            "arrow_metadata_reused_per_role": true,
            "skip_arrow_metadata": true,
            "projection_mask": "Parquet ProjectionMask::roots for the five selected source fields",
            "row_groups": ROW_GROUPS,
            "first_rows_per_group": INPUT_BATCH_ROWS,
            "total_rows_per_role": TOTAL_ROW_LIMIT,
        },
        "native_writer": {
            "retained_strategy": "large_source_text_vortex_write_strategy_with_dictionaries;explicit_source_text_Zstd_field_override",
            "candidate_strategy": "same_table_with_DictionaryPreservingStrategy_wrapping_source_text_Zstd_field_override",
            "cases": "plain+retained_Zstd;dictionary_hint+retained_Zstd;dictionary_hint+bounded_preservation_before_Zstd;dictionary_hint+Zstd_domain_preparation+bounded_preservation_before_Zstd",
            "default_leaf_preserve_input_dictionaries": true,
            "input_batch_rows": INPUT_BATCH_ROWS,
            "row_block_size": WRITER_ROW_BLOCK_SIZE,
            "block_target_bytes": WRITER_BLOCK_TARGET_BYTES,
            "stats_concurrency": 1,
            "native_ingest_memory_limit_bytes": MEMORY_LIMIT_BYTES,
            "per_column_artifact_limit_bytes": COLUMN_FILE_LIMIT_BYTES,
            "stdout_limit_bytes": MAX_STDOUT_BYTES,
            "output_payloads_retained_on_disk": false,
        },
        "memory_boundary": "Native writer and copied native input buffers use NativeIngestMemory; Parquet provider/source Arrow, capped output Vec and reopen/verification buffers may remain outside reservations. Serialized byte caps and reservations are not process RSS bounds.",
        "timing_boundary": "metadata/schema setup recorded once per role; reader build, selected Arrow batch read, and Arrow-to-Vortex conversion once per role and row group; domain preparation and native column writing separately per artifact. Domain preparation includes canonicalization, compaction and Zstd compression for the compressed-domain arm. Canonical verification, encoding inspection and checksums are outside measured spans; no durable or complete ingest claim.",
        "claims": {
            "complete_ingest_cpu": false,
            "speedup_decision": false,
            "all_samples_retained": true,
            "total_rows_read_and_verified_per_role": total_rows / 2,
        },
        "samples": samples,
    });
    let line = format!("SHARDLOOM_R1B_SCREEN={result}\n");
    assert!(
        line.len() <= MAX_STDOUT_BYTES,
        "screen output exceeded stdout limit"
    );
    print!("{line}");
}

fn compress_dictionary_domain(
    input: &VortexArrayRef,
    memory: &super::super::NativeIngestMemory,
) -> VortexArrayRef {
    let dictionary = input.as_::<Dict>();
    if dictionary.values().is_empty() {
        return input.clone();
    }
    let mut ctx = memory.session.create_execution_ctx();
    let canonical = dictionary
        .values()
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let compact = canonical.compact_buffers(&mut ctx).unwrap();
    let values_per_frame = WRITER_ROW_BLOCK_SIZE.clamp(
        1,
        super::super::VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_ZSTD_VALUES_PER_FRAME,
    );
    let compressed = vortex_zstd::Zstd::from_var_bin_view_without_dict(
        &compact,
        super::super::VORTEX_PREPARED_OLAP_WRITER_SOURCE_TEXT_ZSTD_FAST_LEVEL,
        values_per_frame,
        &mut ctx,
    )
    .unwrap()
    .into_array();
    DictArray::try_new(dictionary.codes().clone(), compressed)
        .unwrap()
        .into_array()
}

fn prepare_reader(path: &Path, role: InputRole, column_names: &[&str]) -> PreparedReader {
    let started = Instant::now();
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let metadata_builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        File::open(path).unwrap(),
        options.clone(),
    )
    .unwrap_or_else(|error| panic!("failed to read Parquet metadata: {error}"));
    let source_schema = Arc::clone(metadata_builder.schema());
    let mut selected = column_names
        .iter()
        .map(|name| {
            (
                source_schema
                    .index_of(name)
                    .unwrap_or_else(|error| panic!("required ClickBench column {name}: {error}")),
                (*name).to_owned(),
            )
        })
        .collect::<Vec<_>>();
    selected.sort_by_key(|(index, _)| *index);

    let mut fields = source_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    if role.dictionary_hint() {
        for (_, name) in &selected {
            let index = source_schema.index_of(name).unwrap();
            assert_eq!(
                fields[index].data_type(),
                &ArrowDataType::Utf8,
                "dictionary hint expects UTF-8 source field {name}"
            );
            fields[index] = fields[index]
                .clone()
                .with_data_type(ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::Int32),
                    Box::new(ArrowDataType::Utf8),
                ));
        }
    }
    let hinted_schema = Arc::new(Schema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    let role_options = if role.dictionary_hint() {
        options.with_schema(Arc::clone(&hinted_schema))
    } else {
        options
    };
    let metadata =
        ArrowReaderMetadata::try_new(Arc::clone(metadata_builder.metadata()), role_options)
            .unwrap_or_else(|error| {
                panic!("failed to create reusable Arrow reader metadata: {error}")
            });
    PreparedReader {
        role,
        preparation_elapsed_micros: u64::try_from(started.elapsed().as_micros()).unwrap(),
        metadata,
        projection_roots: selected.iter().map(|(index, _)| *index).collect(),
        columns: selected.into_iter().map(|(_, name)| name).collect(),
    }
}

struct InputSample {
    elapsed_micros: u64,
    rows: usize,
    columns: usize,
}

fn read_and_convert(
    source: &Path,
    row_group: usize,
    prepared: &PreparedReader,
    memory: &super::super::NativeIngestMemory,
) -> (VortexArrayRef, InputSample) {
    let started = Instant::now();
    let file = File::open(source).unwrap();
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(file, prepared.metadata.clone());
    let projection = ProjectionMask::roots(
        builder.parquet_schema(),
        prepared.projection_roots.iter().copied(),
    );
    let mut reader = builder
        .with_row_groups(vec![row_group])
        .with_batch_size(INPUT_BATCH_ROWS)
        .with_projection(projection)
        .build()
        .unwrap_or_else(|error| panic!("failed to build projected row-group reader: {error}"));
    let batch = reader
        .next()
        .unwrap_or_else(|| panic!("row group {row_group} returned no Arrow batch"))
        .unwrap_or_else(|error| panic!("failed to read row group {row_group}: {error}"));
    assert_eq!(batch.num_rows(), INPUT_BATCH_ROWS);
    assert_eq!(batch.num_columns(), prepared.columns.len());
    for (index, name) in prepared.columns.iter().enumerate() {
        assert_eq!(batch.schema().field(index).name(), name);
        if prepared.role.dictionary_hint() {
            assert_eq!(
                batch.schema().field(index).data_type(),
                &ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::Int32),
                    Box::new(ArrowDataType::Utf8),
                )
            );
        } else {
            assert_eq!(
                batch.schema().field(index).data_type(),
                &ArrowDataType::Utf8
            );
        }
    }
    let shape = super::super::FlatColumnarSourceShape {
        projected_columns: prepared
            .columns
            .iter()
            .enumerate()
            .map(
                |(reader_index, name)| super::super::ColumnarProjectedColumn {
                    column: name.clone(),
                    reader_index,
                    dtype_hint: None,
                    arrow_dtype_hint: None,
                },
            )
            .collect(),
    };
    let admission_bytes = super::super::arrow_ownership::batch_copy_allocation_bytes(&batch)
        .unwrap_or_else(|error| panic!("failed to size Arrow-to-native copy: {error}"));
    let mut lease = memory
        .pool
        .reserve(admission_bytes.checked_mul(2).unwrap())
        .unwrap_or_else(|error| panic!("Arrow-to-native copy exceeds bounded memory: {error}"));
    let native = super::super::record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
        &batch,
        &shape,
        &super::IngestStageTimings::default(),
        Some((memory, &mut lease)),
    )
    .unwrap_or_else(|error| panic!("Arrow-to-native conversion failed: {error}"));
    drop(lease);
    (
        native,
        InputSample {
            elapsed_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            rows: batch.num_rows(),
            columns: batch.num_columns(),
        },
    )
}

fn input_column_evidence(
    name: &str,
    column: &VortexArrayRef,
    role: InputRole,
    session: &vortex::session::VortexSession,
) -> Value {
    let is_dictionary = column.is::<Dict>();
    let dictionary = column.as_opt::<Dict>();
    let dictionary_domain_len = dictionary.map(|dict| dict.values().len());
    let allowed = session.enabled_component_ids(vortex::editions::ComponentKind::Array);
    let values_allowed = dictionary.is_some_and(|dict| {
        dict.values()
            .depth_first_traversal()
            .all(|value| value.is_canonical() || allowed.contains(&value.encoding_id()))
    });
    let rows = column.len();
    let native_nbytes = column.nbytes();
    json!({
        "column": name,
        "role": role.name(),
        "dtype": format!("{}", column.dtype()),
        "encoding_id": column.encoding_id().to_string(),
        "rows": rows,
        "native_nbytes": native_nbytes,
        "dictionary_domain_len": dictionary_domain_len,
        "dictionary_admission_inputs": {
            "dict_component_enabled": allowed.contains(&Dict.id()),
            "logical_utf8": column.dtype().is_utf8(),
            "native_dict_array": is_dictionary,
            "nonempty": rows > 0,
            "within_row_block": rows <= WRITER_ROW_BLOCK_SIZE,
            "domain_len_fits_u32": dictionary_domain_len.is_some_and(|len| u32::try_from(len).is_ok()),
            "dictionary_values_canonical_or_session_allowed": values_allowed,
            "nbytes_below_rows_times_16": native_nbytes < (rows as u64).saturating_mul(16),
        },
    })
}

#[allow(clippy::too_many_lines)] // Keep bounded writing, reopen and exact verification in one fixture.
fn write_and_verify_column(
    column_name: &str,
    input_column: &VortexArrayRef,
    expected: &VarBinViewArray,
    memory: &super::super::NativeIngestMemory,
    runtime: &CurrentThreadRuntime,
    writer_case: WriterCase,
) -> (Value, Value, Vec<u8>) {
    let single_column = StructArray::try_new(
        FieldNames::from([column_name]),
        vec![input_column.clone()],
        input_column.len(),
        vortex::array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dtype = single_column.dtype().clone();
    let writer_timing = super::super::VortexWriterStageTiming::default();
    let strategy = if matches!(writer_case, WriterCase::PreserveFsst) {
        use vortex::layout::layouts::{
            chunked::writer::ChunkedLayoutStrategy, flat::writer::FlatLayoutStrategy,
        };
        Arc::new(
            super::super::large_source_fast_load_table_strategy_with_dictionaries(
                WRITER_ROW_BLOCK_SIZE,
                WRITER_BLOCK_TARGET_BYTES,
                1,
                &writer_timing,
                &memory.session,
                true,
            )
            .with_field_writer(
                FieldPath::from_name(column_name),
                Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default())),
            ),
        ) as Arc<dyn vortex::layout::LayoutStrategy>
    } else if writer_case.preserve() {
        let text = super::super::large_source_fast_zstd_text_leaf_strategy(
            WRITER_ROW_BLOCK_SIZE,
            1,
            &writer_timing,
        );
        let preserved = super::DictionaryPreservingStrategy::new(
            text,
            WRITER_ROW_BLOCK_SIZE,
            writer_timing.stages.clone(),
            &memory.session,
        );
        Arc::new(
            super::super::large_source_fast_load_table_strategy_with_dictionaries(
                WRITER_ROW_BLOCK_SIZE,
                WRITER_BLOCK_TARGET_BYTES,
                1,
                &writer_timing,
                &memory.session,
                true,
            )
            .with_field_writer(FieldPath::from_name(column_name), Arc::new(preserved)),
        ) as Arc<dyn vortex::layout::LayoutStrategy>
    } else {
        super::super::large_source_text_vortex_write_strategy_with_dictionaries(
            WRITER_ROW_BLOCK_SIZE,
            WRITER_BLOCK_TARGET_BYTES,
            1,
            1,
            &[column_name.to_owned()],
            &writer_timing,
            &memory.session,
            true,
        )
    };
    let bounded = super::super::bounded_ingest_layout::BoundedIngestLayout::new(
        strategy,
        0,
        memory.pool.reserve(0).unwrap(),
    );
    let options = memory
        .session
        .write_options()
        .with_strategy(Arc::new(bounded));
    let mut bytes = Vec::new();
    let write_started = Instant::now();
    let summary = options
        .blocking(runtime)
        .write(
            LimitedWrite {
                inner: &mut bytes,
                written: 0,
                limit: COLUMN_FILE_LIMIT_BYTES,
            },
            ArrayIteratorAdapter::new(dtype.clone(), [Ok(single_column.clone())].into_iter()),
        )
        .unwrap_or_else(|error| panic!("native write for {column_name} failed: {error}"));
    let write_elapsed_micros =
        u64::try_from(write_started.elapsed().as_micros()).unwrap_or(u64::MAX);
    assert_eq!(summary.row_count(), INPUT_BATCH_ROWS as u64);
    drop(summary);
    assert!(bytes.len() <= COLUMN_FILE_LIMIT_BYTES);

    let file = memory
        .session
        .open_options()
        .open_buffer(bytes.clone())
        .unwrap_or_else(|error| panic!("native artifact reopen for {column_name} failed: {error}"));
    assert_eq!(file.dtype(), &dtype);
    assert_eq!(file.row_count(), INPUT_BATCH_ROWS as u64);
    let inventory = runtime
        .block_on(
            crate::physical_encoding_inventory::inspect_physical_encodings(
                &file,
                crate::physical_encoding_inventory::PhysicalEncodingInspectionLimits {
                    max_layout_nodes: 4096,
                    max_flat_references: 1024,
                    max_segment_bytes: COLUMN_FILE_LIMIT_BYTES as u64,
                    max_total_segment_bytes: COLUMN_FILE_LIMIT_BYTES as u64,
                    max_array_nodes: 8192,
                },
            ),
        )
        .unwrap_or_else(|error| panic!("physical inventory for {column_name} failed: {error}"));
    assert_eq!(inventory["complete_flat_inspection"], true);
    let column_inventory = inventory["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| {
            entry["column_path"] == json!([column_name]) && entry["auxiliary_path"] == json!([])
        })
        .cloned()
        .unwrap_or_else(|| panic!("physical inventory omitted {column_name}"));
    let dictionary_retained = column_inventory["encoding_ids"]
        .as_array()
        .is_some_and(|encodings| encodings.iter().any(|encoding| encoding == "vortex.dict"));

    let mut seen = 0_usize;
    for output_batch in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let mut ctx = memory.session.create_execution_ctx();
        let output_batch = output_batch
            .unwrap()
            .execute::<StructArray>(&mut ctx)
            .unwrap();
        let actual = output_batch
            .unmasked_field(0)
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        assert_column_range_equal(expected, &actual, seen, memory);
        seen += actual.len();
    }
    assert_eq!(seen, INPUT_BATCH_ROWS);
    let counters = writer_timing
        .stages
        .snapshot()
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let artifact = json!({
        "dtype": format!("{}", file.dtype()),
        "rows": file.row_count(),
        "complete_artifact_bytes": bytes.len(),
        "sha256": hex_digest(&sha2::Sha256::digest(&bytes)),
        "all_rows_exactly_equal_to_in_memory_native_input": true,
        "compared_rows": seen,
        "comparison_contract": "decode each block once, compare every UTF8 byte slice and validity exactly",
        "physical_dictionary_retained": dictionary_retained,
        "physical_column_inventory": column_inventory,
    });
    let writer_sample = json!({
        "complete_native_artifact_write_elapsed_micros": write_elapsed_micros,
        "writer_evidence_fields": counters,
        "reserved_bytes_with_input_and_verification_owners_retained": memory.pool.snapshot().reserved_bytes,
    });
    (artifact, writer_sample, bytes)
}

#[path = "vortex_ingest_source_fsst_bench.rs"]
mod fsst_screen;

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{byte:02x}").unwrap();
    }
    result
}

fn assert_column_range_equal(
    expected: &VarBinViewArray,
    actual: &VarBinViewArray,
    start: usize,
    memory: &super::super::NativeIngestMemory,
) {
    assert_eq!(expected.dtype(), actual.dtype());
    assert!(start.checked_add(actual.len()).unwrap() <= expected.len());
    let mut ctx = memory.session.create_execution_ctx();
    let expected_valid = expected
        .varbinview_validity()
        .execute_mask(expected.len(), &mut ctx)
        .unwrap();
    let actual_valid = actual
        .varbinview_validity()
        .execute_mask(actual.len(), &mut ctx)
        .unwrap();
    for row in 0..actual.len() {
        assert_eq!(expected_valid.value(start + row), actual_valid.value(row));
        if actual_valid.value(row) {
            assert_eq!(expected.bytes_at(start + row), actual.bytes_at(row));
        }
    }
}
