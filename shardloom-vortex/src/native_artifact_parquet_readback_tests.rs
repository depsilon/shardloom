use super::*;
use arrow_array::{
    ArrayRef as ArrowArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{
    fs::{self, File, OpenOptions},
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use vortex::{
    VortexSessionDefault as _,
    arrow::ArrowSessionExt as _,
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
        session::RuntimeSessionExt as _,
    },
};

const ROWS: usize = 29;
const SOURCE_ROWS: usize = 5;
const NATIVE_CHUNK_ROWS: usize = 7;
const URLS: [Option<&str>; SOURCE_ROWS] = [
    Some("HTTP://user@WWW.東京.example:443/p"),
    Some(""),
    Some("https://[2001:db8::1]:80"),
    None,
    Some("1abc://x/p"),
];
const REFERERS: [Option<&str>; SOURCE_ROWS] = [
    Some("//www.x/a"),
    Some("https://example"),
    None,
    Some("http://www./"),
    Some("https://user@host:80/p"),
];
const SEARCH_PHRASES: [Option<&str>; SOURCE_ROWS] =
    [Some("λ"), Some(""), None, Some("東京"), Some("x")];
const MINUTES: [Option<u8>; SOURCE_ROWS] = [Some(0), Some(0), Some(59), None, Some(1)];
const MINUTE_STARTS: [Option<i64>; SOURCE_ROWS] = [Some(0), Some(0), Some(-60), None, Some(3660)];
const URL_BYTE_LENGTHS: [Option<u32>; SOURCE_ROWS] = [Some(36), Some(0), Some(24), None, Some(10)];
const REFERER_BYTE_LENGTHS: [Option<u32>; SOURCE_ROWS] =
    [Some(9), Some(15), None, Some(12), Some(22)];
const SEARCH_BYTE_LENGTHS: [Option<u32>; SOURCE_ROWS] = [Some(2), Some(0), None, Some(6), Some(1)];
const URL_DOMAINS: [Option<&str>; SOURCE_ROWS] = [
    Some("東京.example"),
    Some(""),
    Some("[2001:db8::1]"),
    None,
    Some("1abc"),
];
const REFERER_DOMAINS: [Option<&str>; SOURCE_ROWS] =
    [Some("x"), Some("example"), None, Some(""), Some("host")];

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-parquet-readback-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum ValueMutation {
    #[default]
    None,
    UnsignedValue,
    LateNull,
    Minute,
    NanPayload,
    SignedZero,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum SchemaMutation {
    #[default]
    None,
    Reordered,
    WrongType,
    WrongNullability,
    ExtraColumn,
    MissingDerived,
}

fn source_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("EventTime", DataType::Int64, true),
        Field::new("URL", DataType::Utf8, true),
        Field::new("Referer", DataType::Utf8, true),
        Field::new("SearchPhrase", DataType::Utf8, true),
        Field::new("exact_unsigned", DataType::UInt64, false),
        Field::new("float_bits", DataType::Float64, true),
    ]))
}

fn event_time(row: usize) -> Option<i64> {
    [Some(0), Some(59), Some(-1), None, Some(3661)][row % SOURCE_ROWS]
}

fn unsigned_value(row: usize) -> u64 {
    [u64::MAX, (1_u64 << 60) + 3, 0, 7, 8][row % SOURCE_ROWS]
}

fn float_value(row: usize) -> Option<f64> {
    [
        Some(-0.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0123)),
        Some(f64::INFINITY),
        Some(123.5),
        None,
    ][row % SOURCE_ROWS]
}

fn source_batch(rows: Range<usize>) -> RecordBatch {
    let times = Int64Array::from_iter((rows.clone()).map(event_time));
    let urls = StringArray::from_iter((rows.clone()).map(|row| URLS[row % SOURCE_ROWS]));
    let referers = StringArray::from_iter((rows.clone()).map(|row| REFERERS[row % SOURCE_ROWS]));
    let search =
        StringArray::from_iter((rows.clone()).map(|row| SEARCH_PHRASES[row % SOURCE_ROWS]));
    let unsigned = UInt64Array::from_iter_values(rows.clone().map(unsigned_value));
    let floats = Float64Array::from_iter(rows.clone().map(float_value));
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(times),
            Arc::new(urls),
            Arc::new(referers),
            Arc::new(search),
            Arc::new(unsigned),
            Arc::new(floats),
        ],
    )
    .unwrap()
}

fn source_batch_variant(rows: Range<usize>, schema_mutation: SchemaMutation) -> RecordBatch {
    let batch = source_batch(rows.clone());
    let schema = batch.schema();
    let mut columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| (field.as_ref().clone(), Arc::clone(array)))
        .collect::<Vec<_>>();
    match schema_mutation {
        SchemaMutation::None | SchemaMutation::MissingDerived => {}
        SchemaMutation::Reordered => columns.swap(0, 1),
        SchemaMutation::WrongType => {
            columns[4] = (
                Field::new("exact_unsigned", DataType::Int64, false),
                Arc::new(Int64Array::from_iter_values(
                    rows.clone().map(|row| unsigned_value(row).cast_signed()),
                )),
            );
        }
        SchemaMutation::WrongNullability => {
            columns[4].0 = Field::new("exact_unsigned", DataType::UInt64, true);
        }
        SchemaMutation::ExtraColumn => columns.push((
            Field::new("unexpected_extra", DataType::UInt8, false),
            Arc::new(UInt8Array::from(vec![1; rows.len()])),
        )),
    }
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

// Explicit expected values and field types keep the fixture independent of ingestion.
#[allow(clippy::too_many_lines)]
fn prepared_batch(
    rows: Range<usize>,
    value_mutation: ValueMutation,
    schema_mutation: SchemaMutation,
) -> RecordBatch {
    let mut times = Vec::new();
    let mut urls = Vec::new();
    let mut referers = Vec::new();
    let mut search = Vec::new();
    let mut unsigned = Vec::new();
    let mut floats = Vec::new();
    let mut minute = Vec::new();
    let mut minute_start = Vec::new();
    let mut url_lengths = Vec::new();
    let mut url_domains = Vec::new();
    let mut referer_lengths = Vec::new();
    let mut referer_domains = Vec::new();
    let mut search_lengths = Vec::new();

    for row in rows.clone() {
        let slot = row % SOURCE_ROWS;
        times.push(event_time(row));
        urls.push(URLS[slot]);
        referers.push(REFERERS[slot]);
        search.push(if value_mutation == ValueMutation::LateNull && row == 26 {
            None
        } else {
            SEARCH_PHRASES[slot]
        });
        unsigned.push(
            unsigned_value(row)
                + u64::from(value_mutation == ValueMutation::UnsignedValue && row == 26),
        );
        floats.push(match (value_mutation, row, float_value(row)) {
            (ValueMutation::NanPayload, 26, Some(_)) => Some(f64::from_bits(0x7ff8_0000_0000_0124)),
            (ValueMutation::SignedZero, 25, Some(_)) => Some(0.0),
            (_, _, value) => value,
        });

        minute.push(if value_mutation == ValueMutation::Minute && row == 26 {
            Some(7)
        } else {
            MINUTES[slot]
        });
        minute_start.push(MINUTE_STARTS[slot]);
        url_lengths.push(URL_BYTE_LENGTHS[slot]);
        url_domains.push(URL_DOMAINS[slot]);
        referer_lengths.push(REFERER_BYTE_LENGTHS[slot]);
        referer_domains.push(REFERER_DOMAINS[slot]);
        search_lengths.push(SEARCH_BYTE_LENGTHS[slot]);
    }

    let mut columns: Vec<(Field, ArrowArrayRef)> = vec![
        (
            Field::new("EventTime", DataType::Int64, true),
            Arc::new(Int64Array::from(times)),
        ),
        (
            Field::new("URL", DataType::Utf8, true),
            Arc::new(StringArray::from(urls)),
        ),
        (
            Field::new("Referer", DataType::Utf8, true),
            Arc::new(StringArray::from(referers)),
        ),
        (
            Field::new("SearchPhrase", DataType::Utf8, true),
            Arc::new(StringArray::from(search)),
        ),
        (
            Field::new(
                "exact_unsigned",
                if matches!(schema_mutation, SchemaMutation::WrongType) {
                    DataType::Int64
                } else {
                    DataType::UInt64
                },
                matches!(schema_mutation, SchemaMutation::WrongNullability),
            ),
            if matches!(schema_mutation, SchemaMutation::WrongType) {
                Arc::new(Int64Array::from_iter_values(
                    rows.clone().map(|row| unsigned_value(row).cast_signed()),
                )) as ArrowArrayRef
            } else {
                Arc::new(UInt64Array::from(unsigned)) as ArrowArrayRef
            },
        ),
        (
            Field::new("float_bits", DataType::Float64, true),
            Arc::new(Float64Array::from(floats)),
        ),
        (
            Field::new(
                "__shardloom_derived_extract_minute_EventTime",
                DataType::UInt8,
                true,
            ),
            Arc::new(UInt8Array::from(minute)),
        ),
        (
            Field::new(
                "__shardloom_derived_date_trunc_minute_EventTime",
                DataType::Int64,
                true,
            ),
            Arc::new(Int64Array::from(minute_start)),
        ),
        (
            Field::new("__shardloom_derived_utf8_len_URL", DataType::UInt32, true),
            Arc::new(UInt32Array::from(url_lengths)),
        ),
        (
            Field::new("__shardloom_derived_url_domain_URL", DataType::Utf8, true),
            Arc::new(StringArray::from(url_domains)),
        ),
        (
            Field::new(
                "__shardloom_derived_utf8_len_Referer",
                DataType::UInt32,
                true,
            ),
            Arc::new(UInt32Array::from(referer_lengths)),
        ),
        (
            Field::new(
                "__shardloom_derived_url_domain_Referer",
                DataType::Utf8,
                true,
            ),
            Arc::new(StringArray::from(referer_domains)),
        ),
        (
            Field::new(
                "__shardloom_derived_utf8_len_SearchPhrase",
                DataType::UInt32,
                true,
            ),
            Arc::new(UInt32Array::from(search_lengths)),
        ),
    ];

    match schema_mutation {
        SchemaMutation::None | SchemaMutation::WrongType | SchemaMutation::WrongNullability => {}
        SchemaMutation::Reordered => columns.swap(0, 1),
        SchemaMutation::ExtraColumn => columns.push((
            Field::new("unexpected_extra", DataType::UInt8, false),
            Arc::new(UInt8Array::from(vec![1; rows.len()])),
        )),
        SchemaMutation::MissingDerived => {
            columns.pop();
        }
    }
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn write_parquet(path: &Path, rows: usize) {
    let schema = source_schema();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    for start in (0..rows).step_by(4) {
        writer
            .write(&source_batch(start..(start + 4).min(rows)))
            .unwrap();
    }
    writer.close().unwrap();
}

fn write_native(
    path: &Path,
    rows: usize,
    source_only: bool,
    value_mutation: ValueMutation,
    schema_mutation: SchemaMutation,
) {
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let initial = if source_only {
        source_batch_variant(0..rows.min(1), schema_mutation)
    } else {
        prepared_batch(0..rows.min(1), ValueMutation::None, schema_mutation)
    };
    let dtype = session
        .arrow()
        .from_arrow_record_batch(initial.clone(), initial.schema().as_ref())
        .unwrap()
        .dtype()
        .clone();
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(
            crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                rows.div_ceil(NATIVE_CHUNK_ROWS),
            ),
        )
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, dtype);
    for start in (0..rows).step_by(NATIVE_CHUNK_ROWS) {
        let batch = if source_only {
            source_batch_variant(
                start..(start + NATIVE_CHUNK_ROWS).min(rows),
                schema_mutation,
            )
        } else {
            prepared_batch(
                start..(start + NATIVE_CHUNK_ROWS).min(rows),
                value_mutation,
                schema_mutation,
            )
        };
        let array = session
            .arrow()
            .from_arrow_record_batch(batch.clone(), batch.schema().as_ref())
            .unwrap();
        writer.push(array).unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
}

fn limits() -> NativeArtifactComparisonLimits {
    NativeArtifactComparisonLimits {
        memory_bytes_per_source: 32 << 20,
        parallelism_per_source: 2,
        left_batch_rows: 3,
        right_batch_rows: 5,
        window_rows: 11,
        arrow_batch_bytes: 1 << 20,
        max_value_bytes: 4096,
        max_columns: 16,
    }
}

fn compare_error(
    parquet: &Path,
    native: &Path,
    recipe: ParquetReferenceRecipe,
    limits: NativeArtifactComparisonLimits,
) -> String {
    compare_parquet_native_artifact(parquet, native, recipe, limits)
        .unwrap_err()
        .to_string()
}

fn assert_error(
    parquet: &Path,
    native: &Path,
    recipe: ParquetReferenceRecipe,
    limits: NativeArtifactComparisonLimits,
    message: &str,
) {
    let actual = compare_error(parquet, native, recipe, limits);
    assert!(
        actual.contains(message),
        "expected {message:?}, got {actual:?}"
    );
}

#[test]
fn complete_clickbench_readback_matches_independent_parquet_values_and_all_derived_columns() {
    let fixture = Fixture::new();
    let parquet = fixture.path("reference.parquet");
    let native = fixture.path("prepared.vortex");
    write_parquet(&parquet, ROWS);
    write_native(
        &native,
        ROWS,
        false,
        ValueMutation::None,
        SchemaMutation::None,
    );

    let report = compare_parquet_native_artifact(
        &parquet,
        &native,
        ParquetReferenceRecipe::ClickBenchPreparation,
        limits(),
    )
    .unwrap();
    assert_eq!(report["complete_value_equality"], true);
    assert_eq!(report["compared_rows"], 29);
    assert_eq!(report["source_columns"], 6);
    assert_eq!(report["derived_columns"], 7);
    assert_eq!(report["compared_columns"], 13);
    assert_eq!(report["compared_values"], 29 * 13);
    assert!(report["reference_parquet_batches"].as_u64().unwrap() > 0);
    assert!(report["native_batches"].as_u64().unwrap() > 0);
    assert_ne!(
        report["reference_parquet_batches"],
        report["native_batches"]
    );
    let task_limit = report["native_task_concurrency"].as_u64().unwrap();
    assert!((1..=2).contains(&task_limit));
    assert!(report["peak_native_split_tasks"].as_u64().unwrap() <= 2);
    assert_eq!(report["native_split_tasks_after_drain"], 0);
    assert_eq!(report["source_generations_validated"], true);
    assert_eq!(report["memory"]["arrow_final_reserved_bytes"], 0);
    assert_eq!(report["memory"]["native_final_reserved_bytes"], 0);
    assert!(report["equal_spans"].as_u64().unwrap() > 0);
}

#[test]
fn source_only_recipe_and_empty_schema_preserving_artifacts_are_complete() {
    let fixture = Fixture::new();
    let parquet = fixture.path("source.parquet");
    let native = fixture.path("source.vortex");
    write_parquet(&parquet, ROWS);
    write_native(
        &native,
        ROWS,
        true,
        ValueMutation::None,
        SchemaMutation::None,
    );
    let report = compare_parquet_native_artifact(
        &parquet,
        &native,
        ParquetReferenceRecipe::SourceColumns,
        limits(),
    )
    .unwrap();
    assert_eq!(report["compared_rows"], 29);
    assert_eq!(report["compared_columns"], 6);
    assert_eq!(report["compared_values"], 29 * 6);
    assert_eq!(report["derived_columns"], 0);

    let empty_parquet = fixture.path("empty.parquet");
    let empty_native = fixture.path("empty.vortex");
    write_parquet(&empty_parquet, 0);
    write_native(
        &empty_native,
        0,
        false,
        ValueMutation::None,
        SchemaMutation::None,
    );
    let empty = compare_parquet_native_artifact(
        &empty_parquet,
        &empty_native,
        ParquetReferenceRecipe::ClickBenchPreparation,
        limits(),
    )
    .unwrap();
    assert_eq!(empty["compared_rows"], 0);
    assert_eq!(empty["compared_columns"], 13);
    assert_eq!(empty["compared_values"], 0);
    assert_eq!(empty["source_generations_validated"], true);
    assert_eq!(empty["memory"]["arrow_final_reserved_bytes"], 0);
}

#[test]
fn independently_declared_domain_and_byte_length_fixture_covers_url_edges() {
    assert_eq!(MINUTES, [Some(0), Some(0), Some(59), None, Some(1)]);
    assert_eq!(
        MINUTE_STARTS,
        [Some(0), Some(0), Some(-60), None, Some(3660)]
    );
    assert_eq!(
        URL_BYTE_LENGTHS,
        [Some(36), Some(0), Some(24), None, Some(10)]
    );
    assert_eq!(
        REFERER_BYTE_LENGTHS,
        [Some(9), Some(15), None, Some(12), Some(22)]
    );
    assert_eq!(
        SEARCH_BYTE_LENGTHS,
        [Some(2), Some(0), None, Some(6), Some(1)]
    );
    assert_eq!(
        URL_DOMAINS,
        [
            Some("東京.example"),
            Some(""),
            Some("[2001:db8::1]"),
            None,
            Some("1abc")
        ]
    );
    assert_eq!(
        REFERER_DOMAINS,
        [Some("x"), Some("example"), None, Some(""), Some("host")]
    );
    let scheme = regex::Regex::new(r"^[A-Za-z][A-Za-z0-9+.-]*://").unwrap();
    for (input, expected) in [
        ("HTTP://user@WWW.東京.example:443/p", "東京.example"),
        ("", ""),
        ("https://[2001:db8::1]:80", "[2001:db8::1]"),
        ("1abc://x/p", "1abc"),
        ("//www.x/a", "x"),
        ("https://user@host:80/p", "host"),
    ] {
        assert_eq!(domain_reference(input, &scheme), expected, "{input:?}");
    }
}

#[test]
fn minute_start_reference_rejects_signed_underflow_instead_of_wrapping() {
    let input: ArrowArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN)]));
    let scheme = regex::Regex::new(r"^[A-Za-z][A-Za-z0-9+.-]*://").unwrap();
    let error = derived_reference(&input, Transform::MinuteStart, &scheme)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("minute-start reference is outside signed epoch range"),
        "{error}"
    );
}

#[test]
fn readback_detects_late_source_null_and_derived_mismatches() {
    let fixture = Fixture::new();
    let parquet = fixture.path("reference.parquet");
    write_parquet(&parquet, ROWS);
    for (name, mutation, column) in [
        ("unsigned", ValueMutation::UnsignedValue, "exact_unsigned"),
        ("null", ValueMutation::LateNull, "SearchPhrase"),
        (
            "derived",
            ValueMutation::Minute,
            "__shardloom_derived_extract_minute_EventTime",
        ),
    ] {
        let native = fixture.path(&format!("{name}.vortex"));
        write_native(&native, ROWS, false, mutation, SchemaMutation::None);
        let error = compare_error(
            &parquet,
            &native,
            ParquetReferenceRecipe::ClickBenchPreparation,
            limits(),
        );
        assert!(
            error.contains(&format!("value mismatch at column '{column}' row 26")),
            "{error}"
        );
    }
}

#[test]
fn readback_detects_native_row_count_schema_and_extra_or_missing_columns() {
    let fixture = Fixture::new();
    let parquet = fixture.path("reference.parquet");
    write_parquet(&parquet, ROWS);

    let fewer_rows = fixture.path("fewer-rows.vortex");
    write_native(
        &fewer_rows,
        ROWS - 1,
        false,
        ValueMutation::None,
        SchemaMutation::None,
    );
    assert_error(
        &parquet,
        &fewer_rows,
        ParquetReferenceRecipe::ClickBenchPreparation,
        limits(),
        "Parquet and native source row counts differ",
    );

    for (name, mutation, source_only, recipe, expected) in [
        (
            "order",
            SchemaMutation::Reordered,
            true,
            ParquetReferenceRecipe::SourceColumns,
            "Parquet/reference and native schemas differ (names/order/types/nullability)",
        ),
        (
            "type",
            SchemaMutation::WrongType,
            true,
            ParquetReferenceRecipe::SourceColumns,
            "Parquet/reference and native schemas differ (names/order/types/nullability)",
        ),
        (
            "nullability",
            SchemaMutation::WrongNullability,
            true,
            ParquetReferenceRecipe::SourceColumns,
            "Parquet/reference and native schemas differ (names/order/types/nullability)",
        ),
        (
            "extra",
            SchemaMutation::ExtraColumn,
            true,
            ParquetReferenceRecipe::SourceColumns,
            "Parquet/reference and native column counts differ",
        ),
        (
            "missing-derived",
            SchemaMutation::MissingDerived,
            false,
            ParquetReferenceRecipe::ClickBenchPreparation,
            "Parquet/reference and native column counts differ",
        ),
    ] {
        let native = fixture.path(&format!("{name}.vortex"));
        write_native(&native, ROWS, source_only, ValueMutation::None, mutation);
        assert_error(&parquet, &native, recipe, limits(), expected);
    }
}

#[test]
fn readback_detects_exact_float_payload_and_signed_zero_mismatches() {
    let fixture = Fixture::new();
    let parquet = fixture.path("reference.parquet");
    write_parquet(&parquet, ROWS);
    for (name, mutation) in [
        ("nan-payload", ValueMutation::NanPayload),
        ("signed-zero", ValueMutation::SignedZero),
    ] {
        let native = fixture.path(&format!("{name}.vortex"));
        write_native(&native, ROWS, false, mutation, SchemaMutation::None);
        let row = if mutation == ValueMutation::NanPayload {
            26
        } else {
            25
        };
        let error = compare_error(
            &parquet,
            &native,
            ParquetReferenceRecipe::ClickBenchPreparation,
            limits(),
        );
        assert!(
            error.contains(&format!("value mismatch at column 'float_bits' row {row}")),
            "{error}"
        );
    }
}

#[test]
fn readback_rejects_malformed_footer_and_both_admission_limits() {
    let fixture = Fixture::new();
    let valid_parquet = fixture.path("reference.parquet");
    let native = fixture.path("prepared.vortex");
    write_parquet(&valid_parquet, ROWS);
    write_native(
        &native,
        ROWS,
        false,
        ValueMutation::None,
        SchemaMutation::None,
    );

    let malformed = fixture.path("malformed.parquet");
    let mut bytes = vec![0_u8; 12];
    bytes[8..].copy_from_slice(b"NOPE");
    fs::write(&malformed, bytes).unwrap();
    assert_error(
        &malformed,
        &native,
        ParquetReferenceRecipe::ClickBenchPreparation,
        limits(),
        "Parquet footer is unsupported or exceeds admission",
    );

    let mut small_value = limits();
    small_value.max_value_bytes = 8;
    assert_error(
        &valid_parquet,
        &native,
        ParquetReferenceRecipe::ClickBenchPreparation,
        small_value,
        "variable-width value exceeds comparison admission",
    );

    let mut small_batch = limits();
    small_batch.arrow_batch_bytes = 16;
    assert_error(
        &valid_parquet,
        &native,
        ParquetReferenceRecipe::ClickBenchPreparation,
        small_batch,
        "native batch exceeds Arrow expansion admission",
    );

    // A small native batch can fit while the independently decoded Parquet
    // batch retains more memory. Fence and identify that separate boundary.
    let reference_bound_parquet = fixture.path("reference-bound.parquet");
    let reference_bound_native = fixture.path("reference-bound.vortex");
    write_parquet(&reference_bound_parquet, 512);
    write_native(
        &reference_bound_native,
        512,
        false,
        ValueMutation::None,
        SchemaMutation::None,
    );
    let mut small_reference = limits();
    // The native conversion includes a conservative 1 KiB expansion allowance.
    small_reference.arrow_batch_bytes = 2048;
    small_reference.left_batch_rows = 512;
    small_reference.right_batch_rows = 1;
    let message = compare_error(
        &reference_bound_parquet,
        &reference_bound_native,
        ParquetReferenceRecipe::ClickBenchPreparation,
        small_reference,
    );
    assert!(
        message.contains("Parquet/reference batch exceeds admitted Arrow byte limit"),
        "{message}"
    );
    assert!(message.contains("column 'EventTime'"), "{message}");
    assert!(
        message.contains("retained bytes for 512 rows, limit 2048"),
        "{message}"
    );
}

#[test]
fn readback_rejects_parquet_mutation_and_native_path_replacement_after_first_equal_span() {
    for replace_parquet in [true, false] {
        let fixture = Fixture::new();
        let parquet = fixture.path("reference.parquet");
        let native = fixture.path("prepared.vortex");
        let replacement = fixture.path("replacement.tmp");
        write_parquet(&parquet, ROWS);
        write_native(
            &native,
            ROWS,
            false,
            ValueMutation::None,
            SchemaMutation::None,
        );
        let target = if replace_parquet {
            parquet.clone()
        } else {
            native.clone()
        };
        fs::copy(&target, &replacement).unwrap();
        AFTER_EQUAL_SPAN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(replacement, target).unwrap();
            }));
        });

        let error = compare_error(
            &parquet,
            &native,
            ParquetReferenceRecipe::ClickBenchPreparation,
            limits(),
        );
        assert!(error.contains("prepared source changed"), "{error}");
        assert!(AFTER_EQUAL_SPAN.with(|hook| hook.borrow().is_none()));
    }
}

#[test]
fn readback_fences_caller_symlink_aliases_while_descriptors_still_read_the_old_file() {
    for reference_alias in [true, false] {
        let fixture = Fixture::new();
        let parquet = fixture.path("source.parquet");
        let native = fixture.path("native.vortex");
        write_parquet(&parquet, ROWS);
        write_native(
            &native,
            ROWS,
            false,
            ValueMutation::None,
            SchemaMutation::None,
        );
        let initial = if reference_alias { &parquet } else { &native };
        let replacement_target = fixture.path("identical-bytes-new-inode");
        fs::copy(initial, &replacement_target).unwrap();
        let alias = fixture.path("caller-link");
        let replacement_link = fixture.path("replacement-link");
        std::os::unix::fs::symlink(initial, &alias).unwrap();
        std::os::unix::fs::symlink(replacement_target, &replacement_link).unwrap();
        let changed_alias = alias.clone();
        AFTER_EQUAL_SPAN.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                fs::rename(replacement_link, changed_alias).unwrap();
            }));
        });
        let error = compare_error(
            if reference_alias { &alias } else { &parquet },
            if reference_alias { &native } else { &alias },
            ParquetReferenceRecipe::ClickBenchPreparation,
            limits(),
        );
        assert!(error.contains("prepared source changed"), "{error}");
        assert!(AFTER_EQUAL_SPAN.with(|hook| hook.borrow().is_none()));
    }
}
