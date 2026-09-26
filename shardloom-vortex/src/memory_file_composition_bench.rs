//! Ignored, bounded R5.a native composition workflows over the same producer and
//! aggregate kernels. Explicit arms select memory-file or owned-array intake.

use super::*;
use crate::{
    VortexAggregateOrderExpr, VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    local_primitives::prepared_aggregate::PreparedVortexAggregate,
    memory_file_generation::{
        MemoryFileCompositionBounds, MemoryFileGeneration, MemoryFileGenerationEvidence,
    },
    owned_array_source::{OwnedArraySource, OwnedArraySourceBounds},
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::{ColumnRef, DatasetUri};
use std::{collections::BTreeMap, fmt::Write as _, path::PathBuf, time::Instant};
use vortex::array::{
    Columnar, IntoArray as _,
    scalar::{PValue, Scalar, ScalarValue},
};

const SOURCE_ROWS: u64 = 99_997_497;
const CASES: [(u64, u64); 4] = [
    (0, 131_072),
    (50_000_000, 131_072),
    (99_800_000, 131_072),
    (0, 524_288),
];
const COLUMNS: [&str; 3] = ["AdvEngineID", "UserID", "URL"];
const SESSION_BYTES: u64 = 512 * 1024 * 1024;
const REPETITIONS: usize = 3;
const MAX_GROUPS: usize = 1024;

#[derive(Clone, Copy)]
enum Adapter {
    MemoryFile,
    OwnedArray,
}

impl Adapter {
    fn id(self) -> &'static str {
        match self {
            Self::MemoryFile => "shardloom.resident_vortex.memory_file.v1",
            Self::OwnedArray => "shardloom.resident_vortex.owned_array.v1",
        }
    }

    fn admission_stage(self) -> &'static str {
        match self {
            Self::MemoryFile => "memory_file_composition_default_bounds",
            Self::OwnedArray => "owned_array_source_default_bounds",
        }
    }

    fn bounds_json(self) -> Value {
        match self {
            Self::MemoryFile => {
                let bounds = MemoryFileCompositionBounds::default();
                assert_eq!(bounds.storage.max_serialized_bytes, 64 * 1024 * 1024);
                json!({
                    "max_rows": bounds.max_rows,
                    "max_columns": bounds.max_columns,
                    "max_batches": bounds.max_batches,
                    "max_serialized_bytes": bounds.storage.max_serialized_bytes,
                    "max_metadata_bytes": bounds.storage.max_metadata_bytes,
                    "row_group_rows": bounds.layout.row_group_rows,
                    "max_segments": bounds.layout.max_segments,
                })
            }
            Self::OwnedArray => {
                let bounds = OwnedArraySourceBounds::default();
                assert_eq!(bounds.max_logical_bytes, 64 * 1024 * 1024);
                assert_eq!(bounds.max_metadata_bytes, 1024 * 1024);
                json!({
                    "max_rows": bounds.max_rows,
                    "max_columns": bounds.max_columns,
                    "max_batches": bounds.max_batches,
                    "max_logical_bytes": bounds.max_logical_bytes,
                    "max_metadata_bytes": bounds.max_metadata_bytes,
                })
            }
        }
    }
}

enum Source {
    Memory(MemoryFileGeneration),
    Owned(OwnedArraySource),
}

impl Source {
    fn from_owned(input: OwnedVortexResultBatch, adapter: Adapter) -> Result<Self> {
        let cancellation = CancellationToken::default();
        match adapter {
            Adapter::MemoryFile => MemoryFileGeneration::from_owned(
                input,
                MemoryFileCompositionBounds::default(),
                &cancellation,
            )
            .map(Self::Memory),
            Adapter::OwnedArray => OwnedArraySource::from_owned(
                input,
                OwnedArraySourceBounds::default(),
                &cancellation,
            )
            .map(Self::Owned),
        }
    }

    fn source_uri(&self) -> &DatasetUri {
        match self {
            Self::Memory(source) => source.source_uri(),
            Self::Owned(source) => source.source_uri(),
        }
    }

    fn row_count(&self) -> u64 {
        match self {
            Self::Memory(source) => source.row_count(),
            Self::Owned(source) => source.row_count(),
        }
    }

    fn prepare_aggregate(
        &self,
        request: &VortexQueryPrimitiveRequest,
        policy: VortexLocalPrimitiveExecutionPolicy,
    ) -> Result<PreparedVortexAggregate> {
        match self {
            Self::Memory(source) => source.prepare_aggregate(request, policy),
            Self::Owned(source) => source.prepare_aggregate(request, policy),
        }
    }

    fn evidence(&self, logical_input_bytes: u64) -> SourceEvidence {
        match self {
            Self::Memory(source) => SourceEvidence::Memory(source.evidence()),
            Self::Owned(_) => SourceEvidence::Owned {
                logical_input_bytes,
            },
        }
    }
}

enum SourceEvidence {
    Memory(MemoryFileGenerationEvidence),
    Owned { logical_input_bytes: u64 },
}

impl SourceEvidence {
    fn add_to_json(&self, report: &mut Value) {
        match self {
            Self::Memory(evidence) => report["generation"] = generation_json(*evidence),
            Self::Owned {
                logical_input_bytes,
            } => {
                report["owned_array"] = json!({
                    "input_logical_bytes": logical_input_bytes,
                    "evidence_scope": "source_adapter_construction_contract_verified_in_native_certificate_not_provider_allocator_or_decode_counts",
                    "source_specific_file_opens": 0,
                    "construction_array_serializer_calls": 0,
                    "construction_segment_assembly_bytes_copied": 0,
                    "construction_footer_serializer_calls": 0,
                })
            }
        }
    }
}

struct Oracle {
    values: Value,
    values_sha256: String,
    logical_input_bytes: u64,
}

struct StageTimings {
    producer_nanos: u64,
    composition_nanos: u64,
    consumer_nanos: u64,
    drop_nanos: u64,
    complete_nanos: u64,
}

impl StageTimings {
    fn json(&self) -> Value {
        json!({
            "producer_nanos": self.producer_nanos,
            "composition_nanos": self.composition_nanos,
            "consumer_nanos": self.consumer_nanos,
            "drop_nanos": self.drop_nanos,
            "complete_nanos": self.complete_nanos,
        })
    }
}

struct BaselineSample {
    adapter: Adapter,
    repetition: usize,
    timings: StageTimings,
    logical_input_bytes: u64,
    input_batches: usize,
    output_bytes: usize,
    output_sha256: String,
    values_sha256: String,
    groups: usize,
    source_evidence: SourceEvidence,
    session: ResidentSessionSnapshot,
    released_memory: LiveMemorySnapshot,
}

impl BaselineSample {
    fn json(&self) -> Value {
        let mut report = json!({
            "adapter": self.adapter.id(),
            "repetition": self.repetition,
            "timings": self.timings.json(),
            "logical_input_bytes": self.logical_input_bytes,
            "input_batches": self.input_batches,
            "normal_report_output_bytes": self.output_bytes,
            "normal_report_output_sha256": self.output_sha256,
            "ordered_values_sha256": self.values_sha256,
            "complete_ordered_values_match": true,
            "groups": self.groups,
            "session": {
                "prepared_source_opens": self.session.prepared_source_opens,
                "completed_executions": self.session.completed_executions,
                "provider_background_workers": self.session.provider_background_workers,
            },
            "released_memory": {
                "limit_bytes": self.released_memory.limit_bytes,
                "reserved_bytes": self.released_memory.reserved_bytes,
                "peak_reserved_bytes": self.released_memory.peak_reserved_bytes,
                "denied_reservations": self.released_memory.denied_reservations,
            },
            "native_io_certified": true,
            "fallback_attempted": false,
        });
        self.source_evidence.add_to_json(&mut report);
        report
    }
}

struct RangeBaseline {
    row_start: u64,
    row_count: u64,
    oracle: Oracle,
    samples: Vec<SampleOutcome>,
}

enum SampleOutcome {
    Complete(Box<BaselineSample>),
    AdmissionFailure(AdmissionFailure),
}

impl SampleOutcome {
    fn json(&self) -> Value {
        match self {
            Self::Complete(sample) => json!({
                "status": "complete",
                "sample": sample.json(),
            }),
            Self::AdmissionFailure(failure) => failure.json(),
        }
    }
}

struct AdmissionFailure {
    adapter: Adapter,
    repetition: usize,
    error: String,
    logical_input_bytes: u64,
    input_batches: usize,
    producer_nanos: u64,
    composition_attempt_nanos: u64,
    drop_nanos: u64,
    released_memory: LiveMemorySnapshot,
}

impl AdmissionFailure {
    fn json(&self) -> Value {
        json!({
            "status": "admission_failure",
            "adapter": self.adapter.id(),
            "stage": self.adapter.admission_stage(),
            "repetition": self.repetition,
            "error": self.error,
            "logical_input_bytes": self.logical_input_bytes,
            "input_batches": self.input_batches,
            "successful_complete_workflow": false,
            "producer_nanos": self.producer_nanos,
            "composition_attempt_nanos": self.composition_attempt_nanos,
            "drop_nanos": self.drop_nanos,
            "released_reserved_bytes": self.released_memory.reserved_bytes,
            "peak_reserved_bytes": self.released_memory.peak_reserved_bytes,
            "denied_reservations": self.released_memory.denied_reservations,
        })
    }
}

impl RangeBaseline {
    fn json(&self) -> Value {
        json!({
            "row_start": self.row_start,
            "row_end_exclusive": self.row_start + self.row_count,
            "rows": self.row_count,
            "oracle": {
                "method": "native_projected_key_scalars_independent_btree_map",
                "rows_checked": self.row_count,
                "groups": self.oracle.values.as_array().unwrap().len(),
                "logical_input_bytes": self.oracle.logical_input_bytes,
                "ordered_values_sha256": self.oracle.values_sha256,
            },
            "samples": self.samples.iter().map(SampleOutcome::json).collect::<Vec<_>>(),
        })
    }
}

fn generation_json(evidence: MemoryFileGenerationEvidence) -> Value {
    json!({
        "input_logical_bytes": evidence.input_logical_bytes,
        "intake_payload_bytes_copied": evidence.intake_payload_bytes_copied,
        "segment_assembly_bytes_copied": evidence.segment_assembly_bytes_copied,
        "array_serializer_calls": evidence.array_serializer_calls,
        "dictionary_build_calls": evidence.dictionary_build_calls,
        "memory_file_constructions": evidence.memory_file_constructions,
        "source_file_opens": evidence.source_file_opens,
        "memory_segment_requests": evidence.memory_segment_requests,
        "memory_segment_bytes_returned": evidence.memory_segment_bytes_returned,
        "columns": evidence.columns,
        "row_groups": evidence.row_groups,
        "row_group_rows": evidence.row_group_rows,
        "row_group_offset_bytes_built": evidence.row_group_offset_bytes_built,
        "construction_footer_serializer_calls": evidence.construction_footer_serializer_calls,
        "construction_footer_bytes": evidence.construction_footer_bytes,
        "construction_native_materialization_calls": evidence.construction_native_materialization_calls,
        "construction_native_materialization_rows": evidence.construction_native_materialization_rows,
    })
}

fn nanos(start: Instant, end: Instant) -> u64 {
    u64::try_from(end.duration_since(start).as_nanos()).unwrap()
}

fn sha256(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

fn projection(
    source: &PreparedVortexSource,
    start: u64,
    row_count: u64,
) -> PreparedVortexProjection {
    assert_eq!(
        source.file().row_count(),
        SOURCE_ROWS,
        "unexpected source scale"
    );
    source
        .prepare_projection(&COLUMNS, row_count, SESSION_BYTES)
        .unwrap()
        .with_row_range(start..start + row_count)
        .unwrap()
}

fn integer_key(scalar: &Scalar) -> u64 {
    match scalar
        .value()
        .expect("AdvEngineID oracle requires non-null keys")
    {
        ScalarValue::Primitive(PValue::U8(value)) => u64::from(*value),
        ScalarValue::Primitive(PValue::U16(value)) => u64::from(*value),
        ScalarValue::Primitive(PValue::U32(value)) => u64::from(*value),
        ScalarValue::Primitive(PValue::U64(value)) => *value,
        ScalarValue::Primitive(PValue::I8(value)) => u64::try_from(*value).unwrap(),
        ScalarValue::Primitive(PValue::I16(value)) => u64::try_from(*value).unwrap(),
        ScalarValue::Primitive(PValue::I32(value)) => u64::try_from(*value).unwrap(),
        ScalarValue::Primitive(PValue::I64(value)) => u64::try_from(*value).unwrap(),
        _ => panic!("AdvEngineID oracle requires non-negative integer keys"),
    }
}

fn oracle(path: &Path, start: u64, row_count: u64) -> Oracle {
    let session = ResidentVortexSession::new(SESSION_BYTES, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(path).unwrap();
    let prepared = projection(&source, start, row_count);
    let result = prepared.execute().unwrap();
    assert_eq!(result.row_count(), row_count);
    let logical_input_bytes = result.logical_buffer_bytes();
    let mut counts = BTreeMap::<u64, u64>::new();
    let mut context = result.create_execution_ctx();
    let mut rows = 0_u64;
    for array in result.arrays() {
        let fields = array.dtype().as_struct_fields();
        assert_eq!(
            fields.names().iter().map(AsRef::as_ref).collect::<Vec<_>>(),
            COLUMNS
        );
        let key = vortex::expr::get_item(COLUMNS[0], vortex::expr::root())
            .bind(array.dtype())
            .unwrap();
        let keys = array
            .clone()
            .apply_bound(&key)
            .unwrap()
            .execute::<Columnar>(&mut context)
            .unwrap()
            .into_array();
        for row in 0..array.len() {
            let key = integer_key(&keys.execute_scalar(row, &mut context).unwrap());
            *counts.entry(key).or_default() += 1;
            rows += 1;
        }
        assert!(
            counts.len() <= MAX_GROUPS,
            "source exceeds small-result fixture bound"
        );
    }
    assert_eq!(rows, row_count);
    assert_eq!(counts.values().sum::<u64>(), row_count);
    let values = Value::Array(
        counts
            .into_iter()
            .map(|(key, count)| json!({"AdvEngineID": key, "n": count}))
            .collect(),
    );
    let values_sha256 = sha256(&serde_json::to_vec(&values).unwrap());
    drop(context);
    drop(result);
    drop(prepared);
    drop(source);
    drop(session);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        0,
        "oracle retained native owners"
    );
    Oracle {
        values,
        values_sha256,
        logical_input_bytes,
    }
}

fn aggregate_request(generation: &Source) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        generation.source_uri().clone(),
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(COLUMNS[0]).unwrap()],
            vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(COLUMNS[0], false)]),
    )
}

fn report_values(output: &str) -> Value {
    let summary = output
        .lines()
        .find_map(|line| line.strip_prefix("result summary: "))
        .expect("normal report omitted its result summary");
    let payload: Value = serde_json::from_str(summary.rsplit_once(" values=").unwrap().1).unwrap();
    payload
        .get("values")
        .expect("normal report omitted aggregate values")
        .clone()
}

#[allow(clippy::too_many_lines)] // Keep contiguous timing and final-owner release visible together.
fn run(
    path: &Path,
    start: u64,
    row_count: u64,
    repetition: usize,
    expected: &Oracle,
    adapter: Adapter,
) -> SampleOutcome {
    let started = Instant::now();
    let session = ResidentVortexSession::new(SESSION_BYTES, 1).unwrap();
    let memory = session.memory().clone();
    let source = session.prepare_file(path).unwrap();
    let projected = projection(&source, start, row_count);
    let input = projected.execute().unwrap();
    let input_rows = input.row_count();
    let logical_input_bytes = input.logical_buffer_bytes();
    let input_batches = input.arrays().len();
    let producer_end = Instant::now();

    let generation = Source::from_owned(input, adapter);
    let composition_end = Instant::now();
    let generation = match generation {
        Ok(generation) => generation,
        Err(error) => {
            // from_owned consumes and releases the input even on rejection.
            drop(projected);
            drop(source);
            drop(session);
            let finished = Instant::now();
            let released_memory = memory.snapshot();
            assert_eq!(
                released_memory.reserved_bytes, 0,
                "rejected composition retained native owners"
            );
            return SampleOutcome::AdmissionFailure(AdmissionFailure {
                adapter,
                repetition,
                error: error.to_string(),
                logical_input_bytes,
                input_batches,
                producer_nanos: nanos(started, producer_end),
                composition_attempt_nanos: nanos(producer_end, composition_end),
                drop_nanos: nanos(composition_end, finished),
                released_memory,
            });
        }
    };

    let request = aggregate_request(&generation);
    let prepared = generation
        .prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        )
        .unwrap();
    let executed = prepared.execute().unwrap();
    let output = executed.report.to_human_text();
    let consumer_end = Instant::now();

    // These bounded evidence snapshots precede owner release and are charged to
    // teardown. Hashing, JSON parsing, equality, and benchmark JSON are untimed.
    let evidence = generation.evidence(logical_input_bytes);
    let generation_rows = generation.row_count();
    let session_snapshot = session.snapshot();
    let certified = executed.native_io_certificate.is_certified();
    let expected_adapter = executed
        .native_io_certificate
        .source_capability_report
        .adapter_id
        == adapter.id();
    let construction_proof = match adapter {
        Adapter::MemoryFile => true,
        Adapter::OwnedArray => [
            "immutable_array_owner_retained=true",
            "source_specific_file_opens=0",
            "construction_array_serializer_calls=0",
            "construction_segment_assembly_bytes_copied=0",
            "construction_footer_serializer_calls=0",
        ]
        .iter()
        .all(|marker| {
            executed
                .native_io_certificate
                .source_pushdown_report
                .proof_basis
                .contains(*marker)
        }),
    };
    let fallback_attempted = executed.native_io_certificate.fallback_attempted;
    let errors = executed.report.has_errors();
    let output_rows = executed.report.rows_projected;
    drop(executed);
    drop(prepared);
    drop(request);
    drop(generation);
    drop(projected);
    drop(source);
    drop(session);
    let finished = Instant::now();

    // Only the detached normal report text survives the clock for verification;
    // no native result/source/session/generation or provider owner survives.
    let released_memory = memory.snapshot();
    assert_eq!(
        released_memory.reserved_bytes, 0,
        "workflow retained native owners"
    );
    assert_eq!(input_rows, row_count);
    assert_eq!(generation_rows, row_count);
    if let SourceEvidence::Memory(evidence) = &evidence {
        assert_eq!(evidence.input_logical_bytes, logical_input_bytes);
        assert_eq!(evidence.source_file_opens, 0);
    }
    assert_eq!(session_snapshot.prepared_source_opens, 1);
    assert_eq!(session_snapshot.completed_executions, 2);
    assert!(certified && !fallback_attempted && !errors);
    assert!(
        expected_adapter && construction_proof,
        "native source adapter evidence differs"
    );
    let actual = report_values(&output);
    assert_eq!(
        actual, expected.values,
        "complete ordered grouped counts differ"
    );
    let groups = actual.as_array().unwrap().len();
    assert_eq!(output_rows, Some(u64::try_from(groups).unwrap()));
    let values_sha256 = sha256(&serde_json::to_vec(&actual).unwrap());
    assert_eq!(values_sha256, expected.values_sha256);
    let timings = StageTimings {
        producer_nanos: nanos(started, producer_end),
        composition_nanos: nanos(producer_end, composition_end),
        consumer_nanos: nanos(composition_end, consumer_end),
        drop_nanos: nanos(consumer_end, finished),
        complete_nanos: nanos(started, finished),
    };
    assert_eq!(
        timings.complete_nanos,
        timings.producer_nanos
            + timings.composition_nanos
            + timings.consumer_nanos
            + timings.drop_nanos
    );
    SampleOutcome::Complete(Box::new(BaselineSample {
        adapter,
        repetition,
        timings,
        logical_input_bytes,
        input_batches,
        output_bytes: output.len(),
        output_sha256: sha256(output.as_bytes()),
        values_sha256,
        groups,
        source_evidence: evidence,
        session: session_snapshot,
        released_memory,
    }))
}

#[test]
#[ignore = "bounded R5.a native baseline; requires SHARDLOOM_R5A_SOURCE and --release --ignored --exact"]
fn native_memory_file_composition_baseline() {
    complete_workflow(Adapter::MemoryFile);
}

#[test]
#[ignore = "bounded R5.a owned-array candidate; requires SHARDLOOM_R5A_SOURCE and --release --ignored --exact"]
fn native_owned_array_composition_candidate() {
    complete_workflow(Adapter::OwnedArray);
}

#[allow(clippy::assertions_on_constants)]
fn complete_workflow(adapter: Adapter) {
    assert!(!cfg!(debug_assertions), "timing fixture requires --release");
    let path = PathBuf::from(
        std::env::var_os("SHARDLOOM_R5A_SOURCE")
            .expect("set SHARDLOOM_R5A_SOURCE to the existing retained native ClickBench artifact"),
    );
    assert!(
        path.is_file(),
        "SHARDLOOM_R5A_SOURCE must identify an existing file"
    );
    let path = path.canonicalize().unwrap();
    let source_bytes = path.metadata().unwrap().len();
    let bounds = adapter.bounds_json();
    // Compute all four independent references before any timed repetition.
    let mut ranges = CASES
        .into_iter()
        .map(|(row_start, row_count)| RangeBaseline {
            row_start,
            row_count,
            oracle: oracle(&path, row_start, row_count),
            samples: Vec::with_capacity(REPETITIONS),
        })
        .collect::<Vec<_>>();
    for range in &mut ranges {
        for repetition in 1..=REPETITIONS {
            let sample = run(
                &path,
                range.row_start,
                range.row_count,
                repetition,
                &range.oracle,
                adapter,
            );
            let rejected = matches!(sample, SampleOutcome::AdmissionFailure(_));
            range.samples.push(sample);
            if rejected {
                break;
            }
        }
    }
    let (schema, scope, prefix) = match adapter {
        Adapter::MemoryFile => (
            "shardloom.r5a.memory_file_composition_baseline.v1",
            "bounded_native_workflow_baseline_only_no_candidate_or_speedup_claim",
            "SHARDLOOM_R5A_BASELINE",
        ),
        Adapter::OwnedArray => (
            "shardloom.r5a.owned_array_composition_candidate.v1",
            "bounded_native_workflow_owned_array_candidate_no_retention_or_speedup_claim",
            "SHARDLOOM_R5A_CANDIDATE",
        ),
    };
    let report = json!({
        "schema": schema,
        "scope": scope,
        "adapter": adapter.id(),
        "source": path,
        "source_bytes": source_bytes,
        "source_rows": SOURCE_ROWS,
        "projection": COLUMNS,
        "aggregate": "COUNT(*) AS n GROUP BY AdvEngineID ORDER BY AdvEngineID ASC",
        "session_bytes": SESSION_BYTES,
        "projection_output_limit_bytes": SESSION_BYTES,
        "parallelism": 1,
        "repetitions_per_range": REPETITIONS,
        "composition_bounds": bounds,
        "timing_scope": "new_session_open_prepare_project_compose_prepare_aggregate_execute_normal_report_render_native_owner_release",
        "normal_report_sink": "VortexLocalPrimitiveExecutionReport::to_human_text",
        "failed_admission": "recorded_without_success_timing_no_cap_increase_remaining_ranges_continue",
        "teardown_includes_bounded_evidence_snapshots": true,
        "detached_report_text_retained_for_untimed_verification": true,
        "oracle_timing": "all_ranges_before_timed_repetitions",
        "os_cache": "uncontrolled_oracle_may_warm_source",
        "memory_scope": "shared_reservation_pool_not_process_rss_or_uncredited_provider_allocations",
        "external_engine_invoked": false,
        "crate_version": env!("CARGO_PKG_VERSION"),
        "os": std::env::consts::OS,
        "architecture": std::env::consts::ARCH,
        "ranges": ranges.iter().map(RangeBaseline::json).collect::<Vec<_>>(),
    });
    println!("{prefix}={report}");
}

/// Attribution only: remove the entire consumer and composition, retaining the
/// required producer and identical untimed oracles. This is not an equivalent
/// query, an implemented handoff candidate, or a strict bound on allocator RSS.
#[test]
#[ignore = "bounded R5.a producer memory attribution; requires SHARDLOOM_R5A_SOURCE"]
#[allow(clippy::assertions_on_constants)]
fn native_composition_producer_memory_attribution() {
    assert!(!cfg!(debug_assertions), "attribution requires --release");
    let path = PathBuf::from(
        std::env::var_os("SHARDLOOM_R5A_SOURCE")
            .expect("set SHARDLOOM_R5A_SOURCE to the retained native artifact"),
    )
    .canonicalize()
    .unwrap();
    assert!(path.is_file());
    let references = CASES
        .into_iter()
        .map(|(start, row_count)| oracle(&path, start, row_count))
        .collect::<Vec<_>>();
    let mut observations = Vec::new();
    for ((start, row_count), reference) in CASES.into_iter().zip(&references) {
        for repetition in 1..=REPETITIONS {
            let session = ResidentVortexSession::new(SESSION_BYTES, 1).unwrap();
            let memory = session.memory().clone();
            let source = session.prepare_file(&path).unwrap();
            let prepared = projection(&source, start, row_count);
            let result = prepared.execute().unwrap();
            let rows = result.row_count();
            let logical_bytes = result.logical_buffer_bytes();
            drop(result);
            drop(prepared);
            drop(source);
            drop(session);
            let released = memory.snapshot();
            assert_eq!(released.reserved_bytes, 0);
            assert_eq!(rows, row_count);
            assert_eq!(logical_bytes, reference.logical_input_bytes);
            observations.push(json!({
                "row_start": start, "repetition": repetition,
                "row_end_exclusive": start + row_count,
                "rows": rows, "logical_input_bytes": logical_bytes,
                "peak_reserved_bytes": released.peak_reserved_bytes,
                "final_reserved_bytes": released.reserved_bytes,
            }));
        }
    }
    println!(
        "SHARDLOOM_R5A_PRODUCER_ATTRIBUTION={}",
        json!({
            "schema": "shardloom.r5a.producer_memory_attribution.v1",
            "scope": "required_producer_and_same_oracles_only_no_equivalent_query_or_speedup_claim",
            "composition_and_consumer_omitted": true,
            "strict_process_rss_bound": false,
            "source": path, "source_rows": SOURCE_ROWS,
            "projection": COLUMNS, "session_bytes": SESSION_BYTES, "parallelism": 1,
            "observations": observations,
        })
    );
}
