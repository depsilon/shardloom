//! Paired private-writer lifecycle, using actual native consumers and completed
//! filesystem reads. Source generation and independent oracles are untimed.

use super::*;
use crate::resident_session::read_observer::{
    ObservedFileReadAt, ReadObservation, ReadObservationLimits,
};
use serde_json::{Value, json};
use shardloom_exec::live_memory::Budgeted;
use std::time::Duration;
use vortex::{
    array::memory::MemorySessionExt as _,
    expr::{get_item, gt_eq, lit, root, select},
};

const MEMORY_BYTES: u64 = 128 << 20;
const MAX_ARRAYS: usize = 256;
const MAX_OUTPUT_BYTES: u64 = 8 << 20;
const REUSES: usize = 100;

fn elapsed(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap()
}

fn file_digest(path: &Path) -> String {
    use std::fmt::Write as _;
    let mut file = fs::File::open(path).unwrap();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut digest = sha2::Sha256::new();
    loop {
        let length = file.read(&mut buffer).unwrap();
        if length == 0 {
            break;
        }
        digest.update(&buffer[..length]);
    }
    let mut value = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(value, "{byte:02x}").unwrap();
    }
    value
}

fn observed(reader: &ObservedFileReadAt) -> ReadObservation {
    let value = reader.snapshot().unwrap();
    assert_eq!(value.pending_jobs, 0);
    assert_eq!(
        value.failed_read_calls + value.failed_before_read + value.cancelled_before_read,
        0
    );
    assert_eq!(value.rejected_requests, 0);
    value
}

#[derive(Clone, Copy, Debug)]
enum Consumer {
    ProjectIdentifier,
    FilterOnlyText,
}
impl Consumer {
    fn selected(self, row: usize) -> bool {
        match self {
            Self::ProjectIdentifier => true,
            Self::FilterOnlyText => {
                payload(row).is_some_and(|value| value.as_str() >= "ordinary renamed text 9")
            }
        }
    }
}

fn query(
    file: &vortex::file::VortexFile,
    context: &LocalVortexWriteContext,
    memory: &NativeIngestMemory,
    consumer: Consumer,
    source_rows: usize,
) -> Budgeted<Vec<ArrayRef>> {
    let projection = select(["exact_identifier"], root())
        .bind(file.dtype())
        .unwrap();
    let mut scan = file
        .scan()
        .unwrap()
        .with_ordered(true)
        .with_concurrency(2)
        .with_projection(projection);
    if matches!(consumer, Consumer::FilterOnlyText) {
        scan = scan.with_filter(
            gt_eq(
                get_item("renamed_payload", root()),
                lit("ordinary renamed text 9"),
            )
            .bind(file.dtype())
            .unwrap(),
        );
    }
    let lease = memory
        .pool
        .reserve(u64::try_from(MAX_ARRAYS * std::mem::size_of::<ArrayRef>()).unwrap())
        .unwrap();
    let mut arrays = Vec::new();
    arrays.try_reserve_exact(MAX_ARRAYS).unwrap();
    assert!(arrays.capacity() <= MAX_ARRAYS);
    let mut rows = 0_usize;
    let mut bytes = 0_u64;
    let mut execution = memory.session.create_execution_ctx();
    for array in scan.into_array_iter(&context.runtime).unwrap() {
        let array = array.unwrap();
        // The explicit consumer needs typed identifiers. Provider execution is
        // inside query elapsed; independent scalar validation is outside it.
        let field = get_item("exact_identifier", root())
            .bind(array.dtype())
            .unwrap();
        let identifiers = array
            .apply_bound(&field)
            .unwrap()
            .execute::<PrimitiveArray>(&mut execution)
            .unwrap()
            .into_array();
        rows = rows.checked_add(identifiers.len()).unwrap();
        bytes = bytes.checked_add(identifiers.nbytes()).unwrap();
        assert!(rows <= source_rows && bytes <= MAX_OUTPUT_BYTES && arrays.len() < MAX_ARRAYS);
        arrays.push(identifiers);
    }
    Budgeted::new(arrays, lease)
}

fn verify_query(
    arrays: &[ArrayRef],
    consumer: Consumer,
    source_rows: usize,
    memory: &NativeIngestMemory,
) -> usize {
    let mut expected = (0..source_rows).filter(|row| consumer.selected(*row));
    let mut execution = memory.session.create_execution_ctx();
    let mut seen = 0;
    for array in arrays {
        assert_eq!(
            array.dtype(),
            &DType::Primitive(vortex::array::dtype::PType::I64, Nullability::NonNullable)
        );
        for row in 0..array.len() {
            let source_row = expected.next().expect("extra selected row");
            assert_eq!(
                array.execute_scalar(row, &mut execution).unwrap(),
                Scalar::from(BASE + i64::try_from(source_row).unwrap())
            );
            seen += 1;
        }
    }
    assert!(expected.next().is_none(), "missing selected rows");
    seen
}

#[allow(clippy::too_many_lines)] // Keep repeated-call observations and timing scopes together.
fn consumers(
    path: &Path,
    source_rows: usize,
    context: &LocalVortexWriteContext,
    write_lifecycle: u64,
) -> Vec<Value> {
    let mut reports = Vec::new();
    for consumer in [Consumer::ProjectIdentifier, Consumer::FilterOnlyText] {
        let setup = Instant::now();
        let memory = NativeIngestMemory::new(MEMORY_BYTES).unwrap();
        let drivers =
            crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
        let session_setup_nanos = elapsed(setup);
        let open = Instant::now();
        let observer = ObservedFileReadAt::new(
            path,
            memory.session.allocator(),
            context.runtime.handle(),
            ReadObservationLimits {
                max_read_bytes: 16 << 20,
                max_attempted_bytes: 1 << 30,
                max_requests: 4096,
                max_in_flight: 32,
            },
        )
        .unwrap();
        let file = context
            .runtime
            .block_on(
                memory
                    .session
                    .open_options()
                    .with_layout_reader_cache()
                    .open_read(observer.clone()),
            )
            .unwrap();
        let native_open_nanos = elapsed(open);
        let opened = observed(&observer);
        let mut samples = Vec::new();
        let mut query_sum = 0_u64;
        let mut drop_sum = 0_u64;
        let mut verification_sum = 0_u64;
        let mut checkpoints = Vec::new();
        for reuse in 1..=REUSES {
            let before = observed(&observer);
            let started = Instant::now();
            observer.validate_generation().unwrap();
            let result = query(&file, context, &memory, consumer, source_rows);
            observer.validate_generation().unwrap();
            let native_query_nanos = elapsed(started);
            let after = observed(&observer);
            let verification = Instant::now();
            let rows = verify_query(result.value(), consumer, source_rows, &memory);
            let verification_nanos = elapsed(verification);
            assert_eq!(
                observed(&observer),
                after,
                "scalar oracle caused source I/O"
            );
            let dropping = Instant::now();
            drop(result);
            let result_drop_nanos = elapsed(dropping);
            query_sum = query_sum.checked_add(native_query_nanos).unwrap();
            drop_sum = drop_sum.checked_add(result_drop_nanos).unwrap();
            verification_sum = verification_sum.checked_add(verification_nanos).unwrap();
            samples.push(json!({"reuse":reuse, "native_query_nanos":native_query_nanos,
                "owned_result_drop_nanos":result_drop_nanos, "independent_scalar_verification_nanos":verification_nanos,
                "complete_rows":rows, "completed_read_calls":after.completed_read_calls-before.completed_read_calls,
                "completed_read_bytes":after.completed_read_bytes-before.completed_read_bytes}));
            if [1, 10, 100].contains(&reuse) {
                checkpoints.push(json!({"reuse":reuse, "native_query_nanos_sum":query_sum,
                    "owned_result_drop_nanos_sum":drop_sum,
                    "operational_lifecycle_nanos_sum":write_lifecycle + session_setup_nanos + native_open_nanos + query_sum + drop_sum,
                    "consumer_completed_read_bytes":after.completed_read_bytes-opened.completed_read_bytes,
                    "consumer_completed_read_calls":after.completed_read_calls-opened.completed_read_calls,
                    "independent_verification_nanos_excluded_sum":verification_sum,
                    "scope":"sum_of_direct_nonoverlapping_write_open_query_result_drop_spans;excludes_source_generation_oracle_JSON_between_calls_and_final_reader_close;not_continuous_wall"}));
            }
        }
        let close = Instant::now();
        drop(file);
        let final_reads = observer.close_and_drain(Duration::from_secs(10)).unwrap();
        observer.validate_generation().unwrap();
        drop(observer);
        drop(drivers);
        let reader_close_and_join_nanos = elapsed(close);
        assert!(final_reads.closed && final_reads.pending_jobs == 0);
        let snapshot = memory.pool.snapshot();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.denied_reservations, 0);
        reports.push(json!({"consumer":format!("{consumer:?}"), "session_setup_nanos":session_setup_nanos,
            "native_open_nanos":native_open_nanos, "open_completed_read_calls":opened.completed_read_calls,
            "open_completed_read_bytes":opened.completed_read_bytes, "samples":samples, "checkpoints":checkpoints,
            "reader_close_and_join_nanos":reader_close_and_join_nanos, "final_completed_read_bytes":final_reads.completed_read_bytes,
            "final_completed_read_calls":final_reads.completed_read_calls, "peak_in_flight":final_reads.peak_in_flight,
            "owned_peak_bytes":snapshot.peak_reserved_bytes, "owned_bytes_after_close":snapshot.reserved_bytes,
            "cache_scope":"same_generation_file_and_native_layout_reader_cache;no_adapter_segment_or_answer_cache;OS_cache_uncontrolled",
            "read_scope":"completed_exact_filesystem_reads_including_coalescing_gaps;not_device_bytes;all_calls_drained;zero_failures_or_rejections"}));
    }
    reports
}

#[allow(clippy::too_many_lines)] // Explicit writer, durability and independent verification boundaries.
fn write_and_query(
    context: &LocalVortexWriteContext,
    path: &Path,
    composition: Composition,
    choice: StreamFooterLayout,
    groups: usize,
) -> Value {
    let setup = Instant::now();
    let memory = NativeIngestMemory::new(MEMORY_BYTES).unwrap();
    let decision = decision(composition, path);
    let timing = VortexWriterStageTiming::default();
    let batches = (0..groups).map(batch).collect::<Vec<_>>();
    let dtype = batches[0].dtype().clone();
    let logical_input_bytes = batches.iter().map(ArrayRef::nbytes).sum::<u64>();
    let (options, evidence) =
        stream_options(context, &decision, &timing, Some(&memory), &dtype, choice).unwrap();
    let preparation_nanos = elapsed(setup);
    let lifecycle = Instant::now();
    let drivers =
        crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
    let write = Instant::now();
    let summary = options
        .blocking(&context.runtime)
        .write(
            fs::File::create(path).unwrap(),
            ArrayIteratorAdapter::new(dtype.clone(), batches.into_iter().map(Ok)),
        )
        .unwrap();
    let native_write_nanos = elapsed(write);
    assert_eq!(summary.row_count(), u64::try_from(groups * ROWS).unwrap());
    let sync = Instant::now();
    fs::File::open(path).unwrap().sync_all().unwrap();
    let sync_nanos = elapsed(sync);
    let checksum = Instant::now();
    let digest = file_digest(path);
    let checksum_readback_nanos = elapsed(checksum);
    let open = Instant::now();
    let file = context
        .runtime
        .block_on(memory.session.open_options().open_path(path))
        .unwrap();
    let footer_reopen_nanos = elapsed(open);
    assert_eq!(file.row_count(), u64::try_from(groups * ROWS).unwrap());
    assert_eq!(file.dtype(), &dtype);
    let release = Instant::now();
    drop(summary);
    drop(drivers);
    let writer_summary_and_driver_drop_nanos = elapsed(release);
    let write_lifecycle_nanos = elapsed(lifecycle);
    let bytes = fs::metadata(path).unwrap().len();
    assert!(bytes < 32 << 20);
    let segments = file.footer().segment_map().len();
    let hierarchy = file.footer().layout().encoding_id().to_string();
    let verify = Instant::now();
    let mut execution = memory.session.create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&context.runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for local in 0..array.len() {
            assert_row(&array.execute_scalar(local, &mut execution).unwrap(), seen);
            seen += 1;
        }
    }
    assert_eq!(seen, groups * ROWS);
    let independent_complete_reopen_nanos = elapsed(verify);
    drop(execution);
    drop(file);
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
    assert_eq!(memory.pool.snapshot().denied_reservations, 0);
    let mut applied = String::new();
    evidence.append_to(&mut applied);
    json!({"composition":format!("{composition:?}"), "footer_layout":format!("{choice:?}"),
        "input_groups":groups, "rows_per_group":ROWS, "rows":seen, "columns":4,
        "logical_input_bytes":logical_input_bytes, "file_bytes":bytes, "artifact_sha256":digest,
        "physical_segments":segments, "root_layout":hierarchy, "writer_applied":applied,
        "compression_policy":decision.writer_compression_policy, "compression_concurrency":decision.writer_compression_concurrency,
        "preparation_nanos_excluded":preparation_nanos, "native_write_nanos":native_write_nanos,
        "sync_nanos":sync_nanos, "checksum_readback_nanos":checksum_readback_nanos,
        "footer_reopen_nanos":footer_reopen_nanos, "writer_summary_and_driver_drop_nanos":writer_summary_and_driver_drop_nanos,
        "write_lifecycle_nanos":write_lifecycle_nanos, "independent_complete_reopen_nanos_excluded":independent_complete_reopen_nanos,
        "writer_background_drivers":1, "caller":1, "owned_writer_peak_bytes":memory.pool.snapshot().peak_reserved_bytes,
        "owned_writer_bytes_after_close":memory.pool.snapshot().reserved_bytes,
        "consumer_reports":consumers(path, seen, context, write_lifecycle_nanos),
        "lifecycle_scope":"fresh bounded native inputs/session/strategy prepared outside clock;write_finish_sync_fullSHA_footer_reopen_summary_and_driver_drop_inside;full_values_outside;private_writer_seam_not_public_ingest",
        "ownership_scope":"native_host_allocator_and_layout_reference_credits;fixture_arrays_oracle_and_provider_bypass_excluded;not_RSS"})
}

#[test]
#[ignore = "bounded paired release writer lifecycle; root owns serial timings"]
fn column_footer_release_lifecycle_and_repeated_native_consumers() {
    assert!(!std::hint::black_box(cfg!(debug_assertions)));
    assert_eq!(
        column_layout::DEFAULT_STREAM_FOOTER_LAYOUT,
        StreamFooterLayout::RetainedRows
    );
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        for groups in [3, 16] {
            for composition in [Composition::FastLoad, Composition::SourceText] {
                for pair in 0..8 {
                    let order = if pair % 2 == 0 { [StreamFooterLayout::RetainedRows, StreamFooterLayout::ColumnAddressable] }
                        else { [StreamFooterLayout::ColumnAddressable, StreamFooterLayout::RetainedRows] };
                    let mut records = Vec::new();
                    for (position, choice) in order.into_iter().enumerate() {
                        let path = directory.0.join(format!("{groups}-{composition:?}-{pair}-{position}.vortex"));
                        records.push(write_and_query(&context, &path, composition, choice, groups));
                        fs::remove_file(path).unwrap();
                    }
                    assert_eq!(records[0]["compression_policy"], records[1]["compression_policy"]);
                    assert_eq!(records[0]["compression_concurrency"], records[1]["compression_concurrency"]);
                    assert_eq!(records[0]["physical_segments"], records[1]["physical_segments"]);
                    println!("COLUMN_FOOTER_LIFECYCLE {}", json!({"pair":pair, "warmup":pair==0,
                        "actual_release_user_surfaces":cfg!(feature="release-user-surfaces"),
                        "provider_version":crate::UPSTREAM_VORTEX_PROVIDER_VERSION, "records":records,
                        "source_contract":"column_layout_tests::batch/assert_row;fresh_each_writer;3_or_16_batches_of_1033_rows;four_scalar_fields;changed_dictionary_domains_exact_i64_nullable_i16_UTF8",
                        "source_and_binary_manifest":"must_be_pinned_by_serial_runner_outside_measured_process",
                        "decision":"unmeasured_candidate;ordinary_default_unchanged"}));
                }
            }
        }
    });
}
