//! Bounded retained-writer attribution; no overlap candidate is implemented.

use super::writer_occupancy::{
    Observation, ObservedChild, ObservedExecutor, ObservedInput, ObservedRuntime,
};
use super::*;
use serde_json::{Value, json};
use std::io::{BufWriter, Write};
use vortex::{
    array::{ArrayRef, VortexSessionExecute as _, iter::ArrayIteratorAdapter},
    arrow::ArrowSessionExt as _,
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
    layout::LayoutStrategy,
};

const FILE_CAP: u64 = 256 << 20;

struct Output {
    path: PathBuf,
    writer: BufWriter<fs::File>,
    bytes: u64,
}
impl Output {
    fn new(root: &Path, label: &str) -> Self {
        let path = root.join(format!("r9b-{label}-{}.vortex", std::process::id()));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        Self {
            path,
            writer: BufWriter::with_capacity(64 << 10, file),
            bytes: 0,
        }
    }
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self.bytes.checked_add(bytes.len() as u64).unwrap();
        if next > FILE_CAP {
            return Err(std::io::Error::other("R9.b output cap exceeded"));
        }
        let written = self.writer.write(bytes)?;
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn complete_artifact_hash(path: &Path) -> String {
    let mut persisted = fs::File::open(path).unwrap();
    let mut digest = Sha256::new();
    let mut scratch = [0_u8; 65_536];
    loop {
        let read = persisted.read(&mut scratch).unwrap();
        if read == 0 {
            break;
        }
        digest.update(&scratch[..read]);
    }
    sha256_digest_string(digest.finalize())
}

fn verify_complete(
    path: &Path,
    arrays: &[ArrayRef],
    memory: &NativeIngestMemory,
    runtime: &vortex::io::runtime::current::CurrentThreadRuntime,
) -> u64 {
    let file = runtime
        .block_on(memory.session.open_options().open_path(path))
        .unwrap();
    assert_eq!(file.dtype(), arrays[0].dtype());
    let field = memory
        .session
        .arrow()
        .to_arrow_field("", file.dtype())
        .unwrap();
    let mut ctx = memory.session.create_execution_ctx();
    let mut batch = 0;
    let mut offset = 0;
    let mut rows = 0;
    for actual in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let actual = actual.unwrap();
        let mut local = 0;
        while local < actual.len() {
            let n = (actual.len() - local).min(arrays[batch].len() - offset);
            let left = memory
                .session
                .arrow()
                .execute_arrow(
                    actual.slice(local..local + n).unwrap(),
                    Some(&field),
                    &mut ctx,
                )
                .unwrap();
            let right = memory
                .session
                .arrow()
                .execute_arrow(
                    arrays[batch].slice(offset..offset + n).unwrap(),
                    Some(&field),
                    &mut ctx,
                )
                .unwrap();
            assert_eq!(
                left.to_data(),
                right.to_data(),
                "complete values differ at row {rows}"
            );
            rows += n as u64;
            local += n;
            offset += n;
            if offset == arrays[batch].len() {
                batch += 1;
                offset = 0;
            }
        }
    }
    assert_eq!(batch, arrays.len());
    assert_eq!(offset, 0);
    assert_eq!(rows, file.row_count());
    rows
}

fn run(
    region: &mut writer_profile_input::PreparedRegion,
    root: &Path,
    label: &str,
    profiled: bool,
    prefetch_input: bool,
) -> Value {
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        let memory = &mut region.memory;
        let observation = Observation::new();
        let executor = ObservedExecutor::new(context.runtime.handle(), &observation);
        memory.session = memory.session.clone().with_handle(if profiled {
            executor.handle()
        } else { context.runtime.handle() });
        let runtime = ObservedRuntime {
            base: &context.runtime, executor, observation: Arc::clone(&observation),
        };
        let dtype = region.arrays[0].dtype().clone();
        let fields = dtype.as_struct_fields_opt().unwrap().names().iter()
            .filter(|name| !name.as_ref().starts_with("__shardloom_"))
            .filter(|name| dtype.as_struct_fields_opt().unwrap().field(name.as_ref()).unwrap().is_utf8())
            .map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(fields.len(), 28);
        let timing = VortexWriterStageTiming::default();
        let child = large_source_text_vortex_write_strategy_with_dictionaries(
            262_144, 8 << 20, 4, 4, &fields, &timing, &memory.session, true);
        let child: Arc<dyn LayoutStrategy> = if profiled {
            Arc::new(ObservedChild { child, observation: Arc::clone(&observation) })
        } else { child };
        let strategy = bounded_ingest_layout::BoundedIngestLayout::new(
            child, 0, memory.pool.reserve(0).unwrap()).with_input_prefetch(prefetch_input);
        let strategy: Arc<dyn LayoutStrategy> = Arc::new(strategy);
        let strategy = if profiled {
            Arc::new(ObservedInput { child: strategy, observation: Arc::clone(&observation) }) as Arc<dyn LayoutStrategy>
        } else { strategy };
        let options = memory.session.write_options().with_strategy(strategy);
        let iter = ArrayIteratorAdapter::new(dtype, region.arrays.clone().into_iter().map(Ok));
        let before = memory.pool.snapshot().reserved_bytes;
        let mut output = Output::new(root, label);
        let drivers = crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
        let start = Instant::now();
        let writer_scope = profiled.then(|| observation.writer());
        let summary = if profiled { options.blocking(&runtime).write(&mut output, iter) }
            else { options.blocking(&context.runtime).write(&mut output, iter) }.unwrap();
        output.flush().unwrap();
        drop(summary);
        drop(drivers);
        drop(writer_scope);
        let elapsed = u64::try_from(start.elapsed().as_nanos()).unwrap();
        let occupancy = profiled.then(|| observation.snapshot());
        let peak = memory.pool.snapshot().peak_reserved_bytes;
        let complete_rows = verify_complete(&output.path, &region.arrays, memory, &context.runtime);
        // The complete native bytes also cover statistics/footer/encodings. Hash
        // outside the writer clock, streaming through a bounded scratch buffer.
        let artifact_sha256 = complete_artifact_hash(&output.path);
        assert_eq!(memory.pool.snapshot().reserved_bytes, before);
        assert_eq!(memory.pool.snapshot().denied_reservations, 0);
        json!({"profiled":profiled,"prefetch_input":prefetch_input,"complete_writer_nanos":elapsed,
            "complete_artifact_sha256":artifact_sha256,
            "file_bytes":output.bytes,"complete_verified_rows":complete_rows,
            "retained_input_reserved_bytes":before,"cumulative_region_pool_peak_reserved_bytes":peak,
            "peak_scope":"region_lifetime_including_preparation_and_prior_writer_verification;not_per_sample_writer_peak",
            "occupancy":occupancy,"stage_report":timing.stages.snapshot().evidence_fields(),
            "boundary":"native_write_flush_summary_and_driver_drop;preparation_and_complete_value_verification_excluded;not_full_ingest_or_fsync_durability",
            "ownership":"all_prepared_inputs_remain_owned;host_allocator_and_layout_credits_only;source_Arrow_and_provider_bypass_excluded"})
    })
}

#[test]
#[ignore = "manual R9.b bounded native writer attribution; exclusive guarded runner only"]
fn retained_writer_subtree_occupancy_screen() {
    assert!(!std::hint::black_box(cfg!(debug_assertions)));
    let source = PathBuf::from(std::env::var_os("SHARDLOOM_R9B_SOURCE").unwrap());
    let root = PathBuf::from(std::env::var_os("SHARDLOOM_R9B_SCRATCH").unwrap())
        .canonicalize()
        .unwrap();
    assert!(
        root.starts_with(
            PathBuf::from(std::env::var_os("HOME").unwrap()).join("LocalData/shardloom")
        )
    );
    let mut reports = Vec::new();
    for row_group in [0, 113, 225] {
        let mut samples = Vec::new();
        for pair in 0..3 {
            for profiled in if pair % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                // A previous write or verification may populate native statistics.
                // Fresh ownership per sample matches the single-use ingest input.
                let mut region = writer_profile_input::prepare_region(&source, row_group);
                let mut sample = run(
                    &mut region,
                    &root,
                    &format!("rg{row_group}-p{pair}-{profiled}"),
                    profiled,
                    false,
                );
                sample["preparation"] = region.report;
                drop(region.arrays);
                assert_eq!(region.memory.pool.snapshot().reserved_bytes, 0);
                samples.push(sample);
            }
        }
        reports.push(json!({"row_group":row_group,"samples":samples}));
    }
    println!(
        "SHARDLOOM_R9B_SCREEN={}",
        json!({"regions":reports,
        "scope":"retained_writer_attribution_only;fresh_prepared_native_arrays_per_sample;OS_cache_uncontrolled;all_samples_retained"})
    );
}

#[test]
#[ignore = "manual R9.b paired input-lookahead screen; exclusive guarded runner only"]
fn retained_writer_input_lookahead_screen() {
    assert!(!std::hint::black_box(cfg!(debug_assertions)));
    let source = PathBuf::from(std::env::var_os("SHARDLOOM_R9B_SOURCE").unwrap());
    let root = PathBuf::from(std::env::var_os("SHARDLOOM_R9B_SCRATCH").unwrap())
        .canonicalize()
        .unwrap();
    assert!(
        root.starts_with(
            PathBuf::from(std::env::var_os("HOME").unwrap()).join("LocalData/shardloom")
        )
    );
    let mut reports = Vec::new();
    for row_group in [0, 113, 225] {
        let mut samples = Vec::new();
        let mut expected_artifact = None;
        for pair in 0..3 {
            for prefetch in if pair % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let mut region = writer_profile_input::prepare_region(&source, row_group);
                let mut sample = run(
                    &mut region,
                    &root,
                    &format!("rg{row_group}-p{pair}-lookahead{prefetch}"),
                    false,
                    prefetch,
                );
                let artifact = sample["complete_artifact_sha256"].as_str().unwrap();
                if let Some(expected) = expected_artifact.as_ref() {
                    assert_eq!(
                        artifact, expected,
                        "complete artifact/statistics/footer changed"
                    );
                } else {
                    expected_artifact = Some(artifact.to_owned());
                }
                sample["preparation"] = region.report;
                drop(region.arrays);
                assert_eq!(region.memory.pool.snapshot().reserved_bytes, 0);
                samples.push(sample);
            }
        }
        reports.push(json!({"row_group":row_group,"samples":samples}));
    }
    println!(
        "SHARDLOOM_R9B_LOOKAHEAD={}",
        json!({"regions":reports,
        "scope":"paired_bounded_writer_only;fresh_native_inputs_per_sample;complete_values_and_artifact_bytes_equal;not_full_ingest;OS_cache_uncontrolled"})
    );
}
