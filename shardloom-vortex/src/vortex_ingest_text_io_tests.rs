//! Bounded, paired native layout experiment. This deliberately invokes the
//! large-source writer composition below its public admission threshold.
//! Read observations are completed positional filesystem requests, including
//! coalescing gaps and repeats; they are not storage-device or cold-cache I/O.
//! The shuffled control permutes text values while keeping identifiers in the
//! same physical order, so numeric input stays identical across text profiles.

use super::*;
use std::{io::Write as _, os::unix::fs::MetadataExt as _, time::Duration};

use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value, json};
use vortex::{
    array::{
        ArrayRef, Columnar, IntoArray as _, VortexSessionExecute as _,
        dtype::{DType, Nullability, PType},
        memory::MemorySessionExt as _,
    },
    file::{OpenOptionsSessionExt as _, VortexFile, WriteOptionsSessionExt as _},
    io::runtime::BlockingRuntime as _,
    layout::{
        LayoutChildType, LayoutRef,
        layouts::{flat::Flat, zoned::Zoned},
    },
};

use crate::resident_session::{
    ResidentVortexSession,
    read_observer::{ObservedFileReadAt, ReadObservation, ReadObservationLimits},
};

const MIB: u64 = 1 << 20;
const FILE_LIMIT: u64 = 64 * MIB;
const TEXT: &str = "renamed_nullable_label";
const ID: &str = "exact_identifier";
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

struct FixtureGeometry {
    name: &'static str,
    rows: usize,
    zone_rows: usize,
    source_batch_rows: usize,
    suffix_min: usize,
    suffix_variants: usize,
    input_limit: usize,
}

fn configured_geometry() -> FixtureGeometry {
    let geometry = match std::env::var("SHARDLOOM_TEXT_IO_GEOMETRY").as_deref() {
        Ok("production_rows") => FixtureGeometry {
            name: "production_rows",
            rows: 1_048_576,
            zone_rows: 262_144,
            source_batch_rows: 262_144,
            suffix_min: 32,
            suffix_variants: 1,
            input_limit: 48 * 1024 * 1024,
        },
        Ok("diagnostic") | Err(std::env::VarError::NotPresent) => FixtureGeometry {
            name: "diagnostic",
            rows: 32_768,
            zone_rows: 8_192,
            source_batch_rows: 32_768,
            suffix_min: 256,
            suffix_variants: 257,
            input_limit: 32 * 1024 * 1024,
        },
        _ => panic!("SHARDLOOM_TEXT_IO_GEOMETRY must be diagnostic or production_rows"),
    };
    assert!(geometry.rows.is_power_of_two());
    assert_eq!(geometry.rows, 4 * geometry.zone_rows);
    assert_eq!(geometry.rows % geometry.source_batch_rows, 0);
    // Reject before constructing input. Include worst-case text, identifiers,
    // i32 offsets and validity; caller/container metadata remains separately scoped.
    let maximum_text = geometry.rows * (geometry.suffix_min + geometry.suffix_variants + 1);
    let maximum_input = maximum_text
        + geometry.rows * 8
        + (geometry.rows + geometry.rows / geometry.source_batch_rows) * 4
        + geometry.rows.div_ceil(8);
    assert!(maximum_input <= geometry.input_limit);
    geometry
}

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

impl FixtureDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-text-layout-io-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

/// The limit is checked before each native writer call reaches the filesystem.
struct BoundedFile {
    file: fs::File,
    bytes: u64,
}

impl std::io::Write for BoundedFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let length = u64::try_from(bytes.len()).unwrap();
        if length > FILE_LIMIT - self.bytes {
            return Err(std::io::Error::other(
                "text-layout fixture exceeds artifact byte limit",
            ));
        }
        let written = self.file.write(bytes)?;
        self.bytes += u64::try_from(written).unwrap();
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

#[derive(Clone, Copy)]
enum PhysicalOrder {
    Clustered,
    Shuffled,
}

impl PhysicalOrder {
    fn name(self) -> &'static str {
        match self {
            Self::Clustered => "clustered",
            Self::Shuffled => "bijective_text_permutation",
        }
    }

    fn logical_row(self, physical_row: usize, geometry: &FixtureGeometry) -> usize {
        match self {
            Self::Clustered => physical_row,
            // An odd multiplier is a permutation modulo the power-of-two row count.
            Self::Shuffled => (physical_row * 4051 + 17) % geometry.rows,
        }
    }
}

#[derive(Clone, Copy)]
enum Writer {
    Baseline,
    Zoned,
}

impl Writer {
    fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline_text_zstd",
            Self::Zoned => "zoned_text_zstd",
        }
    }
}

#[derive(Clone, Copy)]
struct Query {
    name: &'static str,
    threshold: Option<&'static str>,
    project_text: bool,
    guard_non_null: bool,
}

fn queries() -> Vec<Query> {
    [
        ("zero_zone", Some("~")),
        ("one_zone", Some("z")),
        ("half_zones", Some("m")),
        ("all_rows_including_nulls", None),
    ]
    .into_iter()
    .flat_map(|(name, threshold)| {
        [false, true].into_iter().flat_map(move |project_text| {
            [false, true]
                .into_iter()
                .filter(move |guard| threshold.is_some() || !*guard)
                .map(move |guard_non_null| Query {
                    name,
                    threshold,
                    project_text,
                    guard_non_null,
                })
        })
    })
    .collect()
}

fn label(logical_row: usize, geometry: &FixtureGeometry) -> Option<String> {
    if logical_row / geometry.zone_rows == 1 || logical_row.is_multiple_of(257) {
        return None;
    }
    let prefix = b"anmz"[logical_row / geometry.zone_rows];
    let suffix_len = geometry.suffix_min + logical_row % geometry.suffix_variants;
    let mut bytes = Vec::with_capacity(suffix_len + 2);
    bytes.extend([prefix, b'|']);
    let mut state = u64::try_from(logical_row).unwrap() + 0x9e37_79b9_7f4a_7c15;
    for _ in 0..suffix_len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push(33 + u8::try_from(state % 94).unwrap());
    }
    Some(String::from_utf8(bytes).unwrap())
}

fn hash_row(
    digest: &mut sha2::Sha256,
    physical_row: usize,
    text: Option<&str>,
    include_text: bool,
) {
    digest.update(((1_i64 << 60) + i64::try_from(physical_row).unwrap()).to_le_bytes());
    if include_text {
        digest.update([u8::from(text.is_some())]);
        if let Some(text) = text {
            digest.update(u64::try_from(text.len()).unwrap().to_le_bytes());
            digest.update(text.as_bytes());
        }
    }
}

/// Independent native buffers/statistics per source batch and per writer.
fn fresh_batch(
    order: PhysicalOrder,
    geometry: &FixtureGeometry,
    start: usize,
    digest: &mut sha2::Sha256,
) -> (ArrayRef, usize) {
    let end = start + geometry.source_batch_rows;
    let labels = (start..end)
        .map(|row| label(order.logical_row(row, geometry), geometry))
        .collect::<Vec<_>>();
    let text_bytes = labels.iter().flatten().map(String::len).sum::<usize>();
    let rows = geometry.source_batch_rows;
    let logical_bytes = text_bytes + rows * 8 + (rows + 1) * 4 + rows.div_ceil(8);
    assert!(logical_bytes <= geometry.input_limit);
    for (row, text) in labels.iter().enumerate() {
        hash_row(digest, start + row, text.as_deref(), true);
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(TEXT, DataType::Utf8, true),
            Field::new(ID, DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                labels.iter().map(Option::as_deref).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(
                (start..end).map(|row| (1_i64 << 60) + i64::try_from(row).unwrap()),
            )),
        ],
    )
    .unwrap();
    (
        arrow_record_batch_to_vortex_array(batch).unwrap(),
        logical_bytes,
    )
}

fn fresh_input(order: PhysicalOrder, geometry: &FixtureGeometry) -> (Vec<ArrayRef>, String, usize) {
    let mut digest = sha2::Sha256::new();
    let mut logical_bytes = 0;
    let arrays = (0..geometry.rows)
        .step_by(geometry.source_batch_rows)
        .map(|start| {
            let (array, bytes) = fresh_batch(order, geometry, start, &mut digest);
            logical_bytes += bytes;
            assert!(logical_bytes <= geometry.input_limit);
            array
        })
        .collect();
    (
        arrays,
        sha256_digest_string(digest.finalize()),
        logical_bytes,
    )
}

#[derive(Clone)]
struct Segment {
    id: usize,
    offset: u64,
    bytes: u64,
}

struct Artifact {
    path: PathBuf,
    bytes: u64,
    sha256: String,
    segments: Vec<Segment>,
    report: Value,
}

fn generation(path: &Path) -> Value {
    let m = path.metadata().unwrap();
    json!({"device": m.dev(), "inode": m.ino(), "bytes": m.len(),
        "mtime_seconds": m.mtime(), "mtime_nanos": m.mtime_nsec(),
        "ctime_seconds": m.ctime(), "ctime_nanos": m.ctime_nsec()})
}

#[allow(clippy::too_many_lines)] // Paired writer lifecycle and persisted geometry share one owned artifact.
fn write_artifact(
    directory: &Path,
    order: PhysicalOrder,
    writer: Writer,
    sample: usize,
    geometry: &FixtureGeometry,
) -> Artifact {
    let prepare_started = Instant::now();
    let (input, source_sha256, input_bytes) = fresh_input(order, geometry);
    let source_prepare_ns = prepare_started.elapsed().as_nanos();
    let setup_started = Instant::now();
    let context = LocalVortexWriteContext::open();
    let _drivers =
        crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
    let timing = VortexWriterStageTiming::default();
    let fields = [TEXT.to_string()];
    let selected_strategy = match writer {
        Writer::Baseline => large_source_text_vortex_write_strategy,
        Writer::Zoned => zoned_source_text_vortex_write_strategy,
    }(
        geometry.zone_rows,
        8 * MIB,
        2,
        2,
        &fields,
        &timing,
        &context.session,
    );
    let batch_count = geometry.rows / geometry.source_batch_rows;
    let reference_pool = LiveMemoryPool::new(4096).unwrap();
    let strategy: Arc<dyn vortex::layout::LayoutStrategy> = if batch_count > 1 {
        let reference_bytes = batch_count
            .checked_mul(std::mem::size_of::<LayoutRef>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .unwrap();
        Arc::new(bounded_ingest_layout::BoundedIngestLayout::new(
            selected_strategy,
            batch_count,
            reference_pool.reserve(reference_bytes).unwrap(),
        ))
    } else {
        selected_strategy
    };
    let setup_ns = setup_started.elapsed().as_nanos();
    let path = directory.join(format!(
        "{}-{}-{sample}.vortex",
        order.name(),
        writer.name()
    ));
    let started = Instant::now();
    let mut sink = BoundedFile {
        file: fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap(),
        bytes: 0,
    };
    let dtype = input[0].dtype().clone();
    context
        .session
        .write_options()
        .with_strategy(strategy)
        .blocking(&context.runtime)
        .write(
            &mut sink,
            vortex::array::iter::ArrayIteratorAdapter::new(dtype, input.into_iter().map(Ok)),
        )
        .unwrap();
    sink.flush().unwrap();
    let write_and_flush_ns = started.elapsed().as_nanos();
    let sync_started = Instant::now();
    sink.file.sync_all().unwrap();
    let sync_all_ns = sync_started.elapsed().as_nanos();
    let bytes = sink.bytes;
    drop(sink);
    assert_eq!(bytes, path.metadata().unwrap().len());
    let digest_started = Instant::now();
    let sha256 = sha256_file_digest(&path, "bounded text layout fixture").unwrap();
    let full_readback_sha256_ns = digest_started.elapsed().as_nanos();
    let reopen_started = Instant::now();
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(&path))
        .unwrap();
    let native_footer_reopen_ns = reopen_started.elapsed().as_nanos();
    let geometry_started = Instant::now();
    assert_eq!(file.row_count(), u64::try_from(geometry.rows).unwrap());
    if batch_count > 1 {
        let root = file.footer().layout();
        assert!(
            root.as_opt::<vortex::layout::layouts::chunked::Chunked>()
                .is_some()
        );
        assert_eq!(root.nslots(), batch_count);
        for slot in 0..batch_count {
            assert_eq!(
                root.slot(slot).unwrap().unwrap().row_count(),
                u64::try_from(geometry.source_batch_rows).unwrap()
            );
        }
    }
    let (segments, layout_geometry) = inspect_layout_geometry(&file);
    let geometry_inspection_ns = geometry_started.elapsed().as_nanos();
    let references = reference_pool.snapshot();
    assert_eq!(references.reserved_bytes, 0);
    assert_eq!(references.denied_reservations, 0);
    let report = json!({
        "writer": writer.name(), "physical_order": order.name(), "sample": sample,
        "artifact_bytes": bytes, "artifact_sha256": sha256,
        "source_values_sha256": source_sha256, "source_logical_buffer_bytes": input_bytes,
        "source_prepare_ns": source_prepare_ns, "writer_setup_ns": setup_ns,
        "write_and_flush_ns": write_and_flush_ns, "file_sync_all_ns": sync_all_ns,
        "full_readback_sha256_ns": full_readback_sha256_ns,
        "native_footer_reopen_ns": native_footer_reopen_ns,
        "geometry_inspection_ns": geometry_inspection_ns,
        "lifecycle_excluding_source_and_setup_ns": write_and_flush_ns + sync_all_ns
            + full_readback_sha256_ns + native_footer_reopen_ns,
        "source_generation": generation(&path), "geometry": layout_geometry,
        "fixture_geometry": geometry.name,
        "source_batch_rows": geometry.source_batch_rows, "source_batch_count": batch_count,
        "bounded_per_batch_writer_subtrees": batch_count > 1,
        "layout_reference_memory": {"limit_bytes": references.limit_bytes,
            "peak_reserved_bytes": references.peak_reserved_bytes,
            "live_bytes_after_write": references.reserved_bytes,
            "denied_reservations": references.denied_reservations,
            "scope": "root child LayoutRef capacity only; native writer allocator and source fixture excluded"},
        "directory_publication_durability_claimed": false,
    });
    Artifact {
        path,
        bytes,
        sha256,
        segments,
        report,
    }
}

struct LayoutVisit {
    layout: LayoutRef,
    column: String,
    auxiliary: bool,
    row_offset: Option<u64>,
    path: Vec<String>,
}

/// Full metadata inspection is explicit and outside every query/open timer.
fn inspect_layout_geometry(file: &VortexFile) -> (Vec<Segment>, Value) {
    let segments = file
        .footer()
        .segment_map()
        .iter()
        .enumerate()
        .map(|(id, spec)| Segment {
            id,
            offset: spec.offset,
            bytes: u64::from(spec.length),
        })
        .collect::<Vec<_>>();
    assert!(segments.len() <= 4096);
    let mut pending = vec![LayoutVisit {
        layout: file.footer().layout().clone(),
        column: String::new(),
        auxiliary: false,
        row_offset: Some(0),
        path: Vec::new(),
    }];
    let mut nodes = Vec::new();
    while let Some(mut visit) = pending.pop() {
        assert!(nodes.len() + pending.len() < 4096);
        visit.path.push(visit.layout.encoding_id().to_string());
        let flat_id = visit.layout.as_opt::<Flat>().map(|flat| *flat.segment_id());
        let zoned = visit.layout.as_opt::<Zoned>();
        nodes.push(json!({
            "path": visit.path, "column": visit.column, "auxiliary": visit.auxiliary,
            "row_offset": visit.row_offset, "rows": visit.layout.row_count(),
            "flat_segment_id": flat_id,
            "zone_rows": zoned.map(vortex::layout::Layout::<Zoned>::zone_len),
            "zone_count": zoned.map(vortex::layout::Layout::<Zoned>::nzones),
        }));
        for slot in (0..visit.layout.nslots()).rev() {
            let Some(layout) = visit.layout.slot(slot).unwrap() else {
                continue;
            };
            let relation = visit.layout.slot_type(slot).unwrap();
            let auxiliary = visit.auxiliary || matches!(&relation, LayoutChildType::Auxiliary(_));
            let column = match &relation {
                LayoutChildType::Field(name) if !auxiliary => name.to_string(),
                _ => visit.column.clone(),
            };
            let row_offset = visit
                .row_offset
                .zip(relation.row_offset())
                .map(|(a, b)| a + b);
            let mut path = visit.path.clone();
            path.push(relation.name().to_string());
            pending.push(LayoutVisit {
                layout,
                column,
                auxiliary,
                row_offset,
                path,
            });
        }
    }
    let specs = segments
        .iter()
        .map(|s| {
            json!({
                "id": s.id, "offset": s.offset, "bytes": s.bytes,
                "larger_than_default_coalescing_gap": s.bytes > MIB,
            })
        })
        .collect::<Vec<_>>();
    (
        segments,
        json!({"segments": specs, "layout_nodes": nodes,
        "native_default_coalescing_gap_bytes": MIB,
        "native_default_coalescing_max_range_bytes": 4 * MIB,
        "scope": "persisted footer metadata; no array payload inspection"}),
    )
}

fn observation(report: &ReadObservation) -> Value {
    json!({
        "closed": report.closed, "admitted_requests": report.admitted_requests,
        "attempted_bytes": report.attempted_bytes,
        "completed_read_calls": report.completed_read_calls,
        "completed_read_bytes": report.completed_read_bytes,
        "failed_read_calls": report.failed_read_calls,
        "failed_before_read": report.failed_before_read,
        "cancelled_before_read": report.cancelled_before_read,
        "rejected_requests": report.rejected_requests, "pending_jobs": report.pending_jobs,
        "peak_in_flight": report.peak_in_flight,
        "completed_ranges": report.completed_ranges.iter().map(|range| {
            json!([range.offset, range.length])
        }).collect::<Vec<_>>(),
    })
}

fn assert_observation(report: &ReadObservation, file_bytes: u64) {
    assert_eq!(report.pending_jobs, 0);
    assert_eq!(report.failed_read_calls + report.failed_before_read, 0);
    assert_eq!(report.rejected_requests, 0);
    assert!(report.admitted_requests <= 4096);
    assert!(report.attempted_bytes <= 256 * MIB);
    assert_eq!(report.completed_read_calls, report.completed_ranges.len());
    assert_eq!(
        report.completed_read_bytes,
        report
            .completed_ranges
            .iter()
            .map(|range| u64::try_from(range.length).unwrap())
            .sum::<u64>()
    );
    for range in &report.completed_ranges {
        assert!(range.length <= 32 * 1024 * 1024);
        assert!(range.offset + u64::try_from(range.length).unwrap() <= file_bytes);
    }
}

/// Per-segment coverage is a union, unlike total read bytes which count repeats.
fn segment_coverage(segments: &[Segment], reads: &ReadObservation) -> Vec<Value> {
    segments
        .iter()
        .map(|segment| {
            let end = segment.offset + segment.bytes;
            let mut intersections = reads
                .completed_ranges
                .iter()
                .filter_map(|read| {
                    let start = segment.offset.max(read.offset);
                    let stop = end.min(read.offset + u64::try_from(read.length).unwrap());
                    (start < stop).then_some((start, stop))
                })
                .collect::<Vec<_>>();
            intersections.sort_unstable();
            let mut covered = 0;
            let mut cursor = segment.offset;
            for (start, stop) in intersections {
                covered += stop.saturating_sub(start.max(cursor));
                cursor = cursor.max(stop);
            }
            assert!(covered <= segment.bytes);
            json!({"segment_id": segment.id, "covered_unique_bytes": covered,
            "unread_bytes": segment.bytes - covered})
        })
        .collect()
}

fn verify_rows(
    arrays: &[ArrayRef],
    order: PhysicalOrder,
    query: Query,
    session: &vortex::session::VortexSession,
    geometry: &FixtureGeometry,
) -> (usize, String) {
    let expected = (0..geometry.rows)
        .filter_map(|row| {
            let text = label(order.logical_row(row, geometry), geometry);
            query
                .threshold
                .is_none_or(|threshold| text.as_deref().is_some_and(|text| text >= threshold))
                .then_some((row, text))
        })
        .collect::<Vec<_>>();
    let mut execution = session.create_execution_ctx();
    let mut seen = 0;
    let mut digest = sha2::Sha256::new();
    for array in arrays {
        let schema = array.dtype().as_struct_fields();
        let expected_names = if query.project_text {
            vec![TEXT, ID]
        } else {
            vec![ID]
        };
        assert_eq!(
            schema
                .names()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            expected_names
        );
        assert_eq!(
            schema.field(ID),
            Some(DType::Primitive(PType::I64, Nullability::NonNullable))
        );
        if query.project_text {
            assert_eq!(schema.field(TEXT), Some(DType::Utf8(Nullability::Nullable)));
        }
        // Decode each selected native field once at this explicitly outside-clock
        // correctness boundary; do not repeatedly decompress a text chunk per row.
        let mut field = |name| {
            let projection = vortex::expr::get_item(name, vortex::expr::root())
                .bind(array.dtype())
                .unwrap();
            array
                .clone()
                .apply_bound(&projection)
                .unwrap()
                .execute::<Columnar>(&mut execution)
                .unwrap()
                .into_array()
        };
        let identifiers = field(ID);
        let texts = query.project_text.then(|| field(TEXT));
        for row in 0..array.len() {
            let (expected_row, text) = expected.get(seen).expect("native scan returned extra rows");
            assert_eq!(
                identifiers.execute_scalar(row, &mut execution).unwrap(),
                ((1_i64 << 60) + i64::try_from(*expected_row).unwrap()).into()
            );
            if let Some(texts) = &texts {
                let actual = texts.execute_scalar(row, &mut execution).unwrap();
                if let Some(text) = text {
                    assert_eq!(actual, text.as_str().into());
                } else {
                    assert!(actual.is_null());
                }
            }
            hash_row(
                &mut digest,
                *expected_row,
                text.as_deref(),
                query.project_text,
            );
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len());
    (seen, sha256_digest_string(digest.finalize()))
}

#[allow(clippy::too_many_lines)] // Keep the measured open/scan/drop/drain ownership sequence together.
fn run_query(
    artifact: &Artifact,
    order: PhysicalOrder,
    query: Query,
    geometry: &FixtureGeometry,
) -> Value {
    let preparation = Instant::now();
    let resident = ResidentVortexSession::new(128 * MIB, 2).unwrap();
    let session_prepare_ns = preparation.elapsed().as_nanos();
    let mut report = resident.with_native_session(|session, runtime| {
        let open_started = Instant::now();
        let reader = ObservedFileReadAt::new(&artifact.path, session.allocator(), runtime.handle(),
            ReadObservationLimits { max_read_bytes: 32 * 1024 * 1024,
                max_attempted_bytes: 256 * MIB, max_requests: 4096, max_in_flight: 32 }).unwrap();
        let file = runtime.block_on(session.open_options().open_read(reader.clone())).unwrap();
        let open_ns = open_started.elapsed().as_nanos();
        let opened = reader.snapshot().unwrap();
        assert_observation(&opened, artifact.bytes);
        assert!(!opened.closed && opened.completed_read_calls > 0);
        let scan_started = Instant::now();
        let mut scan = file.scan().unwrap().with_ordered(true);
        if !query.project_text {
            scan = scan.with_projection(vortex::expr::select([ID], vortex::expr::root())
                .bind(file.dtype()).unwrap());
        }
        if let Some(threshold) = query.threshold {
            let field = vortex::expr::get_item(TEXT, vortex::expr::root());
            let comparison = vortex::expr::gt_eq(field.clone(), vortex::expr::lit(threshold));
            let filter = if query.guard_non_null {
                // Both writers use this selection-equivalent WHERE form. It
                // allows native null_count metadata to prove an all-null zone false.
                vortex::expr::and(vortex::expr::is_not_null(field), comparison)
            } else { comparison };
            scan = scan.with_filter(filter.bind(file.dtype()).unwrap());
        }
        let mut arrays = Vec::new();
        let mut rows = 0;
        let mut output_bytes = 0;
        for array in scan.into_array_iter(runtime).unwrap() {
            let array = array.unwrap();
            rows += array.len();
            output_bytes += array.nbytes();
            assert!(rows <= geometry.rows && arrays.len() < 4096 && output_bytes <= 128 * MIB);
            arrays.push(array);
        }
        let native_array_scan_ns = scan_started.elapsed().as_nanos();
        let verify_started = Instant::now();
        let (verified_rows, result_sha256) = verify_rows(&arrays, order, query, session, geometry);
        let independent_scalar_validation_ns = verify_started.elapsed().as_nanos();
        assert_eq!(verified_rows, rows);
        let drop_started = Instant::now();
        drop(arrays);
        drop(file);
        let result_and_native_file_drop_ns = drop_started.elapsed().as_nanos();
        let drain_started = Instant::now();
        let final_reads = reader.close_and_drain(DRAIN_TIMEOUT).unwrap();
        let drain_ns = drain_started.elapsed().as_nanos();
        reader.validate_generation().unwrap();
        assert_observation(&final_reads, artifact.bytes);
        assert!(final_reads.closed);
        assert_eq!(reader.snapshot().unwrap(), final_reads);
        let report = json!({
            "case": query.name, "project_text": query.project_text,
            "explicit_non_null_where_guard": query.guard_non_null,
            "threshold": query.threshold, "rows": verified_rows, "exact_values_passed": true,
            "result_sha256": result_sha256, "session_prepare_ns": session_prepare_ns,
            "open_ns": open_ns, "native_array_scan_ns": native_array_scan_ns,
            "independent_scalar_validation_ns": independent_scalar_validation_ns,
            "result_and_native_file_drop_ns": result_and_native_file_drop_ns, "drain_ns": drain_ns,
            "open_scan_drop_drain_excluding_reference_ns": open_ns + native_array_scan_ns
                + result_and_native_file_drop_ns + drain_ns,
            "open_observation": observation(&opened), "final_observation": observation(&final_reads),
            "post_open_completed_bytes": final_reads.completed_read_bytes - opened.completed_read_bytes,
            "segment_coverage_including_open": segment_coverage(&artifact.segments, &final_reads),
            "source_generation_validated_after_drain": true,
        });
        drop(reader);
        Ok(report)
    }).unwrap();
    let snapshot = resident.snapshot();
    let memory = snapshot.memory;
    assert_eq!(memory.reserved_bytes, 0);
    assert_eq!(memory.denied_reservations, 0);
    report["native_allocator_peak_reserved_bytes"] = json!(memory.peak_reserved_bytes);
    report["native_allocator_live_bytes_after_release"] = json!(memory.reserved_bytes);
    report["native_allocator_denied_reservations"] = json!(memory.denied_reservations);
    report["provider_background_cpu_drivers"] = json!(snapshot.provider_background_workers);
    report["provider_cpu_drivers_including_caller"] =
        json!(snapshot.provider_background_workers + 1);
    report
}

fn paired_queries(first: &(Writer, Vec<Value>), second: &(Writer, Vec<Value>)) -> Vec<Value> {
    let (baseline, candidate) = if matches!(first.0, Writer::Baseline) {
        (&first.1, &second.1)
    } else {
        (&second.1, &first.1)
    };
    baseline.iter().zip(candidate).map(|(control, zoned)| {
        assert_eq!(control["result_sha256"], zoned["result_sha256"]);
        assert_eq!(control["rows"], zoned["rows"]);
        let bytes = |value: &Value| value["final_observation"]["completed_read_bytes"].as_u64().unwrap();
        json!({"case": control["case"], "project_text": control["project_text"],
            "explicit_non_null_where_guard": control["explicit_non_null_where_guard"],
            "baseline_completed_read_bytes": bytes(control), "zoned_completed_read_bytes": bytes(zoned),
            "saved_completed_read_bytes": i128::from(bytes(control)) - i128::from(bytes(zoned)),
            "full_result_equal": true})
    }).collect()
}

#[test]
#[allow(clippy::too_many_lines)] // One paired matrix emits its complete scoped evidence envelope.
fn text_layout_filesystem_reads_and_lifecycle_are_exact_and_bounded() {
    let geometry = configured_geometry();
    let configured = std::env::var("SHARDLOOM_TEXT_IO_SAMPLES").ok();
    let samples = configured
        .as_deref()
        .map_or(1, |value| value.parse::<usize>().unwrap());
    assert!(
        (1..=3).contains(&samples),
        "SHARDLOOM_TEXT_IO_SAMPLES must be 1..=3"
    );
    let warmups = usize::from(configured.is_some());
    let directory = FixtureDirectory::new();
    let mut records = Vec::new();
    let mut comparisons = Vec::new();
    for sample in 0..samples + warmups {
        for order in [PhysicalOrder::Clustered, PhysicalOrder::Shuffled] {
            let writers = if sample % 2 == 0 {
                [Writer::Baseline, Writer::Zoned]
            } else {
                [Writer::Zoned, Writer::Baseline]
            };
            let mut pairs = Vec::new();
            let mut source_hash = None;
            for writer in writers {
                let mut artifact = write_artifact(&directory.0, order, writer, sample, &geometry);
                let digest = artifact.report["source_values_sha256"]
                    .as_str()
                    .unwrap()
                    .to_string();
                if let Some(previous) = &source_hash {
                    assert_eq!(previous, &digest);
                }
                source_hash = Some(digest);
                let queries = queries()
                    .into_iter()
                    .map(|query| run_query(&artifact, order, query, &geometry))
                    .collect::<Vec<_>>();
                assert_eq!(
                    sha256_file_digest(&artifact.path, "post-query immutable fixture").unwrap(),
                    artifact.sha256
                );
                artifact.report["queries"] = json!(queries);
                artifact.report["warmup"] = json!(sample < warmups);
                artifact.report["source_generation_after_all_queries"] = generation(&artifact.path);
                assert_eq!(
                    artifact.report["source_generation"],
                    artifact.report["source_generation_after_all_queries"]
                );
                records.push(artifact.report);
                pairs.push((writer, queries));
                fs::remove_file(&artifact.path).unwrap();
            }
            comparisons.push(json!({"sample": sample, "warmup": sample < warmups,
                "physical_order": order.name(), "queries": paired_queries(&pairs[0], &pairs[1])}));
        }
    }
    assert_eq!(records.len(), 4 * (samples + warmups));
    println!(
        "TEXT_LAYOUT_IO_EVIDENCE {}",
        json!({
            "schema_version": "shardloom.text_layout_io_experiment.v1", "status": "passed",
            "fixture_geometry": geometry.name,
            "rows": geometry.rows, "zone_rows": geometry.zone_rows, "zones": 4,
            "source_batch_rows": geometry.source_batch_rows,
            "source_batch_count": geometry.rows / geometry.source_batch_rows,
            "bounded_per_batch_writer_subtrees": geometry.rows > geometry.source_batch_rows,
            "text_suffix_ascii_bytes": [geometry.suffix_min, geometry.suffix_min + geometry.suffix_variants - 1],
            "text_prefix_bytes": 2, "identifier_base": 1_i64 << 60,
            "samples_per_writer_order": samples, "warmups_per_writer_order": warmups,
            "query_records": records.len() * queries().len(),
            "writer_background_cpu_drivers": 1, "writer_cpu_drivers_including_caller": 2,
            "query_requested_cpu_parallelism": 2,
            "native_version": "0.85.0", "intended_validation_feature": "release-user-surfaces",
            "release_user_surfaces_enabled": cfg!(feature = "release-user-surfaces"),
            "host_os": std::env::consts::OS, "host_arch": std::env::consts::ARCH,
            "available_parallelism": std::thread::available_parallelism().unwrap().get(),
            "limits": {"artifact_bytes": FILE_LIMIT, "input_logical_bytes": geometry.input_limit,
                "single_read_bytes": 32 * MIB, "query_attempted_bytes": 256 * MIB,
                "query_read_events": 4096, "in_flight_read_requests": 32},
            "scope": "forced large-source writer composition below 10M public threshold; production_rows matches configured row blocks and bounded per-batch subtree contract; ordinary public admission unchanged",
            "read_scope": "successful read_exact_at ranges including footer/coalescing/repeats; OS cache state uncontrolled; no storage-device claim",
            "timing_scope": "fresh source arrays per writer; native-array scan timing excludes separate independent scalar validation; reads through final drain included",
            "lifecycle_scope": "native write/flush, file sync_all, full SHA256 readback, native footer reopen; no directory-publication durability claim",
            "ownership_scope": "native query allocator bounded at 128MiB; fixture/parser/observation metadata and process RSS excluded",
            "fixture_cleanup": "owned files removed after each writer case; owned directory removed on scope exit",
            "fallback_attempted": false, "external_engine_invoked": false,
            "artifacts": records, "paired_comparisons": comparisons,
        })
    );
}
