use super::super::ProjectionRequest;
use super::*;
use crate::{binary_object::BinaryObject, resident_memory_source::ResidentMemorySource};
use arrow_array::{Array as _, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use shardloom_core::{ColumnRef, DatasetUri};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-object-native-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn batch(objects: &[Option<&BinaryObject>]) -> RecordBatch {
    let field = crate::native_binary_type::arrow::field(
        "object",
        &crate::native_binary_type::dtype(true).unwrap(),
    )
    .unwrap();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            field,
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                0..i64::try_from(objects.len()).unwrap(),
            )),
            Arc::new(BinaryArray::from(
                objects
                    .iter()
                    .map(|object| object.map(BinaryObject::descriptor))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn source(
    session: &ResidentVortexSession,
    objects: &[Option<&BinaryObject>],
) -> ResidentMemorySource {
    ResidentMemorySource::from_arrow_record_batch(
        session,
        batch(objects),
        &CancellationToken::default(),
    )
    .unwrap()
}

fn write(
    source: &ResidentMemorySource,
    path: &Path,
    limit: Option<usize>,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    let result = source
        .prepare_projection(&["key", "object"], None, limit)?
        .execute_arrays()?;
    NativeSinkPlan::completed(result)?.write(
        &VortexQueryPrimitiveRequest::project(
            DatasetUri::new("memory://objects")?,
            ProjectionRequest::columns(vec![ColumnRef::new("key")?, ColumnRef::new("object")?]),
        ),
        path,
        false,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1)?,
    )
}

fn descriptors(source: &PreparedVortexSource) -> Vec<Option<Vec<u8>>> {
    let result = source
        .prepare_projection(&["object"], source.file().row_count().max(1), 1 << 20)
        .unwrap()
        .execute()
        .unwrap();
    let mut stream = Arc::new(result)
        .arrow_batches(CancellationToken::default())
        .unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.next_batch().unwrap() {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        values.extend(
            (0..column.len()).map(|i| (!column.is_null(i)).then(|| column.value(i).to_vec())),
        );
    }
    values
}

#[test]
fn binary_object_relational_writer_finishes_payloads_before_file_evidence_drain() {
    use crate::local_primitives::{
        VortexLocalPrimitiveRowExportFormat, prepared_relational::prepare_relational,
    };
    use crate::relational_query::{VortexRelationalPlan, VortexRelationalScan};

    // Exercise both a terminal batcher flush and payload readers retained while
    // the producer is still delivering. Neither may close the shared I/O scope.
    for rows in [3, 257] {
        let fixture = Fixture::new();
        let token = CancellationToken::default();
        let expected = vec![37_u8; 131_079];
        let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
        let object = session.binary_object(&expected, &token).unwrap();
        let empty = session.binary_object(&[], &token).unwrap();
        let objects = (0..rows)
            .map(|row| match row % 3 {
                0 => Some(&object),
                1 => None,
                _ => Some(&empty),
            })
            .collect::<Vec<_>>();
        let input = fixture.0.join("input.vortex");
        let memory = source(&session, &objects);
        write(&memory, &input, None).unwrap();
        drop(objects);
        drop((memory, object, empty, session));

        let plan = VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(input.display().to_string()).unwrap(),
            projection: ProjectionRequest::All,
            predicate: None,
        });
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        policy.resource_envelope.memory_budget_bytes = 32 << 20;
        let prepared = prepare_relational(&plan, policy).unwrap();
        let baseline = prepared.snapshot().reserved_without_source_segment_cache();
        let output = fixture.0.join("output.vortex");
        let written = prepared
            .write(&output, VortexLocalPrimitiveRowExportFormat::Vortex, false)
            .unwrap();
        assert_eq!(written.output.rows_written, rows as u64);
        assert!(written.execution.native_io_certificate.is_certified());
        let evidence = &written.execution.native_io_certificate.file_scans;
        assert_eq!(evidence.sources.len(), 1);
        assert_eq!(evidence.contracts.len(), 1);
        assert_eq!(evidence.contracts[0].delivered_rows, rows as u64);
        assert!(evidence.sources[0].completed_read_bytes > 0);
        drop(written);
        assert_eq!(
            prepared.snapshot().reserved_without_source_segment_cache(),
            baseline
        );
        drop(prepared);
        fs::remove_file(input).unwrap();

        let receiving = ResidentVortexSession::new(32 << 20, 1).unwrap();
        let reopened = receiving.prepare_file(&output).unwrap();
        let values = descriptors(&reopened);
        assert_eq!(values.len(), rows);
        for (row, descriptor) in values.iter().enumerate() {
            if row % 3 == 1 {
                assert!(descriptor.is_none());
            } else {
                let object = receiving
                    .binary_object_from_descriptor(descriptor.as_ref().unwrap(), &token)
                    .unwrap();
                let payload = object.read(&token).unwrap();
                if row % 3 == 0 {
                    assert_eq!(&payload[..], expected);
                } else {
                    assert!(payload.is_empty());
                }
            }
        }
    }
}

#[test]
fn binary_object_native_file_reopens_moved_without_originals_and_copies_selected_objects_once() {
    let fixture = Fixture::new();
    let token = CancellationToken::default();
    let expected: Vec<_> = (0..2 * 1024 * 1024 + 3)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let original = fixture.0.join("original.bin");
    fs::write(&original, &expected).unwrap();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let store = session.binary_object_store(&fixture.0, 8 << 20).unwrap();
    let object = store.import_file(&original, &token).unwrap();
    let unselected = store.put_bytes(b"must never be copied", &token).unwrap();
    let empty = session.binary_object(&[], &token).unwrap();
    let source = source(
        &session,
        &[
            Some(&object),
            None,
            Some(&object),
            Some(&empty),
            Some(&unselected),
        ],
    );
    let output = fixture.0.join("owned.vortex");
    let before = session.binary_object_snapshot().unwrap();
    let report = write(&source, &output, Some(4)).unwrap();
    assert_eq!(report.rows_written, 4);
    assert_eq!(
        session.binary_object_snapshot().unwrap().payload_read_bytes - before.payload_read_bytes,
        expected.len() as u64
    );
    let stored = fs::metadata(&output).unwrap().len();
    assert!(
        stored < expected.len() as u64 + 128 * 1024,
        "duplicate payload or oversized directory: {stored}"
    );
    drop((source, object, unselected, empty, store, session));
    fs::remove_file(original).unwrap();
    let moved = fixture.0.join("moved.vortex");
    fs::rename(output, &moved).unwrap();
    let reopened_session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let reopened = reopened_session.prepare_file(&moved).unwrap();
    assert_eq!(
        reopened_session
            .binary_object_snapshot()
            .unwrap()
            .managed_objects,
        2
    );
    assert_eq!(
        reopened_session
            .binary_object_snapshot()
            .unwrap()
            .payload_read_bytes,
        0
    );
    let values = descriptors(&reopened);
    assert_eq!(values.len(), 4);
    assert_eq!(values[0], values[2]);
    assert_eq!(values[1], None);
    assert_eq!(
        reopened_session
            .binary_object_snapshot()
            .unwrap()
            .payload_read_bytes,
        0
    );
    let reader = reopened_session
        .binary_object_from_descriptor(values[0].as_ref().unwrap(), &token)
        .unwrap();
    let empty = reopened_session
        .binary_object_from_descriptor(values[3].as_ref().unwrap(), &token)
        .unwrap();
    assert!(empty.read(&token).unwrap().is_empty());
    drop((empty, reopened, reopened_session));
    assert_eq!(
        &reader.read_range(65_533..65_540, &token).unwrap()[..],
        &expected[65_533..65_540]
    );
    assert_eq!(&reader.read(&token).unwrap()[..], expected);
    drop(reader);
    assert!(moved.exists(), "published file belongs to the caller");
}

#[test]
fn binary_object_native_file_preserves_external_empty_null_and_empty_table() {
    let fixture = Fixture::new();
    let external = fixture.0.join("external.bin");
    fs::write(&external, b"0123456789").unwrap();
    let token = CancellationToken::default();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let reference = session
        .external_binary_object(&external, 3..8, &token)
        .unwrap();
    let source = source(&session, &[None, Some(&reference)]);
    for limit in [Some(0), None] {
        let output = fixture.0.join(format!("external-{limit:?}.vortex"));
        write(&source, &output, limit).unwrap();
        let receiving = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let reopened = receiving.prepare_file(&output).unwrap();
        let values = descriptors(&reopened);
        assert_eq!(values.len(), if limit == Some(0) { 0 } else { 2 });
        assert_eq!(
            receiving.binary_object_snapshot().unwrap().managed_objects,
            0
        );
        if values.len() == 2 {
            assert!(values[0].is_none());
            let object = receiving
                .binary_object_from_descriptor(values[1].as_ref().unwrap(), &token)
                .unwrap();
            assert!(!object.is_managed());
            assert_eq!(&object.read(&token).unwrap()[..], b"34567");
        }
    }
    drop((source, reference, session));
    assert_eq!(fs::read(external).unwrap(), b"0123456789");
}

#[test]
fn binary_object_descriptor_intake_rejects_unbound_and_malformed_cells_before_source_publication() {
    let token = CancellationToken::default();
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let object = session.binary_object(b"owned", &token).unwrap();
    let receiving = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let error =
        ResidentMemorySource::from_arrow_record_batch(&receiving, batch(&[Some(&object)]), &token)
            .err()
            .unwrap();
    assert!(
        error.to_string().contains("SL-BINARY-OBJECT-MISSING"),
        "{error}"
    );
    assert_eq!(
        receiving.binary_object_snapshot().unwrap().managed_objects,
        0
    );
    let schema = batch(&[Some(&object)]).schema();
    let malformed = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(BinaryArray::from(vec![Some(&b"not a descriptor"[..])])),
        ],
    )
    .unwrap();
    let error = ResidentMemorySource::from_arrow_record_batch(&session, malformed, &token)
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("SL-BINARY-OBJECT-DESCRIPTOR"),
        "{error}"
    );
}

fn footer(
    bytes: &[u8],
    session: &ResidentVortexSession,
) -> (vortex::file::Footer, Vec<std::ops::Range<u64>>) {
    let mut ranges = Vec::new();
    let footer = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            crate::binary_object::catalog::read_footer_ranges(
                bytes.len() as u64,
                context.native_session(),
                |offset, length| {
                    let stop = offset + length as u64;
                    ranges.push(offset..stop);
                    Ok(bytes::Bytes::copy_from_slice(
                        &bytes[usize::try_from(offset).unwrap()..usize::try_from(stop).unwrap()],
                    ))
                },
            )
        })
        .unwrap();
    (footer, ranges)
}

#[test]
fn binary_object_footer_reads_exclude_payloads_and_internal_open_keeps_scan_scope_live() {
    use vortex::array::iter::ArrayIteratorExt as _;
    for length in [7, 2 * 1024 * 1024 + 3] {
        let fixture = Fixture::new();
        let token = CancellationToken::default();
        let expected = vec![71; length];
        let owner = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let object = owner.binary_object(&expected, &token).unwrap();
        let input = source(&owner, &[Some(&object)]);
        let output = fixture.0.join("native.vortex");
        write(&input, &output, None).unwrap();
        drop((input, object, owner));
        let receiving = ResidentVortexSession::new(16 << 20, 1).unwrap();
        let bytes = fs::read(&output).unwrap();
        let (metadata, reads) = footer(&bytes, &receiving);
        let map = metadata.segment_map();
        let count = length.div_ceil(crate::binary_object::CHUNK_BYTES);
        let payloads = &map[map.len() - count - 1..map.len() - 1];
        for read in &reads {
            for segment in payloads {
                let end = segment.offset + u64::from(segment.length);
                assert!(
                    read.end <= segment.offset || read.start >= end,
                    "metadata {read:?} overlaps object {segment:?}"
                );
            }
        }
        drop(metadata);
        // Regression: draining the scope inside open_local_file closes it
        // before the caller's following table scan can acquire a read owner.
        let rows = receiving
            .with_native_execution_context(&token, |context| {
                let file = context
                    .runtime()
                    .block_on(context.open_local_file(&output))
                    .unwrap();
                let readback = file
                    .scan()
                    .unwrap()
                    .into_array_iter(context.runtime())
                    .unwrap()
                    .read_all()
                    .unwrap();
                Ok(readback.len())
            })
            .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(
            receiving
                .binary_object_snapshot()
                .unwrap()
                .payload_read_bytes,
            0
        );
        let prepared = receiving.prepare_file(&output).unwrap();
        let values = descriptors(&prepared);
        let reader = receiving
            .binary_object_from_descriptor(values[0].as_ref().unwrap(), &token)
            .unwrap();
        assert_eq!(reader.read(&token).unwrap().as_ref(), expected);
    }
}

#[test]
fn binary_object_corruption_is_lazy_for_payloads_and_atomic_for_catalog_admission() {
    let fixture = Fixture::new();
    let token = CancellationToken::default();
    let expected = vec![93; crate::binary_object::CHUNK_BYTES * 2 + 7];
    let owner = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let object = owner.binary_object(&expected, &token).unwrap();
    let empty = owner.binary_object(&[], &token).unwrap();
    let input = source(&owner, &[Some(&object), Some(&empty)]);
    let output = fixture.0.join("native.vortex");
    write(&input, &output, None).unwrap();
    let original = fs::read(&output).unwrap();
    let (metadata, _) = footer(&original, &owner);
    let map = metadata.segment_map();
    let payload_start = usize::try_from(map[map.len() - 4].offset).unwrap();
    let directory_start = usize::try_from(map.last().unwrap().offset).unwrap();
    let header_start = original
        .windows(8)
        .position(|window| window == b"SLBOLAY1")
        .unwrap();
    drop((metadata, input, object, empty, owner));

    // A fresh arena cannot mask a corrupt file with previously admitted bytes.
    let mut corrupt_payload = original.clone();
    corrupt_payload[payload_start + 11] ^= 1;
    let damaged = fixture.0.join("damaged-payload.vortex");
    fs::write(&damaged, corrupt_payload).unwrap();
    let fresh = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = fresh.prepare_file(&damaged).unwrap();
    let values = descriptors(&prepared);
    let reader = fresh
        .binary_object_from_descriptor(values[0].as_ref().unwrap(), &token)
        .unwrap();
    assert_eq!(
        fresh.binary_object_snapshot().unwrap().payload_read_bytes,
        0
    );
    let begin = crate::binary_object::CHUNK_BYTES as u64 + 3;
    assert_eq!(
        reader
            .read_range(begin..begin + 5, &token)
            .unwrap()
            .as_ref(),
        &[93; 5]
    );
    for result in [reader.read_range(0..1, &token), reader.read(&token)] {
        let error = result.err().unwrap();
        assert!(
            error.to_string().contains("SL-BINARY-OBJECT-INTEGRITY"),
            "{error}"
        );
    }
    assert_eq!(fresh.binary_object_snapshot().unwrap().delivered_bytes, 5);

    for (name, offset) in [
        ("directory", directory_start + 8),
        ("header", header_start + 8),
        ("truncated", original.len() - 1),
    ] {
        let mut bytes = original.clone();
        if name == "truncated" {
            bytes.truncate(offset);
        } else {
            bytes[offset] ^= 1;
        }
        let path = fixture.0.join(format!("{name}.vortex"));
        fs::write(&path, bytes).unwrap();
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let sentinel = session.binary_object(b"existing owner", &token).unwrap();
        let before = session.binary_object_snapshot().unwrap();
        assert!(session.prepare_file(&path).is_err(), "{name}");
        assert_eq!(session.binary_object_snapshot().unwrap(), before, "{name}");
        assert_eq!(sentinel.read(&token).unwrap().as_ref(), b"existing owner");
    }
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    assert!(
        session
            .prepare_file_with_cancellation(&output, &cancelled)
            .is_err()
    );
    assert_eq!(session.binary_object_snapshot().unwrap().managed_objects, 0);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    let denied = ResidentVortexSession::new(1024, 1).unwrap();
    assert!(denied.prepare_file(&output).is_err());
    assert_eq!(denied.binary_object_snapshot().unwrap().managed_objects, 0);
    assert_eq!(denied.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn binary_object_file_transfer_retains_private_cleanup_and_receiving_owner() {
    for from_result in [false, true] {
        let fixture = Fixture::new();
        let token = CancellationToken::default();
        let origin = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let origin_memory = origin.shared_memory_pool();
        let store = origin.binary_object_store(&fixture.0, 1 << 20).unwrap();
        let object = store
            .put_bytes(b"transferred private file", &token)
            .unwrap();
        let descriptor = object.descriptor().to_vec();
        let input = source(&origin, &[Some(&object), Some(&object), None]);
        let receiving = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let receiving_memory = receiving.shared_memory_pool();
        let moved = if from_result {
            let result = input
                .prepare_projection(&["key", "object"], None, None)
                .unwrap()
                .execute_arrays()
                .unwrap();
            ResidentMemorySource::from_completed_result_in_session(&receiving, &result, &token)
                .unwrap()
        } else {
            input.in_session(&receiving, &token).unwrap()
        };
        assert_eq!(
            receiving.binary_object_snapshot().unwrap().managed_objects,
            1
        );
        assert_eq!(
            receiving
                .binary_object_snapshot()
                .unwrap()
                .payload_read_bytes,
            0
        );
        drop((object, store, input, origin));
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
        assert!(
            origin_memory.snapshot().reserved_bytes > 0,
            "shared private cleanup remains charged"
        );
        let reader = receiving
            .binary_object_from_descriptor(&descriptor, &token)
            .unwrap();
        drop((moved, receiving));
        assert_eq!(
            reader.read(&token).unwrap().as_ref(),
            b"transferred private file"
        );
        drop(reader);
        assert_eq!(origin_memory.snapshot().reserved_bytes, 0);
        assert_eq!(receiving_memory.snapshot().reserved_bytes, 0);
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    }
}

#[test]
fn binary_object_escaped_arrow_slices_retain_private_payload_until_last_buffer_drop() {
    for length in [0, 1] {
        let fixture = Fixture::new();
        let token = CancellationToken::default();
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let memory = session.shared_memory_pool();
        let store = session.binary_object_store(&fixture.0, 1 << 20).unwrap();
        let object = store
            .put_bytes(b"owned by escaped Arrow buffer", &token)
            .unwrap();
        let input = source(&session, &[Some(&object)]);
        let result = Arc::new(
            input
                .prepare_projection(&["object"], None, None)
                .unwrap()
                .execute_arrays()
                .unwrap(),
        );
        let mut stream = result.arrow_batches(token).unwrap();
        let batch = stream.next_batch().unwrap().unwrap();
        let escaped = batch.column(0).slice(0, length);
        drop((batch, stream, result, input, object, store, session));
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
        assert!(memory.snapshot().reserved_bytes > 0);
        assert_eq!(escaped.len(), length);
        drop(escaped);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    }
}

#[test]
fn binary_object_writer_stops_payload_reads_on_sink_failure_and_abandoned_input() {
    use std::sync::atomic::AtomicBool;
    struct FailPayload(Arc<AtomicBool>);
    impl std::io::Write for FailPayload {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            assert!(!self.0.load(Ordering::Acquire), "failed sink reused");
            if bytes.len() >= crate::binary_object::CHUNK_BYTES
                && bytes.iter().all(|byte| *byte == 91)
            {
                self.0.store(true, Ordering::Release);
                return Err(std::io::Error::other(
                    "injected binary payload output failure",
                ));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    for complete in [false, true] {
        let fixture = Fixture::new();
        let token = CancellationToken::default();
        let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
        let memory = session.shared_memory_pool();
        let store = session.binary_object_store(&fixture.0, 8 << 20).unwrap();
        let object = store.put_bytes(&vec![91; 4 << 20], &token).unwrap();
        let input = source(&session, &[Some(&object)]);
        let result = input
            .prepare_projection(&["key", "object"], None, None)
            .unwrap()
            .execute_arrays()
            .unwrap();
        let failed = Arc::new(AtomicBool::new(false));
        session
            .with_native_execution_context(&token, |context| {
                let (strategy, objects) = crate::binary_object::writer::strategy(
                    native_flat_layout::SequentialNativeFlatLayout::strategy(1),
                    result.dtype(),
                    context,
                )?;
                let objects = objects.unwrap();
                let mut writer = super::super::native_writer::NativeWriter::new(
                    context.native_session(),
                    context.runtime(),
                    FailPayload(Arc::clone(&failed)),
                    result.dtype().clone(),
                    strategy,
                );
                objects.watch_sink_failure(writer.failure_signal())?;
                for array in result.arrays() {
                    objects.observe(array, context)?;
                    writer.push(array.clone()).unwrap();
                }
                if complete {
                    objects.complete_input();
                    let error = writer.finish().err().unwrap();
                    assert!(
                        error
                            .to_string()
                            .contains("injected binary payload output failure"),
                        "{error}"
                    );
                } else {
                    drop(writer);
                }
                Ok(())
            })
            .unwrap();
        let observed = session.binary_object_snapshot().unwrap();
        assert_eq!(failed.load(Ordering::Acquire), complete);
        if complete {
            assert!(observed.payload_read_bytes >= crate::binary_object::CHUNK_BYTES as u64);
            assert!(
                observed.payload_read_bytes < object.len(),
                "remaining payload was read after output failure: {observed:?}"
            );
        } else {
            assert_eq!(observed.payload_read_bytes, 0);
        }
        drop((result, input, object, store, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    }
}
