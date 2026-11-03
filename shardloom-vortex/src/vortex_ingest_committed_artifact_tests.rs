//! Exercise the actual native writer and its immediate-publication receipt.

use super::*;
use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};

struct Fixture(PathBuf);

impl Fixture {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom_vortex_receipt_{label}_{}",
            std::process::id(),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn target(&self) -> PathBuf {
        self.0.join("prepared.vortex")
    }

    fn scalar_request(&self, filename: &str) -> VortexPreparedStateWriteRequest {
        VortexPreparedStateWriteRequest::new(
            self.0.join(filename),
            vec!["id".into(), "text".into()],
            vec![
                vec![
                    ("id".into(), ScalarValue::Int64(-7)),
                    ("text".into(), ScalarValue::Utf8("α😀".into())),
                ],
                vec![
                    ("id".into(), ScalarValue::Int64(9)),
                    ("text".into(), ScalarValue::Null),
                ],
            ],
            resources(),
        )
    }

    fn publication(&self) -> VortexPreparedOlapStateWriteRequest {
        VortexPreparedOlapStateWriteRequest::new_local(
            "test-source",
            "sha256:source-state",
            "parquet",
            "sha256:source",
            1,
            "sha256:schema",
            2,
            self.target(),
            self.target(),
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn resources() -> ExecutionResources {
    ExecutionResources::from_bytes(32 << 20, 1, ExecutionResourceOrigin::ExecutionCall).unwrap()
}

#[test]
fn native_writer_receipt_retention_is_explicit_at_the_shared_write_boundary() {
    let fixture = Fixture::new("retention");
    let memory = LiveMemoryPool::new(resources().memory_bytes()).unwrap();
    let mut bytes = Vec::new();
    for retain in [false, true] {
        let (report, receipt) = write_flat_scalar_vortex_prepared_state_with_memory_outcome(
            fixture.scalar_request(if retain {
                "proof.vortex"
            } else {
                "report.vortex"
            }),
            &memory,
            retain,
        )
        .unwrap();
        assert_eq!(receipt.is_some(), retain);
        assert_eq!(report.reopen_row_count, 2);
        assert!(
            !report.preparation_spine.fallback_attempted
                && !report.preparation_spine.external_engine_invoked
        );
        bytes.push(fs::read(&report.target_path).unwrap());
        drop(receipt);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(bytes[0], bytes[1]);
}

#[test]
fn native_writer_receipt_preserves_bytes_and_removes_only_publication_checksum_read() {
    let fixture = Fixture::new("fresh");
    let control =
        write_flat_scalar_vortex_prepared_state(fixture.scalar_request("control.vortex")).unwrap();
    let (write, receipt) = write_flat_scalar_vortex_prepared_state_with_receipt(
        fixture.scalar_request("prepared.vortex"),
    )
    .unwrap();
    assert_eq!(
        fs::read(&control.target_path).unwrap(),
        fs::read(fixture.target()).unwrap()
    );
    assert_eq!(write.reopen_row_count, 2);
    assert_eq!(
        write.reopen_verification_status,
        control.reopen_verification_status
    );
    let calls = LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get);
    let published = write_vortex_prepared_olap_state_bundle_with_receipt(
        fixture.publication(),
        receipt.unwrap(),
    )
    .unwrap();
    assert_eq!(
        LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get),
        calls
    );
    assert_eq!(published.status, "prepared_olap_state_ready");
    assert_eq!(published.prepared_artifact_digest, write.artifact_digest);
    assert_eq!(published.prepared_artifact_size_bytes, write.bytes_written);
    assert_eq!(
        published.artifact_fingerprint_source,
        "committed_writer_streaming_sha256"
    );
    assert_eq!(published.artifact_fingerprint_payload_bytes_read, Some(0));
    assert_eq!(published.layout_footer_row_count, "2");
    assert!(published.layout_metadata_persisted_in_artifact);
    assert!(!published.manifest_written);
    assert!(!published.fallback_attempted && !published.external_engine_invoked);
    let fields = published
        .evidence_fields()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        fields["vortex_prepared_olap_state_artifact_fingerprint_payload_bytes_read"],
        "0"
    );

    let independent = write_vortex_prepared_olap_state_bundle(fixture.publication()).unwrap();
    assert_eq!(
        LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get),
        calls + 1
    );
    assert_eq!(
        independent.prepared_artifact_digest,
        published.prepared_artifact_digest
    );
    assert_eq!(independent.state_digest, published.state_digest);
    assert_eq!(
        independent.layout_inventory_digest,
        published.layout_inventory_digest
    );
    assert_eq!(
        independent.artifact_fingerprint_source,
        "independent_artifact_content_sha256"
    );
    assert_eq!(
        independent.artifact_fingerprint_payload_bytes_read,
        Some(write.bytes_written)
    );
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
}

#[test]
fn native_writer_receipt_rejects_mutation_and_same_bytes_replacement() {
    for replacement in [false, true] {
        let fixture = Fixture::new(if replacement { "replace" } else { "mutate" });
        let (_, receipt) = write_flat_scalar_vortex_prepared_state_with_receipt(
            fixture.scalar_request("prepared.vortex"),
        )
        .unwrap();
        let data = fs::read(fixture.target()).unwrap();
        if replacement {
            let substitute = fixture.0.join("substitute.vortex");
            fs::write(&substitute, data).unwrap();
            fs::rename(substitute, fixture.target()).unwrap();
        } else {
            let modified = fs::metadata(fixture.target()).unwrap().modified().unwrap();
            let mut altered = data;
            altered[0] ^= 1;
            fs::write(fixture.target(), altered).unwrap();
            fs::File::options()
                .write(true)
                .open(fixture.target())
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(modified))
                .unwrap();
        }
        let calls = LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get);
        let error = write_vortex_prepared_olap_state_bundle_with_receipt(
            fixture.publication(),
            receipt.unwrap(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("checksum reuse refused"),
            "{error}"
        );
        assert_eq!(
            LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get),
            calls
        );
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn streamed_receipt_keeps_source_generation_and_cancellation_through_publication() {
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    for mode in ["fresh", "source_changed", "cancelled", "report_only"] {
        let fixture = Fixture::new(&format!("stream_{mode}"));
        let source_path = fixture.0.join("source.parquet");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![-7, 9]))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(&source_path).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let cancellation = CancellationToken::default();
        let source =
            IngestColumnarStreamSource::parquet(&source_path, 2, 1, Some(4096), &cancellation)
                .unwrap();
        let request = VortexPreparedStateColumnarStreamWriteRequest::new(
            fixture.target(),
            source,
            resources(),
        );
        let (write, receipt) = if mode == "report_only" {
            write_flat_columnar_vortex_prepared_state_streaming_outcome(
                request,
                &cancellation,
                FloatPolicy::Strict,
                false,
            )
        } else {
            write_flat_columnar_vortex_prepared_state_streaming_with_receipt(
                request,
                &cancellation,
                FloatPolicy::Strict,
            )
        }
        .unwrap();
        assert_eq!(write.reopen_row_count, 2);
        if mode == "report_only" {
            assert!(receipt.is_none());
            let published = write_vortex_prepared_olap_state_bundle(fixture.publication()).unwrap();
            assert_eq!(published.prepared_artifact_digest, write.artifact_digest);
            assert_eq!(
                published.artifact_fingerprint_payload_bytes_read,
                Some(write.bytes_written)
            );
            continue;
        }
        let receipt = receipt.unwrap();
        assert!(!receipt.source_identities.is_empty());
        match mode {
            "source_changed" => fs::write(&source_path, b"changed source generation").unwrap(),
            "cancelled" => cancellation.cancel(),
            _ => {}
        }
        let calls = LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get);
        let published =
            write_vortex_prepared_olap_state_bundle_with_receipt(fixture.publication(), receipt);
        if mode == "fresh" {
            let published = published.unwrap();
            assert_eq!(published.status, "prepared_olap_state_ready");
            assert_eq!(published.prepared_artifact_digest, write.artifact_digest);
            assert_eq!(published.artifact_fingerprint_payload_bytes_read, Some(0));
        } else {
            let error = published.unwrap_err().to_string();
            assert!(
                error.contains(if mode == "cancelled" {
                    "cancel"
                } else {
                    "changed"
                }),
                "{error}"
            );
        }
        assert_eq!(
            LOCAL_FILE_FINGERPRINT_CALLS.with(std::cell::Cell::get),
            calls
        );
    }
}
