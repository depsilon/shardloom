//! Test-only native codec/consumer lifecycle, with no production selector.

use super::*;
use serde_json::{Value, json};
use shardloom_core::{ColumnRef, DatasetUri, PredicateExpr};
use std::{collections::BTreeMap, io::Write};
use vortex::{
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::{DType, FieldNames, FieldPath, Nullability},
        iter::ArrayIteratorAdapter,
        scalar::Scalar,
        validity::Validity,
    },
    encodings::fsst::{fsst_compress, fsst_train_compressor},
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::runtime::BlockingRuntime as _,
    layout::{
        LayoutStrategy,
        layouts::{chunked::writer::ChunkedLayoutStrategy, flat::writer::FlatLayoutStrategy},
    },
};

#[path = "vortex_ingest_text_codec_queries.rs"]
mod queries;

const COLUMN: &str = "renamed_message";
const ID: &str = "exact_identifier";
const BASE: i64 = 1_i64 << 60;
const MAX_ROWS: usize = 16_384;
const MAX_ROW_GROUPS: usize = 16;
const MAX_TEXT_BYTES: usize = 4 << 20;
const MAX_FILE_BYTES: u64 = 16 << 20;
const MEMORY_BYTES: u64 = 64 << 20;
const MAX_REUSE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Codec {
    RetainedZstd,
    Dictionary,
    Fsst,
}
impl Codec {
    fn encoding(self) -> &'static str {
        match self {
            Self::RetainedZstd => "vortex.zstd",
            Self::Dictionary => "vortex.dict",
            Self::Fsst => "vortex.fsst",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Profile {
    Categorical,
    NullableCategorical,
    NullableUnique,
}
impl Profile {
    fn nullable(self) -> bool {
        !matches!(self, Self::Categorical)
    }
    fn value(self, row: usize) -> Option<String> {
        if self.nullable() && row.is_multiple_of(7) {
            return None;
        }
        let key = if matches!(self, Self::NullableUnique) {
            row
        } else {
            row % 23
        };
        if key == 0 {
            return Some(String::new());
        }
        Some(format!(
            "{}-東京-λ-{:06}-literal%_\\-{}",
            if key.is_multiple_of(3) {
                "needle"
            } else {
                "ordinary"
            },
            key,
            "abcd".repeat(key % 13)
        ))
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shardloom-text-codec-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct LimitedWrite<W> {
    inner: W,
    written: u64,
}
impl<W: Write> Write for LimitedWrite<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let len = u64::try_from(bytes.len()).map_err(std::io::Error::other)?;
        if self
            .written
            .checked_add(len)
            .is_none_or(|total| total > MAX_FILE_BYTES)
        {
            return Err(std::io::Error::other(
                "text portfolio artifact byte limit exceeded",
            ));
        }
        let written = self.inner.write(bytes)?;
        self.written += u64::try_from(written).map_err(std::io::Error::other)?;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap()
}
fn sum_nanos(spans: &[u64]) -> u64 {
    spans
        .iter()
        .try_fold(0_u64, |sum, span| sum.checked_add(*span))
        .unwrap()
}
fn digest(bytes: &[u8]) -> String {
    hex_digest(&sha2::Sha256::digest(bytes))
}
fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut text = String::with_capacity(64);
    for byte in bytes {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}

struct Input {
    values: Vec<Option<String>>,
    row_group: usize,
    profile: Profile,
    source_nanos: u64,
    text_bytes: usize,
    source_sha256: String,
}

impl Input {
    fn new(profile: Profile, rows: usize, row_group: usize) -> Self {
        assert!(rows > 0 && rows <= MAX_ROWS && row_group > 0 && row_group <= rows);
        assert!(rows.div_ceil(row_group) <= MAX_ROW_GROUPS);
        // Maximum generated string length is below 128 bytes. Bound allocation
        // before creating any text, including Vec/String metadata separately.
        assert!(rows.checked_mul(128).unwrap() <= MAX_TEXT_BYTES);
        let start = Instant::now();
        let values = (0..rows).map(|row| profile.value(row)).collect::<Vec<_>>();
        let source_nanos = nanos(start);
        let text_bytes = values.iter().flatten().map(String::len).sum::<usize>();
        assert!(text_bytes <= MAX_TEXT_BYTES);
        let mut hash = sha2::Sha256::new();
        for (row, value) in values.iter().enumerate() {
            hash.update((BASE + i64::try_from(row).unwrap()).to_le_bytes());
            hash.update([u8::from(value.is_some())]);
            if let Some(value) = value {
                hash.update(u64::try_from(value.len()).unwrap().to_le_bytes());
                hash.update(value.as_bytes());
            }
        }
        Self {
            values,
            row_group,
            profile,
            source_nanos,
            text_bytes,
            source_sha256: hex_digest(&hash.finalize()),
        }
    }

    fn native_text(&self, values: &[Option<String>]) -> ArrayRef {
        if self.profile.nullable() {
            VarBinViewArray::from_iter_nullable_str(values.iter().map(Option::as_deref))
                .into_array()
        } else {
            VarBinViewArray::from_iter_str(values.iter().map(|value| value.as_deref().unwrap()))
                .into_array()
        }
    }

    fn dictionary(&self, values: &[Option<String>], group: usize) -> ArrayRef {
        let mut domain = values.iter().map(Option::as_deref).collect::<Vec<_>>();
        domain.sort_unstable();
        domain.dedup();
        if !group.is_multiple_of(2) {
            domain.reverse();
        }
        let indexes = domain
            .iter()
            .enumerate()
            .map(|(index, value)| (*value, u32::try_from(index).unwrap()))
            .collect::<BTreeMap<_, _>>();
        let codes = PrimitiveArray::new(
            values
                .iter()
                .map(|value| indexes[&value.as_deref()])
                .collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array();
        let domain = if self.profile.nullable() {
            VarBinViewArray::from_iter_nullable_str(domain).into_array()
        } else {
            VarBinViewArray::from_iter_str(domain.into_iter().map(Option::unwrap)).into_array()
        };
        DictArray::try_new(codes, domain).unwrap().into_array()
    }

    fn prepare(
        &self,
        codec: Codec,
        session: &vortex::session::VortexSession,
    ) -> (Vec<ArrayRef>, u64, u64) {
        let start = Instant::now();
        let mut training = 0_u64;
        let mut ctx = session.create_execution_ctx();
        let arrays = self
            .values
            .chunks(self.row_group)
            .enumerate()
            .map(|(group, values)| {
                let text = match codec {
                    Codec::Dictionary => self.dictionary(values, group),
                    Codec::RetainedZstd => self.native_text(values),
                    Codec::Fsst => {
                        let input = self.native_text(values);
                        let start = Instant::now();
                        let compressor = fsst_train_compressor(&input, &mut ctx).unwrap();
                        training += nanos(start);
                        fsst_compress(&input, &compressor, &mut ctx)
                            .unwrap()
                            .into_array()
                    }
                };
                let ids = PrimitiveArray::new(
                    (0..values.len())
                        .map(|row| BASE + i64::try_from(group * self.row_group + row).unwrap())
                        .collect::<Vec<_>>(),
                    Validity::NonNullable,
                )
                .into_array();
                StructArray::try_new(
                    FieldNames::from([ID, COLUMN]),
                    vec![ids, text],
                    values.len(),
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array()
            })
            .collect::<Vec<_>>();
        (arrays, nanos(start), training)
    }
}

fn strategy(
    codec: Codec,
    memory: &NativeIngestMemory,
    timing: &VortexWriterStageTiming,
) -> Arc<dyn LayoutStrategy> {
    let child = if codec == Codec::RetainedZstd {
        large_source_text_vortex_write_strategy(
            262_144,
            8 << 20,
            1,
            1,
            &[COLUMN.to_owned()],
            timing,
            &memory.session,
        )
    } else {
        let encoded_leaf = Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
        Arc::new(
            large_source_fast_load_table_strategy(262_144, 8 << 20, 1, timing, &memory.session)
                .with_field_writer(FieldPath::from_name(COLUMN), encoded_leaf),
        )
    };
    Arc::new(bounded_ingest_layout::BoundedIngestLayout::new(
        child,
        0,
        memory.pool.reserve(0).unwrap(),
    ))
}

fn verify_values(
    file: &vortex::file::VortexFile,
    input: &Input,
    context: &LocalVortexWriteContext,
) {
    assert_eq!(file.row_count(), u64::try_from(input.values.len()).unwrap());
    let mut ctx = context.session.create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(&context.runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            let scalar = array.execute_scalar(row, &mut ctx).unwrap();
            assert_eq!(
                scalar.as_struct().field(ID),
                Some(Scalar::from(BASE + i64::try_from(seen).unwrap()))
            );
            let nullable = if input.profile.nullable() {
                Nullability::Nullable
            } else {
                Nullability::NonNullable
            };
            let expected = input.values[seen].as_ref().map_or_else(
                || Scalar::null(DType::Utf8(nullable)),
                |value| Scalar::utf8(value.as_str(), nullable),
            );
            assert_eq!(scalar.as_struct().field(COLUMN), Some(expected));
            seen += 1;
        }
    }
    assert_eq!(seen, input.values.len());
}

struct Artifact {
    path: PathBuf,
    evidence: Value,
    operational_nanos: u64,
}

#[allow(clippy::too_many_lines)] // Keep publication, observed ownership release and timing order together.
fn write_artifact(
    context: &LocalVortexWriteContext,
    root: &Path,
    input: &Input,
    codec: Codec,
) -> Artifact {
    let start = Instant::now();
    let memory = NativeIngestMemory::new(MEMORY_BYTES).unwrap();
    let session_setup_nanos = nanos(start);
    let (arrays, prepare_nanos, train_nanos) = input.prepare(codec, &memory.session);
    let start = Instant::now();
    let dtype = arrays[0].dtype().clone();
    let path = root.join(format!("{codec:?}-{:?}.vortex", input.profile));
    let timing = VortexWriterStageTiming::default();
    let options = memory
        .session
        .write_options()
        .with_strategy(strategy(codec, &memory, &timing));
    let strategy_setup_nanos = nanos(start);
    let start = Instant::now();
    let drivers =
        crate::resident_worker_group::ResidentWorkerGroup::new(&context.runtime, 1).unwrap();
    let (summary, publication) =
        shardloom_core::write_workspace_safe_bytes_with_validated_producer(
            root,
            &path,
            false,
            "test-only native text codec portfolio",
            |writer| {
                options
                    .blocking(&context.runtime)
                    .write(
                        LimitedWrite {
                            inner: writer,
                            written: 0,
                        },
                        ArrayIteratorAdapter::new(dtype.clone(), arrays.into_iter().map(Ok)),
                    )
                    .map_err(vortex_error)
            },
            |summary| {
                if summary.row_count() != u64::try_from(input.values.len()).unwrap() {
                    return Err(ShardLoomError::InvalidOperation(
                        "text portfolio writer row count differs".into(),
                    ));
                }
                Ok(())
            },
        )
        .unwrap();
    drop(drivers);
    let write_nanos = nanos(start);
    assert!(publication.bytes_written <= MAX_FILE_BYTES);
    assert_eq!(publication.commit_status, "committed");
    assert!(!publication.fallback_attempted && !publication.external_engine_invoked);
    let start = Instant::now();
    fs::File::open(&path).unwrap().sync_all().unwrap();
    let sync_nanos = nanos(start);
    let start = Instant::now();
    let bytes = fs::read(&path).unwrap();
    let sha256 = digest(&bytes);
    let hash_nanos = nanos(start);
    assert_eq!(
        u64::try_from(bytes.len()).unwrap(),
        publication.bytes_written
    );
    assert_eq!(publication.output_digest, format!("sha256:{sha256}"));
    let start = Instant::now();
    let file = context
        .runtime
        .block_on(context.session.open_options().open_path(&path))
        .unwrap();
    let reopen_nanos = nanos(start);
    assert_eq!(file.dtype(), &dtype);
    let start = Instant::now();
    let inventory = context
        .runtime
        .block_on(
            crate::physical_encoding_inventory::inspect_physical_encodings(
                &file,
                crate::physical_encoding_inventory::PhysicalEncodingInspectionLimits {
                    max_layout_nodes: 4096,
                    max_flat_references: 1024,
                    max_segment_bytes: MAX_FILE_BYTES,
                    max_total_segment_bytes: MAX_FILE_BYTES * 4,
                    max_array_nodes: 8192,
                },
            ),
        )
        .unwrap();
    let inventory_nanos = nanos(start);
    assert_eq!(inventory["complete_flat_inspection"], true);
    let text = inventory["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|column| {
            column["column_path"] == json!([COLUMN]) && column["auxiliary_path"] == json!([])
        })
        .unwrap();
    assert!(
        text["encoding_ids"]
            .as_array()
            .unwrap()
            .iter()
            .any(|encoding| encoding == codec.encoding())
    );
    let start = Instant::now();
    verify_values(&file, input, context);
    let verification_nanos = nanos(start);
    drop(file);
    let start = Instant::now();
    drop(summary);
    let writer_owner_release_nanos = nanos(start);
    let owned = memory.pool.snapshot();
    assert_eq!(owned.reserved_bytes, 0);
    assert_eq!(owned.denied_reservations, 0);
    let operational_nanos = sum_nanos(&[
        input.source_nanos,
        session_setup_nanos,
        prepare_nanos,
        strategy_setup_nanos,
        write_nanos,
        sync_nanos,
        hash_nanos,
        reopen_nanos,
        writer_owner_release_nanos,
    ]);
    let evidence = json!({
        "codec": format!("{codec:?}"), "profile": format!("{:?}", input.profile),
        "source_values_sha256": input.source_sha256, "rows": input.values.len(), "row_group_rows": input.row_group,
        "source_text_bytes": input.text_bytes, "artifact_bytes": bytes.len(), "artifact_sha256": sha256,
        "publication_commit_mode": publication.commit_mode, "publication_commit_status": publication.commit_status,
        "publication_stream_digest": publication.output_digest, "independent_readback_matches_stream_digest": true,
        "source_nanos": input.source_nanos, "prepare_nanos": prepare_nanos, "fsst_train_nanos_in_prepare": train_nanos,
        "native_session_setup_nanos": session_setup_nanos, "writer_strategy_setup_nanos": strategy_setup_nanos,
        "write_publication_and_driver_join_nanos": write_nanos, "extra_file_sync_nanos": sync_nanos,
        "retained_writer_summary_release_nanos": writer_owner_release_nanos,
        "full_readback_sha256_nanos": hash_nanos, "native_footer_reopen_nanos": reopen_nanos,
        "physical_inventory_nanos": inventory_nanos, "full_value_verification_nanos": verification_nanos,
        "one_time_operational_nanos": operational_nanos, "physical_inventory": inventory,
        "one_time_operational_scope": "sum of named elapsed spans; not continuous wall time; diagnostic inventory, independent validation, source hashing and intervening bookkeeping excluded",
        "owned_peak_bytes": owned.peak_reserved_bytes, "owned_bytes_after_drop": owned.reserved_bytes,
        "owned_denials": owned.denied_reservations, "owned_pool_bytes": MEMORY_BYTES,
        "ownership_scope": "native HostAllocator users plus bounded footer references; fixture/FSST/Zstd direct allocations excluded; not RSS",
        "limits": {"rows": MAX_ROWS, "row_groups": MAX_ROW_GROUPS, "text_bytes": MAX_TEXT_BYTES, "artifact_bytes": MAX_FILE_BYTES},
        "writer_background_drivers": 1, "caller": 1,
    });
    Artifact {
        path,
        evidence,
        operational_nanos,
    }
}

#[test]
fn text_codec_portfolio_write_limit_rejects_before_forwarding() {
    let mut writer = LimitedWrite {
        inner: Vec::new(),
        written: MAX_FILE_BYTES - 1,
    };
    assert!(writer.write_all(b"ab").is_err());
    assert!(writer.inner.is_empty());
    assert_eq!(writer.written, MAX_FILE_BYTES - 1);
    writer.write_all(b"a").unwrap();
    assert_eq!(writer.inner, b"a");
    assert_eq!(writer.written, MAX_FILE_BYTES);
    assert!(writer.write_all(b"b").is_err());
    assert_eq!(writer.inner, b"a");
}

fn run_case(
    context: &LocalVortexWriteContext,
    directory: &Directory,
    codec: Codec,
    profile: Profile,
    rows: usize,
    row_group: usize,
    reuse: usize,
) -> Value {
    let case_started = Instant::now();
    assert!((1..=MAX_REUSE).contains(&reuse));
    let input = Input::new(profile, rows, row_group);
    let artifact = write_artifact(context, &directory.0, &input, codec);
    let (samples, checkpoints) = queries::run(
        &artifact.path,
        &input,
        reuse,
        artifact.operational_nanos,
        case_started,
    );
    let mut evidence = artifact.evidence;
    evidence["queries"] = samples;
    evidence["reuse_lifecycle"] = checkpoints;
    evidence["complete_values_verified"] = json!(true);
    // Each bounded owned artifact is removed only after every query and check.
    fs::remove_file(&artifact.path).unwrap();
    evidence["continuous_case_through_validation_and_cleanup_nanos"] = json!(nanos(case_started));
    evidence["continuous_case_scope"] = json!(
        "elapsed from before source preparation through all query validation and owned artifact removal; includes diagnostics, source hashing and bookkeeping; outer shared write context and temporary directory setup excluded"
    );
    evidence["artifact_status"] = json!("removed_owned_test_artifact_after_full_validation");
    evidence
}

#[test]
fn text_codec_portfolio_native_files_and_consumers_match_complete_independent_values() {
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        for profile in [
            Profile::Categorical,
            Profile::NullableCategorical,
            Profile::NullableUnique,
        ] {
            let mut source = None;
            for codec in [Codec::RetainedZstd, Codec::Dictionary, Codec::Fsst] {
                let evidence = run_case(&context, &directory, codec, profile, 257, 129, 1);
                let hash = evidence["source_values_sha256"].clone();
                if let Some(previous) = &source {
                    assert_eq!(previous, &hash);
                }
                source = Some(hash);
            }
        }
    });
}

#[test]
#[ignore = "bounded serial native text lifecycle portfolio; release only, root owns timing"]
fn text_codec_portfolio_release_reuse_1_10_100() {
    assert!(!std::hint::black_box(cfg!(debug_assertions)));
    let directory = Directory::new();
    LOCAL_VORTEX_WRITE_CONTEXT.with(|cell| {
        let context = cell.borrow();
        let codecs = [Codec::RetainedZstd, Codec::Dictionary, Codec::Fsst];
        for sample in 0..3 {
            for profile in [Profile::Categorical, Profile::NullableCategorical, Profile::NullableUnique] {
                let mut source_hash = None;
                for position in 0..3 {
                    let codec = codecs[(position + sample) % codecs.len()];
                    let mut evidence = run_case(&context, &directory, codec, profile, 4096, 2048, MAX_REUSE);
                    let hash = evidence["source_values_sha256"].clone();
                    if let Some(expected) = &source_hash { assert_eq!(expected, &hash); }
                    source_hash = Some(hash);
                    evidence["sample"] = json!(sample);
                    evidence["codec_position"] = json!(position);
                    evidence["provider_version"] = json!(crate::UPSTREAM_VORTEX_PROVIDER_VERSION);
                    evidence["scope"] = json!("forced test-only text portfolio; fresh native query opens; complete reports; no cold-cache or public admission claim");
                    println!("TEXT_CODEC_PORTFOLIO_EVIDENCE {evidence}");
                }
            }
        }
    });
}
