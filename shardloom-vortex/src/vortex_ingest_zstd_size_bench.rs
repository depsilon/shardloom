//! Test-only, actual-source-batch Zstd size admission. No production policy.

use super::*;
use crate::vortex_ingest as ingest;
use vortex::{
    array::expr::{get_item, like, lit, not_like, root},
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy, compressed::CompressingStrategy,
            flat::writer::FlatLayoutStrategy,
        },
    },
};
use vortex_zstd::Zstd;

const BATCH_ROWS: usize = 32_768;
const BATCHES_PER_REGION: usize = 4;
const PROFILES: [Profile; 6] = [
    Profile::new(-3, 8192, false),
    Profile::new(-3, 4096, false),
    Profile::new(-3, 4096, true),
    Profile::new(3, 8192, false),
    Profile::new(3, 4096, false),
    Profile::new(3, 4096, true),
];

#[derive(Clone, Copy, Debug)]
struct Profile {
    level: i32,
    values_per_frame: usize,
    dictionary: bool,
}

impl Profile {
    const fn new(level: i32, values_per_frame: usize, dictionary: bool) -> Self {
        Self {
            level,
            values_per_frame,
            dictionary,
        }
    }

    fn name(self) -> String {
        format!(
            "level_{}_frame_{}_dict_{}",
            self.level, self.values_per_frame, self.dictionary
        )
    }

    const fn retained(self) -> bool {
        self.level == -3 && self.values_per_frame == 8192 && !self.dictionary
    }

    fn leaf(self, timing: &ingest::VortexWriterStageTiming) -> Arc<dyn LayoutStrategy> {
        if self.retained() {
            return ingest::large_source_fast_zstd_text_leaf_strategy(
                WRITER_ROW_BLOCK_SIZE,
                1,
                timing,
            );
        }
        let compressor = move |input: &VortexArrayRef, ctx: &mut vortex::array::ExecutionCtx| {
            let canonical = input.clone().execute::<VarBinViewArray>(ctx)?;
            let compact = canonical.compact_buffers(ctx)?;
            let encoded = if self.dictionary {
                Zstd::from_var_bin_view(&compact, self.level, self.values_per_frame, ctx)?
            } else {
                Zstd::from_var_bin_view_without_dict(
                    &compact,
                    self.level,
                    self.values_per_frame,
                    ctx,
                )?
            };
            Ok(encoded.into_array())
        };
        Arc::new(
            CompressingStrategy::new(
                ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
                compressor,
            )
            .with_stats(&[])
            .with_concurrency(1),
        )
    }
}

fn profile_order(batch: usize) -> [usize; 6] {
    let mut order = std::array::from_fn(|index| (index + batch % 6) % 6);
    if batch >= 6 {
        order.reverse();
    }
    order
}

#[test]
fn zstd_size_screen_balances_every_profile_position() {
    let mut counts = [[0; 6]; 6];
    for batch in 0..12 {
        for (position, profile) in profile_order(batch).into_iter().enumerate() {
            counts[profile][position] += 1;
        }
    }
    assert_eq!(counts, [[2; 6]; 6]);
}

#[test]
fn zstd_size_screen_preserves_nullable_utf8_with_every_profile() {
    let runtime = CurrentThreadRuntime::new();
    let mut memory = ingest::NativeIngestMemory::new(MEMORY_LIMIT_BYTES).unwrap();
    memory.session = memory.session.with_handle(runtime.handle());
    for values in [
        vec![None, Some(""), Some("λ🎉"), Some("google"), Some("a\0b")],
        vec![None; 5],
    ] {
        let expected = VarBinViewArray::from_iter_nullable_str(values);
        let input = expected.clone().into_array();
        for profile in PROFILES {
            write_and_check("value", &input, &expected, profile, &memory, &runtime);
        }
    }
    assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
}

#[test]
#[ignore = "manual bounded native text-size screen; requires SHARDLOOM_R1B_SOURCE"]
#[allow(clippy::too_many_lines)]
fn clickbench_zstd_size_admission_screen() {
    let source = PathBuf::from(std::env::var_os(SOURCE_ENV).expect("explicit source required"));
    assert!(source.is_file());
    let runtime = CurrentThreadRuntime::new();
    let mut memory = ingest::NativeIngestMemory::new(MEMORY_LIMIT_BYTES).unwrap();
    memory.session = memory.session.with_handle(runtime.handle());
    let prepared = prepare_reader(&source, InputRole::Plain, &COLUMN_NAMES);
    let mut samples = Vec::new();
    for (region, row_group) in ROW_GROUPS.into_iter().enumerate() {
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            File::open(&source).unwrap(),
            prepared.metadata.clone(),
        );
        let projection = ProjectionMask::roots(
            builder.parquet_schema(),
            prepared.projection_roots.iter().copied(),
        );
        let mut reader = builder
            .with_row_groups(vec![row_group])
            .with_batch_size(BATCH_ROWS)
            .with_projection(projection)
            .build()
            .unwrap();
        for batch_index in 0..BATCHES_PER_REGION {
            let started = Instant::now();
            let batch = reader.next().expect("selected batch exists").unwrap();
            assert_eq!(batch.num_rows(), BATCH_ROWS);
            let shape = ingest::FlatColumnarSourceShape {
                projected_columns: prepared
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(reader_index, name)| {
                        assert_eq!(batch.schema().field(reader_index).name(), name);
                        ingest::ColumnarProjectedColumn {
                            column: name.clone(),
                            reader_index,
                            dtype_hint: None,
                            arrow_dtype_hint: None,
                        }
                    })
                    .collect(),
            };
            let bytes = ingest::arrow_ownership::batch_copy_allocation_bytes(&batch).unwrap();
            let mut lease = memory.pool.reserve(bytes.checked_mul(2).unwrap()).unwrap();
            let input = ingest::record_batch_to_vortex_from_arrow_provider_profiled_with_memory(
                &batch,
                &shape,
                &ingest::IngestStageTimings::default(),
                Some((&memory, &mut lease)),
            )
            .unwrap();
            drop(lease);
            drop(batch);
            let source_micros = micros(started);
            let order = profile_order(region * BATCHES_PER_REGION + batch_index);
            let mut cases = Vec::new();
            for (column_index, name) in prepared.columns.iter().enumerate() {
                let column = input.as_::<Struct>().unmasked_field(column_index).clone();
                let expected = column
                    .clone()
                    .execute::<VarBinViewArray>(&mut memory.session.create_execution_ctx())
                    .unwrap();
                let mut statistics = None;
                for profile_index in order {
                    let sample = write_and_check(
                        name,
                        &column,
                        &expected,
                        PROFILES[profile_index],
                        &memory,
                        &runtime,
                    );
                    let observed = sample["footer_statistics"].clone();
                    if let Some(reference) = &statistics {
                        assert_eq!(reference, &observed, "profile changed footer statistics");
                    } else {
                        statistics = Some(observed);
                    }
                    cases.push(sample);
                }
            }
            drop(input);
            assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
            samples.push(
                json!({"row_group":row_group,"batch_index":batch_index,"rows":BATCH_ROWS,
                "source_read_and_conversion_micros":source_micros,"profile_order":order,
                "cases":cases,"native_reserved_bytes_after_batch":0}),
            );
        }
    }
    let report = json!({
        "schema_version":"shardloom.native_zstd_size_screen.v1",
        "source":source,"provider":"Vortex 0.90.0 native Zstd and native file writer/scan",
        "columns":prepared.columns,"row_groups":ROW_GROUPS,"batches_per_region":BATCHES_PER_REGION,
        "rows_per_batch":BATCH_ROWS,"profiles":PROFILES.map(Profile::name),
        "limits":{"admitted_memory_bytes":MEMORY_LIMIT_BYTES,"column_file_bytes":COLUMN_FILE_LIMIT_BYTES,
            "stdout_bytes":MAX_STDOUT_BYTES},
        "memory_boundary":"native input and codec buffers/workspaces admitted; Parquet, file Vec, inspection parts and report allocations are not an RSS guarantee",
        "timing_boundary":"source read/conversion separate; native write includes compact/codec/file persistence; reads include complete decode and exact comparisons; predicates include native scan and count; structural witnesses excluded",
        "public_query_dispatch_proven":false,"full_ingest_proven":false,
        "fallback_attempted":false,"external_engine_invoked":false,"samples":samples,
    });
    let output = serde_json::to_vec(&report).unwrap();
    assert!(output.len() <= MAX_STDOUT_BYTES);
    println!(
        "NATIVE_ZSTD_SIZE_SCREEN {}",
        std::str::from_utf8(&output).unwrap()
    );
}

#[allow(clippy::too_many_lines)]
fn write_and_check(
    name: &str,
    input: &VortexArrayRef,
    expected: &VarBinViewArray,
    profile: Profile,
    memory: &ingest::NativeIngestMemory,
    runtime: &CurrentThreadRuntime,
) -> Value {
    let single = StructArray::try_new(
        FieldNames::from([name]),
        vec![input.clone()],
        input.len(),
        vortex::array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let dtype = single.dtype().clone();
    let timing = ingest::VortexWriterStageTiming::default();
    let table = ingest::large_source_fast_load_table_strategy_with_dictionaries(
        WRITER_ROW_BLOCK_SIZE,
        WRITER_BLOCK_TARGET_BYTES,
        1,
        &timing,
        &memory.session,
        true,
    )
    .with_field_writer(FieldPath::from_name(name), profile.leaf(&timing));
    let bounded = ingest::bounded_ingest_layout::BoundedIngestLayout::new(
        Arc::new(table),
        0,
        memory.pool.reserve(0).unwrap(),
    );
    let mut bytes = Vec::new();
    let started = Instant::now();
    let summary = memory
        .session
        .write_options()
        .with_strategy(Arc::new(bounded))
        .blocking(runtime)
        .write(
            LimitedWrite {
                inner: &mut bytes,
                written: 0,
                limit: COLUMN_FILE_LIMIT_BYTES,
            },
            ArrayIteratorAdapter::new(dtype.clone(), [Ok(single)].into_iter()),
        )
        .unwrap();
    let write_micros = micros(started);
    assert_eq!(summary.row_count(), expected.len() as u64);
    drop(summary);
    let file_bytes = bytes.len();
    let sha = hex_digest(&sha2::Sha256::digest(&bytes));
    let file = memory.session.open_options().open_buffer(bytes).unwrap();
    assert_eq!(file.dtype(), &dtype);
    assert_eq!(file.row_count(), expected.len() as u64);
    let sizes = file.footer().compressed_field_sizes().unwrap();
    let column_bytes = sizes.get(&FieldPath::from_name(name)).unwrap();
    assert!(column_bytes <= sizes.total() && sizes.total() <= file_bytes as u64);
    let mut witnesses = Vec::new();
    for batch in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let batch = batch
            .unwrap()
            .execute::<StructArray>(&mut memory.session.create_execution_ctx())
            .unwrap();
        let column = batch
            .unmasked_field(0)
            .as_opt::<Zstd>()
            .expect("persisted Zstd leaf required");
        let parts = column.data().clone().into_parts(column.validity().unwrap());
        let dictionary_bytes = parts
            .dictionary
            .as_ref()
            .map_or(0, vortex::buffer::Buffer::len);
        let frame_rows: Vec<_> = parts
            .metadata
            .frames
            .iter()
            .map(|frame| frame.n_values)
            .collect();
        assert!(
            frame_rows
                .iter()
                .all(|&rows| rows <= u64::try_from(profile.values_per_frame).unwrap())
        );
        if expected.len() == BATCH_ROWS {
            assert_eq!(
                column.len(),
                BATCH_ROWS,
                "writer changed the source compression batch"
            );
            assert_eq!(
                dictionary_bytes > 0,
                profile.dictionary,
                "training witness differs from selected profile"
            );
        }
        witnesses.push(
            json!({"rows":column.len(),"dictionary_bytes":dictionary_bytes,
            "frame_count":parts.frames.len(),"frame_value_counts":frame_rows,
            "compressed_frame_bytes":parts.frames.iter().map(vortex::buffer::Buffer::len).sum::<usize>()}),
        );
    }
    assert_eq!(witnesses.len(), 1, "one physical source batch per artifact");
    let read_micros: Vec<_> = (0..3)
        .map(|_| {
            let started = Instant::now();
            let mut seen = 0;
            for batch in file
                .scan()
                .unwrap()
                .with_ordered(true)
                .into_array_iter(runtime)
                .unwrap()
            {
                let mut ctx = memory.session.create_execution_ctx();
                let batch = batch.unwrap().execute::<StructArray>(&mut ctx).unwrap();
                let actual = batch
                    .unmasked_field(0)
                    .clone()
                    .execute::<VarBinViewArray>(&mut ctx)
                    .unwrap();
                assert_column_range_equal(expected, &actual, seen, memory);
                seen += actual.len();
            }
            assert_eq!(seen, expected.len());
            micros(started)
        })
        .collect();
    let mut ctx = memory.session.create_execution_ctx();
    let valid = expected
        .varbinview_validity()
        .execute_mask(expected.len(), &mut ctx)
        .unwrap();
    let mut predicates = Vec::new();
    for negated in [false, true] {
        let count = (0..expected.len())
            .filter(|&row| {
                valid.value(row)
                    && (std::str::from_utf8(&expected.bytes_at(row))
                        .unwrap()
                        .contains("google")
                        != negated)
            })
            .count();
        let times: Vec<_> = (0..3)
            .map(|_| {
                let predicate = if negated {
                    not_like(get_item(name, root()), lit("%google%"))
                } else {
                    like(get_item(name, root()), lit("%google%"))
                };
                let started = Instant::now();
                let actual = file
                    .scan()
                    .unwrap()
                    .with_filter(predicate.bind(file.dtype()).unwrap())
                    .with_projection(lit(1_i64).bind(file.dtype()).unwrap())
                    .into_array_iter(runtime)
                    .unwrap()
                    .map(|batch| batch.unwrap().len())
                    .sum::<usize>();
                assert_eq!(actual, count);
                micros(started)
            })
            .collect();
        predicates.push(json!({"needle":"google","negated":negated,"expected_count":count,"native_scan_count_micros":times}));
    }
    json!({"column":name,"profile":profile.name(),"retained_production_leaf":profile.retained(),
        "rows":expected.len(),"complete_file_bytes":file_bytes,"physical_column_bytes":column_bytes,
        "all_segment_bytes":sizes.total(),"sha256":sha,
        "footer_statistics":format!("{:?}",file.footer().statistics()),
        "persisted_zstd_witnesses":witnesses,"native_write_micros":write_micros,
        "native_read_and_exact_compare_micros":read_micros,"predicates":predicates,
        "all_values_validity_order_dtype_exact":true})
}

fn micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap()
}
