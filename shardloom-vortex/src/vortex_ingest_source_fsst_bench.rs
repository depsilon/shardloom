//! Test-only R6.b admission: retained source Zstd versus native FSST and LIKE.

use super::*;
use vortex::{
    array::{
        arrays::{BoolArray, ConstantArray, bool::BoolArrayExt as _},
        expr::{get_item, like, lit, not_like, root},
        scalar_fn::fns::like::{Like, LikeKernel, LikeOptions},
    },
    encodings::fsst::{FSST, fsst_compress, fsst_train_compressor},
};

const SELECTED: [&str; 2] = ["Title", "URL"];

#[test]
#[ignore = "manual bounded R6.b FSST admission; requires SHARDLOOM_R1B_SOURCE"]
#[allow(clippy::too_many_lines)]
fn clickbench_fsst_predicate_admission_screen() {
    let source = PathBuf::from(std::env::var_os(SOURCE_ENV).expect("explicit source required"));
    assert!(source.is_file());
    let runtime = CurrentThreadRuntime::new();
    let mut memory = super::super::super::NativeIngestMemory::new(MEMORY_LIMIT_BYTES).unwrap();
    memory.session = memory.session.with_handle(runtime.handle());
    let reader = prepare_reader(&source, InputRole::Plain, &SELECTED);
    let mut samples = Vec::new();
    for (region, row_group) in ROW_GROUPS.into_iter().enumerate() {
        let (input, read_sample) = read_and_convert(&source, row_group, &reader, &memory);
        let mut cases = Vec::new();
        for (index, column_name) in reader.columns.iter().enumerate() {
            let column = input.as_::<Struct>().unmasked_field(index).clone();
            let expected = column
                .clone()
                .execute::<VarBinViewArray>(&mut memory.session.create_execution_ctx())
                .unwrap();
            let order = if region.is_multiple_of(2) {
                [false, true]
            } else {
                [true, false]
            };
            for fsst in order {
                let mut training_micros = 0;
                let started = Instant::now();
                let prepared = if fsst {
                    let mut ctx = memory.session.create_execution_ctx();
                    let train_started = Instant::now();
                    let compressor = fsst_train_compressor(&column, &mut ctx).unwrap();
                    training_micros = micros(train_started);
                    fsst_compress(&column, &compressor, &mut ctx)
                        .unwrap()
                        .into_array()
                } else {
                    column.clone()
                };
                let preparation_micros = micros(started);
                let (artifact, writer, bytes) = write_and_verify_column(
                    column_name,
                    &prepared,
                    &expected,
                    &memory,
                    &runtime,
                    if fsst {
                        WriterCase::PreserveFsst
                    } else {
                        WriterCase::Retained
                    },
                );
                if fsst {
                    assert!(
                        artifact["physical_column_inventory"]["encoding_ids"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|x| x == "vortex.fsst")
                    );
                }
                let file = memory.session.open_options().open_buffer(bytes).unwrap();
                let patterns: &[(&str, bool)] = if column_name == "Title" {
                    &[("Google", false), ("Google", true)]
                } else {
                    &[("google", false), ("google", true), (".google.", true)]
                };
                let mut queries = Vec::new();
                for &(needle, negated) in patterns {
                    let mut ctx = memory.session.create_execution_ctx();
                    let valid = expected
                        .varbinview_validity()
                        .execute_mask(expected.len(), &mut ctx)
                        .unwrap();
                    let oracle = (0..expected.len())
                        .map(|row| {
                            valid.value(row)
                                && (std::str::from_utf8(&expected.bytes_at(row))
                                    .unwrap()
                                    .contains(needle)
                                    != negated)
                        })
                        .collect::<Vec<_>>();
                    let oracle_count = oracle.iter().filter(|x| **x).count();
                    let mut kernel_times = Vec::new();
                    let mut scan_times = Vec::new();
                    let mut fsst_kernel_calls = Vec::new();
                    for _ in 0..3 {
                        let (elapsed, calls) = check_native_kernel(
                            &file, &oracle, needle, negated, fsst, &memory, &runtime,
                        );
                        kernel_times.push(elapsed);
                        fsst_kernel_calls.push(calls);
                        let predicate = if negated {
                            not_like(
                                get_item(column_name.as_str(), root()),
                                lit(format!("%{needle}%")),
                            )
                        } else {
                            like(
                                get_item(column_name.as_str(), root()),
                                lit(format!("%{needle}%")),
                            )
                        };
                        let started = Instant::now();
                        let count = file
                            .scan()
                            .unwrap()
                            .with_filter(predicate.bind(file.dtype()).unwrap())
                            .with_projection(lit(1_i64).bind(file.dtype()).unwrap())
                            .into_array_iter(&runtime)
                            .unwrap()
                            .map(|batch| batch.unwrap().len())
                            .sum::<usize>();
                        scan_times.push(micros(started));
                        assert_eq!(count, oracle_count);
                    }
                    queries.push(
                        json!({"needle":needle,"negated":negated,"expected_count":oracle_count,
                        "native_scan_complete_count_micros":scan_times,
                        "native_block_read_and_kernel_micros":kernel_times,
                        "explicit_fsst_kernel_calls":fsst_kernel_calls,
                        "all_boolean_selections_exact":true,"native_scan_counts_exact":true,
                        "public_query_dispatch_proven":false}),
                    );
                }
                cases.push(json!({"column":column_name,"codec":if fsst {"fsst"} else {"retained_zstd"},
                    "prepare_micros":preparation_micros,"fsst_training_micros_in_prepare":training_micros,
                    "prepared_nbytes":prepared.nbytes(),"artifact":artifact,"writer":writer,"queries":queries}));
            }
        }
        drop(input);
        assert_eq!(memory.pool.snapshot().reserved_bytes, 0);
        samples.push(json!({"row_group":row_group,"rows":read_sample.rows,
            "source_reader_conversion_micros":read_sample.elapsed_micros,
            "cases":cases,"reserved_bytes_after_region":0}));
    }
    let result = json!({"schema_version":"shardloom.r6b_fsst_admission.v1","source":source,
        "provider":"Vortex 0.85.0 native FSST compression, Flat persistence, explicit LikeKernel and native scan",
        "metadata_preparation_micros":reader.preparation_elapsed_micros,
        "columns":SELECTED,"row_groups":ROW_GROUPS,"rows_per_region":INPUT_BATCH_ROWS,
        "limits":{"credited_memory_bytes":MEMORY_LIMIT_BYTES,"artifact_bytes_each":COLUMN_FILE_LIMIT_BYTES,"stdout_bytes":MAX_STDOUT_BYTES},
        "memory_boundary":"provider compression, Arrow input, output, reopen and verification buffers may allocate outside native pool; not RSS limit",
        "timing_boundary":"source once per region; preparation/training and write separately; kernel span includes native block scan/struct execution, predicate construction/execution and mask materialization; exact verification excluded; native filter count span includes scan/result iteration, excludes reopen; no public complete query or ingest claim",
        "cache_and_order":"uncontrolled warm OS/provider state; codec order alternates by region; kernel witness precedes native filter scan; all three repetitions retained",
        "samples":samples,"fallback_attempted":false,"external_engine_invoked":false});
    let line = format!("SHARDLOOM_R6B_SCREEN={result}\n");
    assert!(line.len() <= MAX_STDOUT_BYTES);
    print!("{line}");
}

#[allow(clippy::too_many_arguments)]
fn check_native_kernel(
    file: &vortex::file::VortexFile,
    oracle: &[bool],
    needle: &str,
    negated: bool,
    fsst: bool,
    memory: &super::super::super::NativeIngestMemory,
    runtime: &CurrentThreadRuntime,
) -> (u64, usize) {
    let started = Instant::now();
    let mut actual = Vec::with_capacity(oracle.len());
    let mut calls = 0;
    for batch in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let mut ctx = memory.session.create_execution_ctx();
        let batch = batch.unwrap().execute::<StructArray>(&mut ctx).unwrap();
        let column = batch.unmasked_field(0).clone();
        let pattern = ConstantArray::new(format!("%{needle}%"), column.len()).into_array();
        let options = LikeOptions {
            negated,
            case_insensitive: false,
        };
        let result = if fsst {
            let encoded = column
                .as_opt::<FSST>()
                .expect("persisted scan must retain FSST for explicit kernel witness");
            calls += 1;
            <FSST as LikeKernel>::like(encoded, &pattern, options, &mut ctx)
                .unwrap()
                .expect("pattern must reach encoded FSST kernel")
        } else {
            Like::try_new(column, pattern, options)
                .unwrap()
                .into_array()
        };
        let result = result.execute::<BoolArray>(&mut ctx).unwrap();
        let validity = result
            .validity()
            .unwrap()
            .execute_mask(result.len(), &mut ctx)
            .unwrap();
        let bits = result.to_bit_buffer();
        actual.extend((0..result.len()).map(|row| validity.value(row) && bits.value(row)));
    }
    let elapsed = micros(started);
    assert_eq!(actual, oracle);
    assert_eq!(calls > 0, fsst);
    (elapsed, calls)
}

fn micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap()
}
