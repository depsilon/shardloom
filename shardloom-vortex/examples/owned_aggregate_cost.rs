//! Bounded representation-cost screen over the public prepared aggregate API.
//! Complete validation is outside measured construction/drop intervals. This is
//! neither an alternative executor nor a binary-format persistence comparison.

#[cfg(unix)]
mod native {
    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use shardloom_core::{ColumnRef, DatasetUri};
    use shardloom_vortex::{
        VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
        VortexSimpleAggregateRequest,
        local_primitives::{
            VortexLocalPrimitiveExecutionPolicy,
            prepared_aggregate::{
                ExecutedVortexAggregate, PreparedVortexAggregate, prepare_aggregate,
            },
        },
        resident_session::OwnedVortexResultBatch,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        hint::black_box,
        io::Read as _,
        path::{Path, PathBuf},
        time::{Instant, SystemTime, UNIX_EPOCH},
    };
    use vortex::{
        VortexSessionDefault as _,
        array::{
            IntoArray as _,
            arrays::{
                Primitive, PrimitiveArray, Struct, StructArray, struct_::StructArrayExt as _,
            },
            dtype::{DType, Nullability, PType},
            validity::Validity,
        },
        file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
        io::{
            runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        session::VortexSession,
    };

    type Error = Box<dyn std::error::Error>;
    const GROUPS: usize = 32_768;
    const ROWS: usize = GROUPS * 8;
    const BATCH_ROWS: usize = 8192;
    const WARMUPS: usize = 3;
    const PAIRS: usize = 20;
    const KEY: &str = "delivery_zone";
    const VALUE: &str = "package_identifier";
    const COUNT: &str = "distinct_packages";

    struct Options {
        workspace: PathBuf,
        revision: String,
        build_label: String,
    }

    impl Options {
        fn parse() -> Result<Self, Error> {
            let mut workspace = None;
            let mut revision = None;
            let mut build_label = None;
            let mut args = std::env::args().skip(1);
            while let Some(flag) = args.next() {
                let value = args.next().ok_or("each option requires a value")?;
                match flag.as_str() {
                    "--workspace" => workspace = Some(PathBuf::from(value)),
                    "--source-revision" => revision = Some(value),
                    "--build-label" => build_label = Some(value),
                    _ => return Err(format!("unknown option: {flag}").into()),
                }
            }
            let workspace = workspace.ok_or(
                "usage: owned_aggregate_cost --workspace EXISTING_LOCALDATA_DIRECTORY --source-revision COMMIT --build-label LABEL",
            )?;
            if !workspace.is_absolute() {
                return Err("workspace must be absolute".into());
            }
            let workspace = fs::canonicalize(workspace)?;
            let local_root = fs::canonicalize(
                PathBuf::from(std::env::var_os("HOME").ok_or("HOME is unavailable")?)
                    .join("LocalData/shardloom"),
            )?;
            if !workspace.is_dir() || !workspace.starts_with(local_root) {
                return Err("workspace must resolve under ~/LocalData/shardloom".into());
            }
            let revision: String = revision.ok_or("--source-revision is required")?;
            if !(7..=40).contains(&revision.len())
                || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err("source revision must be a 7..=40 digit hexadecimal commit".into());
            }
            let build_label = build_label.ok_or("--build-label is required")?;
            if build_label.is_empty() || build_label.len() > 512 {
                return Err("build label must contain 1..=512 bytes".into());
            }
            Ok(Self {
                workspace,
                revision,
                build_label,
            })
        }
    }

    struct Fixture {
        path: PathBuf,
        expected: Vec<(i64, u64)>,
        sha256: String,
        bytes: u64,
        physical_splits: Vec<(u64, u64)>,
    }

    fn digest(path: &Path) -> Result<String, Error> {
        use std::fmt::Write as _;

        let mut file = fs::File::open(path)?;
        let mut hash = Sha256::new();
        let mut buffer = vec![0_u8; 65_536];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        let mut encoded = String::from("sha256:");
        for byte in hash.finalize() {
            write!(encoded, "{byte:02x}")?;
        }
        Ok(encoded)
    }

    #[allow(clippy::too_many_lines)] // Keep deterministic pairs, independent oracle and file publication together.
    fn fixture(workspace: &Path) -> Result<Fixture, Error> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = workspace.join(format!(
            "owned-aggregate-{}-{stamp}.vortex",
            std::process::id()
        ));
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut oracle = BTreeMap::<i64, BTreeSet<u64>>::new();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let dtype = DType::struct_(
            [
                (KEY, DType::Primitive(PType::I64, Nullability::NonNullable)),
                (
                    VALUE,
                    DType::Primitive(PType::U64, Nullability::NonNullable),
                ),
            ],
            Nullability::NonNullable,
        );
        let mut writer = session
            .write_options()
            .blocking(&runtime)
            .writer(&mut file, dtype.clone());
        for start in (0..ROWS).step_by(BATCH_ROWS) {
            let mut keys = Vec::with_capacity(BATCH_ROWS);
            let mut values = Vec::with_capacity(BATCH_ROWS);
            for row in start..start + BATCH_ROWS {
                let group = row % GROUPS;
                let pass = row / GROUPS;
                let key = match group {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    _ => i64::try_from(group)? - i64::try_from(GROUPS / 2)?,
                };
                let slot = pass % (2 + group % 3);
                let value = if slot == 0 {
                    u64::MAX
                } else {
                    (1_u64 << 60) + u64::try_from(group * 4 + slot)?
                };
                keys.push(key);
                values.push(value);
                oracle.entry(key).or_default().insert(value);
            }
            writer.push(
                StructArray::try_new(
                    [KEY, VALUE].into(),
                    vec![
                        PrimitiveArray::new(keys, Validity::NonNullable).into_array(),
                        PrimitiveArray::new(values, Validity::NonNullable).into_array(),
                    ],
                    BATCH_ROWS,
                    Validity::NonNullable,
                )?
                .into_array(),
            )?;
        }
        if writer.finish()?.row_count() != u64::try_from(ROWS)? {
            return Err("fixture writer changed row count".into());
        }
        file.sync_all()?;
        drop(file);
        let reopened = runtime.block_on(session.open_options().open_path(&path))?;
        if reopened.row_count() != u64::try_from(ROWS)? || reopened.dtype() != &dtype {
            return Err("fixture reopen changed dtype or row count".into());
        }
        let physical_splits = reopened
            .splits()?
            .into_iter()
            .map(|range| (range.start, range.end))
            .collect();
        let mut expected = oracle
            .into_iter()
            .map(|(key, values)| Ok((key, u64::try_from(values.len())?)))
            .collect::<Result<Vec<_>, Error>>()?;
        expected.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        if expected.len() != GROUPS {
            return Err("fixture did not produce the declared independent groups".into());
        }
        Ok(Fixture {
            sha256: digest(&path)?,
            bytes: fs::metadata(&path)?.len(),
            path,
            expected,
            physical_splits,
        })
    }

    fn query(path: &Path, limit: usize) -> Result<VortexQueryPrimitiveRequest, Error> {
        Ok(VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.to_string_lossy().into_owned())?,
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new(KEY)?],
                vec![VortexSimpleAggregateMeasure::new(
                    "count_distinct",
                    Some(ColumnRef::new(VALUE)?),
                    COUNT.into(),
                )],
            )
            .with_order_by(vec![
                VortexAggregateOrderExpr::new(COUNT, true),
                VortexAggregateOrderExpr::new(KEY, false),
            ]),
        )
        .with_source_order_limit(limit))
    }

    fn inspect_report(
        result: &ExecutedVortexAggregate,
        expected: &[(i64, u64)],
        owned: bool,
    ) -> Result<Value, Error> {
        let report = &result.report;
        if !result.native_io_certificate.is_certified()
            || result.native_io_certificate.fallback_attempted
            || report.has_errors()
            || report.fallback_execution_allowed
            || report.arrow_converted
            || report.spill_io_performed
            || report.rows_scanned != u64::try_from(ROWS)?
        {
            return Err("complete native execution or no-fallback evidence failed".into());
        }
        let text = report
            .result_summary
            .as_ref()
            .ok_or("result summary is absent")?;
        let (_, payload) = text
            .rsplit_once(" values=")
            .ok_or("result payload is absent")?;
        let work: Value = serde_json::from_str(payload)?;
        let materialized = if owned {
            0
        } else {
            u64::try_from(expected.len())?
        };
        if work["materialized_group_value_count"] != materialized
            || work["candidate_groups"] != u64::try_from(GROUPS)?
            || work["rows"] != u64::try_from(expected.len())?
            || work["aggregate_workers_rows"] != u64::try_from(ROWS)?
            || work["aggregate_workers_partition_native_handoffs"] != 0
            || work["aggregate_workers_outstanding_chunks"] != 0
        {
            return Err("representation or complete-worker evidence changed".into());
        }
        if owned {
            if !work["values"].is_null() {
                return Err("owned output rendered JSON rows".into());
            }
        } else {
            let rows = work["values"].as_array().ok_or("JSON rows are absent")?;
            if rows.len() != expected.len() {
                return Err("JSON row count changed".into());
            }
            for (row, &(key, count)) in rows.iter().zip(expected) {
                if row.as_object().is_none_or(|fields| fields.len() != 2)
                    || row[KEY].as_i64() != Some(key)
                    || row[COUNT].as_u64() != Some(count)
                {
                    return Err("complete JSON result differs from independent oracle".into());
                }
            }
        }
        Ok(json!({
            "materialized_group_value_count": materialized,
            "result_summary_utf8_bytes": text.len(),
            "input_arrays_read": report.arrays_read_count,
            "max_input_chunk_rows": report.max_chunk_rows,
            "worker_cpu_ceiling": work["aggregate_workers_cpu_ceiling"],
            "compute_threads": work["aggregate_workers_compute_threads"],
            "provider_background_workers": work["aggregate_workers_provider_background_workers"],
            "worker_completed_chunks": work["aggregate_workers_completed_chunks"],
            "native_io_certified": true, "complete_values_verified": true,
        }))
    }

    fn inspect_arrays(
        result: &OwnedVortexResultBatch,
        expected: &[(i64, u64)],
    ) -> Result<(), Error> {
        if result.arrays().len() != 1 || result.row_count() != u64::try_from(expected.len())? {
            return Err("owned result shape changed".into());
        }
        let array = &result.arrays()[0];
        let fields = array
            .as_opt::<Struct>()
            .ok_or("owned output is not a physical Struct")?;
        let keys = fields.unmasked_field_by_name(KEY)?;
        let counts = fields.unmasked_field_by_name(COUNT)?;
        if keys.dtype() != &DType::Primitive(PType::I64, Nullability::NonNullable)
            || counts.dtype() != &DType::Primitive(PType::U64, Nullability::NonNullable)
        {
            return Err("owned original integer dtypes changed".into());
        }
        let keys = keys
            .as_opt::<Primitive>()
            .ok_or("owned keys are not primitive")?;
        let counts = counts
            .as_opt::<Primitive>()
            .ok_or("owned counts are not primitive")?;
        let keys = keys.as_slice::<i64>();
        let counts = counts.as_slice::<u64>();
        if keys.len() != expected.len()
            || counts.len() != expected.len()
            || keys
                .iter()
                .zip(counts)
                .zip(expected)
                .any(|((&key, &count), &expected)| (key, count) != expected)
        {
            return Err("complete owned result differs from independent oracle".into());
        }
        Ok(())
    }

    fn nanos(started: Instant) -> Result<u64, Error> {
        Ok(u64::try_from(started.elapsed().as_nanos())?)
    }

    fn sample(
        prepared: &PreparedVortexAggregate,
        expected: &[(i64, u64)],
        owned: bool,
        baseline: u64,
    ) -> Result<Value, Error> {
        let started = Instant::now();
        let (construction_nanos, drop_nanos, logical_bytes, reserved_at_return, mut work) = if owned
        {
            let result = black_box(prepared.execute_owned()?);
            let construction = nanos(started)?;
            let reserved = prepared.snapshot().memory.reserved_bytes;
            let work = inspect_report(&result.execution, expected, true)?;
            inspect_arrays(&result.result, expected)?;
            let bytes = result.result.logical_buffer_bytes();
            let dropping = Instant::now();
            drop(black_box(result));
            (construction, nanos(dropping)?, Some(bytes), reserved, work)
        } else {
            let result = black_box(prepared.execute()?);
            let construction = nanos(started)?;
            let reserved = prepared.snapshot().memory.reserved_bytes;
            let work = inspect_report(&result, expected, false)?;
            let dropping = Instant::now();
            drop(black_box(result));
            (construction, nanos(dropping)?, None, reserved, work)
        };
        let snapshot = prepared.snapshot();
        if snapshot.memory.reserved_bytes != baseline {
            return Err(
                "query/result credits did not return to the prepared-source baseline".into(),
            );
        }
        let work = work
            .as_object_mut()
            .ok_or("sample evidence is not an object")?;
        work.insert(
            "arm".into(),
            if owned {
                "owned_columns"
            } else {
                "json_report_rows"
            }
            .into(),
        );
        work.insert("construction_nanos".into(), construction_nanos.into());
        work.insert("result_drop_nanos".into(), drop_nanos.into());
        work.insert(
            "construction_plus_drop_nanos".into(),
            construction_nanos
                .checked_add(drop_nanos)
                .ok_or("sample timing overflow")?
                .into(),
        );
        work.insert("owned_logical_buffer_bytes".into(), logical_bytes.into());
        work.insert(
            "shared_reserved_bytes_at_result_return".into(),
            reserved_at_return.into(),
        );
        work.insert(
            "shared_reserved_bytes_after_drop".into(),
            snapshot.memory.reserved_bytes.into(),
        );
        work.insert(
            "shared_session_cumulative_peak_reserved_bytes".into(),
            snapshot.memory.peak_reserved_bytes.into(),
        );
        Ok(Value::Object(std::mem::take(work)))
    }

    fn latency(samples: &[Value], arm: &str) -> Value {
        let mut ordered = samples
            .iter()
            .filter(|sample| sample["phase"] == "measurement" && sample["arm"] == arm)
            .map(|sample| {
                sample["construction_plus_drop_nanos"]
                    .as_u64()
                    .expect("constructed sample timing")
            })
            .collect::<Vec<_>>();
        ordered.sort_unstable();
        let percentile = |percent: usize| ordered[(ordered.len() * percent).div_ceil(100) - 1];
        json!({"samples": ordered.len(), "percentile_method": "nearest_rank", "median_nanos": percentile(50), "p95_nanos": percentile(95), "min_nanos": ordered.first(), "max_nanos": ordered.last()})
    }

    fn measure(fixture: &Fixture, workers: usize, limit: usize) -> Result<Value, Error> {
        let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(workers, 1)?;
        let preparing = Instant::now();
        let prepared = prepare_aggregate(&query(&fixture.path, limit)?, policy)?;
        let prepare_nanos = nanos(preparing)?;
        let baseline = prepared.snapshot().memory.reserved_bytes;
        if baseline != 0 {
            return Err("fixture preparation unexpectedly retains query payload credits".into());
        }
        let mut samples = Vec::with_capacity(2 * (WARMUPS + PAIRS));
        for (phase, rounds) in [("warmup", WARMUPS), ("measurement", PAIRS)] {
            for pair in 0..rounds {
                let order = if pair % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                };
                for (position, owned) in order.into_iter().enumerate() {
                    let mut result =
                        sample(&prepared, &fixture.expected[..limit], owned, baseline)?;
                    result["phase"] = phase.into();
                    result["pair"] = pair.into();
                    result["position_in_pair"] = position.into();
                    samples.push(result);
                }
            }
        }
        let snapshot = prepared.snapshot();
        if snapshot.prepared_source_opens != 1
            || snapshot.completed_executions != u64::try_from(2 * (WARMUPS + PAIRS))?
        {
            return Err("prepared source reuse or fresh execution count differs".into());
        }
        let json_latency = latency(&samples, "json_report_rows");
        let owned_latency = latency(&samples, "owned_columns");
        Ok(json!({
            "requested_parallelism": workers, "limit": limit, "offset": 0,
            "query": "GROUP BY delivery_zone; COUNT DISTINCT package_identifier AS distinct_packages; ORDER BY distinct_packages DESC, delivery_zone ASC; LIMIT K",
            "memory_budget_bytes": policy.resource_envelope.memory_budget_bytes,
            "warmups_per_arm": WARMUPS, "balanced_measurement_pairs": PAIRS,
            "prepare_nanos_excluded": prepare_nanos,
            "json_report_rows": json_latency, "owned_columns": owned_latency,
            "raw_samples_in_execution_order": samples,
            "prepared_source_opens": snapshot.prepared_source_opens,
            "completed_executions": snapshot.completed_executions,
            "shared_session_cumulative_peak_reserved_bytes": snapshot.memory.peak_reserved_bytes,
            "final_shared_reserved_bytes": snapshot.memory.reserved_bytes,
        }))
    }

    pub(super) fn run() -> Result<(), Error> {
        let options = Options::parse()?;
        let fixture = fixture(&options.workspace)?;
        let mut cases = Vec::new();
        for workers in [1, 4] {
            for limit in [32, GROUPS] {
                cases.push(measure(&fixture, workers, limit)?);
            }
        }
        if digest(&fixture.path)? != fixture.sha256
            || fs::metadata(&fixture.path)?.len() != fixture.bytes
        {
            return Err("fixture bytes changed during the screen".into());
        }
        println!(
            "{}",
            json!({
                "schema_version": "shardloom.owned_aggregate_representation_cost.v1",
                "source_revision_supplied_by_runner": options.revision,
                "build_label_supplied_by_runner": options.build_label,
                "executable": std::env::current_exe()?, "crate_version": env!("CARGO_PKG_VERSION"),
                "upstream_vortex_provider_version": shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
                "os": std::env::consts::OS, "architecture": std::env::consts::ARCH,
                "available_parallelism": std::thread::available_parallelism()?.get(),
                "fixture": {"path": fixture.path, "sha256": fixture.sha256, "bytes": fixture.bytes,
                    "rows": ROWS, "groups": GROUPS, "rows_per_group": 8,
                    "schema": "delivery_zone:nonnullable i64; package_identifier:nonnullable u64",
                    "integer_boundaries": "i64 MIN/MAX group keys; u64 MAX and values above 2^53; duplicates across writer batches",
                    "writer": "pinned upstream Vortex default file writer; primitive input; preparation excluded",
                    "writer_input_batch_rows": BATCH_ROWS, "physical_row_ranges": fixture.physical_splits,
                    "lifetime": "new create-only fixture retained for reproducibility; never overwrites an existing target"},
                "cases": cases,
                "timing_boundary": "sum of separate monotonic intervals: fresh prepared query through result/report/certificate construction, plus returned result drop; complete validation occurs between intervals and is excluded",
                "validation_effect": "verification touches result buffers before the timed drop and may affect caches; the same sequencing is used for both arms",
                "cache_policy": "single immutable file; source/layout metadata retained per case; fresh aggregate state each call; OS cache uncontrolled; no answer cache",
                "oracle": "independent standard-library BTreeMap/BTreeSet over generated input pairs, exact count-descending/key-ascending sort; all output rows checked on every call",
                "memory_scope": "shared session reservation counters only; JSON report buffers, oracle/harness allocations, provider bypass allocations and process RSS are not measured; cumulative peaks combine both arms and are not per-arm peaks",
                "bytes_scope": "owned_logical_buffer_bytes counts native result buffer bytes; result_summary_utf8_bytes counts serialized report text; neither is total allocation or unique retained memory",
                "comparison_scope": "JSON result reports versus owned columns through the existing public prepared API; excludes preparation, fixture construction, transport and persistence; no format or ingest causality claim",
                "claim_gate_status": "bounded_screen_requires_review_of_raw_samples_and_materiality;not_Full43_or_product_wide_performance_evidence",
                "fallback_attempted": false, "external_engine_invoked": false,
            })
        );
        Ok(())
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::run()
}

#[cfg(not(unix))]
fn main() {
    eprintln!("owned aggregate screen requires Unix source-generation identity");
}
