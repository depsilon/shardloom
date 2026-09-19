//! Bounded SUM/AVG computed-result baseline screen for the public aggregate API.
//! This measures execution and returned-result drop separately; oracle checks are
//! deliberately outside both intervals.

#[cfg(unix)]
mod native {
    use serde_json::{Value, json};
    use sha2::{Digest as _, Sha256};
    use shardloom_core::{ColumnRef, DatasetUri};
    use shardloom_vortex::{
        VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
        VortexSimpleAggregateRequest,
        local_primitives::{
            VortexLocalPrimitiveExecutionPolicy, prepared_aggregate::prepare_aggregate,
        },
    };
    use std::{
        collections::BTreeMap,
        fs,
        hint::black_box,
        io::{Read as _, Write as _},
        path::{Path, PathBuf},
        time::{Instant, SystemTime, UNIX_EPOCH},
    };
    use vortex::{
        VortexSessionDefault as _,
        array::{
            IntoArray as _,
            arrays::{PrimitiveArray, StructArray},
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
    const GROUPS: usize = 65_536;
    const ROWS: usize = GROUPS * 4;
    const BATCH_ROWS: usize = 8_192;
    const KEY: &str = "delivery_zone";
    const VALUE: &str = "package_value";

    #[derive(Clone, Copy)]
    enum Agg {
        Sum,
        Avg,
    }
    impl Agg {
        fn name(self) -> &'static str {
            match self {
                Self::Sum => "sum",
                Self::Avg => "avg",
            }
        }
        fn out(self) -> &'static str {
            match self {
                Self::Sum => "total_package_value",
                Self::Avg => "avg_package_value",
            }
        }
    }
    struct Fixture {
        path: PathBuf,
        sha256: String,
        bytes: u64,
        oracle: Vec<(i64, i64, u64)>,
    }
    struct Lock(PathBuf);
    impl Lock {
        fn acquire(path: PathBuf) -> Result<Self, Error> {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            let owned = Self(path);
            writeln!(file, "{}", std::process::id())?;
            file.sync_all()?;
            Ok(owned)
        }
    }
    impl Drop for Lock {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn digest(path: &Path) -> Result<String, Error> {
        use std::fmt::Write as _;
        let mut f = fs::File::open(path)?;
        let mut h = Sha256::new();
        let mut b = vec![0; 65_536];
        loop {
            let n = f.read(&mut b)?;
            if n == 0 {
                break;
            }
            h.update(&b[..n]);
        }
        let mut out = String::from("sha256:");
        for byte in h.finalize() {
            write!(out, "{byte:02x}")?;
        }
        Ok(out)
    }
    fn options() -> Result<(PathBuf, String), Error> {
        let mut w = None;
        let mut r = None;
        let mut a = std::env::args().skip(1);
        while let Some(k) = a.next() {
            let v = a.next().ok_or("each option requires a value")?;
            match k.as_str() {
                "--workspace" if w.is_none() => w = Some(PathBuf::from(v)),
                "--source-revision" if r.is_none() => r = Some(v),
                _ => return Err("unknown option".into()),
            }
        }
        let w = w.ok_or("--workspace is required")?;
        if !w.is_absolute() {
            return Err("workspace must be absolute".into());
        }
        let w = fs::canonicalize(w)?;
        let root = fs::canonicalize(
            PathBuf::from(std::env::var_os("HOME").ok_or("HOME unavailable")?)
                .join("LocalData/shardloom"),
        )?;
        if !w.is_dir() || !w.starts_with(root) {
            return Err("workspace must resolve under ~/LocalData/shardloom".into());
        }
        let r = r.ok_or("--source-revision is required")?;
        if r.len() != 40 || !r.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("source revision must be full 40-hex".into());
        }
        Ok((w, r))
    }
    fn fixture(workspace: &Path) -> Result<(Fixture, Lock), Error> {
        let lock = Lock::acquire(workspace.join("computed-result-cost.lock"))?;
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = workspace.join(format!(
            "computed-result-cost-{stamp}-{}.vortex",
            std::process::id()
        ));
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let dtype = DType::struct_(
            [
                (KEY, DType::Primitive(PType::I64, Nullability::NonNullable)),
                (
                    VALUE,
                    DType::Primitive(PType::I64, Nullability::NonNullable),
                ),
            ],
            Nullability::NonNullable,
        );
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut writer = session
            .write_options()
            .blocking(&runtime)
            .writer(&mut file, dtype.clone());
        let mut totals: BTreeMap<i64, (i64, u64)> = BTreeMap::new();
        for start in (0..ROWS).step_by(BATCH_ROWS) {
            let mut keys = Vec::with_capacity(BATCH_ROWS);
            let mut values = Vec::with_capacity(BATCH_ROWS);
            for row in start..start + BATCH_ROWS {
                let group = row % GROUPS;
                let key = i64::try_from(group)? - 32_768;
                let value = i64::try_from(group % 101 + row / GROUPS)?;
                keys.push(key);
                values.push(value);
                let total = totals.entry(key).or_default();
                total.0 += value;
                total.1 += 1;
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
        if writer.finish()?.row_count() != ROWS as u64 {
            return Err("writer changed row count".into());
        }
        file.sync_all()?;
        drop(file);
        let reopened = runtime.block_on(session.open_options().open_path(&path))?;
        if reopened.row_count() != ROWS as u64 || reopened.dtype() != &dtype {
            return Err("reopen schema or rows changed".into());
        }
        let mut oracle = totals
            .into_iter()
            .map(|(k, (sum, n))| (k, sum, n))
            .collect::<Vec<_>>();
        if oracle.len() != GROUPS || oracle.iter().any(|(_, _, count)| *count != 4) {
            return Err("fixture changed group geometry".into());
        }
        // Every group has four rows, so SUM and AVG have identical ranking.
        oracle.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let bytes = fs::metadata(&path)?.len();
        let sha256 = digest(&path)?;
        Ok((
            Fixture {
                path,
                sha256,
                bytes,
                oracle,
            },
            lock,
        ))
    }
    fn query(p: &Path, a: Agg, limit: usize) -> Result<VortexQueryPrimitiveRequest, Error> {
        let request = VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY)?],
            vec![VortexSimpleAggregateMeasure::new(
                a.name(),
                Some(ColumnRef::new(VALUE)?),
                a.out().into(),
            )],
        )
        .with_order_by(vec![
            VortexAggregateOrderExpr::new(a.out(), true),
            VortexAggregateOrderExpr::new(KEY, false),
        ]);
        Ok(VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(p.to_string_lossy().into_owned())?,
            request,
        )
        .with_source_order_limit(limit))
    }
    fn check_rows(v: &Value, a: Agg, expected: &[(i64, i64, u64)]) -> Result<(), Error> {
        let rows = v.as_array().ok_or("values absent")?;
        if rows.len() != expected.len() {
            return Err("result row count changed".into());
        }
        for (r, (k, sum, n)) in rows.iter().zip(expected) {
            if r.as_object().is_none_or(|row| row.len() != 2) || r[KEY].as_i64() != Some(*k) {
                return Err("key order/value differs".into());
            }
            let exact = match a {
                // The native numeric SUM contract returns a JSON float too.
                Agg::Sum => r[a.out()].as_f64() == Some(f64::from(i32::try_from(*sum)?)),
                // The fixed fixture's small integer totals divided by four
                // are exact binary fractions. No rounding/tolerance is needed.
                Agg::Avg => {
                    r[a.out()].as_f64()
                        == Some(f64::from(i32::try_from(*sum)?) / f64::from(u32::try_from(*n)?))
                }
            };
            if !exact {
                return Err("aggregate value differs".into());
            }
        }
        Ok(())
    }
    fn run_case(fixture: &Fixture, a: Agg, workers: usize, limit: usize) -> Result<Value, Error> {
        let pol = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(workers, 1)?;
        let prep = prepare_aggregate(&query(&fixture.path, a, limit)?, pol)?;
        let rejection = match prep.execute_owned() {
            Ok(_) => return Err("execute_owned unexpectedly admitted".into()),
            Err(error) => error.to_string(),
        };
        if !rejection.contains("owned aggregate output: requires one non-null identity key")
            || prep.snapshot().completed_executions != 0
        {
            return Err("owned rejection changed or executed the query".into());
        }
        let exp = &fixture.oracle[..limit];
        let mut calls = Vec::new();
        for call in 0..3 {
            let execution_started = Instant::now();
            let r = black_box(prep.execute()?);
            let execution_nanos = u64::try_from(execution_started.elapsed().as_nanos())?;
            if r.report.has_errors()
                || !r.native_io_certificate.is_certified()
                || r.native_io_certificate.fallback_attempted
                || r.report.fallback_execution_allowed
                || r.report.arrow_converted
                || r.report.spill_io_performed
                || r.report.rows_scanned != ROWS as u64
            {
                return Err("native execution/certificate boundary changed".into());
            }
            let summary = r.report.result_summary.as_ref().ok_or("summary absent")?;
            let summary_json: Value = serde_json::from_str(
                summary
                    .rsplit_once(" values=")
                    .ok_or("summary payload absent")?
                    .1,
            )?;
            check_rows(&summary_json["values"], a, exp)?;
            if summary_json["rows"] != limit || summary_json["candidate_groups"] != GROUPS {
                return Err("completed group/output geometry changed".into());
            }
            let mut spans = serde_json::Map::new();
            for field in [
                "aggregate_result_finalization_nanos",
                "aggregate_first_pass_scan_next_nanos",
                "aggregate_first_pass_reader_evidence_nanos",
                "aggregate_first_pass_accessor_nanos",
                "aggregate_first_pass_group_update_nanos",
            ] {
                let value = summary_json[field].as_u64().ok_or("caller span absent")?;
                spans.insert(field.into(), value.into());
            }
            let bytes = summary.len();
            let output_strategy = summary_json["group_output_strategy"].clone();
            let update_strategy = summary_json["aggregate_update_strategy"].clone();
            // Release verifier allocations outside the result-drop clock.
            drop(summary_json);
            let drop_started = Instant::now();
            drop(black_box(r));
            let drop_nanos = u64::try_from(drop_started.elapsed().as_nanos())?;
            calls.push(json!({
                "call":call, "warmup":call==0,
                "execution_nanos":execution_nanos, "drop_nanos":drop_nanos,
                "complete_nanos":execution_nanos.checked_add(drop_nanos).ok_or("clock overflow")?,
                "result_summary_bytes":bytes, "caller_spans":spans,
                "group_output_strategy":output_strategy,
                "aggregate_update_strategy":update_strategy,
                "complete_values_verified":true,
            }));
        }
        let snap = prep.snapshot();
        if snap.completed_executions != 3 {
            return Err("execution count changed".into());
        }
        Ok(
            json!({"aggregate":a.name(),"workers":workers,"limit":limit,"output_rows":limit,"groups":GROUPS,"calls":calls,"owned_rejection":rejection,"report_has_errors":false,"native_certificate_is_certified":true,"resource_policy":pol.resource_envelope.memory_budget_bytes}),
        )
    }
    pub fn run() -> Result<(), Error> {
        let (w, rev) = options()?;
        let (f, _lock) = fixture(&w)?;
        let mut cases = Vec::new();
        for a in [Agg::Sum, Agg::Avg] {
            for workers in [1, 4] {
                for limit in [32, GROUPS] {
                    cases.push(run_case(&f, a, workers, limit)?);
                }
            }
        }
        if digest(&f.path)? != f.sha256 || fs::metadata(&f.path)?.len() != f.bytes {
            return Err("fixture changed".into());
        }
        println!(
            "{}",
            json!({"schema_version":"shardloom.computed_result_cost.v1","source_revision":rev,"binary_sha256":digest(&std::env::current_exe()?)?,"fixture":{"path":f.path,"bytes":f.bytes,"sha256":f.sha256,"rows":ROWS,"groups":GROUPS,"writer_input_batch_rows":BATCH_ROWS,"schema":"delivery_zone:nonnullable i64; package_value:nonnullable i64","writer":"pinned Vortex default writer"},"cases":cases,"timing":"execution and result drop only; prepare, fixture, oracle and transport excluded","caller_span_scope":"disjoint caller elapsed scopes; provider activity may overlap; finalization includes ranking/render and state drop but excludes later report annotations","memory_scope":"one GiB session policy; total RSS, harness/oracle and JSON allocation are not measured or enforced by this policy","bytes_scope":"serialized result summary bytes; not allocation, copy, decode or transport bytes","cache_scope":"retained source; fresh aggregate state; uncontrolled OS cache; no answer cache","fallback_attempted":false,"external_engine_invoked":false})
        );
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn complete_oracle_rejects_truncation_tail_order_and_fraction_errors() {
            let expected = [(5, 10, 4), (-3, 6, 4)];
            for (agg, values) in [
                (Agg::Sum, [json!(10.0), json!(6.0)]),
                (Agg::Avg, [json!(2.5), json!(1.5)]),
            ] {
                let rows = json!([{KEY:5,agg.out():values[0]}, {KEY:-3,agg.out():values[1]}]);
                check_rows(&rows, agg, &expected).unwrap();
                assert!(check_rows(&json!([rows[0]]), agg, &expected).is_err());
                assert!(check_rows(&json!([rows[1], rows[0]]), agg, &expected).is_err());
                let mut corrupt = rows.clone();
                corrupt[1][agg.out()] = json!(1.75);
                assert!(check_rows(&corrupt, agg, &expected).is_err());
                corrupt = rows.clone();
                corrupt[1][KEY] = json!(-2);
                assert!(check_rows(&corrupt, agg, &expected).is_err());
                corrupt = rows;
                corrupt[1]["extra"] = json!(0);
                assert!(check_rows(&corrupt, agg, &expected).is_err());
            }
        }

        #[test]
        fn denied_lock_preserves_the_existing_owner() {
            let path = std::env::temp_dir().join(format!(
                "computed-cost-lock-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let first = Lock::acquire(path.clone()).unwrap();
            assert!(Lock::acquire(path.clone()).is_err());
            assert!(path.exists());
            drop(first);
            assert!(!path.exists());
        }
    }
}
#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::run()
}
#[cfg(not(unix))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    Err("computed_result_cost requires unix".into())
}
