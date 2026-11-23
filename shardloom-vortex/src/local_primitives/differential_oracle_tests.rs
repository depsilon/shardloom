//! Offline seeded differential correctness tests against a pure-Rust exact
//! `i128` oracle (`E2E-VERIFY-01`, no external query engines, zero `unsafe`).

use super::*;
use crate::VortexAggregateOrderExpr;
use crate::relational_query::VortexRelationalSpillPolicy;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, Result, StatValue};
use shardloom_exec::compute_pool::CancellationToken;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use vortex::array::IntoArray as _;
use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex::array::dtype::{DType, FieldNames, Nullability, PType};
use vortex::array::validity::Validity;

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    const fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn gen_range(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        usize::try_from(self.next_u64() % u64::try_from(upper).expect("usize fits u64"))
            .expect("modulo upper fits usize")
    }
}

struct TempFileGuard(PathBuf);

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct TempDirGuard(PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default, Clone)]
struct OracleStats {
    rows: u64,
    non_null: u64,
    sum: i128,
    min: Option<i64>,
    max: Option<i64>,
    distinct: BTreeSet<i64>,
}

impl OracleStats {
    fn push(&mut self, value: Option<i64>) {
        self.rows += 1;
        if let Some(v) = value {
            self.non_null += 1;
            self.sum += i128::from(v);
            self.min = Some(self.min.map_or(v, |prev| prev.min(v)));
            self.max = Some(self.max.map_or(v, |prev| prev.max(v)));
            self.distinct.insert(v);
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn expected_sum_f64(&self) -> Option<f64> {
        (self.non_null > 0).then_some(self.sum as f64)
    }

    #[allow(clippy::cast_precision_loss)]
    fn expected_avg_f64(&self) -> Option<f64> {
        (self.non_null > 0).then(|| (self.sum as f64) / (self.non_null as f64))
    }
}

fn parse_values_json(summary: &str) -> serde_json::Value {
    let (_, payload) = summary
        .rsplit_once(" values=")
        .expect("simple aggregate values payload");
    serde_json::from_str(payload).expect("simple aggregate values json")
}

fn generate_mixed_value(rng: &mut XorShift64, index: usize) -> Option<i64> {
    if rng.gen_range(100) < 15 {
        return None;
    }
    let small = i64::try_from(rng.gen_range(19)).expect("small fits") - 9;
    let base = 1_i64 << 54;
    let wide = i64::MAX / 4;
    let value = match index % 6 {
        0 => base + small,
        1 => -base,
        2 => wide + small,
        3 => -wide,
        _ => i64::try_from(rng.gen_range(500)).expect("small range") - 250,
    };
    Some(value)
}

#[allow(clippy::cast_precision_loss)]
fn assert_float_close(
    actual: Option<f64>,
    expected: Option<f64>,
    expected_exact_i128: Option<i128>,
    label: &str,
) {
    match (actual, expected) {
        (None, None) => {}
        (Some(a), Some(e)) => {
            if let Some(exact) = expected_exact_i128
                && exact.unsigned_abs() <= (1_u128 << 53)
            {
                let exact_f64 = exact as f64;
                assert_eq!(a, exact_f64, "{label}: expected exact {exact_f64}, got {a}");
                return;
            }
            let scale = e.abs().max(1.0);
            assert!(
                (a - e).abs() <= 1e-12 * scale,
                "{label}: actual={a}, expected={e}"
            );
        }
        other => panic!("{label}: float presence mismatch: {other:?}"),
    }
}

fn json_optional_f64(value: &serde_json::Value) -> Option<f64> {
    if value.is_null() {
        None
    } else {
        Some(value.as_f64().expect("json number as f64"))
    }
}

fn json_optional_i64(value: &serde_json::Value) -> Option<i64> {
    if value.is_null() {
        None
    } else {
        Some(value.as_i64().expect("json number as i64"))
    }
}

fn assert_scalar_row_matches(values: &serde_json::Value, oracle: &OracleStats, label: &str) {
    assert_eq!(values["rows"].as_u64(), Some(oracle.rows), "{label}: rows");
    assert_eq!(
        values["non_null_val"].as_u64(),
        Some(oracle.non_null),
        "{label}: non_null_val"
    );
    assert_eq!(
        values["distinct_val"].as_u64(),
        Some(u64::try_from(oracle.distinct.len()).expect("len fits u64")),
        "{label}: distinct_val"
    );
    assert_eq!(
        json_optional_i64(&values["min_val"]),
        oracle.min,
        "{label}: min_val"
    );
    assert_eq!(
        json_optional_i64(&values["max_val"]),
        oracle.max,
        "{label}: max_val"
    );
    assert_float_close(
        json_optional_f64(&values["sum_val"]),
        oracle.expected_sum_f64(),
        (oracle.non_null > 0).then_some(oracle.sum),
        &format!("{label}: sum_val"),
    );
    assert_float_close(
        json_optional_f64(&values["avg_val"]),
        oracle.expected_avg_f64(),
        None,
        &format!("{label}: avg_val"),
    );
}

fn scalar_oracle_measures() -> Vec<VortexSimpleAggregateMeasure> {
    vec![
        VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
        VortexSimpleAggregateMeasure::new(
            "count",
            Some(ColumnRef::new("val").expect("val")),
            "non_null_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("val").expect("val")),
            "sum_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("val").expect("val")),
            "avg_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("val").expect("val")),
            "min_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "max",
            Some(ColumnRef::new("val").expect("val")),
            "max_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("val").expect("val")),
            "distinct_val".to_string(),
        ),
    ]
}

fn build_scalar_oracle_fixture(
    seed: u64,
    threshold: i64,
    path: &std::path::Path,
) -> (DatasetUri, OracleStats, OracleStats) {
    let mut rng = XorShift64::new(seed);
    let row_count = 160 + rng.gen_range(80);
    let mut keys_col = Vec::with_capacity(row_count);
    let mut vals_col = Vec::with_capacity(row_count);
    let mut unfiltered = OracleStats::default();
    let mut filtered = OracleStats::default();

    for idx in 0..row_count {
        let k = i64::try_from(rng.gen_range(200)).expect("k fits");
        // Ensure cancellation pairs occur symmetrically inside matching predicate slices too.
        let v = if idx % 10 < 2 {
            Some(if idx.is_multiple_of(2) {
                1_i64 << 54
            } else {
                -(1_i64 << 54)
            })
        } else {
            generate_mixed_value(&mut rng, idx)
        };
        let k_effective = if idx % 10 < 2 { threshold + 1 } else { k };
        unfiltered.push(v);
        if k_effective >= threshold {
            filtered.push(v);
        }
        keys_col.push(k_effective);
        vals_col.push(v);
    }

    let array = StructArray::try_new(
        FieldNames::from(["key", "val"]),
        vec![
            PrimitiveArray::from_iter(keys_col).into_array(),
            PrimitiveArray::from_option_iter(vals_col).into_array(),
        ],
        row_count,
        Validity::NonNullable,
    )
    .expect("struct")
    .into_array();

    super::tests::write_array(path, &array).expect("write vortex fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    (uri, unfiltered, filtered)
}

fn verify_scalar_oracle_seed(policy: VortexLocalPrimitiveExecutionPolicy, seed: u64) {
    let threshold = 100_i64;
    let path = super::tests::unique_vortex_path(&format!("diff-scalar-{seed}"));
    let _guard = TempFileGuard(path.clone());
    let (uri, unfiltered, filtered) = build_scalar_oracle_fixture(seed, threshold, &path);

    let request_all = VortexQueryPrimitiveRequest::simple_aggregate(
        uri.clone(),
        VortexSimpleAggregateRequest::new(scalar_oracle_measures()),
    );
    let report_all = execute_vortex_local_primitive_with_policy(&request_all, policy)
        .expect("unfiltered report");
    assert_eq!(
        report_all.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert!(!report_all.fallback_execution_allowed);
    assert!(!report_all.external_effects_executed);
    let payload_all = parse_values_json(report_all.result_summary.as_deref().expect("summary"));
    assert_scalar_row_matches(
        &payload_all["values"],
        &unfiltered,
        &format!("seed={seed} unfiltered"),
    );

    let mut request_filtered = VortexQueryPrimitiveRequest::simple_aggregate(
        uri,
        VortexSimpleAggregateRequest::new(scalar_oracle_measures()),
    );
    request_filtered.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("key").expect("key"),
        op: ComparisonOp::GtEq,
        value: StatValue::Int64(threshold),
    });
    let report_filtered = execute_vortex_local_primitive_with_policy(&request_filtered, policy)
        .expect("filtered report");
    assert_eq!(
        report_filtered.status,
        VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert!(!report_filtered.fallback_execution_allowed);
    assert!(!report_filtered.external_effects_executed);
    let payload_filtered =
        parse_values_json(report_filtered.result_summary.as_deref().expect("summary"));
    assert_scalar_row_matches(
        &payload_filtered["values"],
        &filtered,
        &format!("seed={seed} filtered"),
    );
}

#[test]
fn differential_scalar_and_filtered_aggregates_match_exact_i128_oracle() {
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy");
    for seed in [1_u64, 7, 42, 1337, 9001, 20_261_122] {
        verify_scalar_oracle_seed(policy, seed);
    }
}

fn zipf_group_name(rng: &mut XorShift64) -> String {
    let roll = rng.gen_range(100);
    if roll < 45 {
        format!("g{}", rng.gen_range(2))
    } else if roll < 75 {
        format!("g{}", 2 + rng.gen_range(3))
    } else if roll < 92 {
        format!("g{}", 5 + rng.gen_range(7))
    } else {
        format!("g{}", 12 + rng.gen_range(8))
    }
}

fn build_grouped_oracle_fixture(
    seed: u64,
    path: &std::path::Path,
) -> (DatasetUri, BTreeMap<String, OracleStats>) {
    let mut rng = XorShift64::new(seed);
    let mut groups_col: Vec<String> = Vec::with_capacity(320);
    let mut flags_col: Vec<i64> = Vec::with_capacity(320);
    let mut vals_col: Vec<Option<i64>> = Vec::with_capacity(320);
    let mut oracle_groups: BTreeMap<String, OracleStats> = BTreeMap::new();

    // Inject two guaranteed equal-row-count tie groups passing flag > 0.
    for tie_grp in ["tie_b", "tie_a"] {
        for idx in 0..12 {
            let val = if idx % 2 == 0 {
                Some((1_i64 << 54) + 5)
            } else {
                Some(-(1_i64 << 54))
            };
            groups_col.push(tie_grp.to_string());
            flags_col.push(1);
            vals_col.push(val);
            oracle_groups
                .entry(tie_grp.to_string())
                .or_default()
                .push(val);
        }
    }

    for idx in 0..276_usize {
        let grp = zipf_group_name(&mut rng);
        let flag = i64::from(rng.gen_range(5) != 0);
        let val = if flag == 1 && idx.is_multiple_of(6) {
            Some(1_i64 << 54)
        } else if flag == 1 && idx % 6 == 1 {
            Some(-(1_i64 << 54))
        } else {
            generate_mixed_value(&mut rng, idx)
        };
        if flag > 0 {
            oracle_groups.entry(grp.clone()).or_default().push(val);
        }
        groups_col.push(grp);
        flags_col.push(flag);
        vals_col.push(val);
    }

    let row_count = groups_col.len();
    let array = StructArray::try_new(
        FieldNames::from(["grp", "flag", "val"]),
        vec![
            VarBinViewArray::from_iter_str(groups_col.iter().map(String::as_str)).into_array(),
            PrimitiveArray::from_iter(flags_col).into_array(),
            PrimitiveArray::from_option_iter(vals_col).into_array(),
        ],
        row_count,
        Validity::NonNullable,
    )
    .expect("struct")
    .into_array();

    super::tests::write_array(path, &array).expect("write grouped fixture");
    let uri = DatasetUri::new(path.display().to_string()).expect("uri");
    (uri, oracle_groups)
}

fn verify_grouped_oracle_seed(
    policy: VortexLocalPrimitiveExecutionPolicy,
    seed: u64,
    limit: usize,
) {
    let path = super::tests::unique_vortex_path(&format!("diff-grouped-{seed}"));
    let _guard = TempFileGuard(path.clone());
    let (uri, oracle_groups) = build_grouped_oracle_fixture(seed, &path);

    let measures = vec![
        VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
        VortexSimpleAggregateMeasure::new(
            "sum",
            Some(ColumnRef::new("val").expect("val")),
            "sum_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("val").expect("val")),
            "avg_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("val").expect("val")),
            "min_val".to_string(),
        ),
        VortexSimpleAggregateMeasure::new(
            "max",
            Some(ColumnRef::new("val").expect("val")),
            "max_val".to_string(),
        ),
    ];

    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        uri,
        VortexSimpleAggregateRequest::grouped(vec![ColumnRef::new("grp").expect("grp")], measures)
            .with_order_by(vec![
                VortexAggregateOrderExpr::new("rows", true),
                VortexAggregateOrderExpr::new("grp", false),
            ]),
    )
    .with_source_order_limit(limit);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("flag").expect("flag"),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(0),
    });

    let report = execute_vortex_local_primitive_with_policy(&request, policy).expect("report");
    assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.external_effects_executed);

    let payload = parse_values_json(report.result_summary.as_deref().expect("summary"));
    let actual_rows = payload["values"].as_array().expect("grouped rows array");

    let mut expected: Vec<(String, OracleStats)> = oracle_groups.into_iter().collect();
    expected.sort_by(|(la, sa), (lb, sb)| sb.rows.cmp(&sa.rows).then_with(|| la.cmp(lb)));
    expected.truncate(limit);

    assert_eq!(actual_rows.len(), expected.len(), "seed={seed}: top-k len");
    for (idx, (actual, (exp_grp, exp_stats))) in actual_rows.iter().zip(expected.iter()).enumerate()
    {
        assert_eq!(
            actual["grp"].as_str(),
            Some(exp_grp.as_str()),
            "seed={seed} rank={idx} grp"
        );
        assert_eq!(
            actual["rows"].as_u64(),
            Some(exp_stats.rows),
            "seed={seed} rank={idx} rows"
        );
        assert_eq!(
            json_optional_i64(&actual["min_val"]),
            exp_stats.min,
            "seed={seed} rank={idx} min_val"
        );
        assert_eq!(
            json_optional_i64(&actual["max_val"]),
            exp_stats.max,
            "seed={seed} rank={idx} max_val"
        );
        assert_float_close(
            json_optional_f64(&actual["sum_val"]),
            exp_stats.expected_sum_f64(),
            (exp_stats.non_null > 0).then_some(exp_stats.sum),
            &format!("seed={seed} rank={idx} sum_val"),
        );
        assert_float_close(
            json_optional_f64(&actual["avg_val"]),
            exp_stats.expected_avg_f64(),
            None,
            &format!("seed={seed} rank={idx} avg_val"),
        );
    }
}

#[test]
fn differential_zipf_grouped_aggregates_with_ties_and_limit_match_oracle() {
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).expect("policy");
    for seed in [11_u64, 99, 2025, 88_888] {
        verify_grouped_oracle_seed(policy, seed, 7);
    }
}

#[derive(Debug, Clone)]
struct RelationalGroupRow {
    grp: i64,
    count_star: u64,
    count_val: u64,
    sum_val: Option<f64>,
    avg_val: Option<f64>,
    min_val: Option<i64>,
    max_val: Option<i64>,
    count_distinct_val: u64,
}

fn relational_spec() -> native_relational_aggregate::Spec {
    use native_relational_aggregate::{Function, Measure, Spec};

    let grp_dtype = DType::Primitive(PType::I64, Nullability::NonNullable);
    let val_dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    let count_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let float_dtype = DType::Primitive(PType::F64, Nullability::Nullable);

    let make_measure = |function: Function, column: Option<&str>, dtype: DType| {
        let distinct_fields = if matches!(function, Function::CountDistinct) {
            vec![
                ("group".into(), count_dtype.clone()),
                ("value".into(), val_dtype.clone()),
            ]
        } else {
            vec![]
        };
        let distinct_names = if distinct_fields.is_empty() {
            vec![]
        } else {
            vec!["group".into(), "value".into()]
        };
        Measure {
            function,
            column: column.map(str::to_string),
            secondary_column: None,
            quantile_level: None,
            separator: None,
            filter_column: None,
            selection: None,
            dtype,
            input_ptype: column.map(|_| PType::I64),
            decimal_source: None,
            decimal_output: None,
            distinct_fields,
            distinct_names,
        }
    };

    let measures = vec![
        make_measure(Function::Count, None, count_dtype.clone()),
        make_measure(Function::Count, Some("val"), count_dtype.clone()),
        make_measure(Function::Sum, Some("val"), float_dtype.clone()),
        make_measure(Function::Avg, Some("val"), float_dtype.clone()),
        make_measure(Function::Min, Some("val"), val_dtype.clone()),
        make_measure(Function::Max, Some("val"), val_dtype.clone()),
        make_measure(Function::CountDistinct, Some("val"), count_dtype.clone()),
    ];
    let fields = vec![
        ("grp".into(), grp_dtype.clone()),
        ("count_star".into(), count_dtype.clone()),
        ("count_val".into(), count_dtype.clone()),
        ("sum_val".into(), float_dtype.clone()),
        ("avg_val".into(), float_dtype),
        (
            "min_val".into(),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ),
        (
            "max_val".into(),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ),
        ("count_distinct_val".into(), count_dtype),
    ];

    Spec {
        fields,
        groups: vec![("grp".into(), grp_dtype)],
        group_names: vec!["grp".into()],
        measures,
    }
}

fn cell_to_i64(cell: native_relational_keys::Cell) -> Option<i64> {
    match cell {
        native_relational_keys::Cell::Null => None,
        native_relational_keys::Cell::NegativeInteger(v) => Some(v),
        native_relational_keys::Cell::NonnegativeInteger(v) => {
            Some(i64::try_from(v).expect("nonnegative fits i64"))
        }
        other => panic!("unexpected integer cell: {other:?}"),
    }
}

fn cell_to_u64(cell: native_relational_keys::Cell) -> u64 {
    match cell {
        native_relational_keys::Cell::NonnegativeInteger(v) => v,
        other => panic!("unexpected u64 cell: {other:?}"),
    }
}

fn cell_to_opt_f64(cell: native_relational_keys::Cell) -> Option<f64> {
    match cell {
        native_relational_keys::Cell::Null => None,
        native_relational_keys::Cell::Float(bits) => Some(f64::from_bits(bits)),
        other => panic!("unexpected float cell: {other:?}"),
    }
}

fn decode_relational_batches(
    batches: &[vortex::array::ArrayRef],
    context: &crate::resident_session::NativeExecutionContext<'_>,
) -> Result<Vec<RelationalGroupRow>> {
    use native_relational_expression::keys;

    let mut out = Vec::new();
    for batch in batches {
        let grp_col = keys(&logical_field_from_native_array(batch, "grp")?, context)?;
        let cstar_col = keys(
            &logical_field_from_native_array(batch, "count_star")?,
            context,
        )?;
        let cval_col = keys(
            &logical_field_from_native_array(batch, "count_val")?,
            context,
        )?;
        let sum_col = keys(&logical_field_from_native_array(batch, "sum_val")?, context)?;
        let avg_col = keys(&logical_field_from_native_array(batch, "avg_val")?, context)?;
        let min_col = keys(&logical_field_from_native_array(batch, "min_val")?, context)?;
        let max_col = keys(&logical_field_from_native_array(batch, "max_val")?, context)?;
        let cdist_col = keys(
            &logical_field_from_native_array(batch, "count_distinct_val")?,
            context,
        )?;
        for row in 0..batch.len() {
            out.push(RelationalGroupRow {
                grp: cell_to_i64(grp_col.raw_cell(row)?).expect("non-null group key"),
                count_star: cell_to_u64(cstar_col.raw_cell(row)?),
                count_val: cell_to_u64(cval_col.raw_cell(row)?),
                sum_val: cell_to_opt_f64(sum_col.raw_cell(row)?),
                avg_val: cell_to_opt_f64(avg_col.raw_cell(row)?),
                min_val: cell_to_i64(min_col.raw_cell(row)?),
                max_val: cell_to_i64(max_col.raw_cell(row)?),
                count_distinct_val: cell_to_u64(cdist_col.raw_cell(row)?),
            });
        }
    }
    out.sort_by_key(|r| r.grp);
    Ok(out)
}

fn build_relational_oracle_chunks(
    chunk_count: usize,
    chunk_rows: usize,
) -> (Vec<vortex::array::ArrayRef>, BTreeMap<i64, OracleStats>) {
    let mut rng = XorShift64::new(0xDEC0_DE55_1234_5678);
    let mut chunks = Vec::with_capacity(chunk_count);
    let mut oracle: BTreeMap<i64, OracleStats> = BTreeMap::new();

    for chunk_idx in 0..chunk_count {
        let mut grp_vec = Vec::with_capacity(chunk_rows);
        let mut val_vec = Vec::with_capacity(chunk_rows);
        for row_idx in 0..chunk_rows {
            let global_idx = chunk_idx * chunk_rows + row_idx;
            let grp = match rng.gen_range(100) {
                0..=39 => i64::try_from(rng.gen_range(3)).expect("small"),
                40..=79 => i64::try_from(3 + rng.gen_range(10)).expect("small"),
                _ => i64::try_from(13 + rng.gen_range(37)).expect("small"),
            };
            let val = if global_idx.is_multiple_of(8) {
                Some((1_i64 << 54) + grp)
            } else if global_idx % 8 == 1 {
                Some(-(1_i64 << 54))
            } else {
                generate_mixed_value(&mut rng, global_idx)
            };
            oracle.entry(grp).or_default().push(val);
            grp_vec.push(grp);
            val_vec.push(val);
        }
        chunks.push(
            StructArray::try_new(
                FieldNames::from(["grp", "val"]),
                vec![
                    PrimitiveArray::from_iter(grp_vec).into_array(),
                    PrimitiveArray::from_option_iter(val_vec).into_array(),
                ],
                chunk_rows,
                Validity::NonNullable,
            )
            .expect("chunk struct")
            .into_array(),
        );
    }
    (chunks, oracle)
}

fn assert_relational_oracle_matches(
    unspilled_rows: &[RelationalGroupRow],
    spilled_rows: &[RelationalGroupRow],
    oracle: &BTreeMap<i64, OracleStats>,
) {
    assert_eq!(unspilled_rows.len(), oracle.len());
    assert_eq!(spilled_rows.len(), oracle.len());
    for ((u, s), (exp_grp, exp_stats)) in unspilled_rows
        .iter()
        .zip(spilled_rows.iter())
        .zip(oracle.iter())
    {
        assert_eq!(u.grp, *exp_grp);
        assert_eq!(s.grp, *exp_grp);
        assert_eq!(u.count_star, exp_stats.rows);
        assert_eq!(s.count_star, exp_stats.rows);
        assert_eq!(u.count_val, exp_stats.non_null);
        assert_eq!(s.count_val, exp_stats.non_null);
        assert_eq!(u.min_val, exp_stats.min);
        assert_eq!(s.min_val, exp_stats.min);
        assert_eq!(u.max_val, exp_stats.max);
        assert_eq!(s.max_val, exp_stats.max);
        let exp_distinct = u64::try_from(exp_stats.distinct.len()).expect("len fits u64");
        assert_eq!(u.count_distinct_val, exp_distinct);
        assert_eq!(s.count_distinct_val, exp_distinct);
        assert_float_close(
            u.sum_val,
            exp_stats.expected_sum_f64(),
            (exp_stats.non_null > 0).then_some(exp_stats.sum),
            &format!("unspilled grp={exp_grp} sum"),
        );
        assert_float_close(
            s.sum_val,
            exp_stats.expected_sum_f64(),
            (exp_stats.non_null > 0).then_some(exp_stats.sum),
            &format!("spilled grp={exp_grp} sum"),
        );
        assert_float_close(
            u.avg_val,
            exp_stats.expected_avg_f64(),
            None,
            &format!("unspilled grp={exp_grp} avg"),
        );
        assert_float_close(
            s.avg_val,
            exp_stats.expected_avg_f64(),
            None,
            &format!("spilled grp={exp_grp} avg"),
        );
    }
}

#[test]
fn differential_relational_aggregate_unspilled_vs_forced_spill_matches_oracle() {
    let chunk_count = 24_usize;
    let chunk_rows = 500_usize;
    let (chunks, oracle) = build_relational_oracle_chunks(chunk_count, chunk_rows);

    let spec = relational_spec();
    let input_fields = vec![
        (
            "grp".into(),
            DType::Primitive(PType::I64, Nullability::NonNullable),
        ),
        (
            "val".into(),
            DType::Primitive(PType::I64, Nullability::Nullable),
        ),
    ];

    let workspace = unique_temp_spill_dir("diff-rel-agg-spill");
    let _dir_guard = TempDirGuard(workspace.clone());

    let session = ResidentVortexSession::new(64 << 20, 1).expect("session");
    let memory = session.memory().clone();
    let (unspilled_rows, spilled_rows) = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut aggregate =
                native_relational_aggregate::Aggregate::new(&spec, context.memory())?;
            for chunk in &chunks {
                aggregate.consume(chunk, context, 256)?;
            }
            let mut unspilled_batches = Vec::new();
            aggregate.finish(context, 256, &mut |batch| {
                unspilled_batches.push(batch);
                Ok(())
            })?;
            drop(aggregate);
            let unspilled_decoded = decode_relational_batches(&unspilled_batches, context)?;
            drop(unspilled_batches);

            let spill_policy = VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20)?;
            let spill_state = native_relational_spill::State::new(&spill_policy, context)?;
            let mut spilled_batches = Vec::new();
            let agg_report = native_relational_aggregate_spill::run(
                &spec,
                &input_fields,
                &spill_state,
                context,
                256,
                |emit| {
                    for chunk in &chunks {
                        emit(chunk.clone())?;
                    }
                    Ok(())
                },
                &mut |batch| {
                    spilled_batches.push(batch);
                    Ok(())
                },
            )?;
            assert_eq!(
                agg_report.input_rows,
                u64::try_from(chunk_count * chunk_rows).expect("rows")
            );
            let spill_report = spill_state.finish()?;
            assert!(
                spill_report.runs_written >= 1,
                "expected at least one spilled run, got {}",
                spill_report.runs_written
            );
            assert!(spill_report.owned_cleanup_completed);
            let spilled_decoded = decode_relational_batches(&spilled_batches, context)?;
            drop(spilled_batches);
            Ok((unspilled_decoded, spilled_decoded))
        })
        .expect("relational aggregate differential run");
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    assert_relational_oracle_matches(&unspilled_rows, &spilled_rows, &oracle);
}

fn unique_temp_spill_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("shardloom-{name}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp spill dir");
    dir
}
