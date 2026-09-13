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
                Primitive, PrimitiveArray, Struct, StructArray, VarBin, VarBinViewArray,
                struct_::StructArrayExt as _, varbin::VarBinArraySlotsExt as _,
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

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum KeyKind {
        Integer,
        Utf8,
    }

    impl KeyKind {
        fn parse(value: Option<&str>, family: AggregateFamily) -> Result<Self, Error> {
            match value {
                None | Some("integer") => Ok(Self::Integer),
                Some("utf8") if family == AggregateFamily::Count => Ok(Self::Utf8),
                Some("utf8") => Err("UTF8 keys require --aggregate-family count".into()),
                Some(_) => Err("key kind must be integer or utf8".into()),
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::Integer => "integer",
                Self::Utf8 => "utf8",
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum AggregateFamily {
        CountDistinct,
        Count,
    }

    impl AggregateFamily {
        fn parse(value: Option<&str>) -> Result<Self, Error> {
            match value {
                None | Some("count_distinct") => Ok(Self::CountDistinct),
                Some("count") => Ok(Self::Count),
                Some(_) => Err("aggregate family must be count_distinct or count".into()),
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::CountDistinct => "count_distinct",
                Self::Count => "count",
            }
        }

        fn output_column(self) -> &'static str {
            match self {
                Self::CountDistinct => "distinct_packages",
                Self::Count => "packages",
            }
        }

        fn query_summary(self) -> &'static str {
            match self {
                Self::CountDistinct => {
                    "GROUP BY delivery_zone; COUNT DISTINCT package_identifier AS distinct_packages; ORDER BY distinct_packages DESC, delivery_zone ASC; LIMIT K"
                }
                Self::Count => {
                    "GROUP BY delivery_zone; COUNT(*) AS packages; ORDER BY packages DESC; native deterministic delivery_zone ASC ties; LIMIT K"
                }
            }
        }

        fn oracle_description(self) -> &'static str {
            match self {
                Self::CountDistinct => {
                    "independent standard-library BTreeMap/BTreeSet over generated input pairs, exact count-descending/key-ascending sort; all output rows checked on every call"
                }
                Self::Count => {
                    "independent standard-library BTreeMap counting every generated input row, exact count-descending/key-ascending sort; all output rows checked on every call"
                }
            }
        }
    }

    struct Options {
        workspace: PathBuf,
        revision: String,
        build_label: String,
        family: AggregateFamily,
        key_kind: KeyKind,
    }

    impl Options {
        fn parse() -> Result<Self, Error> {
            let mut workspace = None;
            let mut revision = None;
            let mut build_label = None;
            let mut family = None;
            let mut key_kind = None;
            let mut args = std::env::args().skip(1);
            while let Some(flag) = args.next() {
                let value = args.next().ok_or("each option requires a value")?;
                match flag.as_str() {
                    "--workspace" => workspace = Some(PathBuf::from(value)),
                    "--source-revision" => revision = Some(value),
                    "--build-label" => build_label = Some(value),
                    "--aggregate-family" => {
                        if family.replace(value).is_some() {
                            return Err("aggregate family must be supplied at most once".into());
                        }
                    }
                    "--key-kind" => {
                        if key_kind.replace(value).is_some() {
                            return Err("key kind must be supplied at most once".into());
                        }
                    }
                    _ => return Err(format!("unknown option: {flag}").into()),
                }
            }
            let workspace = workspace.ok_or(
                "usage: owned_aggregate_cost --workspace EXISTING_LOCALDATA_DIRECTORY --source-revision COMMIT --build-label LABEL [--aggregate-family count_distinct|count] [--key-kind integer|utf8]",
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
            let family = AggregateFamily::parse(family.as_deref())?;
            Ok(Self {
                workspace,
                revision,
                build_label,
                family,
                key_kind: KeyKind::parse(key_kind.as_deref(), family)?,
            })
        }
    }

    struct Fixture {
        family: AggregateFamily,
        key_kind: KeyKind,
        path: PathBuf,
        expected: ExpectedRows,
        sha256: String,
        bytes: u64,
        physical_splits: Vec<(u64, u64)>,
    }

    enum ExpectedRows {
        Integer(Vec<(i64, u64)>),
        Utf8(Vec<(String, u64)>),
    }

    impl ExpectedRows {
        fn prefix(&self, limit: usize) -> ExpectedSlice<'_> {
            match self {
                Self::Integer(rows) => ExpectedSlice::Integer(&rows[..limit]),
                Self::Utf8(rows) => ExpectedSlice::Utf8(&rows[..limit]),
            }
        }
    }

    #[derive(Clone, Copy)]
    enum ExpectedSlice<'a> {
        Integer(&'a [(i64, u64)]),
        Utf8(&'a [(String, u64)]),
    }

    impl ExpectedSlice<'_> {
        fn len(self) -> usize {
            match self {
                Self::Integer(rows) => rows.len(),
                Self::Utf8(rows) => rows.len(),
            }
        }

        fn key_kind(self) -> KeyKind {
            match self {
                Self::Integer(_) => KeyKind::Integer,
                Self::Utf8(_) => KeyKind::Utf8,
            }
        }
    }

    enum IndependentOracle {
        CountDistinct(BTreeMap<i64, BTreeSet<u64>>),
        Count(BTreeMap<i64, u64>),
    }

    impl IndependentOracle {
        fn new(family: AggregateFamily) -> Self {
            match family {
                AggregateFamily::CountDistinct => Self::CountDistinct(BTreeMap::new()),
                AggregateFamily::Count => Self::Count(BTreeMap::new()),
            }
        }

        fn record(&mut self, key: i64, value: u64) -> Result<(), Error> {
            match self {
                Self::CountDistinct(groups) => {
                    groups.entry(key).or_default().insert(value);
                }
                Self::Count(groups) => {
                    let count = groups.entry(key).or_default();
                    *count = count.checked_add(1).ok_or("oracle count overflow")?;
                }
            }
            Ok(())
        }

        fn finish(self) -> Result<Vec<(i64, u64)>, Error> {
            let mut expected = match self {
                Self::CountDistinct(groups) => groups
                    .into_iter()
                    .map(|(key, values)| Ok((key, u64::try_from(values.len())?)))
                    .collect::<Result<Vec<_>, Error>>()?,
                Self::Count(groups) => groups.into_iter().collect(),
            };
            expected.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
            Ok(expected)
        }
    }

    fn fixture_pair(row: usize, family: AggregateFamily) -> Result<(i64, u64), Error> {
        let mut group = row % GROUPS;
        let pass = row / GROUPS;
        // Preserve the original distinct fixture. The COUNT profile redistributes
        // the final four passes between adjacent keys to exercise both rank and
        // ties: all groups remain present with either four or twelve rows.
        if family == AggregateFamily::Count && pass >= 4 && group % 2 == 1 {
            group -= 1;
        }
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
        Ok((key, value))
    }

    fn fixture_text(row: usize) -> String {
        let mut group = row % GROUPS;
        if row / GROUPS >= 4 && group % 2 == 1 {
            group -= 1;
        }
        match group {
            0 => String::new(),
            1 => "\0".into(),
            2 => "é".into(),
            3 => "e\u{301}".into(),
            _ => format!(
                "delivery-zone/東京/κόσμος/long-shared-prefix/escaped=\"\\\n\0/{group:08x}/{}",
                "abcdefghijklmnop".repeat(4),
            ),
        }
    }

    fn record_text(oracle: &mut BTreeMap<String, u64>, key: &str) -> Result<(), Error> {
        let count = oracle.entry(key.to_owned()).or_default();
        *count = count.checked_add(1).ok_or("UTF8 oracle count overflow")?;
        Ok(())
    }

    fn finish_text(oracle: BTreeMap<String, u64>) -> Vec<(String, u64)> {
        let mut expected = oracle.into_iter().collect::<Vec<_>>();
        expected.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        expected
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
    fn fixture(
        workspace: &Path,
        family: AggregateFamily,
        key_kind: KeyKind,
    ) -> Result<Fixture, Error> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = workspace.join(format!(
            "owned-aggregate-{}-{stamp}.vortex",
            std::process::id()
        ));
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut oracle = IndependentOracle::new(family);
        let mut text_oracle = BTreeMap::new();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let dtype = DType::struct_(
            [
                (
                    KEY,
                    match key_kind {
                        KeyKind::Integer => DType::Primitive(PType::I64, Nullability::NonNullable),
                        KeyKind::Utf8 => DType::Utf8(Nullability::NonNullable),
                    },
                ),
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
            let mut text_keys = Vec::new();
            if key_kind == KeyKind::Utf8 {
                text_keys.reserve_exact(BATCH_ROWS);
            }
            let mut values = Vec::with_capacity(BATCH_ROWS);
            for row in start..start + BATCH_ROWS {
                let (key, value) = fixture_pair(row, family)?;
                match key_kind {
                    KeyKind::Integer => {
                        keys.push(key);
                        oracle.record(key, value)?;
                    }
                    KeyKind::Utf8 => {
                        let key = fixture_text(row);
                        record_text(&mut text_oracle, &key)?;
                        text_keys.push(key);
                    }
                }
                values.push(value);
            }
            let keys = match key_kind {
                KeyKind::Integer => PrimitiveArray::new(keys, Validity::NonNullable).into_array(),
                KeyKind::Utf8 => {
                    VarBinViewArray::from_iter_str(text_keys.iter().map(String::as_str))
                        .into_array()
                }
            };
            writer.push(
                StructArray::try_new(
                    [KEY, VALUE].into(),
                    vec![
                        keys,
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
        let expected = match key_kind {
            KeyKind::Integer => ExpectedRows::Integer(oracle.finish()?),
            KeyKind::Utf8 => ExpectedRows::Utf8(finish_text(text_oracle)),
        };
        let groups = match &expected {
            ExpectedRows::Integer(rows) => rows.len(),
            ExpectedRows::Utf8(rows) => rows.len(),
        };
        if groups != GROUPS {
            return Err("fixture did not produce the declared independent groups".into());
        }
        Ok(Fixture {
            family,
            key_kind,
            sha256: digest(&path)?,
            bytes: fs::metadata(&path)?.len(),
            path,
            expected,
            physical_splits,
        })
    }

    fn query(
        path: &Path,
        limit: usize,
        family: AggregateFamily,
    ) -> Result<VortexQueryPrimitiveRequest, Error> {
        let mut order = vec![VortexAggregateOrderExpr::new(family.output_column(), true)];
        if family == AggregateFamily::CountDistinct {
            order.push(VortexAggregateOrderExpr::new(KEY, false));
        }
        Ok(VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.to_string_lossy().into_owned())?,
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new(KEY)?],
                vec![VortexSimpleAggregateMeasure::new(
                    family.name(),
                    if family == AggregateFamily::CountDistinct {
                        Some(ColumnRef::new(VALUE)?)
                    } else {
                        None
                    },
                    family.output_column().into(),
                )],
            )
            .with_order_by(order),
        )
        .with_source_order_limit(limit))
    }

    #[allow(clippy::too_many_lines)] // Keep complete execution, representation and worker evidence in one check.
    fn inspect_report(
        result: &ExecutedVortexAggregate,
        expected: ExpectedSlice<'_>,
        owned: bool,
        family: AggregateFamily,
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
        let materialized = if owned
            || (family == AggregateFamily::Count && expected.key_kind() == KeyKind::Integer)
        {
            0
        } else if expected.key_kind() == KeyKind::Utf8 {
            work["materialized_group_value_count"]
                .as_u64()
                .ok_or("UTF8 materialization evidence is absent")?
        } else {
            u64::try_from(expected.len())?
        };
        if work["materialized_group_value_count"] != materialized
            || work["candidate_groups"] != u64::try_from(GROUPS)?
            || work["rows"] != u64::try_from(expected.len())?
            || work["aggregate_workers_rows"] != u64::try_from(ROWS)?
            || work["aggregate_workers_outstanding_chunks"] != 0
            || work["aggregate_workers_submitted_chunks"]
                .as_u64()
                .is_none_or(|count| count == 0)
            || work["aggregate_workers_submitted_chunks"]
                != work["aggregate_workers_completed_chunks"]
        {
            return Err("representation or complete-worker evidence changed".into());
        }
        match family {
            AggregateFamily::CountDistinct
                if work["aggregate_workers_partition_native_handoffs"] != 0 =>
            {
                return Err(
                    "distinct worker screen unexpectedly handed off native partitions".into(),
                );
            }
            AggregateFamily::Count
                if expected.key_kind() == KeyKind::Integer
                    && !work["aggregate_workers_partition_native_handoffs"].is_null() =>
            {
                return Err(
                    "integer COUNT screen unexpectedly used a partition handoff route".into(),
                );
            }
            _ => {}
        }
        if owned {
            if !work["values"].is_null() {
                return Err("owned output rendered JSON rows".into());
            }
            if let ExpectedSlice::Utf8(rows) = expected {
                let (text_bytes, native_bytes) = utf8_bytes(rows)?;
                if work["aggregate_result_boundary"]
                    != "owned_native_utf8_columns;no_JSON_or_StatValue_output_rows"
                    || work["aggregate_result_utf8_bytes"] != text_bytes
                    || work["aggregate_result_native_buffer_bytes"] != native_bytes
                {
                    return Err(
                        "owned UTF8 output boundary or exact buffer evidence differs".into(),
                    );
                }
            }
        } else {
            match expected {
                ExpectedSlice::Integer(rows) => inspect_json_rows(&work["values"], rows, family)?,
                ExpectedSlice::Utf8(rows) => inspect_utf8_json_rows(&work["values"], rows, family)?,
            }
        }
        Ok(json!({
            "aggregate_family": family.name(), "aggregate_output_column": family.output_column(),
            "key_kind": expected.key_kind().name(),
            "materialized_group_value_count": materialized,
            "complete_result_rows_verified": expected.len(),
            "group_output_strategy": work["group_output_strategy"],
            "aggregate_workers_rows": work["aggregate_workers_rows"],
            "aggregate_workers_partition_native_handoffs": work["aggregate_workers_partition_native_handoffs"],
            "result_summary_utf8_bytes": text.len(),
            "aggregate_result_boundary": work["aggregate_result_boundary"],
            "aggregate_result_utf8_bytes": work["aggregate_result_utf8_bytes"],
            "aggregate_result_native_buffer_bytes": work["aggregate_result_native_buffer_bytes"],
            "candidate_group_scope": work["candidate_group_scope"],
            "input_arrays_read": report.arrays_read_count,
            "max_input_chunk_rows": report.max_chunk_rows,
            "worker_cpu_ceiling": work["aggregate_workers_cpu_ceiling"],
            "compute_threads": work["aggregate_workers_compute_threads"],
            "provider_background_workers": work["aggregate_workers_provider_background_workers"],
            "worker_completed_chunks": work["aggregate_workers_completed_chunks"],
            "worker_peak_active_workers": work["aggregate_workers_peak_active_workers"],
            "worker_busy_elapsed_nanos": work["aggregate_workers_worker_busy_elapsed_nanos"],
            "worker_evidence_scope": "completed input jobs and all accepted rows; created thread count does not prove multiple distinct workers executed",
            "native_io_certified": true, "complete_values_verified": true,
        }))
    }

    fn inspect_json_rows(
        rows: &Value,
        expected: &[(i64, u64)],
        family: AggregateFamily,
    ) -> Result<(), Error> {
        let rows = rows.as_array().ok_or("JSON rows are absent")?;
        if rows.len() != expected.len() {
            return Err("JSON row count changed".into());
        }
        for (row, &(key, count)) in rows.iter().zip(expected) {
            if row.as_object().is_none_or(|fields| fields.len() != 2)
                || row[KEY].as_i64() != Some(key)
                || row[family.output_column()].as_u64() != Some(count)
            {
                return Err("complete JSON result differs from independent oracle".into());
            }
        }
        Ok(())
    }

    fn inspect_arrays(
        result: &OwnedVortexResultBatch,
        expected: ExpectedSlice<'_>,
        family: AggregateFamily,
    ) -> Result<(), Error> {
        if result.arrays().len() != 1 || result.row_count() != u64::try_from(expected.len())? {
            return Err("owned result shape changed".into());
        }
        match expected {
            ExpectedSlice::Integer(rows) => inspect_array(&result.arrays()[0], rows, family),
            ExpectedSlice::Utf8(rows) => {
                inspect_utf8_array(&result.arrays()[0], rows, family)?;
                if result.logical_buffer_bytes() != utf8_bytes(rows)?.1 {
                    return Err(
                        "owned UTF8 logical buffer bytes differ from complete oracle".into(),
                    );
                }
                Ok(())
            }
        }
    }

    fn inspect_array(
        array: &vortex::array::ArrayRef,
        expected: &[(i64, u64)],
        family: AggregateFamily,
    ) -> Result<(), Error> {
        let expected_dtype = DType::struct_(
            [
                (KEY, DType::Primitive(PType::I64, Nullability::NonNullable)),
                (
                    family.output_column(),
                    DType::Primitive(PType::U64, Nullability::NonNullable),
                ),
            ],
            Nullability::NonNullable,
        );
        if array.dtype() != &expected_dtype || array.len() != expected.len() {
            return Err("owned result dtype, columns or row count changed".into());
        }
        let fields = array
            .as_opt::<Struct>()
            .ok_or("owned output is not a physical Struct")?;
        let keys = fields.unmasked_field_by_name(KEY)?;
        let counts = fields.unmasked_field_by_name(family.output_column())?;
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

    fn utf8_bytes(expected: &[(String, u64)]) -> Result<(u64, u64), Error> {
        let text = expected
            .iter()
            .try_fold(0_u64, |total, (key, _)| -> Result<u64, Error> {
                total
                    .checked_add(u64::try_from(key.len())?)
                    .ok_or_else(|| "UTF8 oracle byte overflow".into())
            })?;
        let native = u64::try_from(expected.len())?
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(8))
            .and_then(|bytes| bytes.checked_add(text))
            .ok_or("UTF8 native oracle byte overflow")?;
        Ok((text, native))
    }

    fn inspect_utf8_json_rows(
        rows: &Value,
        expected: &[(String, u64)],
        family: AggregateFamily,
    ) -> Result<(), Error> {
        let rows = rows.as_array().ok_or("UTF8 JSON rows are absent")?;
        if family != AggregateFamily::Count || rows.len() != expected.len() {
            return Err("UTF8 JSON family or row count changed".into());
        }
        for (row, (key, count)) in rows.iter().zip(expected) {
            if row.as_object().is_none_or(|fields| fields.len() != 2)
                || row[KEY].as_str() != Some(key.as_str())
                || row[family.output_column()].as_u64() != Some(*count)
            {
                return Err("complete UTF8 JSON result differs from independent oracle".into());
            }
        }
        Ok(())
    }

    fn inspect_utf8_array(
        array: &vortex::array::ArrayRef,
        expected: &[(String, u64)],
        family: AggregateFamily,
    ) -> Result<(), Error> {
        let expected_dtype = DType::struct_(
            [
                (KEY, DType::Utf8(Nullability::NonNullable)),
                (
                    family.output_column(),
                    DType::Primitive(PType::U64, Nullability::NonNullable),
                ),
            ],
            Nullability::NonNullable,
        );
        if family != AggregateFamily::Count
            || array.dtype() != &expected_dtype
            || array.len() != expected.len()
        {
            return Err("owned UTF8 family, dtype, columns or row count changed".into());
        }
        let fields = array
            .as_opt::<Struct>()
            .ok_or("owned UTF8 output is not a physical Struct")?;
        let keys = fields.unmasked_field_by_name(KEY)?;
        let counts = fields.unmasked_field_by_name(family.output_column())?;
        let keys = keys
            .as_opt::<VarBin>()
            .ok_or("owned UTF8 keys are not physical VarBin")?;
        if keys.offsets().dtype() != &DType::Primitive(PType::U64, Nullability::NonNullable) {
            return Err("owned UTF8 offsets are not nonnullable U64".into());
        }
        let offset_array = keys
            .offsets()
            .as_opt::<Primitive>()
            .ok_or("owned UTF8 offsets are not primitive")?;
        let offsets = offset_array.as_slice::<u64>();
        let count_array = counts
            .as_opt::<Primitive>()
            .ok_or("owned UTF8 counts are not primitive")?;
        let counts = count_array.as_slice::<u64>();
        let bytes = keys.bytes();
        if offsets.len() != expected.len() + 1
            || counts.len() != expected.len()
            || offsets.first().copied() != Some(0)
            || offsets.last().copied() != Some(u64::try_from(bytes.len())?)
        {
            return Err("owned UTF8 complete offset/count geometry differs".into());
        }
        for ((range, count), (key, expected_count)) in offsets.windows(2).zip(counts).zip(expected)
        {
            let start = usize::try_from(range[0])?;
            let end = usize::try_from(range[1])?;
            if bytes.get(start..end) != Some(key.as_bytes()) || count != expected_count {
                return Err(
                    "complete owned UTF8 bytes or U64 count differ from independent oracle".into(),
                );
            }
        }
        if array.nbytes() != utf8_bytes(expected)?.1 {
            return Err("owned UTF8 physical buffer bytes differ from independent oracle".into());
        }
        Ok(())
    }

    fn nanos(started: Instant) -> Result<u64, Error> {
        Ok(u64::try_from(started.elapsed().as_nanos())?)
    }

    fn sample(
        prepared: &PreparedVortexAggregate,
        expected: ExpectedSlice<'_>,
        owned: bool,
        baseline: u64,
        family: AggregateFamily,
    ) -> Result<Value, Error> {
        let started = Instant::now();
        let (construction_nanos, drop_nanos, logical_bytes, reserved_at_return, mut work) = if owned
        {
            let result = black_box(prepared.execute_owned()?);
            let construction = nanos(started)?;
            let reserved = prepared.snapshot().memory.reserved_bytes;
            let work = inspect_report(&result.execution, expected, true, family)?;
            inspect_arrays(&result.result, expected, family)?;
            let bytes = result.result.logical_buffer_bytes();
            let dropping = Instant::now();
            drop(black_box(result));
            (construction, nanos(dropping)?, Some(bytes), reserved, work)
        } else {
            let result = black_box(prepared.execute()?);
            let construction = nanos(started)?;
            let reserved = prepared.snapshot().memory.reserved_bytes;
            let work = inspect_report(&result, expected, false, family)?;
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
        let prepared = prepare_aggregate(&query(&fixture.path, limit, fixture.family)?, policy)?;
        let prepare_nanos = nanos(preparing)?;
        let baseline = prepared.snapshot().memory.reserved_bytes;
        if fixture.family == AggregateFamily::CountDistinct && baseline != 0 {
            return Err("fixture preparation unexpectedly retains query payload credits".into());
        }
        // COUNT preparation can retain generation-bound source metadata. Every
        // measured call must return to this same pre-execution baseline; the
        // native lifetime tests separately require zero after the handle drops.
        let mut samples = Vec::with_capacity(2 * (WARMUPS + PAIRS));
        for (phase, rounds) in [("warmup", WARMUPS), ("measurement", PAIRS)] {
            for pair in 0..rounds {
                let order = if pair % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                };
                for (position, owned) in order.into_iter().enumerate() {
                    let mut result = sample(
                        &prepared,
                        fixture.expected.prefix(limit),
                        owned,
                        baseline,
                        fixture.family,
                    )?;
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
            "query": fixture.family.query_summary(), "aggregate_family": fixture.family.name(),
            "key_kind": fixture.key_kind.name(),
            "aggregate_output_column": fixture.family.output_column(),
            "memory_budget_bytes": policy.resource_envelope.memory_budget_bytes,
            "warmups_per_arm": WARMUPS, "balanced_measurement_pairs": PAIRS,
            "prepare_nanos_excluded": prepare_nanos,
            "prepared_source_reserved_bytes": baseline,
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
        let fixture = fixture(&options.workspace, options.family, options.key_kind)?;
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
                "aggregate_family": options.family.name(),
                "key_kind": options.key_kind.name(),
                "aggregate_output_column": options.family.output_column(),
                "source_revision_supplied_by_runner": options.revision,
                "build_label_supplied_by_runner": options.build_label,
                "executable": std::env::current_exe()?, "crate_version": env!("CARGO_PKG_VERSION"),
                "upstream_vortex_provider_version": shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
                "os": std::env::consts::OS, "architecture": std::env::consts::ARCH,
                "available_parallelism": std::thread::available_parallelism()?.get(),
                "fixture": {"path": fixture.path, "sha256": fixture.sha256, "bytes": fixture.bytes,
                    "rows": ROWS, "groups": GROUPS,
                    "rows_per_group": if options.family == AggregateFamily::CountDistinct { Some(8) } else { None },
                    "rows_per_group_distribution": if options.family == AggregateFamily::CountDistinct {
                        json!([{"rows":8,"groups":GROUPS}])
                    } else {
                        json!([{"rows":4,"groups":GROUPS / 2},{"rows":12,"groups":GROUPS / 2}])
                    },
                    "schema": if options.key_kind == KeyKind::Integer { "delivery_zone:nonnullable i64; package_identifier:nonnullable u64" } else { "delivery_zone:nonnullable utf8; package_identifier:nonnullable u64" },
                    "integer_boundaries": if options.key_kind == KeyKind::Integer { Some("i64 MIN/MAX group keys; u64 MAX and values above 2^53; duplicates across writer batches") } else { None },
                    "utf8_boundaries": if options.key_kind == KeyKind::Utf8 { Some("empty; embedded NUL, quote, backslash and newline; composed/decomposed Unicode; long shared prefixes; unique exact bytes repeated across writer batches") } else { None },
                    "writer": if options.key_kind == KeyKind::Integer { "pinned upstream Vortex default file writer; primitive input; preparation excluded" } else { "pinned upstream Vortex default file writer; VarBinView UTF8 keys and primitive U64 values; preparation excluded" },
                    "writer_input_batch_rows": BATCH_ROWS, "physical_row_ranges": fixture.physical_splits,
                    "lifetime": "new create-only fixture retained for reproducibility; never overwrites an existing target"},
                "cases": cases,
                "timing_boundary": "sum of separate monotonic intervals: fresh prepared query through result/report/certificate construction, plus returned result drop; complete validation occurs between intervals and is excluded",
                "validation_effect": "verification touches result buffers before the timed drop and may affect caches; the same sequencing is used for both arms",
                "cache_policy": "single immutable file; source/layout metadata retained per case; fresh aggregate state each call; OS cache uncontrolled; no answer cache",
                "oracle": if options.key_kind == KeyKind::Integer { options.family.oracle_description() } else { "independent standard-library BTreeMap<String,u64> counting every generated input string; count-descending/exact UTF8-byte-ascending sort; every JSON string and physical VarBin byte range plus exact U64 count checked on every call" },
                "memory_scope": "shared session reservation counters only; JSON report buffers, oracle/harness allocations, provider bypass allocations and process RSS are not measured; cumulative peaks combine both arms and are not per-arm peaks",
                "bytes_scope": "owned_logical_buffer_bytes counts native result buffer bytes; result_summary_utf8_bytes counts serialized report text; neither is total allocation or unique retained memory",
                "comparison_scope": "JSON result reports versus owned columns through the existing public prepared API; excludes preparation, fixture construction, transport and persistence; no format or ingest causality claim",
                "claim_gate_status": "bounded_screen_requires_review_of_raw_samples_and_materiality;not_Full43_or_product_wide_performance_evidence",
                "fallback_attempted": false, "external_engine_invoked": false,
            })
        );
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use vortex::array::arrays::VarBinArray;

        fn text_array(rows: &[(String, u64)]) -> vortex::array::ArrayRef {
            let mut offsets = vec![0_u64];
            let mut bytes = Vec::new();
            for (key, _) in rows {
                bytes.extend_from_slice(key.as_bytes());
                offsets.push(u64::try_from(bytes.len()).unwrap());
            }
            StructArray::try_new(
                [KEY, AggregateFamily::Count.output_column()].into(),
                vec![
                    VarBinArray::try_new(
                        PrimitiveArray::new(offsets, Validity::NonNullable).into_array(),
                        bytes.into(),
                        DType::Utf8(Nullability::NonNullable),
                        Validity::NonNullable,
                    )
                    .unwrap()
                    .into_array(),
                    PrimitiveArray::new(
                        rows.iter().map(|(_, count)| *count).collect::<Vec<_>>(),
                        Validity::NonNullable,
                    )
                    .into_array(),
                ],
                rows.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array()
        }

        #[test]
        fn utf8_key_selection_preserves_integer_default_and_rejects_distinct() {
            for family in [AggregateFamily::CountDistinct, AggregateFamily::Count] {
                assert_eq!(KeyKind::parse(None, family).unwrap(), KeyKind::Integer);
                assert_eq!(
                    KeyKind::parse(Some("integer"), family).unwrap(),
                    KeyKind::Integer
                );
                for invalid in ["", "UTF8", "text", "i64"] {
                    assert!(KeyKind::parse(Some(invalid), family).is_err());
                }
            }
            assert_eq!(
                KeyKind::parse(Some("utf8"), AggregateFamily::Count).unwrap(),
                KeyKind::Utf8
            );
            assert!(KeyKind::parse(Some("utf8"), AggregateFamily::CountDistinct).is_err());
        }

        #[test]
        fn utf8_fixture_has_complete_unique_groups_rank_ties_and_bounded_native_output() {
            let mut oracle = BTreeMap::new();
            for row in 0..ROWS {
                record_text(&mut oracle, &fixture_text(row)).unwrap();
            }
            let expected = finish_text(oracle);
            assert_eq!(expected.len(), GROUPS);
            assert_eq!(
                expected.iter().map(|(_, count)| count).sum::<u64>(),
                u64::try_from(ROWS).unwrap()
            );
            assert!(expected[..GROUPS / 2].iter().all(|(_, count)| *count == 12));
            assert!(expected[GROUPS / 2..].iter().all(|(_, count)| *count == 4));
            for partition in [&expected[..GROUPS / 2], &expected[GROUPS / 2..]] {
                assert!(
                    partition
                        .windows(2)
                        .all(|rows| rows[0].0.as_bytes() < rows[1].0.as_bytes())
                );
            }
            for key in ["", "\0", "é", "e\u{301}"] {
                assert!(expected.iter().any(|(value, _)| value == key));
            }
            let (text_bytes, native_bytes) = utf8_bytes(&expected).unwrap();
            assert!(text_bytes > 3 * 1024 * 1024);
            assert!(native_bytes < 8 * 1024 * 1024);
            inspect_utf8_array(&text_array(&expected), &expected, AggregateFamily::Count).unwrap();
            assert_eq!(utf8_bytes(&[]).unwrap(), (0, 8));
            inspect_utf8_array(&text_array(&[]), &[], AggregateFamily::Count).unwrap();
        }

        #[test]
        fn utf8_complete_verifiers_reject_tail_bytes_count_precision_type_and_truncation() {
            let expected = vec![
                ("".into(), u64::MAX),
                ("é".into(), (1_u64 << 60) + 1),
                ("e\u{301}".into(), 3),
                ("東京\0\"\\\n".into(), 2),
            ];
            let json_rows = |rows: &[(String, u64)]| {
                Value::Array(
                    rows.iter()
                        .map(|(key, count)| json!({KEY:key, "packages":count}))
                        .collect(),
                )
            };
            let json = json_rows(&expected);
            inspect_utf8_json_rows(&json, &expected, AggregateFamily::Count).unwrap();
            inspect_utf8_array(&text_array(&expected), &expected, AggregateFamily::Count).unwrap();
            assert!(inspect_utf8_json_rows(&json, &expected[..3], AggregateFamily::Count).is_err());
            assert!(
                inspect_utf8_array(
                    &text_array(&expected),
                    &expected[..3],
                    AggregateFamily::Count
                )
                .is_err()
            );
            assert!(
                inspect_utf8_json_rows(&json, &expected, AggregateFamily::CountDistinct).is_err()
            );
            assert!(
                inspect_utf8_array(
                    &text_array(&expected),
                    &expected,
                    AggregateFamily::CountDistinct
                )
                .is_err()
            );
            for changed in [
                {
                    let mut rows = expected.clone();
                    rows[3].0.push('x');
                    rows
                },
                {
                    let mut rows = expected.clone();
                    rows[1].1 -= 1;
                    rows
                },
                {
                    let mut rows = expected.clone();
                    rows.swap(1, 2);
                    rows
                },
            ] {
                assert!(
                    inspect_utf8_json_rows(&json_rows(&changed), &expected, AggregateFamily::Count)
                        .is_err()
                );
                assert!(
                    inspect_utf8_array(&text_array(&changed), &expected, AggregateFamily::Count)
                        .is_err()
                );
            }
            let mut rounded = json;
            rounded[1]["packages"] = json!(1_152_921_504_606_846_976.0_f64);
            assert!(inspect_utf8_json_rows(&rounded, &expected, AggregateFamily::Count).is_err());
            let signed = StructArray::try_new(
                [KEY, "packages"].into(),
                vec![
                    text_array(&expected)
                        .as_opt::<Struct>()
                        .unwrap()
                        .unmasked_field_by_name(KEY)
                        .unwrap()
                        .clone(),
                    PrimitiveArray::new(vec![1_i64; expected.len()], Validity::NonNullable)
                        .into_array(),
                ],
                expected.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            assert!(inspect_utf8_array(&signed, &expected, AggregateFamily::Count).is_err());
        }

        #[test]
        fn aggregate_family_default_and_requests_preserve_distinct_and_select_count() {
            assert_eq!(
                AggregateFamily::parse(None).unwrap(),
                AggregateFamily::CountDistinct
            );
            assert_eq!(
                AggregateFamily::parse(Some("count_distinct")).unwrap(),
                AggregateFamily::CountDistinct
            );
            assert_eq!(
                AggregateFamily::parse(Some("count")).unwrap(),
                AggregateFamily::Count
            );
            for value in ["", "COUNT", "count_column", "sum"] {
                assert!(AggregateFamily::parse(Some(value)).is_err());
            }
            for family in [AggregateFamily::CountDistinct, AggregateFamily::Count] {
                let request = query(Path::new("/source.vortex"), GROUPS, family).unwrap();
                let aggregate = request.simple_aggregate.unwrap();
                assert_eq!(request.source_order_limit, Some(GROUPS));
                assert_eq!(aggregate.measures.len(), 1);
                assert_eq!(aggregate.measures[0].function, family.name());
                assert_eq!(aggregate.measures[0].alias, family.output_column());
                assert_eq!(aggregate.order_by[0].column, family.output_column());
                assert!(aggregate.order_by[0].descending);
                if family == AggregateFamily::CountDistinct {
                    assert_eq!(
                        aggregate.measures[0].column,
                        Some(ColumnRef::new(VALUE).unwrap())
                    );
                    assert_eq!(aggregate.order_by.len(), 2);
                    assert_eq!(
                        aggregate.order_by[1],
                        VortexAggregateOrderExpr::new(KEY, false)
                    );
                } else {
                    assert_eq!(aggregate.measures[0].column, None);
                    assert_eq!(aggregate.order_by.len(), 1);
                    assert_eq!(
                        aggregate.projected_columns(),
                        vec![ColumnRef::new(KEY).unwrap()]
                    );
                }
            }
        }

        #[test]
        fn independent_oracles_distinguish_duplicate_rows_and_preserve_exact_order() {
            let pairs = [
                (i64::MAX, u64::MAX),
                (i64::MIN, u64::MAX),
                (i64::MAX, u64::MAX),
                (0, u64::MAX),
                (i64::MIN, 1 << 60),
                (0, 1 << 60),
                (i64::MAX, u64::MAX),
            ];
            for (family, expected) in [
                (
                    AggregateFamily::Count,
                    vec![(i64::MAX, 3), (i64::MIN, 2), (0, 2)],
                ),
                (
                    AggregateFamily::CountDistinct,
                    vec![(i64::MIN, 2), (0, 2), (i64::MAX, 1)],
                ),
            ] {
                let mut oracle = IndependentOracle::new(family);
                for (key, value) in pairs {
                    oracle.record(key, value).unwrap();
                }
                assert_eq!(oracle.finish().unwrap(), expected);
                assert!(IndependentOracle::new(family).finish().unwrap().is_empty());
            }
        }

        #[test]
        fn fixture_profiles_keep_all_groups_and_count_ranks_with_ties() {
            assert_eq!(
                fixture_pair(0, AggregateFamily::CountDistinct).unwrap(),
                (i64::MIN, u64::MAX)
            );
            assert_eq!(
                fixture_pair(GROUPS, AggregateFamily::CountDistinct).unwrap(),
                (i64::MIN, (1 << 60) + 1)
            );
            assert_eq!(
                fixture_pair(4 * GROUPS + 1, AggregateFamily::CountDistinct).unwrap(),
                (i64::MAX, (1 << 60) + 5)
            );
            assert_eq!(
                fixture_pair(4 * GROUPS + 1, AggregateFamily::Count).unwrap(),
                (i64::MIN, u64::MAX)
            );
            for family in [AggregateFamily::CountDistinct, AggregateFamily::Count] {
                let mut counts = IndependentOracle::new(AggregateFamily::Count);
                for row in 0..ROWS {
                    let (key, value) = fixture_pair(row, family).unwrap();
                    counts.record(key, value).unwrap();
                }
                let counts = counts.finish().unwrap();
                assert_eq!(counts.len(), GROUPS);
                assert_eq!(
                    counts.iter().map(|(_, count)| count).sum::<u64>(),
                    u64::try_from(ROWS).unwrap()
                );
                if family == AggregateFamily::CountDistinct {
                    assert!(counts.iter().all(|(_, count)| *count == 8));
                } else {
                    assert!(counts[..GROUPS / 2].iter().all(|(_, count)| *count == 12));
                    assert!(counts[GROUPS / 2..].iter().all(|(_, count)| *count == 4));
                    assert_eq!(counts.first(), Some(&(i64::MIN, 12)));
                    assert_eq!(counts.last(), Some(&(i64::MAX, 4)));
                }
            }
        }

        #[test]
        fn complete_result_checks_reject_wrong_family_truncation_values_and_types() {
            let expected = [(i64::MAX, 3), (i64::MIN, 2), (0, 2)];
            for family in [AggregateFamily::CountDistinct, AggregateFamily::Count] {
                let rows = Value::Array(
                    expected
                        .iter()
                        .map(|&(key, count)| json!({KEY:key, (family.output_column()):count}))
                        .collect(),
                );
                inspect_json_rows(&rows, &expected, family).unwrap();
                assert!(inspect_json_rows(&rows, &expected[..2], family).is_err());
                let wrong_family = if family == AggregateFamily::Count {
                    AggregateFamily::CountDistinct
                } else {
                    AggregateFamily::Count
                };
                assert!(inspect_json_rows(&rows, &expected, wrong_family).is_err());
                let mut wrong = rows.clone();
                wrong[2][family.output_column()] = 1.into();
                assert!(inspect_json_rows(&wrong, &expected, family).is_err());
                let array = |values: Vec<u64>| {
                    StructArray::try_new(
                        [KEY, family.output_column()].into(),
                        vec![
                            PrimitiveArray::new(
                                expected.map(|(key, _)| key).to_vec(),
                                Validity::NonNullable,
                            )
                            .into_array(),
                            PrimitiveArray::new(values, Validity::NonNullable).into_array(),
                        ],
                        expected.len(),
                        Validity::NonNullable,
                    )
                    .unwrap()
                    .into_array()
                };
                inspect_array(&array(vec![3, 2, 2]), &expected, family).unwrap();
                assert!(inspect_array(&array(vec![3, 2, 1]), &expected, family).is_err());
                assert!(inspect_array(&array(vec![3, 2, 2]), &expected, wrong_family).is_err());
                let signed = StructArray::try_new(
                    [KEY, family.output_column()].into(),
                    vec![
                        PrimitiveArray::new(vec![i64::MAX, i64::MIN, 0], Validity::NonNullable)
                            .into_array(),
                        PrimitiveArray::new(vec![3_i64, 2, 2], Validity::NonNullable).into_array(),
                    ],
                    3,
                    Validity::NonNullable,
                )
                .unwrap()
                .into_array();
                assert!(inspect_array(&signed, &expected, family).is_err());
            }
        }
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
