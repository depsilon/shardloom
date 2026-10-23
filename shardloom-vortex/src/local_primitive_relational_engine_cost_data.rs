//! Deterministic inputs and independent complete-result expectations.

use super::*;
use std::collections::BTreeSet;

pub(super) const CASES: &[&str] = &[
    "value_text",
    "value_numeric",
    "aggregate_text",
    "aggregate_numeric",
    "aggregate_interleaved",
    "aggregate_count",
    "aggregate_distinct",
    "sort_many",
    "sort_single",
    "sort_limit",
    "predicate_filter",
    "predicate_projected",
    "predicate_single",
    "join_semi_duplicate",
    "join_anti_duplicate",
    "join_semi_unique",
    "join_anti_unique",
];

#[derive(Clone, Copy)]
pub(super) struct Spec {
    pub(super) case: &'static str,
    pub(super) rows: usize,
}

impl Spec {
    pub(super) fn join(self) -> bool {
        self.case.starts_with("join_")
    }
    pub(super) fn streaming(self) -> bool {
        self.join() || self.case.starts_with("predicate_") || self.case.starts_with("sort_")
    }
    fn text(self) -> bool {
        !matches!(
            self.case,
            "value_numeric" | "aggregate_numeric" | "aggregate_count"
        ) && !self.case.starts_with("predicate_")
            && !self.join()
    }
    fn wide(self, right: bool) -> bool {
        self.case == "sort_limit" || (self.join() && right)
    }
    pub(super) fn partitions(self) -> Vec<(usize, usize)> {
        if self.case == "sort_single" {
            assert!(self.rows <= 2048);
            return vec![(0, self.rows)];
        }
        let mut result = vec![(0, 0)];
        let mut start = 0;
        for size in [17, 31, 127, 1, 0, 61, 509, 0, 2039].into_iter().cycle() {
            if start == self.rows {
                break;
            }
            let end = self.rows.min(start + size);
            result.push((start, end));
            start = end;
        }
        result.push((self.rows, self.rows));
        result
    }
}

pub(super) struct Data {
    pub(super) ids: Vec<i64>,
    pub(super) numbers: Vec<Option<i64>>,
    labels: Vec<Option<String>>,
    payloads: Vec<String>,
    spec: Spec,
    right: bool,
}

impl Data {
    pub(super) fn new(spec: Spec, right: bool) -> Self {
        let ids = (0..spec.rows)
            .map(|row| i64::try_from(row).unwrap())
            .collect();
        let numbers = (0..spec.rows)
            .map(|row| {
                if spec.join() {
                    let modulus = if right {
                        if spec.case.ends_with("unique") {
                            spec.rows
                        } else {
                            257
                        }
                    } else {
                        1024
                    };
                    (!row.is_multiple_of(if right { 17 } else { 19 }))
                        .then(|| i64::try_from(row % modulus).unwrap())
                } else {
                    (!row.is_multiple_of(11)).then(|| i64::try_from(row * 17 % 257).unwrap() - 128)
                }
            })
            .collect();
        let labels = if spec.text() {
            (0..spec.rows)
                .map(|row| {
                    (!row.is_multiple_of(13)).then(|| {
                        let padding = if spec.case.starts_with("sort_") {
                            56
                        } else {
                            120
                        };
                        format!("{:08}{}", row % 1024, "x".repeat(padding))
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        let payloads = if spec.wide(right) {
            (0..spec.rows)
                .map(|row| format!("{:08}{}", row, "p".repeat(248)))
                .collect()
        } else {
            Vec::new()
        };
        Self {
            ids,
            numbers,
            labels,
            payloads,
            spec,
            right,
        }
    }

    pub(super) fn source(
        &self,
        session: &ResidentVortexSession,
        range: std::ops::Range<usize>,
        batch: bool,
    ) -> shardloom_core::Result<ResidentMemorySource> {
        let labels = self
            .labels
            .get(range.clone())
            .unwrap_or(&[])
            .iter()
            .map(Option::as_deref)
            .collect::<Vec<_>>();
        let payloads = self
            .payloads
            .get(range.clone())
            .unwrap_or(&[])
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let mut columns = vec![
            MemoryColumn {
                name: "id",
                values: MemoryColumnValues::Int64NonNullable(&self.ids[range.clone()]),
            },
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(&self.numbers[range.clone()]),
            },
        ];
        if self.spec.text() {
            columns.push(MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(&labels),
            });
        }
        if self.spec.wide(self.right) {
            columns.push(MemoryColumn {
                name: "payload",
                values: MemoryColumnValues::Utf8(&payloads),
            });
        }
        if batch {
            ResidentMemorySource::from_batch_columns(session, &columns)
        } else {
            ResidentMemorySource::from_columns(
                session,
                &columns,
                MemorySourceBounds {
                    max_input_rows: 65_536,
                    max_output_rows: 65_536,
                    max_input_bytes: 64 << 20,
                    max_output_bytes: 64 << 20,
                },
            )
        }
    }

    pub(super) fn schema(&self) -> DType {
        let mut fields = vec![
            ("id", DType::Primitive(PType::I64, Nullability::NonNullable)),
            ("n", DType::Primitive(PType::I64, Nullability::Nullable)),
        ];
        if self.spec.text() {
            fields.push(("s", DType::Utf8(Nullability::Nullable)));
        }
        if self.spec.wide(self.right) {
            fields.push(("payload", DType::Utf8(Nullability::Nullable)));
        }
        record_dtype(fields)
    }

    pub(super) fn row(&self, row: usize) -> Value {
        let mut result = json!({"id":self.ids[row], "n":self.numbers[row]});
        if self.spec.text() {
            result["s"] = json!(self.labels[row]);
        }
        if self.spec.wide(self.right) {
            result["payload"] = json!(self.payloads[row]);
        }
        result
    }

    pub(super) fn input_identity(&self) -> Value {
        let rows = (0..self.spec.rows)
            .map(|row| self.row(row))
            .collect::<Vec<_>>();
        json!({"rows":self.spec.rows,"schema":self.schema(),
            "complete_values_sha256":digest(&serde_json::to_vec(&rows).unwrap()),
            "generator":"native-engine-cost-v2","right":self.right})
    }

    fn truth(&self, row: usize, single: bool) -> Option<bool> {
        let n = self.numbers[row];
        if single {
            return n.map(|n| n > 10);
        }
        let left = n.map(|n| n > 10 && n < 100);
        let right = self.ids[row] < 77 && self.ids[row] >= 3;
        if right { Some(true) } else { left }
    }

    #[allow(clippy::too_many_lines)] // Keep frozen independent values and their complete schemas together.
    pub(super) fn expected(&self, right: Option<&Self>) -> (Vec<Value>, DType) {
        let nullable_i64 = || DType::Primitive(PType::I64, Nullability::Nullable);
        let u64_type = || DType::Primitive(PType::U64, Nullability::NonNullable);
        let f64_type = || DType::Primitive(PType::F64, Nullability::Nullable);
        match self.spec.case {
            "value_text" => (
                (0..self.spec.rows)
                    .map(|row| {
                        json!({"id":self.ids[row],
                "value":self.labels[row].as_ref().map(|s| s.chars().count())})
                    })
                    .collect(),
                record_dtype(vec![
                    ("id", DType::Primitive(PType::I64, Nullability::NonNullable)),
                    ("value", nullable_i64()),
                ]),
            ),
            "value_numeric" => (
                (0..self.spec.rows)
                    .map(|row| {
                        json!({"id":self.ids[row],
                "value":self.numbers[row].map(|n| n + 3)})
                    })
                    .collect(),
                record_dtype(vec![
                    ("id", DType::Primitive(PType::I64, Nullability::NonNullable)),
                    ("value", nullable_i64()),
                ]),
            ),
            "aggregate_text" => {
                let values = self.labels.iter().flatten().collect::<Vec<_>>();
                (
                    vec![
                        json!({"a":values.iter().min(),"b":values.iter().max(),"c":values.len(),
                    "d":values.iter().min(),"e":values.iter().max(),"f":values.len(),"rows":self.spec.rows}),
                    ],
                    record_dtype(vec![
                        ("a", DType::Utf8(Nullability::Nullable)),
                        ("b", DType::Utf8(Nullability::Nullable)),
                        ("c", u64_type()),
                        ("d", DType::Utf8(Nullability::Nullable)),
                        ("e", DType::Utf8(Nullability::Nullable)),
                        ("f", u64_type()),
                        ("rows", u64_type()),
                    ]),
                )
            }
            "aggregate_numeric" => {
                let values = self.numbers.iter().flatten().copied().collect::<Vec<_>>();
                let sum = f64::from(i32::try_from(values.iter().sum::<i64>()).unwrap());
                (
                    vec![
                        json!({"a":sum,"b":sum / f64::from(u32::try_from(values.len()).unwrap()),"c":values.iter().min(),
                    "d":values.iter().max(),"e":values.len(),"f":sum}),
                    ],
                    record_dtype(vec![
                        ("a", f64_type()),
                        ("b", f64_type()),
                        ("c", nullable_i64()),
                        ("d", nullable_i64()),
                        ("e", u64_type()),
                        ("f", f64_type()),
                    ]),
                )
            }
            "aggregate_interleaved" => {
                let strings = self.labels.iter().flatten().collect::<Vec<_>>();
                let numbers = self.numbers.iter().flatten().collect::<Vec<_>>();
                (
                    vec![json!({"a":strings.iter().min(),"b":numbers.iter().min(),
                    "c":strings.iter().max(),"d":numbers.iter().max(),"e":strings.len(),
                    "f":numbers.len(),"rows":self.spec.rows})],
                    record_dtype(vec![
                        ("a", DType::Utf8(Nullability::Nullable)),
                        ("b", nullable_i64()),
                        ("c", DType::Utf8(Nullability::Nullable)),
                        ("d", nullable_i64()),
                        ("e", u64_type()),
                        ("f", u64_type()),
                        ("rows", u64_type()),
                    ]),
                )
            }
            "aggregate_count" => (
                vec![json!({"rows":self.spec.rows})],
                record_dtype(vec![("rows", u64_type())]),
            ),
            "aggregate_distinct" => (
                vec![json!({
                "a":self.labels.iter().flatten().collect::<BTreeSet<_>>().len(),
                "b":self.numbers.iter().flatten().collect::<BTreeSet<_>>().len()})],
                record_dtype(vec![("a", u64_type()), ("b", u64_type())]),
            ),
            "sort_many" | "sort_single" | "sort_limit" => {
                let mut rows = (0..self.spec.rows).collect::<Vec<_>>();
                rows.sort_by(|&a, &b| {
                    self.numbers[a]
                        .cmp(&self.numbers[b])
                        .then_with(|| match (&self.labels[a], &self.labels[b]) {
                            (None, None) => std::cmp::Ordering::Equal,
                            (None, Some(_)) => std::cmp::Ordering::Greater,
                            (Some(_), None) => std::cmp::Ordering::Less,
                            (Some(a), Some(b)) => b.cmp(a),
                        })
                        .then_with(|| self.ids[a].cmp(&self.ids[b]))
                });
                if self.spec.case == "sort_limit" {
                    rows = rows.into_iter().skip(17).take(23).collect();
                }
                (
                    rows.into_iter().map(|row| self.row(row)).collect(),
                    self.schema(),
                )
            }
            "predicate_filter" | "predicate_single" => (
                (0..self.spec.rows)
                    .filter(|&row| {
                        self.truth(row, self.spec.case == "predicate_single") == Some(true)
                    })
                    .map(|row| self.row(row))
                    .collect(),
                self.schema(),
            ),
            "predicate_projected" => (
                (0..self.spec.rows)
                    .map(|row| {
                        json!({"id":self.ids[row],
                "value":self.truth(row,false)})
                    })
                    .collect(),
                record_dtype(vec![
                    ("id", DType::Primitive(PType::I64, Nullability::NonNullable)),
                    ("value", DType::Bool(Nullability::Nullable)),
                ]),
            ),
            case if case.starts_with("join_") => {
                let keys = right
                    .expect("join build input")
                    .numbers
                    .iter()
                    .flatten()
                    .copied()
                    .collect::<BTreeSet<_>>();
                let semi = case.starts_with("join_semi_");
                (
                    (0..self.spec.rows)
                        .filter(|&row| self.numbers[row].is_some_and(|n| keys.contains(&n)) == semi)
                        .map(|row| json!({"id":self.ids[row],"n":self.numbers[row]}))
                        .collect(),
                    self.schema(),
                )
            }
            _ => panic!("frozen engine cost case"),
        }
    }
}

pub(super) fn record_dtype(fields: Vec<(&str, DType)>) -> DType {
    DType::struct_(
        fields
            .into_iter()
            .map(|(name, dtype)| (name.to_owned(), dtype))
            .collect::<Vec<_>>(),
        Nullability::NonNullable,
    )
}
