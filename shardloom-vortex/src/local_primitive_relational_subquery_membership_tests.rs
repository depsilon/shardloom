//! Complete predicate oracles and actual native key populations at pressure.

use super::*;
use crate::local_primitives::native_relational_subquery::BEFORE_CACHE_HIT;

const ROWS: usize = 8193;
const WIDTH: usize = 2048;
const GRANT: u64 = 4 << 20;
type WideRow = (Option<i64>, Option<String>);

#[path = "local_primitive_relational_subquery_membership_failure_tests.rs"]
mod failure_tests;

#[path = "local_primitive_relational_subquery_membership_typed_tests.rs"]
mod typed_tests;

#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_relational_subquery_membership_writer_tests.rs"]
mod writer_tests;

fn text(row: usize, width: usize) -> String {
    format!("{row:08}{}", "p".repeat(width))
}

fn inner_row(row: usize, width: usize, hot: bool) -> WideRow {
    (
        (!row.is_multiple_of(13)).then_some(if hot {
            1
        } else {
            i64::try_from(row % 5).unwrap()
        }),
        (!row.is_multiple_of(17)).then(|| text(row / 2, width)),
    )
}

fn outer_rows(width: usize) -> Vec<WideRow> {
    vec![
        (Some(1), Some(text(0, width))),
        (Some(4), Some(text(ROWS, width))),
        (None, Some(text(1, width))),
        (Some(2), None),
        (Some(0), Some(text(ROWS / 2, width))),
    ]
}

struct WideInput {
    rows: usize,
    width: usize,
    hot: bool,
    calls: [u64; 2],
    prior: Option<Weak<MemoryLease>>,
}

impl WideInput {
    fn new(rows: usize, width: usize, hot: bool) -> Self {
        Self {
            rows,
            width,
            hot,
            calls: [0; 2],
            prior: None,
        }
    }

    fn next(
        &mut self,
        uri: &DatasetUri,
        ordinal: u64,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        let index = usize::from(uri.as_str() == URIS[1]);
        assert_eq!(ordinal, self.calls[index]);
        self.calls[index] += 1;
        let rows = if index == 0 {
            if ordinal > 0 {
                return Ok(None);
            }
            outer_rows(self.width)
        } else {
            let start = usize::try_from(ordinal).unwrap() * 127;
            if start >= self.rows {
                return Ok(None);
            }
            (start..(start + 127).min(self.rows))
                .map(|row| inner_row(row, self.width, self.hot))
                .collect()
        };
        let numbers = rows.iter().map(|row| row.0).collect::<Vec<_>>();
        let strings = rows.iter().map(|row| row.1.as_deref()).collect::<Vec<_>>();
        let batch = source(session, &numbers, &strings)?;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

fn compare_order(order: std::cmp::Ordering, op: ComparisonOp) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match op {
        ComparisonOp::Eq => order == Equal,
        ComparisonOp::NotEq => order != Equal,
        ComparisonOp::Lt => order == Less,
        ComparisonOp::LtEq => order != Greater,
        ComparisonOp::Gt => order == Greater,
        ComparisonOp::GtEq => order != Less,
    }
}

fn key_order(left: &WideRow, right: &WideRow, name: &str) -> Option<std::cmp::Ordering> {
    match name {
        "n" => left.0.zip(right.0).map(|(left, right)| left.cmp(&right)),
        "s" => left
            .1
            .as_ref()
            .zip(right.1.as_ref())
            .map(|(left, right)| left.cmp(right)),
        _ => panic!("unexpected oracle key"),
    }
}

fn oracle(
    query: &VortexRelationalSubquery,
    left: &WideRow,
    rows: usize,
    width: usize,
    hot: bool,
) -> Value {
    let all = matches!(
        query.kind,
        Kind::Quantified {
            quantifier: Quantifier::All,
            ..
        }
    );
    let mut answers = Vec::new();
    for index in 0..rows {
        let right = inner_row(index, width, hot);
        if !query.correlation.iter().all(|key| {
            key_order(left, &right, key.left.as_str()) == Some(std::cmp::Ordering::Equal)
        }) {
            continue;
        }
        let value = match &query.kind {
            Kind::Exists => Some(true),
            Kind::In { columns } => {
                let comparisons = columns
                    .iter()
                    .map(|key| key_order(left, &right, key.left.as_str()))
                    .collect::<Vec<_>>();
                if comparisons
                    .iter()
                    .any(|value| value.is_some_and(|order| !order.is_eq()))
                {
                    Some(false)
                } else if comparisons.iter().any(Option::is_none) {
                    None
                } else {
                    Some(true)
                }
            }
            Kind::Quantified {
                columns,
                comparison,
                ..
            } => key_order(left, &right, columns.left.as_str())
                .map(|order| compare_order(order, *comparison)),
            Kind::Scalar => panic!("predicate oracle received scalar"),
        };
        answers.push(value);
    }
    let answer = if answers.contains(&Some(!all)) {
        Some(!all)
    } else if answers.contains(&None) {
        None
    } else {
        Some(all)
    };
    answer.map_or(Value::Null, |value| {
        json!(if query.negated { !value } else { value })
    })
}

fn expected_wide(
    query: &VortexRelationalSubquery,
    rows: usize,
    width: usize,
    hot: bool,
) -> Vec<Value> {
    outer_rows(width)
        .iter()
        .map(|row| {
            json!({
                "n": row.0, "s": row.1, "answer": oracle(query, row, rows, width, hot),
            })
        })
        .collect()
}

fn prepared(
    query: &VortexRelationalSubquery,
    directory: &std::path::Path,
) -> PreparedVortexRelational {
    prepare_sources(
        &VortexRelationalPlan::Subquery(Box::new(query.clone())),
        2,
        GRANT,
    )
    .unwrap()
    .with_spill(VortexRelationalSpillPolicy::new(directory, 128 << 20, 1 << 20).unwrap())
    .unwrap()
}

fn complete(
    prepared: &PreparedVortexRelational,
    query: &VortexRelationalSubquery,
    rows: usize,
    width: usize,
    hot: bool,
) -> ExecutedVortexRelational {
    let mut input = WideInput::new(rows, width, hot);
    let expected = expected_wide(query, rows, width, hot);
    let mut actual = Vec::new();
    let report = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| input.next(uri, ordinal, session))
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            actual.extend(values(&array, context)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(input.calls, [2, (rows.div_ceil(127) + 1) as u64]);
    assert!(
        input
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
    assert_eq!(report.spillable_subquery_stages, 1);
    assert_eq!(report.spillable_subquery_input_rows, rows as u64);
    assert_eq!(report.spillable_subquery_probe_rows, 5);
    assert_eq!(report.input.as_ref().unwrap().replay_sources, 0);
    assert!(report.input.as_ref().unwrap().end_of_input_observed);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.fallback_attempted);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
    report
}

fn cases() -> Vec<VortexRelationalSubquery> {
    let mut cases = vec![
        query(
            input_scan(0),
            input_scan(1),
            Kind::In {
                columns: vec![key("s")],
            },
        ),
        query(
            input_scan(0),
            input_scan(1),
            Kind::In {
                columns: vec![key("n"), key("s")],
            },
        ),
    ];
    let mut exists = query(input_scan(0), input_scan(1), Kind::Exists);
    exists.correlation = vec![key("s")];
    cases.push(exists);
    for (comparison, quantifier) in [
        (ComparisonOp::Lt, Quantifier::Any),
        (ComparisonOp::GtEq, Quantifier::All),
    ] {
        let mut quantified = query(
            input_scan(0),
            input_scan(1),
            Kind::Quantified {
                columns: key("s"),
                comparison,
                quantifier,
            },
        );
        quantified.correlation = vec![key("n")];
        cases.push(quantified);
    }
    let mut hot = query(
        input_scan(0),
        input_scan(1),
        Kind::In {
            columns: vec![key("s")],
        },
    );
    hot.correlation = vec![key("n")];
    cases.push(hot);
    cases
}

#[test]
fn streaming_subquery_membership_wide_keys_spill_at_original_grant_with_complete_predicates() {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("predicate-pressure");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"keep").unwrap();
    for (index, query) in cases().iter().enumerate() {
        let prepared = prepared(query, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for execution in 1..=2 {
            let report = complete(&prepared, query, ROWS, WIDTH, index == 5);
            assert_eq!(report.spilled_subquery_stages, 1);
            let spill = report.spill.as_ref().unwrap();
            assert!(
                spill.runs_written > 2 && spill.merge_passes > 0,
                "{spill:?}"
            );
            assert!(report.spillable_subquery_lookup_blocks > 0);
            assert_eq!(report.runtime.completed_executions, execution);
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
            assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
        }
    }
}

#[test]
fn streaming_subquery_membership_cache_hit_revalidates_generation_then_fresh_run_completes() {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("predicate-cache-generation");
    fs::create_dir(&directory).unwrap();
    let query = cases().remove(0);
    let prepared = prepared(&query, &directory);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let captured = std::rc::Rc::new(std::cell::RefCell::new(None));
    let observed = std::rc::Rc::clone(&captured);
    BEFORE_CACHE_HIT.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |path| {
            let replacement = path.with_extension("replaced");
            fs::copy(path, &replacement).unwrap();
            fs::rename(replacement, path).unwrap();
            *observed.borrow_mut() = Some(path.to_path_buf());
        }));
    });
    let mut input = WideInput::new(ROWS, WIDTH, false);
    let error = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| input.next(uri, ordinal, session))
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("prepared source changed"), "{error}");
    BEFORE_CACHE_HIT.with(|hook| assert!(hook.borrow().is_none()));
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let replaced = captured.borrow().clone().unwrap();
    let preserved = fs::read(&replaced).unwrap();
    assert!(
        VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20)
            .unwrap()
            .cleanup_abandoned(replaced.parent().unwrap())
            .is_err()
    );
    drop(complete(&prepared, &query, ROWS, WIDTH, false));
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read(replaced).unwrap(), preserved);
}
