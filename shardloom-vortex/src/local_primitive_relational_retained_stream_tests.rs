//! Complete retained results compete with input/output owners and native spill.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use std::{cell::Cell, path::Path, sync::Weak};

const GRANT: u64 = 16 << 20;
const WIDTH: usize = 2048;
const INPUT_ROWS: usize = 251;

fn population(recursive: bool) -> usize {
    if recursive { 4097 } else { 8193 }
}

fn payload(row: usize) -> String {
    format!("{row:08}{}", "r".repeat(WIDTH - 8))
}

fn source(
    session: &ResidentVortexSession,
    first: usize,
    end: usize,
) -> Result<ResidentMemorySource> {
    let numbers = vec![Some(0); end - first];
    let strings = (first..end).map(payload).collect::<Vec<_>>();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(&numbers),
            },
            MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(
                    &strings
                        .iter()
                        .map(|value| Some(value.as_str()))
                        .collect::<Vec<_>>(),
                ),
            },
        ],
    )
}

struct Producer {
    rows: usize,
    total: usize,
    calls: usize,
    prior: Option<Weak<MemoryLease>>,
    ended: Cell<bool>,
}

impl Producer {
    fn new(total: usize) -> Self {
        Self {
            rows: 0,
            total,
            calls: 0,
            prior: None,
            ended: Cell::new(false),
        }
    }

    fn next(&mut self, session: &ResidentVortexSession) -> Result<Option<ResidentMemorySource>> {
        assert!(
            !self.ended.get(),
            "retained relation reopened a completed producer"
        );
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0),
            "retained relation kept a producer buffer across demand"
        );
        self.calls += 1;
        if self.rows == self.total {
            self.ended.set(true);
            return Ok(None);
        }
        let end = (self.rows + INPUT_ROWS).min(self.total);
        let batch = source(session, self.rows, end)?;
        self.prior = Some(batch.batch_release_witness()?);
        self.rows = end;
        Ok(Some(batch))
    }
}

fn plan(recursive: bool) -> Plan {
    if !recursive {
        return Plan::Materialized(Arc::new(scan()));
    }
    let identity = RecursionId::default();
    let frontier = Plan::RecursiveInput(RecursiveInput {
        identity: identity.clone(),
        columns: vec!["n".into(), "s".into()],
    });
    let selected = Plan::Filter(Box::new(Filter {
        input: frontier,
        predicate: compare(col("n"), ComparisonOp::Lt, lit(ScalarValue::Int64(2))),
    }));
    Plan::Recursive(Arc::new(Recursive {
        identity,
        anchor: scan(),
        distinct: false,
        step: project(
            selected,
            vec![
                (
                    "n",
                    binary(col("n"), BinaryOp::Add, lit(ScalarValue::Int64(1))),
                ),
                ("s", col("s")),
            ],
        ),
    }))
}

fn stream_prepare(recursive: bool, grant: u64) -> Result<PreparedVortexRelational> {
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = grant;
    prepare_relational_with_schema(resources, |schemas| {
        schemas.register_batch_source(DatasetUri::new(SOURCE)?, |session| source(session, 0, 0))?;
        Ok(plan(recursive))
    })
}

fn spilled(
    prepared: PreparedVortexRelational,
    directory: &Path,
    quota: u64,
) -> PreparedVortexRelational {
    prepared
        .with_spill(VortexRelationalSpillPolicy::new(directory, quota, 1 << 20).unwrap())
        .unwrap()
}

fn execute(
    prepared: &PreparedVortexRelational,
    recursive: bool,
) -> Result<ExecutedVortexRelational> {
    let total = population(recursive);
    let mut producer = Producer::new(total);
    let ended = Cell::new(false);
    let mut next = |session: &ResidentVortexSession| {
        let batch = producer.next(session)?;
        ended.set(batch.is_none());
        Ok(batch)
    };
    let mut delivered = 0;
    let report = prepared.with_batch_input(&mut next)?.for_each_json_batch(
        &CancellationToken::default(),
        257,
        1 << 20,
        |batch| {
            assert!(ended.get(), "complete input must precede retained output");
            for row in
                serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value()).unwrap()
            {
                assert_eq!(
                    row,
                    json!({"n":delivered / total, "s":payload(delivered % total)})
                );
                delivered += 1;
            }
            Ok(())
        },
    )?;
    assert_eq!(delivered, total * if recursive { 3 } else { 1 });
    assert_eq!(producer.rows, total);
    assert_eq!(producer.calls, total.div_ceil(INPUT_ROWS) + 1);
    assert!(producer.ended.get());
    assert!(
        producer
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.fallback_attempted);
    assert_eq!(
        report.composition.recursive_iterations,
        if recursive { 3 } else { 0 }
    );
    Ok(report)
}

#[test]
fn retained_stream_pressure_refuses_resident_and_spills_complete_materialized_and_recursive_results()
 {
    for recursive in [false, true] {
        let denied = stream_prepare(recursive, GRANT).unwrap();
        let baseline = denied.snapshot().memory.reserved_bytes;
        let error = execute(&denied, recursive)
            .err()
            .expect("retention exceeds the resident grant");
        assert!(error.to_string().contains("reservation denied"), "{error}");
        assert_eq!(denied.snapshot().completed_executions, 0);
        assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);

        let ample = stream_prepare(recursive, 128 << 20).unwrap();
        let control = execute(&ample, recursive).unwrap();
        assert_eq!(control.composition.spilled_relations, 0);
        drop(control);
        drop(ample);

        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let directory = fixture.0.join("retained-pressure-runs");
        fs::create_dir(&directory).unwrap();
        let prepared = spilled(
            stream_prepare(recursive, GRANT).unwrap(),
            &directory,
            128 << 20,
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for _ in 0..2 {
            let report = execute(&prepared, recursive).unwrap();
            let spill = report.spill.as_ref().unwrap();
            assert!(report.composition.spilled_relations > 0);
            assert!(report.composition.retained_payload_bytes_copied > GRANT);
            assert!(spill.runs_written > 0 && spill.owned_cleanup_completed);
            assert!(spill.peak_disk_bytes <= spill.quota_bytes);
            assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
            eprintln!(
                "{}",
                json!({"family":"retained_relation", "recursive":recursive,
                "input_rows":population(recursive),"output_rows":report.output_rows,
                "retained_bytes":report.composition.retained_payload_bytes_copied,
                "spilled_relations":report.composition.spilled_relations,
                "runs_written":spill.runs_written,"peak_disk_bytes":spill.peak_disk_bytes,
                "peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"grant":GRANT})
            );
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

fn runs(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.is_dir().then_some(path)
        })
        .flat_map(|path| {
            fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
        })
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "vortex")
        })
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)] // One fault matrix shares ownership and retry assertions.
fn retained_stream_faults_discard_provisional_results_and_native_runs() {
    for recursive in [false, true] {
        for failure in [
            "producer",
            "schema",
            "input-cancel",
            "corrupt",
            "quota",
            "consumer",
            "output-cancel",
        ] {
            let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
            let directory = fixture.0.join("retained-fault-runs");
            fs::create_dir(&directory).unwrap();
            let foreign = directory.join("foreign");
            fs::write(&foreign, b"unrelated").unwrap();
            let prepared = spilled(
                stream_prepare(recursive, GRANT).unwrap(),
                &directory,
                if failure == "quota" {
                    2 << 20
                } else {
                    128 << 20
                },
            );
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            let triggered = Cell::new(false);
            let mut state = Producer::new(population(recursive));
            let mut producer = |session: &ResidentVortexSession| {
                if state.calls == 6
                    && matches!(failure, "producer" | "schema" | "input-cancel" | "corrupt")
                {
                    let live = runs(&directory);
                    assert!(!live.is_empty(), "fault must follow actual retained spill");
                    triggered.set(true);
                    match failure {
                        "producer" => return Err(failed("retained producer sentinel")),
                        "schema" => {
                            return ResidentMemorySource::from_batch_columns(
                                session,
                                &[MemoryColumn {
                                    name: "wrong",
                                    values: MemoryColumnValues::Int64(&[Some(1)]),
                                }],
                            )
                            .map(Some);
                        }
                        "input-cancel" => token.cancel(),
                        "corrupt" => fs::OpenOptions::new()
                            .write(true)
                            .open(&live[0])
                            .unwrap()
                            .set_len(1)
                            .unwrap(),
                        _ => unreachable!(),
                    }
                }
                state.next(session)
            };
            let result = prepared
                .with_batch_input(&mut producer)
                .unwrap()
                .for_each_batch(&token, |_, _| {
                    assert_ne!(runs(&directory), Vec::<PathBuf>::new());
                    triggered.set(true);
                    match failure {
                        "consumer" => Err(failed("retained consumer sentinel")),
                        "output-cancel" => {
                            token.cancel();
                            Ok(())
                        }
                        _ => panic!("input failure reached a retained consumer: {failure}"),
                    }
                });
            let error = result
                .err()
                .expect("failed retention cannot certify success");
            let expected = match failure {
                "producer" => "retained producer sentinel",
                "schema" => "declared schema",
                "corrupt" => "byte length changed",
                "quota" => "quota",
                "consumer" => "retained consumer sentinel",
                _ => "cancel",
            };
            assert!(
                error.to_string().contains(expected),
                "{recursive} {failure}: {error}"
            );
            assert!(triggered.get() || failure == "quota");
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
            assert_eq!(fs::read(&foreign).unwrap(), b"unrelated");
            drop(prepared);
            let fresh = spilled(
                stream_prepare(recursive, GRANT).unwrap(),
                &directory,
                128 << 20,
            );
            execute(&fresh, recursive).unwrap();
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        }
    }
}
