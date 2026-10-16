//! Credited unary-state acceptance, separate from source replay spill.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;

const TOTAL: usize = 8193;
const INPUT_WIDTH: usize = 251;
const GRANT: u64 = 16 << 20;

fn text(key: usize, width: usize) -> String {
    if width == 0 {
        String::new()
    } else {
        format!("{key:08}{}", "u".repeat(width - 8))
    }
}

fn expected_row(index: usize, width: usize, unique: bool) -> Value {
    let key = if unique { index } else { index % 3 };
    json!({"n":key,"s":text(key,width),"items":[key,null]})
}

struct WideInput {
    total: usize,
    width: usize,
    unique: bool,
    position: usize,
    calls: u64,
    ended: bool,
    prior: Option<Weak<MemoryLease>>,
}

impl WideInput {
    fn new(total: usize, width: usize, unique: bool) -> Self {
        Self {
            total,
            width,
            unique,
            position: 0,
            calls: 0,
            ended: false,
            prior: None,
        }
    }

    fn next(
        &mut self,
        ordinal: u64,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        assert!(!self.ended, "unary pressure producer reopened");
        assert_eq!(ordinal, self.calls);
        self.calls += 1;
        if self.position == self.total {
            self.ended = true;
            return Ok(None);
        }
        let end = (self.position + INPUT_WIDTH).min(self.total);
        let keys = (self.position..end)
            .map(|index| if self.unique { index } else { index % 3 })
            .collect::<Vec<_>>();
        let numbers = keys
            .iter()
            .map(|&key| Some(i64::try_from(key).unwrap()))
            .collect::<Vec<_>>();
        let strings = keys
            .iter()
            .map(|&key| text(key, self.width))
            .collect::<Vec<_>>();
        let strings = strings
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let lists = keys
            .iter()
            .map(|key| format!("[{key},null]"))
            .collect::<Vec<_>>();
        let lists = lists
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let dtype = DType::List(
            DType::Primitive(PType::I16, Nullability::Nullable).into(),
            Nullability::Nullable,
        );
        let dtype_json = serde_json::to_string(&dtype).unwrap();
        let batch = ResidentMemorySource::from_batch_columns(
            session,
            &[
                MemoryColumn {
                    name: "n",
                    values: MemoryColumnValues::Int64(&numbers),
                },
                MemoryColumn {
                    name: "s",
                    values: MemoryColumnValues::Utf8(&strings),
                },
                MemoryColumn {
                    name: "items",
                    values: MemoryColumnValues::TypedJson {
                        dtype_json: &dtype_json,
                        values: &lists,
                    },
                },
            ],
        )?;
        self.position = end;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

fn workspace() -> (Fixture, PathBuf) {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("unary-runs");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"preserve").unwrap();
    (fixture, directory)
}

fn with_spill(
    prepared: PreparedVortexRelational,
    directory: &std::path::Path,
) -> PreparedVortexRelational {
    prepared
        .with_spill(VortexRelationalSpillPolicy::new(directory, 128 << 20, 1 << 20).unwrap())
        .unwrap()
}

fn clean(directory: &std::path::Path) {
    assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"preserve");
    assert_eq!(fs::read_dir(directory).unwrap().count(), 1);
}

fn bounded_expected(name: &str, index: usize) -> Value {
    let selected = match name {
        "distinct" | "dedup-First" => Some(index),
        "dedup-Last" | "tail" => Some(TOTAL - 3 + index),
        // Predeclared by independently ranking all 8,193 seed-7 scores.
        "sample" => Some([865, 1724, 3204][index]),
        _ => None,
    };
    if let Some(selected) = selected {
        return expected_row(selected, 2048, false);
    }
    match name {
        "mask-First" => json!({"duplicated":index >= 3}),
        "mask-Last" => json!({"duplicated":index + 3 < TOTAL}),
        "mask-AllDuplicates" => json!({"duplicated":true}),
        "expression" => {
            let mut value = expected_row(index, 2048, false);
            value["ordinal"] = json!(index + 10);
            value
        }
        "melt" => json!({"s":text(index % 3,2048),"variable":"n","value":index % 3}),
        "rolling" => {
            let sum: u32 = (index.saturating_sub(1)..(index + 2).min(TOTAL))
                .map(|n| u32::try_from(n % 3).unwrap())
                .sum();
            json!({"total":f64::from(sum)})
        }
        "explode" => {
            let mut value = expected_row(index / 2, 2048, false);
            value["items"] = if index.is_multiple_of(2) {
                json!((index / 2) % 3)
            } else {
                Value::Null
            };
            value
        }
        _ => panic!("unexpected bounded unary family {name}"),
    }
}

#[test]
fn streaming_unary_bounded_state_completes_input_larger_than_its_grant() {
    for (name, request, _) in cases(&ROWS) {
        let prepared = prepare_unary(&unary(scan(), request), GRANT).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut input = WideInput::new(TOTAL, 2048, false);
        let mut delivered = 0;
        let report = prepared
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_json_batch(&CancellationToken::default(), 257, 1 << 20, |batch| {
                for row in serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap() {
                    assert_eq!(
                        row,
                        bounded_expected(&name, delivered),
                        "{name}/{delivered}"
                    );
                    delivered += 1;
                }
                Ok(())
            })
            .unwrap();
        let expected = match name.as_str() {
            "distinct" | "dedup-First" | "dedup-Last" | "tail" | "sample" => 3,
            "dedup-AllDuplicates" => 0,
            "explode" => TOTAL * 2,
            _ => TOTAL,
        };
        assert_eq!(delivered, expected, "{name}");
        assert!(input.ended);
        assert_eq!(input.calls, TOTAL.div_ceil(INPUT_WIDTH) as u64 + 1);
        let trace = report.input.as_ref().unwrap();
        assert!(
            trace.input_logical_bytes > GRANT
                && trace.end_of_input_observed
                && trace.output_ownership_detached
        );
        assert_eq!(trace.max_retained_input_batches, 1);
        assert_eq!(trace.replay_sources, 0);
        assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        eprintln!(
            "{}",
            json!({"family":"streaming_unary_bounded_state","operation":name,"input_rows":TOTAL,
            "input_logical_bytes":trace.input_logical_bytes,"grant":GRANT,"complete_output_rows":delivered,
            "peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"unary_state_spill":false})
        );
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_unary_resident_keys_and_sample_population_refuse_excess_even_with_replay_spill() {
    let mut requests = cases(&ROWS)
        .into_iter()
        .filter(|(name, _, _)| {
            name == "distinct" || name.starts_with("dedup-") || name.starts_with("mask-")
        })
        .map(|(name, mut request, expected)| {
            // Wide, unique key values must reach membership storage; a unique
            // small integer key would fit and would test a different boundary.
            match request.kind {
                Kind::DropDuplicateRows => {
                    request.deduplicate_key_projection = Some(columns(&["s"]));
                }
                Kind::DuplicateMaskRows => request.projection = columns(&["s"]),
                _ => {}
            }
            (name, request, expected)
        })
        .collect::<Vec<_>>();
    let mut fraction = request(Kind::SampleRows, &["n", "s", "items"]);
    fraction.sample_fraction = Some(0.5);
    fraction.sample_seed = Some(7);
    requests.push(("sample-fraction".into(), fraction, vec![]));
    let mut replacement = request(Kind::SampleRows, &["n", "s", "items"]);
    replacement.source_order_limit = Some(3);
    replacement.sample_seed = Some(7);
    replacement.sample_with_replacement = true;
    requests.push(("sample-replacement".into(), replacement, vec![]));
    assert_eq!(requests.len(), 9);
    for (name, request, _) in requests {
        for spill in [false, true] {
            let (_fixture, directory) = workspace();
            let mut prepared = prepare_unary(&unary(scan(), request.clone()), GRANT).unwrap();
            if spill {
                prepared = with_spill(prepared, &directory);
            }
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut input = WideInput::new(TOTAL, 2048, true);
            let error = prepared
                .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
                .unwrap()
                .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
                .err()
                .expect("resident unary state exceeds grant");
            assert!(
                error.to_string().contains("reservation denied"),
                "{name}/{spill}: {error}"
            );
            assert!(!input.ended && input.position > INPUT_WIDTH);
            assert!(
                input
                    .prior
                    .as_ref()
                    .is_none_or(|owner| owner.strong_count() == 0)
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&directory);
        }
    }
}

#[test]
fn streaming_future_duplicate_masks_charge_population_identities_separately_from_keys() {
    const COUNT: usize = 262_145;
    const SMALL: u64 = 8 << 20;
    for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
        let mut req = request(Kind::DuplicateMaskRows, &["n"]);
        req.duplicate_keep = keep;
        for grant in [SMALL, 32 << 20] {
            if keep == Keep::First && grant != SMALL {
                continue;
            }
            let (_fixture, directory) = workspace();
            let prepared = with_spill(
                prepare_unary(&unary(scan(), req.clone()), grant).unwrap(),
                &directory,
            );
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut input = WideInput::new(COUNT, 0, false);
            let mut delivered = 0;
            let result = prepared
                .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
                .unwrap()
                .for_each_batch(&CancellationToken::default(), |array, context| {
                    for row in values(&array, context)? {
                        let duplicate = match keep {
                            Keep::First => delivered >= 3,
                            Keep::Last => delivered + 3 < COUNT,
                            Keep::AllDuplicates => true,
                        };
                        assert_eq!(row, json!({"duplicated":duplicate}));
                        delivered += 1;
                    }
                    Ok(())
                });
            if grant == SMALL && keep != Keep::First {
                let error = result
                    .err()
                    .expect("future mask population capacity exceeds small grant");
                // Three narrow keys are fixed after row three. The denied request is the
                // 524,288-entry replacement for the (ordinal,key-index) identity vector.
                assert!(
                    error.to_string().contains("requested=8388608"),
                    "{keep:?}: {error}"
                );
                assert!(!input.ended && input.position == COUNT);
                assert_eq!(delivered, 0);
                assert_eq!(prepared.snapshot().completed_executions, 0);
            } else {
                let report = result.unwrap();
                assert!(input.ended);
                assert_eq!(delivered, COUNT);
                assert!(report.input.as_ref().unwrap().end_of_input_observed);
                assert!(report.runtime.memory.peak_reserved_bytes <= grant);
                assert!(report.native_io_certificate.is_certified());
                assert_eq!(report.input.as_ref().unwrap().replay_sources, 0);
                drop(report);
            }
            assert!(
                input
                    .prior
                    .as_ref()
                    .is_none_or(|owner| owner.strong_count() == 0)
            );
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&directory);
        }
    }
}

fn repeated_tail() -> VortexRelationalPlan {
    let mut req = request(Kind::TailRows, &["n", "s", "items"]);
    req.source_order_limit = Some(3);
    let branch = unary(scan(), req);
    VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: branch.clone(),
        right: branch,
        kind: crate::relational_query::VortexRelationalSetKind::UnionAll,
    }))
}

fn execute_repeated_tail(prepared: &PreparedVortexRelational) -> Result<ExecutedVortexRelational> {
    let mut input = WideInput::new(TOTAL, 2048, true);
    let mut delivered = 0;
    let report = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert_eq!(uri.as_str(), "memory://stream");
            input.next(ordinal, session)
        })?
        .for_each_batch(&CancellationToken::default(), |array, context| {
            for row in values(&array, context)? {
                assert_eq!(row, expected_row(TOTAL - 3 + delivered % 3, 2048, true));
                delivered += 1;
            }
            Ok(())
        })?;
    assert_eq!(delivered, 6);
    assert!(input.ended);
    assert_eq!(input.position, TOTAL);
    assert_eq!(input.calls, TOTAL.div_ceil(INPUT_WIDTH) as u64 + 1);
    assert!(
        input
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
    let trace = report.input.as_ref().unwrap();
    assert_eq!(trace.sources, 1);
    assert_eq!(trace.rows, TOTAL as u64);
    assert_eq!(trace.replay_sources, 1);
    assert_eq!(trace.replay_passes, 1);
    assert_eq!(trace.replay_rows, TOTAL as u64);
    assert_eq!(trace.replay_rows_detached, TOTAL as u64);
    assert!(trace.input_logical_bytes > GRANT && trace.replay_buffer_bytes_copied > GRANT);
    assert!(trace.end_of_input_observed && trace.output_ownership_detached);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    Ok(report)
}

#[test]
fn streaming_repeated_unary_branches_use_one_original_producer_and_explicit_native_replay() {
    let (_fixture, directory) = workspace();
    let denied = prepare_unary(&repeated_tail(), GRANT).unwrap();
    let baseline = denied.snapshot().memory.reserved_bytes;
    let error = execute_repeated_tail(&denied)
        .err()
        .expect("unspilled source replay exceeds grant");
    assert!(error.to_string().contains("reservation denied"), "{error}");
    assert_eq!(denied.snapshot().completed_executions, 0);
    assert_eq!(denied.snapshot().memory.reserved_bytes, baseline);
    drop(denied);

    let ample = prepare_unary(&repeated_tail(), 128 << 20).unwrap();
    let baseline = ample.snapshot().memory.reserved_bytes;
    let report = execute_repeated_tail(&ample).unwrap();
    assert_eq!(report.input.as_ref().unwrap().replay_spilled_sources, 0);
    let ample_peak = report.runtime.memory.peak_reserved_bytes;
    drop(report);
    assert_eq!(ample.snapshot().memory.reserved_bytes, baseline);
    drop(ample);

    let spilled = with_spill(prepare_unary(&repeated_tail(), GRANT).unwrap(), &directory);
    let baseline = spilled.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let report = execute_repeated_tail(&spilled).unwrap();
        let trace = report.input.as_ref().unwrap();
        let spill = report.spill.as_ref().unwrap();
        assert_eq!(trace.replay_spilled_sources, 1);
        assert!(spill.runs_written > 0 && spill.merge_passes > 0);
        assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
        assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(report.runtime.completed_executions, execution);
        eprintln!(
            "{}",
            json!({"family":"streaming_unary_source_replay","execution":execution,
            "input_rows":TOTAL,"input_logical_bytes":trace.input_logical_bytes,"grant":GRANT,
            "peak_reserved_bytes":report.runtime.memory.peak_reserved_bytes,"ample_peak_reserved_bytes":ample_peak,
            "runs_written":spill.runs_written,"merge_passes":spill.merge_passes,
            "peak_disk_bytes":spill.peak_disk_bytes,"unary_state_spill":false,
            "complete_output_rows":report.output_rows,"owned_cleanup_completed":spill.owned_cleanup_completed})
        );
        drop(report);
        assert_eq!(spilled.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}
