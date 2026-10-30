use super::*;
use crate::local_primitives::prepared_relational::batch_input::{Execution, Input, RankedConsumer};
use shardloom_exec::live_memory::MemoryLease;

struct Hook<'a> {
    fixture: &'a ParquetFixture,
    failure: &'static str,
    token: CancellationToken,
    positions: Vec<u64>,
    pressure: Option<MemoryLease>,
    retained: Option<ArrayRef>,
    rank_calls: usize,
    payload_calls: usize,
}

impl RankedConsumer for Hook<'_> {
    fn rank(&mut self, array: &ArrayRef, _: u64, _: &NativeExecutionContext<'_>) -> Result<()> {
        self.rank_calls += 1;
        if self.failure == "alias" {
            self.retained = Some(array.clone());
        }
        if self.failure == "rank_cancel" {
            self.token.cancel();
        }
        Ok(())
    }

    fn select(&mut self, context: &NativeExecutionContext<'_>) -> Result<&[u64]> {
        match self.failure {
            "generation" => {
                let path = self.fixture.0.join("replacement.parquet");
                fs::copy(self.fixture.path(), &path).unwrap();
                fs::rename(path, self.fixture.path()).unwrap();
            }
            "select_cancel" => self.token.cancel(),
            "pressure" => {
                let memory = context.memory().snapshot();
                self.pressure = Some(
                    context
                        .memory()
                        .reserve(memory.limit_bytes - memory.reserved_bytes)?,
                );
            }
            "duplicate" => self.positions = vec![0, 0],
            "outside" => self.positions = vec![397],
            _ => {}
        }
        Ok(&self.positions)
    }

    fn payload(&mut self, _: &ArrayRef, _: u64, _: &NativeExecutionContext<'_>) -> Result<()> {
        self.payload_calls += 1;
        if self.failure == "consumer" {
            return Err(failed("ranked payload consumer sentinel"));
        }
        Ok(())
    }

    fn complete(&self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn ranked_demand_phase_failures_reject_completion_release_credits_and_allow_fresh_retry() {
    for failure in [
        "generation",
        "rank_cancel",
        "select_cancel",
        "pressure",
        "duplicate",
        "outside",
        "consumer",
        "alias",
    ] {
        let fixture = fixture(0, 397, 7);
        let plan = limit(
            sorted(
                fixture.scan(&[], None),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            2,
        );
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let mut hook = Hook {
            fixture: &fixture,
            failure,
            token: token.clone(),
            positions: vec![0],
            pressure: None,
            retained: None,
            rank_calls: 0,
            payload_calls: 0,
        };
        let result = prepared
            .session
            .with_native_execution_context(&token, |context| {
                let input = Execution::new(&prepared, None)?;
                assert!(input.run_ranked(0, &["id".into()], 2, context, &mut hook)?);
                Ok(())
            });
        let error = result.expect_err("phase failure must not complete");
        assert!(hook.rank_calls > 0, "{failure}");
        if failure == "consumer" {
            assert!(hook.payload_calls > 0 && error.to_string().contains("consumer sentinel"));
        } else {
            assert_eq!(hook.payload_calls, 0, "{failure}");
        }
        if failure == "alias" {
            assert!(
                error.to_string().contains("input aliases remain live"),
                "{error}"
            );
        }
        drop(hook);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            initial,
            "{failure}"
        );
        if failure == "generation" {
            assert!(
                prepared
                    .collect_jsonl(&CancellationToken::default())
                    .is_err()
            );
        }
        let fresh = fixture.prepare(&plan);
        let result = fresh.collect_jsonl(&CancellationToken::default()).unwrap();
        assert_eq!(json_rows(&result), vec![expected(0), expected(1)]);
        assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 1);
    }
}

#[test]
fn ranked_demand_post_consumer_failure_and_native_output_lifetime_preserve_owners() {
    for failure in ["generation", "cancel", "consumer"] {
        let fixture = fixture(0, 397, 7);
        let plan = limit(
            sorted(
                fixture.scan(&[], None),
                vec![key("id", false, NullOrder::Last)],
            ),
            0,
            2,
        );
        let prepared = fixture.prepare(&plan);
        let initial = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let mut calls = 0;
        let result = prepared.for_each_batch(&token, |_, _| {
            calls += 1;
            match failure {
                "generation" => {
                    let path = fixture.0.join("replacement.parquet");
                    fs::copy(fixture.path(), &path).unwrap();
                    fs::rename(path, fixture.path()).unwrap();
                }
                "cancel" => token.cancel(),
                _ => return Err(failed("final ranked consumer sentinel")),
            }
            Ok(())
        });
        assert!(calls > 0 && result.is_err());
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
    }
    let fixture = fixture(0, 397, 7);
    let prepared = fixture.prepare(&limit(
        sorted(
            fixture.scan(&[], None),
            vec![key("id", false, NullOrder::Last)],
        ),
        3,
        5,
    ));
    let memory = prepared.session.shared_memory_pool();
    let result = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.execution.input.as_ref().unwrap().ranked_sources, 1);
    drop((prepared, fixture));
    let mut execution = result.result.create_execution_ctx();
    let values = result
        .result
        .arrays()
        .iter()
        .flat_map(|array| {
            crate::local_primitives::logical_field_from_native_array(array, "id")
                .unwrap()
                .execute::<PrimitiveArray>(&mut execution)
                .unwrap()
                .as_slice::<i64>()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![3, 4, 5, 6, 7]);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop((execution, result));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
