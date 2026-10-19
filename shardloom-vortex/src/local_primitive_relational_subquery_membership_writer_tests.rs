//! Complete predicate results through one execution and all admitted sinks.

use super::*;
use crate::local_primitives::{
    VortexLocalPrimitiveRowExportFormat as Format,
    prepared_relational::tests::writer_tests::{FORMATS, read_rows},
};
use std::fmt::Write as _;

#[test]
fn subquery_membership_spilled_fanout_reopens_all_eight_destinations_and_preserves_targets() {
    for (index, query) in cases().iter().enumerate() {
        let (fixture, directory) = super::failure_tests::workspace();
        let prepared = prepared(query, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let targets = FORMATS.map(|format| {
            (
                fixture.0.join(format!("answer.{}", format.as_str())),
                format,
            )
        });
        let expected = expected_wide(query, ROWS, WIDTH, index == 5);
        let mut input = WideInput::new(ROWS, WIDTH, index == 5);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                assert!(targets.iter().all(|(path, _)| !path.exists()));
                input.next(uri, ordinal, session)
            })
            .unwrap()
            .write_many_controlled(&targets, false, &CancellationToken::default())
            .unwrap();
        assert_eq!(input.calls, [2, (ROWS.div_ceil(127) + 1) as u64]);
        assert_eq!(result.execution.output_rows, expected.len() as u64);
        assert_eq!(result.execution.spillable_subquery_stages, 1);
        assert_eq!(result.execution.spillable_subquery_input_rows, ROWS as u64);
        assert_eq!(result.execution.input.as_ref().unwrap().replay_sources, 0);
        assert!(result.execution.runtime.memory.peak_reserved_bytes <= GRANT);
        assert!(result.execution.spill.as_ref().unwrap().runs_written > 2);
        assert!(result.execution.spill.as_ref().unwrap().merge_passes > 0);
        assert!(
            result
                .execution
                .spill
                .as_ref()
                .unwrap()
                .owned_cleanup_completed
        );
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(!result.execution.native_io_certificate.fallback_attempted);
        assert_eq!(result.outputs.len(), FORMATS.len());
        assert_eq!(result.replay.as_ref().unwrap().replays, FORMATS.len());
        assert_eq!(
            result.replay.as_ref().unwrap().rows_detached,
            expected.len() as u64
        );
        for (path, format) in &targets {
            if *format == Format::Csv {
                let mut csv = String::from("n,s,answer\n");
                for row in &expected {
                    let cell = |name: &str| match &row[name] {
                        Value::Null => String::new(),
                        Value::String(text) => text.clone(),
                        value => value.to_string(),
                    };
                    writeln!(csv, "{},{},{}", cell("n"), cell("s"), cell("answer")).unwrap();
                }
                assert_eq!(fs::read_to_string(path).unwrap(), csv);
            } else {
                assert_eq!(
                    read_rows(path, *format, &prepared.output_dtype().unwrap()),
                    expected
                );
            }
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        super::failure_tests::clean(&directory);
        let retained = targets
            .iter()
            .map(|(path, _)| fs::read(path).unwrap())
            .collect::<Vec<_>>();
        let mut called = false;
        let error = prepared
            .with_batch_inputs(&mut |_, _, _| {
                called = true;
                Ok(None)
            })
            .unwrap()
            .write_many_controlled(&targets, false, &CancellationToken::default())
            .err()
            .unwrap()
            .to_string();
        assert!(!called);
        assert!(error.contains("exist"), "{error}");
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        for ((path, _), bytes) in targets.iter().zip(retained) {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
        super::failure_tests::clean(&directory);
    }
}
