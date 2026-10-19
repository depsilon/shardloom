//! Set finalization and fanout replay share one grant without larger replay blocks.

use super::super::super::super::writer_tests::{FORMATS, read_rows};
use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;

#[test]
fn streaming_set_membership_fanout_keeps_native_replay_blocks_bounded() {
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let (fixture, directory) = workspace();
        let foreign = directory.join("foreign");
        fs::write(&foreign, b"preserve unrelated file").unwrap();
        let prepared = with_spill(
            prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
            &directory,
            128 << 20,
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let targets = FORMATS.map(|format| {
            (
                fixture.0.join(format!("output.{}", format.as_str())),
                format,
            )
        });
        let mut inputs = WideInputs::new(true);
        inputs.right_offset = ROWS / 2;
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                assert!(targets.iter().all(|(path, _)| !path.exists()));
                inputs.next(uri, ordinal, session)
            })
            .unwrap()
            .write_many_controlled(&targets, false, &CancellationToken::default())
            .unwrap();
        let expected = match kind {
            SetKind::UnionDistinct => 0..ROWS + ROWS / 2,
            SetKind::Intersect => ROWS / 2..ROWS,
            SetKind::Except => 0..ROWS / 2,
            SetKind::UnionAll => unreachable!(),
        };
        let execution = &result.execution;
        assert_eq!(inputs.ended, [true; 2]);
        assert_eq!(execution.output_rows, expected.len() as u64);
        assert_eq!(execution.ordered_set_stages, 1);
        assert_eq!(execution.ordered_set_groups, (ROWS + ROWS / 2) as u64);
        assert_eq!(execution.runtime.completed_executions, 1);
        assert!(execution.runtime.memory.peak_reserved_bytes <= GRANT);
        assert_eq!(execution.input.as_ref().unwrap().replay_sources, 0);
        assert!(execution.input.as_ref().unwrap().input_logical_bytes > 2 * GRANT);
        assert!(execution.native_io_certificate.is_certified());
        assert!(
            !execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        let replay = result.replay.as_ref().unwrap();
        assert!(replay.spilled);
        assert_eq!(replay.replays, FORMATS.len());
        assert_eq!(replay.rows_detached, expected.len() as u64);
        let spill = execution.spill.as_ref().unwrap();
        assert!(spill.run_block_rows <= 256);
        assert!(spill.runs_written > 3 && spill.merge_passes > 1);
        assert!(spill.owned_cleanup_completed);
        assert_eq!(result.outputs.len(), FORMATS.len());
        for (path, format) in &targets {
            if *format == Format::Csv {
                let csv = fs::read_to_string(path).unwrap();
                let mut rows = csv.lines();
                assert_eq!(rows.next(), Some("n,s"));
                for key in expected.clone() {
                    assert_eq!(rows.next().unwrap(), format!("{key},{}", text(key)));
                }
                assert_eq!(rows.next(), None);
            } else {
                // Reopen through the existing independent reader/reference helper;
                // its materialization is outside the constrained producer grant.
                let rows = read_rows(path, *format, &prepared.output_dtype().unwrap());
                assert_eq!(rows.len(), expected.len());
                for (row, key) in rows.iter().zip(expected.clone()) {
                    check_row(row, key);
                }
            }
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read(&foreign).unwrap(), b"preserve unrelated file");
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
    }
}
