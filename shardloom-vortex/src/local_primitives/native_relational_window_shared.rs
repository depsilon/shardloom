//! Share contiguous compatible resident frames without retaining another owner.

use super::{Values, Window, failed, frame};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;

impl Window<'_> {
    pub(super) fn evaluate_frames(
        &self,
        functions: &[usize],
        rows: &[usize],
        peers: &[usize],
        values: &mut [Values],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let mut start = 0;
        while start < functions.len() {
            let function = functions[start];
            let Some(primary) = &self.spec.frames[function] else {
                start += 1;
                continue;
            };
            let mut end = start + 1;
            while end < functions.len() {
                if (end - start).is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let Some(companion) = &self.spec.frames[functions[end]] else {
                    break;
                };
                if !frame::can_share(primary, companion) {
                    break;
                }
                end += 1;
            }
            if end == start + 1 {
                self.evaluate_frame(primary, rows, peers, &mut values[function], context)?;
            } else {
                self.evaluate_shared_frames(&functions[start..end], rows, peers, values, context)?;
            }
            start = end;
        }
        Ok(())
    }

    fn evaluate_shared_frames(
        &self,
        functions: &[usize],
        rows: &[usize],
        peers: &[usize],
        values: &mut [Values],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        check_companion_cancelled(context)?;
        let primary = self.shared_frame(functions[0])?;
        let mut cursor = frame::Cursor::default();
        let mut state = frame::State::new(primary, context.memory())?;
        let mut input = frame::Resident {
            rows,
            table: &self.table,
        };
        let mut peer_edge = |index| Ok(peers.get(index).copied().unwrap_or(rows.len()));
        let mut peer = 0;
        let mut cutoff = functions.len();
        let mut companions_since_check = 0usize;
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.window_shared_groups += 1;
            counts.window_shared_outputs += functions.len() as u64;
        });
        for position in 0..rows.len() {
            if position.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            while peers[peer + 1] <= position {
                peer += 1;
            }
            let at = frame::Position {
                rows: rows.len(),
                peer_start: peers[peer],
                peer_end: peers[peer + 1],
                peer,
                row: position,
            };
            let range = cursor.advance(&primary.frame, &at, &mut input, &mut peer_edge, context)?;
            let ranges = frame::intervals(range, &at, primary.frame.exclusion);
            let (value, summary) =
                state.advance_shared(ranges, primary, rows.len(), &mut input, context)?;
            write(&mut values[functions[0]], rows[position], value)?;
            for (member, &function) in functions.iter().enumerate().take(cutoff).skip(1) {
                if companions_since_check == 1024 {
                    check_companion_cancelled(context)?;
                    companions_since_check = 0;
                }
                companions_since_check += 1;
                let companion = self.shared_frame(function)?;
                if state
                    .finish_shared(primary, companion, summary, context)
                    .and_then(|value| write(&mut values[function], rows[position], value))
                    .is_err()
                {
                    // Retain only the earliest member index, not a diagnostic
                    // allocation. Earlier declarations must finish first.
                    cutoff = member;
                    break;
                }
            }
        }
        // Restore function-major error precedence with the existing evaluator.
        // Its state must not overlap the shared owner's memory reservation.
        drop(state);
        for &function in &functions[cutoff..] {
            #[cfg(all(test, debug_assertions))]
            crate::local_primitives::native_relational_observation::record(|counts| {
                counts.window_exceptional_suffix_functions += 1;
            });
            self.evaluate_frame(
                self.shared_frame(function)?,
                rows,
                peers,
                &mut values[function],
                context,
            )?;
        }
        Ok(())
    }

    fn shared_frame(&self, function: usize) -> Result<&frame::Spec> {
        self.spec.frames[function]
            .as_ref()
            .ok_or_else(|| failed("shared window frame is absent"))
    }
}

fn check_companion_cancelled(context: &NativeExecutionContext<'_>) -> Result<()> {
    #[cfg(all(test, debug_assertions))]
    crate::local_primitives::native_relational_observation::record(|counts| {
        counts.window_companion_cancellation_checks += 1;
    });
    context.check_cancelled()
}

fn write(values: &mut Values, row: usize, value: frame::Value) -> Result<()> {
    values.set_frame(row, value)?;
    #[cfg(all(test, debug_assertions))]
    crate::local_primitives::native_relational_observation::record(|counts| {
        counts.window_resident_frame_writes += 1;
    });
    Ok(())
}
