// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::data::ArrayData;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::RunEndBuffer;
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;
use num_traits::ToPrimitive;

use super::{EqualFrame, EqualSlice};

pub(super) enum RunEqualStepper<'a> {
    I16(Box<RunStepperState<'a, i16>>),
    I32(Box<RunStepperState<'a, i32>>),
    I64(Box<RunStepperState<'a, i64>>),
}

pub(super) struct RunStepperState<'a, T: ArrowNativeType> {
    l_array: RunArrayData<'a, T>,
    r_array: RunArrayData<'a, T>,
    l_phys: usize,
    r_phys: usize,
    processed: usize,
    len: usize,
}

/// Enqueues comparisons for two `RunEndEncoded` arrays onto `stack`.
pub(super) fn push_run_equal<'a>(
    slice: EqualSlice<'a>,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let lhs_index_type = match slice.lhs.data_type() {
        DataType::RunEndEncoded(f, _) => f.data_type(),
        _ => unreachable!(),
    };

    match lhs_index_type {
        DataType::Int16 => push_run_equal_inner::<i16>(slice, RunEqualStepper::I16, stack),
        DataType::Int32 => push_run_equal_inner::<i32>(slice, RunEqualStepper::I32, stack),
        DataType::Int64 => push_run_equal_inner::<i64>(slice, RunEqualStepper::I64, stack),
        _ => unreachable!(),
    }
}

pub(super) fn step_run_equal<'a>(
    stepper: RunEqualStepper<'a>,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    match stepper {
        RunEqualStepper::I16(state) => advance_run_stepper(state, RunEqualStepper::I16, stack),
        RunEqualStepper::I32(state) => advance_run_stepper(state, RunEqualStepper::I32, stack),
        RunEqualStepper::I64(state) => advance_run_stepper(state, RunEqualStepper::I64, stack),
    }
}

struct RunArrayData<'a, T: ArrowNativeType> {
    run_ends: RunEndBuffer<T>,
    values: &'a ArrayData,
}

impl<'a, T: ArrowNativeType + ToPrimitive> RunArrayData<'a, T> {
    fn new(data: &'a ArrayData, start: usize, len: usize) -> Self {
        debug_assert!(
            data.child_data().len() == 2,
            "RunEndEncoded arrays are guaranteed to have 2 children [run_ends, values]"
        );
        let run_ends_data = &data.child_data()[0];
        let raw_run_ends_buffer = &run_ends_data.buffers()[0];
        // SAFETY: we're reconstructing RunEndBuffer from a known valid RunArray
        let run_ends = unsafe {
            RunEndBuffer::<T>::new_unchecked(
                ScalarBuffer::new(
                    raw_run_ends_buffer.clone(),
                    run_ends_data.offset(),
                    run_ends_data.len(),
                ),
                data.offset() + start,
                len,
            )
        };

        let values = &data.child_data()[1];
        Self { run_ends, values }
    }

    fn run_end(&self, index: usize) -> usize {
        self.run_ends.values()[index].as_usize()
    }

    fn get_start_end_physical_indices(&self) -> (usize, usize) {
        let start = self.run_ends.get_start_physical_index();
        let end = self.run_ends.get_end_physical_index();
        (start, end)
    }
}

fn push_run_equal_inner<'a, T: ArrowNativeType + ToPrimitive>(
    slice: EqualSlice<'a>,
    wrap: fn(Box<RunStepperState<'a, T>>) -> RunEqualStepper<'a>,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    let EqualSlice {
        lhs,
        rhs,
        lhs_start,
        rhs_start,
        len,
    } = slice;
    if len == 0 {
        return true;
    }

    let l_array = RunArrayData::<T>::new(lhs, lhs_start, len);
    let r_array = RunArrayData::<T>::new(rhs, rhs_start, len);

    let (l_start_phys, l_end_phys) = l_array.get_start_end_physical_indices();
    let (r_start_phys, r_end_phys) = r_array.get_start_end_physical_indices();
    let l_runs = l_end_phys - l_start_phys + 1;
    let r_runs = r_end_phys - r_start_phys + 1;

    if l_runs == r_runs {
        // When the boundaries align perfectly, we don't need the complex stepping loop that calculates overlaps.
        // Instead, we can simply treat the underlying values arrays as if they were standard primitive arrays.
        let l_iter = l_array.run_ends.sliced_values();
        let r_iter = r_array.run_ends.sliced_values();
        let physical_match = l_iter.zip(r_iter).all(|(l_re, r_re)| l_re == r_re);

        if physical_match {
            // Both arrays are partitioned identically.
            // We can just verify if the physical values in those partitions match.
            stack.push(EqualFrame::Range(EqualSlice {
                lhs: l_array.values,
                rhs: r_array.values,
                lhs_start: l_start_phys,
                rhs_start: r_start_phys,
                len: l_runs,
            }));
            return true;
        }
    }

    let state = Box::new(RunStepperState {
        l_array,
        r_array,
        l_phys: l_start_phys,
        r_phys: r_start_phys,
        processed: 0,
        len,
    });
    advance_run_stepper(state, wrap, stack)
}

fn advance_run_stepper<'a, T: ArrowNativeType + ToPrimitive>(
    mut state: Box<RunStepperState<'a, T>>,
    wrap: fn(Box<RunStepperState<'a, T>>) -> RunEqualStepper<'a>,
    stack: &mut Vec<EqualFrame<'a>>,
) -> bool {
    if state.processed >= state.len {
        return true;
    }
    let cur_l_phys = state.l_phys;
    let cur_r_phys = state.r_phys;
    let l_values = state.l_array.values;
    let r_values = state.r_array.values;

    let l_run_end = state.l_array.run_end(cur_l_phys);
    let r_run_end = state.r_array.run_end(cur_r_phys);

    let l_remaining_in_run = l_run_end - (state.l_array.run_ends.offset() + state.processed);
    let r_remaining_in_run = r_run_end - (state.r_array.run_ends.offset() + state.processed);
    let remaining_in_range = state.len - state.processed;

    let step = l_remaining_in_run
        .min(r_remaining_in_run)
        .min(remaining_in_range);
    state.processed += step;

    if state.l_array.run_ends.offset() + state.processed == l_run_end {
        state.l_phys += 1;
    }
    if state.r_array.run_ends.offset() + state.processed == r_run_end {
        state.r_phys += 1;
    }

    if state.processed < state.len {
        stack.push(EqualFrame::RunEndStepper(wrap(state)));
    }
    stack.push(EqualFrame::Range(EqualSlice {
        lhs: l_values,
        rhs: r_values,
        lhs_start: cur_l_phys,
        rhs_start: cur_r_phys,
        len: 1,
    }));
    true
}
