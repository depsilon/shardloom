// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::reader::memory::tests::{Counters, Memory as TestMemory};
use arrow::array::{Array, Int64Array};
use std::sync::Mutex;

pub(super) fn budget(limit: usize) -> (Memory, Arc<Mutex<Counters>>) {
    let counters = Arc::new(Mutex::new(Counters::default()));
    (
        Some(Arc::new(TestMemory {
            counters: counters.clone(),
            limit,
        })),
        counters,
    )
}

pub(super) struct Literal<T>(std::vec::IntoIter<T>);

impl<T> Literal<T> {
    pub(super) fn new(values: Vec<T>) -> Self {
        Self(values.into_iter())
    }
}

impl<T: Copy> PrimitiveValueDecoder<T> for Literal<T> {
    fn decode(&mut self, out: &mut [T]) -> Result<()> {
        for value in out {
            *value = self.0.next().ok_or_else(|| {
                crate::error::OutOfSpecSnafu {
                    msg: "truncated literal test stream",
                }
                .build()
            })?;
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) -> Result<()> {
        for _ in 0..count {
            self.0.next().ok_or_else(|| {
                crate::error::OutOfSpecSnafu {
                    msg: "truncated literal test skip",
                }
                .build()
            })?;
        }
        Ok(())
    }
}

struct FailedPresent;

impl PrimitiveValueDecoder<bool> for FailedPresent {
    fn decode(&mut self, _out: &mut [bool]) -> Result<()> {
        crate::error::OutOfSpecSnafu {
            msg: "deliberate present failure",
        }
        .fail()
    }

    fn skip(&mut self, _count: usize) -> Result<()> {
        self.decode(&mut [])
    }
}

#[test]
fn present_decode_failures_survive_nullable_parent_combination() {
    let parent = NullBuffer::from(vec![true, false, true]);
    for parent in [None, Some(&parent)] {
        let mut present = Some(PresentDecoder {
            inner: Box::new(FailedPresent),
            memory: None,
        });
        let result = derive_present_vec(&mut present, parent, 3).transpose();
        assert!(
            result.is_err(),
            "a failed presence stream cannot become all-valid"
        );
    }
}

#[test]
fn primitive_allocation_denial_precedes_decode_and_slices_retain_credit() {
    let (memory, counters) = budget(32);
    let mut decoder = Int64ArrayDecoder::new(
        Box::new(Literal::new(vec![7, -2, i64::MAX, 11])),
        None,
        memory,
    );
    assert!(decoder.next_batch(5, None).is_err());
    assert_eq!(counters.lock().unwrap().live, 0);
    let array = decoder.next_batch(4, None).unwrap();
    assert_eq!(
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, -2, i64::MAX, 11]
    );
    assert_eq!(counters.lock().unwrap().live, 32);
    let slice = array.slice(1, 2);
    drop(array);
    drop(decoder);
    assert_eq!(counters.lock().unwrap().live, 32);
    assert_eq!(
        slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[-2, i64::MAX]
    );
    drop(slice);
    assert_eq!(counters.lock().unwrap().live, 0);
}

#[test]
fn nullable_boolean_buffers_hold_credits_and_failed_decode_refunds() {
    let (memory, counters) = budget(64);
    let present = PresentDecoder {
        inner: Box::new(Literal::new(vec![true, false, true, true])),
        memory: memory.clone(),
    };
    let mut decoder = BooleanArrayDecoder::new(
        Box::new(Literal::new(vec![true, false, true])),
        Some(present),
        memory.clone(),
    );
    let array = decoder.next_batch(4, None).unwrap();
    let values = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(true), None, Some(false), Some(true)]
    );
    assert_eq!(counters.lock().unwrap().live, 2);
    assert!(counters.lock().unwrap().peak > 2);
    let slice = array.slice(1, 2);
    drop(array);
    drop(decoder);
    assert_eq!(counters.lock().unwrap().live, 2);
    drop(slice);
    assert_eq!(counters.lock().unwrap().live, 0);
    let mut failed = Int64ArrayDecoder::new(Box::new(Literal::new(vec![1])), None, memory);
    assert!(failed.next_batch(4, None).is_err());
    assert_eq!(counters.lock().unwrap().live, 0);
}
