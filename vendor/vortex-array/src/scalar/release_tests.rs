// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::panic::{self, AssertUnwindSafe};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;

use bytes::Bytes;
use parking_lot::Mutex;
use vortex_buffer::{Alignment, BufferString, ByteBuffer};

use crate::IntoArray;
use crate::arrays::ConstantArray;
use crate::dtype::{DType, Nullability, StructFields, UnionVariants};
use crate::expr::stats::{Precision, Stat};
use crate::memory::HostMemoryReservation;
use crate::scalar::{Scalar, ScalarValue, UnionValue};
use crate::stats::StatsSet;

const DEPTH: usize = 8192;
const STACK_BYTES: usize = 128 * 1024;
const KINDS: [&str; 3] = ["tuple", "union", "variant"];

struct CountedBuffer {
    bytes: [u8; 4],
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for CountedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for CountedBuffer {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn counted_buffer(drops: &Arc<AtomicUsize>) -> ByteBuffer {
    ByteBuffer::from_bytes_aligned(
        Bytes::from_owner(CountedBuffer {
            bytes: *b"leaf",
            drops: Arc::clone(drops),
        }),
        Alignment::none(),
    )
}

fn leaf(drops: &Arc<AtomicUsize>) -> Scalar {
    Scalar {
        value: Some(ScalarValue::Binary(counted_buffer(drops))),
        dtype: DType::Binary(Nullability::Nullable),
    }
}

fn wrap(kind: &str, child: Scalar) -> Scalar {
    // Construct type/value-consistent fixtures in the defining module without
    // calling the still-recursive public validator. This test proves cleanup,
    // not deep validation, comparison, formatting, cloning or serialization.
    match kind {
        "tuple" => {
            let (dtype, value) = child.into_parts();
            Scalar {
                value: Some(ScalarValue::Tuple(vec![None, value, None])),
                dtype: DType::List(Arc::new(dtype), Nullability::Nullable),
            }
        }
        "union" => {
            let (dtype, value) = child.into_parts();
            Scalar {
                value: Some(ScalarValue::Union(UnionValue::new(0, value))),
                dtype: DType::Union(
                    UnionVariants::new(["value"].into(), vec![dtype]).unwrap(),
                    Nullability::Nullable,
                ),
            }
        }
        "variant" => Scalar {
            value: Some(ScalarValue::Variant(Box::new(child))),
            dtype: DType::Variant(Nullability::Nullable),
        },
        _ => panic!("unknown scalar cleanup shape"),
    }
}

fn chain(kind: &str, drops: &Arc<AtomicUsize>) -> Scalar {
    (0..DEPTH).fold(leaf(drops), |child, index| {
        wrap(
            if kind == "mixed" {
                KINDS[index % KINDS.len()]
            } else {
                kind
            },
            child,
        )
    })
}

fn run_case(case: &str) {
    let drops = Arc::new(AtomicUsize::new(0));
    if KINDS.contains(&case) || case == "mixed" {
        drop(chain(case, &drops));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        return;
    }
    match case {
        "value_only" => drop(chain("mixed", &drops).into_value()),
        "parts" => {
            let (dtype, value) = chain("mixed", &drops).into_parts();
            drop(dtype);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(value);
        }
        "tuple_accessor" => {
            let root = wrap("tuple", chain("mixed", &drops));
            let value = root.into_value().unwrap();
            let ptr = value.as_list().as_ptr();
            let values = value.into_list();
            assert_eq!(values.as_ptr(), ptr);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(values);
        }
        "union_accessor" => {
            let root = wrap("union", chain("mixed", &drops));
            let value = root.into_value().unwrap().into_union();
            assert_eq!(value.type_id(), 0);
            assert!(value.child_value().is_some());
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(value);
        }
        "variant_accessor" => {
            let root = wrap("variant", chain("mixed", &drops));
            let value = root.into_value().unwrap().into_variant();
            assert!(value.value().is_some());
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(value);
        }
        "constant_and_stats" => {
            let root = ConstantArray::new(chain("mixed", &drops), 1).into_array();
            root.statistics().set(
                Stat::Min,
                Precision::Exact(chain("mixed", &drops).into_value().unwrap()),
            );
            drop(root);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            return;
        }
        "raw_stats" => {
            drop(StatsSet::of(
                Stat::Min,
                Precision::Exact(chain("mixed", &drops).into_value().unwrap()),
            ));
        }
        "wide" => {
            let mut elements = Vec::new();
            for _ in 0..512 {
                elements.push(Some(ScalarValue::Variant(Box::new(leaf(&drops)))));
                elements.push(None);
            }
            elements.push(Some(ScalarValue::Variant(Box::new(chain("mixed", &drops)))));
            let value = Scalar {
                value: Some(ScalarValue::Tuple(elements)),
                dtype: DType::List(
                    Arc::new(DType::Variant(Nullability::Nullable)),
                    Nullability::Nullable,
                ),
            };
            drop(value);
            assert_eq!(drops.load(Ordering::SeqCst), 513);
            return;
        }
        "partial" => {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let _partial: Vec<_> = (0..3)
                    .map(|index| {
                        assert!(index < 2, "intentional partial scalar construction");
                        chain("mixed", &drops)
                    })
                    .collect();
            }));
            assert!(result.is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            return;
        }
        "unwind" => {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let _value = chain("mixed", &drops);
                panic!("intentional scalar construction unwind");
            }));
            assert!(result.is_err());
        }
        _ => panic!("unknown scalar cleanup case"),
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn deep_cleanup_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_SCALAR_CLEANUP_CHILD_CASE";
    const COMPLETED: &str = "SHARDLOOM_SCALAR_CLEANUP_COMPLETED";
    if let Ok(case) = env::var(SELECTOR) {
        let completed = format!("{COMPLETED} {case}");
        thread::Builder::new()
            .stack_size(STACK_BYTES)
            .spawn(move || run_case(&case))
            .unwrap()
            .join()
            .unwrap();
        println!("\n{completed}");
        return;
    }
    for case in KINDS.into_iter().chain([
        "mixed",
        "value_only",
        "parts",
        "tuple_accessor",
        "union_accessor",
        "variant_accessor",
        "constant_and_stats",
        "raw_stats",
        "wide",
        "partial",
        "unwind",
    ]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "scalar::release::tests::deep_cleanup_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "scalar cleanup case {case}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETED} {case}")),
            "child test did not execute scalar cleanup case {case}",
        );
    }
}

#[test]
fn consuming_buffer_accessors_preserve_the_original_storage() {
    let drops = Arc::new(AtomicUsize::new(0));
    let buffer = counted_buffer(&drops);
    let ptr = buffer.as_ptr();
    let value = ScalarValue::Binary(buffer).into_binary();
    assert_eq!(value.as_ptr(), ptr);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let buffer = counted_buffer(&drops);
    let ptr = buffer.as_ptr();
    let value = ScalarValue::Utf8(BufferString::try_from(buffer).unwrap()).into_utf8();
    assert_eq!(value.as_bytes().as_ptr(), ptr);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

struct Credit {
    id: usize,
    released: Arc<Mutex<Vec<usize>>>,
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.released.lock().push(self.id);
    }
}

struct OrderedBuffer {
    bytes: [u8; 4],
    id: usize,
    credits: Vec<Weak<Credit>>,
    released: Arc<Mutex<Vec<usize>>>,
    panic: bool,
}

impl AsRef<[u8]> for OrderedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for OrderedBuffer {
    fn drop(&mut self) {
        assert!(self.credits.iter().all(|credit| credit.upgrade().is_some()));
        self.released.lock().push(self.id);
        assert!(!self.panic, "intentional scalar buffer owner panic");
    }
}

fn ordered_buffer(
    id: usize,
    credits: Vec<Weak<Credit>>,
    released: &Arc<Mutex<Vec<usize>>>,
    panic: bool,
) -> ScalarValue {
    ScalarValue::Binary(ByteBuffer::from_bytes_aligned(
        Bytes::from_owner(OrderedBuffer {
            bytes: *b"leaf",
            id,
            credits,
            released: Arc::clone(released),
            panic,
        }),
        Alignment::none(),
    ))
}

fn credited_struct(fields: Vec<(DType, ScalarValue)>, credit: Arc<Credit>) -> Scalar {
    let mut dtypes = Vec::new();
    let mut values = Vec::new();
    for (dtype, value) in fields {
        dtypes.push(dtype.into());
        values.push(Some(value));
    }
    Scalar {
        value: Some(ScalarValue::Tuple(values)),
        dtype: DType::Struct(
            StructFields::from_reserved_fields(
                (0..dtypes.len()).map(|index| index.to_string()).collect(),
                dtypes,
                HostMemoryReservation::new(credit),
            ),
            Nullability::NonNullable,
        ),
    }
}

#[test]
fn scalar_schema_credit_outlives_its_value() {
    let released = Arc::new(Mutex::new(Vec::new()));
    let credit = Arc::new(Credit {
        id: 2,
        released: Arc::clone(&released),
    });
    let value = ordered_buffer(1, vec![Arc::downgrade(&credit)], &released, false);
    let root = credited_struct(
        vec![(DType::Binary(Nullability::NonNullable), value)],
        credit,
    );
    drop(root);
    assert_eq!(*released.lock(), [1, 2]);
}

#[test]
fn variant_schema_credits_outlive_descendants_even_on_unwind() {
    for panic in [false, true] {
        let released = Arc::new(Mutex::new(Vec::new()));
        let parent = Arc::new(Credit {
            id: 4,
            released: Arc::clone(&released),
        });
        let child = Arc::new(Credit {
            id: 2,
            released: Arc::clone(&released),
        });
        let leaf = ordered_buffer(
            1,
            vec![Arc::downgrade(&parent), Arc::downgrade(&child)],
            &released,
            panic,
        );
        let child = credited_struct(vec![(DType::Binary(Nullability::NonNullable), leaf)], child);
        let sibling = ordered_buffer(3, vec![Arc::downgrade(&parent)], &released, false);
        let parent = credited_struct(
            vec![
                (
                    DType::Variant(Nullability::NonNullable),
                    ScalarValue::Variant(Box::new(child)),
                ),
                (DType::Binary(Nullability::NonNullable), sibling),
            ],
            parent,
        );
        let root = ScalarValue::Variant(Box::new(parent));
        let result = panic::catch_unwind(AssertUnwindSafe(|| drop(root)));
        assert_eq!(result.is_err(), panic);
        assert_eq!(*released.lock(), [1, 2, 3, 4]);
    }
}
