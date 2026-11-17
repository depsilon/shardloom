//! Original-order parity and private deep-key ownership proof.

use super::*;
use crate::local_primitives::native_relational_keys::Preparation;
use shardloom_core::FloatPolicy;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{
    cmp::Ordering,
    hash::Hasher as _,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayParts, ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{
            BoolArray, ChunkedArray, ConstantArray, DictArray, FixedSizeListArray, ListViewArray,
            PrimitiveArray, StructArray, VarBin, VarBinArray,
        },
        dtype::{FieldNames, Nullability},
        scalar::Scalar,
        validity::Validity,
    },
    buffer::Buffer,
    session::VortexSession,
};

fn record(names: &[&str], children: Vec<ArrayRef>, rows: usize, validity: Validity) -> ArrayRef {
    StructArray::new(
        FieldNames::from_iter(names.iter().copied()),
        children,
        rows,
        validity,
    )
    .into_array()
}

fn list(elements: ArrayRef, offsets: &[u32], sizes: &[u32], validity: Validity) -> ArrayRef {
    ListViewArray::new(
        elements,
        PrimitiveArray::from_iter(offsets.iter().copied()).into_array(),
        PrimitiveArray::from_iter(sizes.iter().copied()).into_array(),
        validity,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .into_array()
}

fn fixture() -> ArrayRef {
    let numbers = record(
        &["float", "flag"],
        vec![
            PrimitiveArray::from_iter([f64::NAN, -0.0, 0.0, 3.0]).into_array(),
            BoolArray::from_iter([true, false, false, true]).into_array(),
        ],
        4,
        Validity::from_iter([false, true, true, true]),
    );
    let lists = list(
        VarBinArray::from(vec![Some("α"), None, Some("東京"), Some("last")]).into_array(),
        &[0, 1, 1, 0],
        &[4, 2, 2, 0],
        Validity::from_iter([false, true, true, true]),
    );
    let fixed = FixedSizeListArray::new(
        PrimitiveArray::from_option_iter([None, Some(1i64), Some(1), Some(2)]).into_array(),
        1,
        Validity::NonNullable,
        4,
    )
    .into_array();
    record(
        &["list", "n:;", "fixed"],
        vec![lists, numbers, fixed],
        4,
        Validity::from_iter([false, true, true, true]),
    )
}

enum Bound {
    Current(Column),
    Original(reference::Column),
}

impl Bound {
    fn new(
        array: &ArrayRef,
        original: bool,
        memory: &LiveMemoryPool,
        token: &CancellationToken,
        purpose: Preparation,
        policy: FloatPolicy,
    ) -> Result<Self> {
        let mut ctx = VortexSession::default().create_execution_ctx();
        if original {
            reference::Column::new(array, &mut ctx, memory, token, None, purpose, policy)
                .map(Self::Original)
        } else {
            Column::new(array, &mut ctx, memory, token, None, purpose, policy).map(Self::Current)
        }
    }

    fn compare(&self, row: usize, other: &Self, other_row: usize) -> Result<Ordering> {
        match (self, other) {
            (Self::Original(a), Self::Original(b)) => a.compare_at(row, b, other_row),
            (Self::Current(a), Self::Current(b)) => a.compare_at(row, b, other_row),
            _ => unreachable!("each observation has one implementation"),
        }
    }

    fn validate(&self, row: usize) -> Result<()> {
        match self {
            Self::Original(a) => a.validate_at(row),
            Self::Current(a) => a.validate_at(row),
        }
    }

    fn hash(&self, row: usize) -> (std::result::Result<bool, String>, u64) {
        let mut hash = rustc_hash::FxHasher::default();
        let res = match self {
            Self::Original(a) => a.hash_into(row, &mut hash),
            Self::Current(a) => a.hash_into(row, &mut hash),
        };
        (res.map_err(|e| e.to_string()), hash.finish())
    }

    fn write(&self, row: usize, out: &mut impl std::fmt::Write) -> Result<()> {
        match self {
            Self::Original(a) => a.write_exact_key(row, out),
            Self::Current(a) => a.write_exact_key(row, out),
        }
    }
}

fn error<T>(result: Result<T>) -> std::result::Result<T, String> {
    result.map_err(|e| e.to_string())
}

#[test]
fn nested_key_continuations_preserve_original_values_hashes_and_error_precedence() {
    let input = fixture();
    let chunked = ChunkedArray::try_new(
        vec![input.slice(0..2).unwrap(), input.slice(2..4).unwrap()],
        input.dtype().clone(),
    )
    .unwrap()
    .into_array();
    let dictionary = DictArray::try_new(
        PrimitiveArray::from_iter([3u8, 0, 2, 1]).into_array(),
        input.clone(),
    )
    .unwrap()
    .into_array();
    let different = record(
        &["other"],
        vec![PrimitiveArray::from_iter([1i32, 2, 3, 4]).into_array()],
        4,
        Validity::NonNullable,
    );
    let inputs = [
        input.clone(),
        input.slice(1..3).unwrap(),
        input.slice(0..0).unwrap(),
        chunked,
        dictionary,
        different,
        record(&[], vec![], 2, Validity::NonNullable),
    ];
    for purpose in [
        Preparation::Values,
        Preparation::Hash,
        Preparation::HashOnDemand,
    ] {
        for policy in [FloatPolicy::Strict, FloatPolicy::Extended] {
            let memory = LiveMemoryPool::new(16 << 20).unwrap();
            let token = CancellationToken::default();
            let current = inputs
                .iter()
                .map(|a| Bound::new(a, false, &memory, &token, purpose, policy).unwrap())
                .collect::<Vec<_>>();
            let original = inputs
                .iter()
                .map(|a| Bound::new(a, true, &memory, &token, purpose, policy).unwrap())
                .collect::<Vec<_>>();
            let retained = memory.snapshot().reserved_bytes;
            for (index, a) in inputs.iter().enumerate() {
                for row in 0..=a.len() {
                    assert_eq!(
                        error(current[index].validate(row)),
                        error(original[index].validate(row))
                    );
                    assert_eq!(current[index].hash(row), original[index].hash(row));
                    let (mut out, mut old_out) = (String::new(), String::new());
                    assert_eq!(
                        error(current[index].write(row, &mut out)),
                        error(original[index].write(row, &mut old_out))
                    );
                    assert_eq!(out, old_out);
                    for (other, b) in inputs.iter().enumerate() {
                        for other_row in 0..=b.len() {
                            assert_eq!(
                                error(current[index].compare(row, &current[other], other_row)),
                                error(original[index].compare(row, &original[other], other_row)),
                                "input {index}/{other}, row {row}/{other_row}",
                            );
                        }
                    }
                }
            }
            assert_eq!(memory.snapshot().reserved_bytes, retained);
            token.cancel();
            for (current, original) in current.iter().zip(&original) {
                assert_eq!(
                    error(current.validate(usize::MAX)),
                    error(original.validate(usize::MAX))
                );
                assert_eq!(current.hash(0), original.hash(0));
                assert_eq!(
                    error(current.compare(0, current, 0)),
                    error(original.compare(0, original, 0))
                );
            }
            drop((current, original));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn nested_key_continuations_preserve_short_circuit_and_list_prefix_order() {
    let invalid = record(
        &["first", "late"],
        vec![
            PrimitiveArray::from_iter([1i64, 2]).into_array(),
            PrimitiveArray::from_iter([f64::NAN, f64::NAN]).into_array(),
        ],
        2,
        Validity::NonNullable,
    );
    for original in [true, false] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let key = Bound::new(
            &invalid,
            original,
            &memory,
            &CancellationToken::default(),
            Preparation::Hash,
            FloatPolicy::Strict,
        )
        .unwrap();
        assert_eq!(key.compare(0, &key, 1).unwrap(), Ordering::Less);
        assert!(key.validate(0).is_err());
        assert!(key.hash(0).0.is_err());
        drop(key);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    for (a, b, expected) in [
        (vec![1i64], vec![1i64, 2], Ordering::Less),
        (vec![3], vec![1, 2], Ordering::Greater),
    ] {
        let left = list(
            PrimitiveArray::from_iter(a.iter().copied()).into_array(),
            &[0],
            &[u32::try_from(a.len()).unwrap()],
            Validity::NonNullable,
        );
        let right = list(
            PrimitiveArray::from_iter(b.iter().copied()).into_array(),
            &[0],
            &[u32::try_from(b.len()).unwrap()],
            Validity::NonNullable,
        );
        for original in [true, false] {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let token = CancellationToken::default();
            let left = Bound::new(
                &left,
                original,
                &memory,
                &token,
                Preparation::Hash,
                FloatPolicy::Strict,
            )
            .unwrap();
            let right = Bound::new(
                &right,
                original,
                &memory,
                &token,
                Preparation::Hash,
                FloatPolicy::Strict,
            )
            .unwrap();
            assert_eq!(left.compare(0, &right, 0).unwrap(), expected);
            drop((left, right));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Error(usize),
    Cancel(usize),
    Panic(usize),
}

struct Writer {
    calls: Vec<String>,
    token: CancellationToken,
    fault: Fault,
}

impl std::fmt::Write for Writer {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.calls.push(value.into());
        match self.fault {
            Fault::Error(at) if at == self.calls.len() => return Err(std::fmt::Error),
            Fault::Cancel(at) if at == self.calls.len() => self.token.cancel(),
            Fault::Panic(at) if at == self.calls.len() => panic!("nested key writer"),
            _ => {}
        }
        Ok(())
    }
}

fn written(
    input: &ArrayRef,
    original: bool,
    fault: Fault,
) -> (Vec<String>, std::result::Result<(), String>) {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let token = CancellationToken::default();
    let key = Bound::new(
        input,
        original,
        &memory,
        &token,
        Preparation::Values,
        FloatPolicy::Strict,
    )
    .unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let mut writer = Writer {
        calls: Vec::new(),
        token,
        fault,
    };
    let result = catch_unwind(AssertUnwindSafe(|| key.write(1, &mut writer))).map_or_else(
        |p| {
            assert_eq!(p.downcast_ref::<&str>(), Some(&"nested key writer"));
            Err("writer panic".into())
        },
        error,
    );
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(key);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    (writer.calls, result)
}

#[test]
fn nested_key_continuations_keep_exact_writer_order_failure_and_cancellation() {
    let input = fixture();
    let expected = written(&input, true, Fault::None);
    assert_eq!(written(&input, false, Fault::None), expected);
    assert!(expected.1.is_ok());
    for at in 1..=expected.0.len() {
        for fault in [Fault::Error(at), Fault::Cancel(at)] {
            assert_eq!(written(&input, false, fault), written(&input, true, fault));
        }
    }
    assert_eq!(
        written(&input, false, Fault::Panic(9)),
        written(&input, true, Fault::Panic(9))
    );
    // Fail after the inline prefix has spilled, with live child and parent
    // frames. The same original writer calls must precede every exit.
    for shape in 0..4 {
        let input = deep(32, shape);
        let expected = written(&input, true, Fault::None);
        assert_eq!(written(&input, false, Fault::None), expected);
        for at in [expected.0.len() / 2, expected.0.len()] {
            for fault in [Fault::Error(at), Fault::Cancel(at), Fault::Panic(at)] {
                assert_eq!(written(&input, false, fault), written(&input, true, fault));
            }
        }
    }
}

fn wrap(mut array: ArrayRef, depth: usize, shape: usize) -> ArrayRef {
    for level in 0..depth {
        array = match if shape == 3 { level % 3 } else { shape } {
            0 => record(
                &["child", "tail"],
                vec![array, PrimitiveArray::from_iter([1u8, 1, 1]).into_array()],
                3,
                Validity::NonNullable,
            ),
            1 => list(array, &[0, 1, 2], &[1, 1, 1], Validity::NonNullable),
            _ => FixedSizeListArray::new(array, 1, Validity::NonNullable, 3).into_array(),
        };
    }
    array
}

fn deep(depth: usize, shape: usize) -> ArrayRef {
    wrap(
        PrimitiveArray::from_iter([7i64, 7, 9]).into_array(),
        depth,
        shape,
    )
}

#[test]
fn nested_key_continuations_handle_deep_native_owners_and_final_release() {
    std::thread::Builder::new()
        .stack_size(128 << 10)
        .spawn(|| {
            for shape in 0..4 {
                let memory = LiveMemoryPool::new(64 << 20).unwrap();
                let payload = Arc::new(memory.reserve(24).unwrap());
                let witness = Arc::downgrade(&payload);
                let buffer = Buffer::from(vec![7i64, 7, 9]).with_retained_owner(payload);
                let array = wrap(
                    PrimitiveArray::new(buffer, Validity::NonNullable).into_array(),
                    8192,
                    shape,
                );
                let token = CancellationToken::default();
                let key = Bound::new(
                    &array,
                    false,
                    &memory,
                    &token,
                    Preparation::Hash,
                    FloatPolicy::Strict,
                )
                .unwrap();
                drop(array);
                assert!(witness.upgrade().is_some());
                let retained = memory.snapshot().reserved_bytes;
                assert_eq!(key.compare(0, &key, 1).unwrap(), Ordering::Equal);
                assert_eq!(key.compare(0, &key, 2).unwrap(), Ordering::Less);
                key.validate(0).unwrap();
                assert_eq!(key.hash(0), key.hash(1));
                assert!(key.hash(0).0.unwrap());
                assert_ne!(key.hash(0).1, key.hash(2).1);
                let (mut a, mut b) = (String::new(), String::new());
                key.write(0, &mut a).unwrap();
                key.write(1, &mut b).unwrap();
                assert_eq!(a, b);
                assert_eq!(memory.snapshot().reserved_bytes, retained);
                token.cancel();
                assert!(key.validate(0).is_err());
                assert!(key.hash(0).0.is_err());
                assert!(key.compare(0, &key, 0).is_err());
                assert!(key.write(0, &mut a).is_err());
                drop(key);
                assert!(witness.upgrade().is_none());
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn nested_key_continuations_refund_binding_and_operation_growth_refusals() {
    let input = deep(32, 0);
    for budget in [1, 1024, 4096, 16384] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        assert!(
            Bound::new(
                &input,
                false,
                &memory,
                &CancellationToken::default(),
                Preparation::Hash,
                FloatPolicy::Strict
            )
            .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(memory.snapshot().denied_reservations > 0);
    }
    let memory = LiveMemoryPool::configurable(16 << 20).unwrap();
    let token = CancellationToken::default();
    let key = Bound::new(
        &input,
        false,
        &memory,
        &token,
        Preparation::Hash,
        FloatPolicy::Strict,
    )
    .unwrap();
    let retained = memory.snapshot().reserved_bytes;
    let pressure = memory.reserve((16 << 20) - retained).unwrap();
    assert!(key.compare(0, &key, 1).is_err());
    assert!(key.validate(0).is_err());
    assert!(key.hash(0).0.is_err());
    let mut out = String::new();
    assert!(key.write(0, &mut out).is_err());
    assert_ne!(out, "");
    assert_eq!(memory.snapshot().reserved_bytes, 16 << 20);
    drop(pressure);
    key.validate(0).unwrap();
    for operation in 0..4 {
        memory.set_limit_bytes(retained + 1024).unwrap();
        let denied_before = memory.snapshot().denied_reservations;
        let result = match operation {
            0 => key.compare(0, &key, 1).map(|_| ()),
            1 => key.validate(0),
            2 => key
                .hash(0)
                .0
                .map(|_| ())
                .map_err(shardloom_core::ShardLoomError::InvalidOperation),
            _ => key.write(0, &mut String::new()),
        };
        assert!(result.is_err());
        let observed = memory.snapshot();
        assert!(observed.denied_reservations > denied_before);
        assert!(
            observed.allocation_peak_reserved_bytes > retained,
            "the first heap capacity was admitted before replacement refusal"
        );
        assert_eq!(observed.reserved_bytes, retained);
    }
    memory.set_limit_bytes(16 << 20).unwrap();
    drop(key);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    // Public admission stays bounded until the complete native lifecycle closes.
    let mut ctx = VortexSession::default().create_execution_ctx();
    assert!(KeyColumn::new(&input, &mut ctx, &memory, &CancellationToken::default()).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn nested_key_continuations_keep_first_provider_error_and_release_partial_owners() {
    let binary = VarBinArray::from(vec![&b"good"[..], &b"\xff"[..], &b"last"[..]]);
    let slots = binary.slots().iter().cloned().collect();
    let invalid = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        3,
        binary.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let unsupported =
        ConstantArray::new(Scalar::null(DType::Variant(Nullability::Nullable)), 3).into_array();
    for utf8_first in [true, false] {
        let children = if utf8_first {
            vec![invalid.clone(), unsupported.clone()]
        } else {
            vec![unsupported.clone(), invalid.clone()]
        };
        for validity in [Validity::NonNullable, Validity::AllInvalid] {
            let array = wrap(
                record(&["first", "second"], children.clone(), 3, validity),
                6,
                3,
            );
            let mut previous = None;
            for original in [true, false] {
                let memory = LiveMemoryPool::new(16 << 20).unwrap();
                let token = CancellationToken::default();
                let failure = Bound::new(
                    &array,
                    original,
                    &memory,
                    &token,
                    Preparation::Hash,
                    FloatPolicy::Strict,
                )
                .err()
                .expect("invalid nested key must fail")
                .to_string();
                if utf8_first {
                    assert!(failure.contains("utf-8"), "{failure}");
                } else {
                    assert!(failure.contains("key requires integer"), "{failure}");
                }
                if let Some(expected) = previous.replace(failure) {
                    assert_eq!(previous.as_ref(), Some(&expected));
                }
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                token.cancel();
                assert!(
                    Bound::new(
                        &array,
                        original,
                        &memory,
                        &token,
                        Preparation::Hash,
                        FloatPolicy::Strict
                    )
                    .is_err()
                );
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}
