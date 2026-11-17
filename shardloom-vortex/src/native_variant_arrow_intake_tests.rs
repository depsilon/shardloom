//! Arrow Variant validation on borrowed original row coordinates.

use super::{failed, reference, validate, walk};
use arrow_array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, GenericListArray, Int8Array, Int16Array,
    Int64Array, OffsetSizeTrait, StructArray, UInt64Array,
    types::{Int8Type, Int16Type},
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use parquet_variant::{Variant, VariantBuilder};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

fn fields(children: &[(&str, ArrayRef)], valid: Option<Vec<bool>>) -> ArrayRef {
    let fields = children
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), *name != "metadata"))
        .collect();
    Arc::new(StructArray::new(
        fields,
        children.iter().map(|(_, a)| a.clone()).collect(),
        valid.map(NullBuffer::from),
    ))
}

fn binary(values: &[Option<&[u8]>]) -> ArrayRef {
    Arc::new(BinaryArray::from_opt_vec(values.to_vec()))
}

fn shell(child: ArrayRef) -> ArrayRef {
    fields(&[("typed_value", child)], None)
}

fn list<O: OffsetSizeTrait>(
    values: ArrayRef,
    lengths: impl IntoIterator<Item = usize>,
) -> ArrayRef {
    Arc::new(GenericListArray::<O>::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::from_lengths(lengths),
        values,
        None,
    ))
}

fn metadata(names: &[&str]) -> Vec<u8> {
    VariantBuilder::new()
        .with_field_names(names.iter().copied())
        .with_value(Variant::Null)
        .finish()
        .0
}

#[derive(Default, Clone, Copy)]
enum Fault {
    #[default]
    None,
    Cancel(usize),
    Panic(usize),
}

fn observe<T>(
    with_memory: bool,
    fault: Fault,
    run: impl FnOnce(Option<&LiveMemoryPool>, &dyn Fn() -> Result<()>) -> Result<T>,
) -> (std::result::Result<T, String>, usize) {
    let memory = LiveMemoryPool::new(16 << 20).unwrap();
    let calls = Cell::new(0);
    let check = || {
        calls.set(calls.get() + 1);
        match fault {
            Fault::Cancel(stop) if calls.get() == stop => Err(failed("intake test cancelled")),
            Fault::Panic(stop) if calls.get() == stop => panic!("intake test callback"),
            _ => Ok(()),
        }
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        run(with_memory.then_some(&memory), &check)
    }))
    .map_or_else(
        |panic| {
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"intake test callback"));
            Err("callback panic".into())
        },
        |res| res.map_err(|err| err.to_string()),
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    (result, calls.get())
}

fn compare_parts(
    meta: &[u8],
    raw: Option<&ArrayRef>,
    typed: Option<&ArrayRef>,
    row: usize,
    with_memory: bool,
) -> std::result::Result<bool, String> {
    let old = observe(with_memory, Fault::None, |pool, check| {
        reference::parts(meta, raw, typed, row, pool, check, 0)
    });
    assert_eq!(
        observe(with_memory, Fault::None, |pool, check| walk::parts(
            meta, raw, typed, row, pool, check, 24
        )),
        old
    );
    old.0
}

#[test]
fn arrow_variant_intake_parts_preserve_residuals_nulls_slices_and_first_errors() {
    let mut builder = VariantBuilder::new().with_field_names(["a", "b"]);
    let mut object = builder.new_object();
    object.insert("b", Variant::Int32(13));
    object.finish();
    let (meta, bytes) = builder.finish();
    let raw = binary(&[Some(&bytes), None, Some(&[0])]);
    let absent = metadata(&[]);
    let typed: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let bad: ArrayRef = Arc::new(UInt64Array::from(vec![7, 8, 9]));
    let good = shell(typed.clone());
    let wrong_shell = fields(&[("bad", typed.clone())], Some(vec![false, true, true]));
    let first = fields(&[("a", good.clone()), ("b", good.clone())], None);
    let invalid = fields(
        &[("a", good.clone()), ("unknown", wrong_shell.clone())],
        None,
    );
    // The second visible list row reaches the last shell. Its index is relative
    // to the values array, even when both the outer list and its child are sliced.
    let packed = fields(
        &[("typed_value", bad.clone())],
        Some(vec![true, false, true]),
    );
    let mut lists = Vec::new();
    for large in [false, true] {
        let child = packed.slice(1, 2);
        let full = if large {
            list::<i64>(child, [0, 1, 1])
        } else {
            list::<i32>(child, [0, 1, 1])
        };
        lists.push(full.slice(1, 2));
    }
    for with_memory in [false, true] {
        for typed in [&typed, &bad, &first, &invalid, &wrong_shell] {
            for row in 0..3 {
                for raw in [None, Some(&raw)] {
                    for meta in [&meta, &absent] {
                        let _ = compare_parts(meta, raw, Some(typed), row, with_memory);
                    }
                }
            }
        }
        for array in &lists {
            assert_eq!(
                compare_parts(&meta, None, Some(array), 0, with_memory),
                Ok(true)
            );
            assert!(
                compare_parts(&meta, None, Some(array), 1, with_memory)
                    .unwrap_err()
                    .contains("unsupported shredded physical dtype")
            );
        }
        assert!(
            compare_parts(&meta, Some(&raw), Some(&first), 0, with_memory)
                .unwrap_err()
                .contains("overlap")
        );
        assert_eq!(compare_parts(&meta, None, None, 0, with_memory), Ok(false));
        assert!(
            compare_parts(&absent, None, Some(&first), 0, with_memory)
                .unwrap_err()
                .contains("absent")
        );
    }
}

fn dictionary_input(meta: &[u8], bad: bool) -> ArrayRef {
    let data = fields(
        &[
            ("metadata", binary(&[Some(meta), Some(meta), Some(meta)])),
            (
                "value",
                binary(&[Some(&[0]), Some(if bad { &[3] } else { &[0] }), None]),
            ),
            (
                "typed_value",
                Arc::new(Int64Array::from(vec![Some(0), None, Some(3)])),
            ),
        ],
        Some(vec![false, true, true]),
    );
    let inner = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), Some(2), None, Some(0)]),
        data,
    )
    .unwrap();
    Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![Some(0), None, Some(3), Some(2), Some(1), Some(0)]),
            Arc::new(inner),
        )
        .unwrap()
        .slice(1, 5),
    )
}

#[test]
fn arrow_variant_intake_dictionary_rows_preserve_callback_order_and_cleanup() {
    let meta = metadata(&["typed_value"]);
    for bad in [false, true] {
        let data = dictionary_input(&meta, bad);
        for with_memory in [false, true] {
            let both = |fault| {
                let old = observe(with_memory, fault, |memory, check| {
                    reference::validate(data.as_ref(), 0, 5, memory, check)
                });
                assert_eq!(
                    observe(with_memory, fault, |memory, check| validate(
                        data.as_ref(),
                        0,
                        5,
                        memory,
                        check
                    )),
                    old
                );
                old
            };
            let (res, calls) = both(Fault::None);
            assert_eq!(res.is_err(), bad, "{res:?}");
            for stop in 1..=calls {
                assert!(
                    both(Fault::Cancel(stop))
                        .0
                        .unwrap_err()
                        .contains("test cancelled")
                );
            }
            assert_eq!(both(Fault::Panic(calls)).0.unwrap_err(), "callback panic");
        }
    }
}

/// Arrow arrays and fields have their own last-owner behavior. Keep all caller
/// owners alive during this borrowed validation test and release parents first.
struct Fixture(Vec<ArrayRef>);
impl Drop for Fixture {
    fn drop(&mut self) {
        while self.0.pop().is_some() {}
    }
}

impl Fixture {
    fn new(leaf: ArrayRef, shape: usize, depth: usize) -> Self {
        let mut fixture = Self(vec![leaf]);
        for level in 0..depth {
            let shell = shell(fixture.root().clone());
            fixture.0.push(shell.clone());
            let parent = match if shape == 3 { level % 3 } else { shape } {
                0 => fields(&[("a", shell)], None),
                1 => list::<i32>(shell, [1]),
                _ => list::<i64>(shell, [1]),
            };
            fixture.0.push(parent);
        }
        fixture
    }

    fn root(&self) -> &ArrayRef {
        self.0.last().unwrap()
    }
}

fn deep_tree() {
    let meta = metadata(&["a", "z"]);
    let memory = LiveMemoryPool::new(128 << 20).unwrap();
    for shape in 0..4 {
        let input = Fixture::new(Arc::new(Int64Array::from(vec![1])), shape, 8192);
        let run = |budget, check: &dyn Fn() -> Result<()>, depth| {
            walk::parts(&meta, None, Some(input.root()), 0, budget, check, depth)
        };
        assert!(run(Some(&memory), &|| Ok(()), usize::MAX).unwrap());
        assert!(
            run(None, &|| Ok(()), 24)
                .unwrap_err()
                .to_string()
                .contains("depth boundary")
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        if shape == 3 {
            let denied = LiveMemoryPool::new(4096).unwrap();
            assert!(
                run(Some(&denied), &|| Ok(()), usize::MAX)
                    .unwrap_err()
                    .to_string()
                    .contains("reservation denied")
            );
            assert_eq!(denied.snapshot().reserved_bytes, 0);
            let calls = Cell::new(0);
            let failure = run(
                Some(&memory),
                &|| {
                    calls.set(calls.get() + 1);
                    if calls.get() == 4096 {
                        return Err(failed("deep cancellation"));
                    }
                    Ok(())
                },
                usize::MAX,
            )
            .unwrap_err();
            assert!(failure.to_string().contains("deep cancellation"));
            assert_eq!(calls.get(), 4096);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            calls.set(0);
            let panic = catch_unwind(AssertUnwindSafe(|| {
                run(
                    Some(&memory),
                    &|| {
                        calls.set(calls.get() + 1);
                        assert_ne!(calls.get(), 4096, "deep intake callback");
                        Ok(())
                    },
                    usize::MAX,
                )
            }));
            assert!(panic.is_err());
            assert_eq!(calls.get(), 4096);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            let invalid: ArrayRef = Arc::new(UInt64Array::from(vec![1]));
            let late = fields(
                &[
                    ("a", shell(input.root().clone())),
                    ("z", shell(invalid.clone())),
                ],
                None,
            );
            assert!(
                walk::parts(
                    &meta,
                    None,
                    Some(&late),
                    0,
                    Some(&memory),
                    &|| Ok(()),
                    usize::MAX
                )
                .unwrap_err()
                .to_string()
                .contains("unsupported shredded physical dtype")
            );
            let bad = Fixture::new(invalid, shape, 8192);
            assert!(
                walk::parts(
                    &meta,
                    None,
                    Some(bad.root()),
                    0,
                    Some(&memory),
                    &|| Ok(()),
                    usize::MAX
                )
                .unwrap_err()
                .to_string()
                .contains("unsupported shredded physical dtype")
            );
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn arrow_variant_intake_deep_borrowed_values_and_credit_release_on_small_stack() {
    const CHILD: &str = "SHARDLOOM_DEEP_BORROWED_VARIANT_INTAKE";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(deep_tree)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            concat!(
                module_path!(),
                "::arrow_variant_intake_deep_borrowed_values_and_credit_release_on_small_stack"
            )
            .strip_prefix("shardloom_vortex::")
            .unwrap(),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
