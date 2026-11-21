// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::any::Any;
use std::env;
use std::panic::{self, AssertUnwindSafe};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use parking_lot::Mutex;
use vortex_session::registry::CachedId;

use crate::dtype::extension::ForeignExtDType;
use crate::dtype::{DType, Nullability, PType, StructFields, UnionVariants};
use crate::memory::HostMemoryReservation;

const DEPTH: usize = 8192;
const STACK_BYTES: usize = 128 * 1024;
const KINDS: [&str; 6] = ["list", "fixed_list", "struct", "map", "union", "extension"];

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counted_leaf(drops: &Arc<AtomicUsize>) -> DType {
    DType::Struct(
        StructFields::from_reserved_fields(
            Default::default(),
            Vec::new(),
            HostMemoryReservation::new(DropCount(Arc::clone(drops))),
        ),
        Nullability::NonNullable,
    )
}

fn wrap(kind: &str, child: DType) -> DType {
    match kind {
        "list" => DType::List(Arc::new(child), Nullability::NonNullable),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 2, Nullability::NonNullable),
        "struct" => DType::Struct(
            StructFields::from_iter([("child", child)]),
            Nullability::NonNullable,
        ),
        "map" => DType::map(
            DType::Primitive(PType::I64, Nullability::NonNullable),
            child,
            false,
            Nullability::NonNullable,
        )
        .unwrap(),
        "union" => DType::Union(
            UnionVariants::new(["child"].into(), vec![child]).unwrap(),
            Nullability::NonNullable,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.deep_cleanup");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![7], child).unwrap())
        }
        _ => panic!("unknown test shape"),
    }
}

fn chain(kind: &str, depth: usize, drops: &Arc<AtomicUsize>) -> DType {
    (0..depth).fold(counted_leaf(drops), |child, index| {
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
        drop(chain(case, DEPTH, &drops));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        return;
    }
    match case {
        "detached" => {
            for kind in KINDS {
                let drops = Arc::new(AtomicUsize::new(0));
                let root = chain(kind, DEPTH, &drops);
                let child = root
                    .try_child_dtypes()
                    .nth(usize::from(kind == "map"))
                    .unwrap()
                    .unwrap()
                    .clone();
                drop(root);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(child);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }
        "weak" => {
            let shared = Arc::new(chain("mixed", DEPTH, &drops));
            let weak = Arc::downgrade(&shared);
            let root = DType::List(shared, Nullability::NonNullable);
            drop(root);
            assert!(weak.upgrade().is_none());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "concurrent" => {
            for kind in KINDS.into_iter().cycle().take(24) {
                let drops = Arc::new(AtomicUsize::new(0));
                let root = chain(kind, DEPTH, &drops);
                let roots = [root.clone(), root];
                let barrier = Arc::new(Barrier::new(2));
                let workers = roots.map(|root| {
                    let barrier = Arc::clone(&barrier);
                    thread::Builder::new()
                        .stack_size(STACK_BYTES)
                        .spawn(move || {
                            barrier.wait();
                            drop(root);
                        })
                        .unwrap()
                });
                for worker in workers {
                    worker.join().unwrap();
                }
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }
        "raw_container" => {
            for kind in KINDS {
                let drops = Arc::new(AtomicUsize::new(0));
                let root = chain(kind, DEPTH, &drops);
                let retained: Box<dyn Any> = match &root {
                    DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                        Box::new(Arc::clone(child))
                    }
                    DType::Struct(fields, _) => Box::new(fields.clone()),
                    DType::Map(fields, _) => Box::new(fields.clone()),
                    DType::Union(fields, _) => Box::new(fields.clone()),
                    DType::Extension(extension) => Box::new(extension.clone()),
                    _ => unreachable!("constructed compound dtype"),
                };
                drop(root);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(retained);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
        }
        "shared_tree" => {
            let mut root = chain("mixed", DEPTH, &drops);
            for _ in 0..128 {
                root = DType::Struct(
                    StructFields::from_iter([("left", root.clone()), ("right", root)]),
                    Nullability::NonNullable,
                );
            }
            drop(root);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "partial" => {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                StructFields::new(
                    ["one"].into(),
                    vec![chain("mixed", DEPTH, &drops), chain("list", DEPTH, &drops)],
                )
            }));
            assert!(result.is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
        "unwind" => {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let _root = chain("mixed", DEPTH, &drops);
                panic!("intentional construction unwind");
            }));
            assert!(result.is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        _ => panic!("unknown cleanup case"),
    }
}

// A stack overflow aborts a process. Isolate each case so it becomes a normal
// test failure, retaining its label and output, rather than losing the suite.
#[test]
fn deep_cleanup_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_DTYPE_CLEANUP_CHILD_CASE";
    const COMPLETED: &str = "SHARDLOOM_DTYPE_CLEANUP_COMPLETED";
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
        "detached",
        "weak",
        "concurrent",
        "raw_container",
        "shared_tree",
        "partial",
        "unwind",
    ]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "dtype::release::tests::deep_cleanup_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "cleanup case {case}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETED} {case}")),
            "child test did not execute cleanup case {case}",
        );
    }
}

struct OrderedCredit {
    id: usize,
    released: Arc<Mutex<Vec<usize>>>,
}

impl Drop for OrderedCredit {
    fn drop(&mut self) {
        self.released.lock().push(self.id);
    }
}

fn ordered_node(id: usize, children: Vec<DType>, released: &Arc<Mutex<Vec<usize>>>) -> DType {
    DType::Struct(
        StructFields::from_reserved_fields(
            (0..children.len()).map(|index| index.to_string()).collect(),
            children.into_iter().map(Into::into).collect(),
            HostMemoryReservation::new(OrderedCredit {
                id,
                released: Arc::clone(released),
            }),
        ),
        Nullability::NonNullable,
    )
}

#[test]
fn metadata_credit_outlives_descendants_in_field_order() {
    let released = Arc::new(Mutex::new(Vec::new()));
    let root = ordered_node(
        4,
        vec![
            ordered_node(1, vec![], &released),
            ordered_node(3, vec![ordered_node(2, vec![], &released)], &released),
        ],
        &released,
    );
    drop(root);
    assert_eq!(*released.lock(), [1, 2, 3, 4]);
}

#[test]
fn panicking_owner_still_releases_descendants_before_ancestor_credit() {
    struct PanickingCredit(Arc<Mutex<Vec<usize>>>);
    impl Drop for PanickingCredit {
        fn drop(&mut self) {
            self.0.lock().push(1);
            panic!("intentional credit-owner panic");
        }
    }
    let released = Arc::new(Mutex::new(Vec::new()));
    let leaf = DType::Struct(
        StructFields::from_reserved_fields(
            Default::default(),
            Vec::new(),
            HostMemoryReservation::new(PanickingCredit(Arc::clone(&released))),
        ),
        Nullability::NonNullable,
    );
    let root = ordered_node(
        4,
        vec![
            ordered_node(2, vec![leaf], &released),
            ordered_node(3, vec![], &released),
        ],
        &released,
    );
    let result = panic::catch_unwind(AssertUnwindSafe(|| drop(root)));
    assert!(result.is_err());
    assert_eq!(*released.lock(), [1, 2, 3, 4]);
}
