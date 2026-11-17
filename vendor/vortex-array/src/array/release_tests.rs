// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::panic::{self, AssertUnwindSafe};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Weak};
use std::thread;

use bytes::Bytes;
use parking_lot::Mutex;
use vortex_buffer::{Alignment, ByteBuffer, buffer};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::array::{ArrayParts, ForeignArray, ForeignArrayData};
use crate::arrays::{ConstantArray, ExtensionArray, FixedSizeListArray, ListArray, StructArray};
use crate::buffer::BufferHandle;
use crate::dtype::extension::ForeignExtDType;
use crate::dtype::{DType, Nullability, PType, StructFields};
use crate::memory::HostMemoryReservation;
use crate::validity::Validity;
use crate::{Array, ArrayRef, ArraySlots, IntoArray, VortexSessionExecute};

const DEPTH: usize = 8192;
const STACK_BYTES: usize = 128 * 1024;
const KINDS: [&str; 5] = ["struct", "list", "fixed_list", "extension", "opaque"];

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn foreign(dtype: DType, slots: ArraySlots, buffers: Vec<BufferHandle>) -> ArrayRef {
    static ID: CachedId = CachedId::new("test.array_cleanup");
    Array::<ForeignArray>::try_from_parts(ArrayParts::new(
        ForeignArray::new(*ID),
        dtype,
        1,
        ForeignArrayData::new(Vec::new(), buffers),
        slots,
    ))
    .unwrap()
    .into_array()
}

fn opaque(slots: impl IntoIterator<Item = Option<ArrayRef>>) -> ArrayRef {
    foreign(
        DType::Primitive(PType::I32, Nullability::NonNullable),
        slots.into_iter().collect(),
        Vec::new(),
    )
}

fn chain(kind: &str, owned: bool, drops: &Arc<AtomicUsize>) -> ArrayRef {
    let kind = kind.to_owned();
    let drops = Arc::clone(drops);
    // This test isolates destruction. Construction still uses recursive dtype
    // equality, particularly for extension storage, and has a separate proof
    // obligation before public deep admission. It runs on an explicit larger
    // stack; the returned handle is released on the small test stack.
    thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let mut child = ConstantArray::new(17i32, 1)
                .into_array()
                .with_retained_owner(Arc::new(DropCount(Arc::clone(&drops))))
                .unwrap();
            let offsets = buffer![0u32, 1].into_array();
            let mut ctx = VortexSession::empty().create_execution_ctx();
            for index in 0..DEPTH {
                child = match if kind == "mixed" {
                    KINDS[index % KINDS.len()]
                } else {
                    kind.as_str()
                } {
                    "struct" => {
                        StructArray::try_new(["child"].into(), [child], 1, Validity::NonNullable)
                            .unwrap()
                            .into_array()
                    }
                    "list" => {
                        ListArray::try_new(child, offsets.clone(), Validity::NonNullable, &mut ctx)
                            .unwrap()
                            .into_array()
                    }
                    "fixed_list" => FixedSizeListArray::try_new(child, 1, Validity::NonNullable, 1)
                        .unwrap()
                        .into_array(),
                    "extension" => {
                        static ID: CachedId = CachedId::new("test.array_cleanup_extension");
                        let dtype =
                            ForeignExtDType::from_parts(*ID, vec![7], child.dtype().clone())
                                .unwrap();
                        ExtensionArray::try_new(dtype, child).unwrap().into_array()
                    }
                    "opaque" => opaque([None, Some(child), None]),
                    _ => panic!("unknown array cleanup shape"),
                };
                if owned {
                    child = child
                        .with_retained_owner(Arc::new(DropCount(Arc::clone(&drops))))
                        .unwrap();
                }
            }
            child
        })
        .unwrap()
        .join()
        .unwrap()
}

fn run_case(case: &str) {
    let drops = Arc::new(AtomicUsize::new(0));
    if KINDS.contains(&case) || case == "mixed" {
        drop(chain(case, false, &drops));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        return;
    }
    match case {
        "owned_mixed" => {
            drop(chain("mixed", true, &drops));
            assert_eq!(drops.load(Ordering::SeqCst), DEPTH + 1);
        }
        "detached" => {
            let root = chain("mixed", false, &drops);
            let child = root.slots().iter().flatten().next().unwrap().clone();
            drop(root);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(child);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "weak" => {
            let inner = chain("opaque", false, &drops).into_inner();
            let weak = Arc::downgrade(&inner);
            drop(ArrayRef::from_erased_inner(inner));
            assert!(weak.upgrade().is_none());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "concurrent" => {
            for kind in ["opaque", "struct", "list", "fixed_list", "mixed"]
                .into_iter()
                .cycle()
                .take(24)
            {
                let drops = Arc::new(AtomicUsize::new(0));
                let root = chain(kind, true, &drops);
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
                assert_eq!(drops.load(Ordering::SeqCst), DEPTH + 1);
            }
        }
        "parts" => {
            let root = chain("opaque", false, &drops);
            let parts = root.downcast::<ForeignArray>().try_into_parts().unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            let root = Array::<ForeignArray>::try_from_parts(parts).unwrap();
            drop(root.try_into_parts().unwrap());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "raw_typed" => {
            let root = chain("opaque", false, &drops);
            let inner = root.downcast_inner::<ForeignArray>().unwrap();
            let weak = Arc::downgrade(&inner);
            drop(inner);
            assert!(weak.upgrade().is_none());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "shared_tree" => {
            let mut root = chain("mixed", false, &drops);
            for _ in 0..128 {
                root = opaque([Some(root.clone()), None, Some(root)]);
            }
            drop(root);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "partial" => {
            let result = StructArray::try_new(
                ["child"].into(),
                [chain("mixed", false, &drops)],
                0,
                Validity::NonNullable,
            );
            assert!(result.is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
        "unwind" => {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let _root = chain("mixed", true, &drops);
                panic!("intentional array construction unwind");
            }));
            assert!(result.is_err());
            assert_eq!(drops.load(Ordering::SeqCst), DEPTH + 1);
        }
        _ => panic!("unknown array cleanup case"),
    }
}

#[test]
fn deep_cleanup_on_small_stack() {
    const SELECTOR: &str = "SHARDLOOM_ARRAY_CLEANUP_CHILD_CASE";
    const COMPLETED: &str = "SHARDLOOM_ARRAY_CLEANUP_COMPLETED";
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
        "owned_mixed",
        "detached",
        "weak",
        "concurrent",
        "parts",
        "raw_typed",
        "shared_tree",
        "partial",
        "unwind",
    ]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "array::release::tests::deep_cleanup_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "array cleanup case {case}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETED} {case}")),
            "child test did not execute array cleanup case {case}",
        );
    }
}

struct OrderedOwner {
    id: usize,
    released: Arc<Mutex<Vec<usize>>>,
    panic: bool,
}

impl Drop for OrderedOwner {
    fn drop(&mut self) {
        self.released.lock().push(self.id);
        assert!(!self.panic, "intentional array owner panic");
    }
}

fn ordered_node(
    id: usize,
    slots: impl IntoIterator<Item = Option<ArrayRef>>,
    released: &Arc<Mutex<Vec<usize>>>,
    panic: bool,
) -> ArrayRef {
    opaque(slots)
        .with_retained_owner(Arc::new(OrderedOwner {
            id,
            released: Arc::clone(released),
            panic,
        }))
        .unwrap()
}

#[test]
fn retained_owners_outlive_descendants_in_slot_order_even_on_unwind() {
    for panic in [false, true] {
        let released = Arc::new(Mutex::new(Vec::new()));
        let leaf = ordered_node(1, [], &released, panic);
        let root = ordered_node(
            4,
            [
                None,
                Some(ordered_node(2, [Some(leaf), None], &released, false)),
                Some(ordered_node(3, [], &released, false)),
            ],
            &released,
            false,
        );
        let result = panic::catch_unwind(AssertUnwindSafe(|| drop(root)));
        assert_eq!(result.is_err(), panic);
        assert_eq!(*released.lock(), [1, 2, 3, 4]);
    }
}

struct PayloadProbe {
    bytes: [u8; 4],
    owner: Weak<OrderedOwner>,
    released: Arc<Mutex<Vec<usize>>>,
}

impl AsRef<[u8]> for PayloadProbe {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for PayloadProbe {
    fn drop(&mut self) {
        assert!(self.owner.upgrade().is_some());
        self.released.lock().push(2);
    }
}

#[test]
fn metadata_panic_keeps_owner_alive_for_data_and_pending_slots() {
    struct PanickingMetadata {
        owner: Weak<OrderedOwner>,
        released: Arc<Mutex<Vec<usize>>>,
    }
    impl Drop for PanickingMetadata {
        fn drop(&mut self) {
            assert!(self.owner.upgrade().is_some());
            self.released.lock().push(1);
            panic!("intentional metadata cleanup panic");
        }
    }
    let released = Arc::new(Mutex::new(Vec::new()));
    let owner = Arc::new(OrderedOwner {
        id: 4,
        released: Arc::clone(&released),
        panic: false,
    });
    let dtype = DType::Struct(
        StructFields::from_reserved_fields(
            Default::default(),
            Vec::new(),
            HostMemoryReservation::new(PanickingMetadata {
                owner: Arc::downgrade(&owner),
                released: Arc::clone(&released),
            }),
        ),
        Nullability::NonNullable,
    );
    let buffer = ByteBuffer::from_bytes_aligned(
        Bytes::from_owner(PayloadProbe {
            bytes: [1, 2, 3, 4],
            owner: Arc::downgrade(&owner),
            released: Arc::clone(&released),
        }),
        Alignment::none(),
    );
    let root = foreign(
        dtype,
        [Some(ordered_node(3, [], &released, false))]
            .into_iter()
            .collect(),
        vec![BufferHandle::new_host(buffer)],
    )
    .with_retained_owner(owner)
    .unwrap();
    let result = panic::catch_unwind(AssertUnwindSafe(|| drop(root)));
    assert!(result.is_err());
    assert_eq!(*released.lock(), [1, 2, 3, 4]);
}
