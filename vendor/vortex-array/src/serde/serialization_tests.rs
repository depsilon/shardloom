// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use parking_lot::Mutex;
use vortex_buffer::Alignment;
use vortex_session::registry::CachedId;

use super::*;
use crate::array::{ArrayDeserialization, new_foreign_array};
use crate::buffer::BufferHandle;
use crate::dtype::{DType, Nullability};
use crate::expr::stats::{Precision, Stat};
use crate::serde::SerializeOptions;
use crate::{ArrayPlugin, ArraySerialization};

#[path = "serialization_reference.rs"]
mod reference;

fn memory_id() -> ArrayId {
    static ID: CachedId = CachedId::new("test.iterative_array_serialization");
    *ID
}

fn wire_id() -> ArrayId {
    static ID: CachedId = CachedId::new("test.iterative_array_serialization.v2");
    *ID
}

#[derive(Debug)]
struct Plugin {
    calls: Arc<Mutex<Vec<usize>>>,
    fail: Option<usize>,
    unsupported: Option<usize>,
    replacement: Option<(Vec<ByteBuffer>, Vec<ArrayRef>)>,
}

impl ArrayPlugin for Plugin {
    fn id(&self) -> ArrayId {
        memory_id()
    }

    fn write_ids(&self) -> Vec<ArrayId> {
        vec![memory_id(), wire_id()]
    }

    fn read_ids(&self) -> Vec<ArrayId> {
        vec![]
    }

    fn serialize(
        &self,
        array: &ArrayRef,
        _session: &VortexSession,
    ) -> VortexResult<Option<ArraySerialization>> {
        self.calls.lock().push(array.len());
        if self.fail == Some(array.len()) {
            vortex_bail!("serializer refused node {}", array.len());
        }
        if self.unsupported == Some(array.len()) {
            return Ok(None);
        }
        let (buffers, children) = match &self.replacement {
            Some((buffers, children)) if array.len() == 1 => (buffers.clone(), children.clone()),
            _ => (array.buffers(), array.children()),
        };
        Ok(Some(ArraySerialization::new(
            if array.len().is_multiple_of(2) {
                wire_id()
            } else {
                memory_id()
            },
            array.len().to_le_bytes().to_vec(),
            buffers,
            children,
        )))
    }

    fn deserialize(
        &self,
        _parts: ArrayDeserialization<'_>,
        _session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        vortex_bail!("serialization-only test provider")
    }
}

fn session(
    fail: Option<usize>,
    unsupported: Option<usize>,
    replacement: Option<(Vec<ByteBuffer>, Vec<ArrayRef>)>,
) -> (VortexSession, Arc<Mutex<Vec<usize>>>) {
    let session = VortexSession::empty();
    let calls = Arc::new(Mutex::new(Vec::new()));
    session.arrays().register(Plugin {
        calls: Arc::clone(&calls),
        fail,
        unsupported,
        replacement,
    });
    (session, calls)
}

fn node(label: usize, buffers: Vec<ByteBuffer>, children: Vec<ArrayRef>) -> ArrayRef {
    new_foreign_array(
        memory_id(),
        DType::Bool(Nullability::NonNullable),
        label,
        vec![],
        buffers.into_iter().map(BufferHandle::new_host).collect(),
        children.into_iter().map(Some).collect(),
    )
    .unwrap()
}

fn fixture() -> ArrayRef {
    let bytes = ByteBuffer::from(vec![1, 2, 3]);
    let aligned = ByteBuffer::zeroed_aligned(17, Alignment::new(64));
    let leaf = node(4, vec![aligned], vec![]);
    leaf.statistics()
        .set(Stat::NullCount, Precision::exact(0u64));
    leaf.statistics()
        .set(Stat::IsSorted, Precision::exact(true));
    node(
        1,
        vec![bytes],
        vec![
            node(2, vec![], vec![leaf.clone()]),
            node(3, vec![], vec![leaf]),
        ],
    )
}

fn compare_buffers(actual: &[ByteBuffer], expected: &[ByteBuffer]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.as_ref(), expected.as_ref());
        assert_eq!(actual.alignment(), expected.alignment());
    }
}

#[test]
fn wire_bytes_buffers_padding_and_callback_order_match_original() {
    let root = fixture();
    for include_padding in [false, true] {
        for offset in [0, 1, 7, 31, 64, 65] {
            let (session, calls) = session(None, None, None);
            let ctx = ArrayContext::empty();
            let old_ctx = ArrayContext::empty();
            let options = SerializeOptions {
                offset,
                include_padding,
            };
            let actual = root.serialize(&ctx, &session, &options).unwrap();
            assert_eq!(*calls.lock(), vec![1, 2, 4, 3, 4]);
            calls.lock().clear();
            let expected = reference::serialize(&root, &old_ctx, &session, &options).unwrap();
            assert_eq!(*calls.lock(), vec![1, 2, 4, 3, 4]);
            compare_buffers(&actual, &expected);
            assert_eq!(ctx.to_ids(), old_ctx.to_ids());
            assert_eq!(ctx.to_ids(), vec![memory_id(), wire_id()]);
        }
    }
    let (session, _) = session(None, None, None);
    let tree = ArraySerializationTree::try_new(&session, &root).unwrap();
    let buffers = tree.buffers();
    assert_eq!(buffers.len(), 3);
    assert_eq!(buffers[0].as_ptr(), root.buffers()[0].as_ptr());
    assert_eq!(buffers[1].as_ptr(), buffers[2].as_ptr());
}

#[test]
fn substituted_wire_children_and_buffers_are_used_once() {
    let root = fixture();
    let buffer = ByteBuffer::from(vec![99, 0, 41]);
    let child = node(8, vec![buffer.clone()], vec![]);
    let (session, calls) = session(None, None, Some((vec![buffer.clone()], vec![child])));
    let ctx = ArrayContext::empty();
    let options = SerializeOptions::default();
    let actual = root.serialize(&ctx, &session, &options).unwrap();
    assert_eq!(*calls.lock(), vec![1, 8]);
    calls.lock().clear();
    let expected = reference::serialize(&root, &ArrayContext::empty(), &session, &options).unwrap();
    assert_eq!(*calls.lock(), vec![1, 8]);
    compare_buffers(&actual, &expected);
    let tree = ArraySerializationTree::try_new(&session, &root).unwrap();
    assert_eq!(tree.buffers().len(), 2);
    assert!(
        tree.buffers()
            .iter()
            .all(|actual| actual.as_ptr() == buffer.as_ptr())
    );
}

#[test]
fn callback_failure_unsupported_and_allowed_ids_preserve_error_boundaries() {
    let root = fixture();
    for (fail, unsupported) in [(Some(4), None), (None, Some(4))] {
        let (session, calls) = session(fail, unsupported, None);
        let actual = root
            .serialize(
                &ArrayContext::empty(),
                &session,
                &SerializeOptions::default(),
            )
            .err()
            .unwrap()
            .to_string();
        assert_eq!(*calls.lock(), vec![1, 2, 4]);
        calls.lock().clear();
        let expected = reference::serialize(
            &root,
            &ArrayContext::empty(),
            &session,
            &SerializeOptions::default(),
        )
        .err()
        .unwrap()
        .to_string();
        assert_eq!(actual, expected);
        assert_eq!(*calls.lock(), vec![1, 2, 4]);
    }
    // Context refusal occurs during writing, after every serializer has run.
    // Compare the unfinished builder too: prior metadata must remain identical.
    let (session, calls) = session(None, None, None);
    for allowed in [vec![], vec![memory_id()]] {
        let ctx = ArrayContext::empty().with_allowed_ids(allowed.iter().copied().collect());
        let old_ctx = ArrayContext::empty().with_allowed_ids(allowed.iter().copied().collect());
        let actual = ArrayNodeFlatBuffer::try_new(&ctx, &session, &root).unwrap();
        assert_eq!(*calls.lock(), vec![1, 2, 4, 3, 4]);
        calls.lock().clear();
        let expected = reference::ArrayNodeFlatBuffer::try_new(&old_ctx, &session, &root).unwrap();
        assert_eq!(*calls.lock(), vec![1, 2, 4, 3, 4]);
        calls.lock().clear();
        let mut builder = FlatBufferBuilder::new();
        let mut old_builder = FlatBufferBuilder::new();
        assert_eq!(
            actual
                .try_write_flatbuffer(&mut builder)
                .err()
                .unwrap()
                .to_string(),
            expected
                .try_write_flatbuffer(&mut old_builder)
                .err()
                .unwrap()
                .to_string(),
        );
        assert_eq!(builder.unfinished_data(), old_builder.unfinished_data());
        assert_eq!(ctx.to_ids(), old_ctx.to_ids());
    }
}

#[test]
fn total_buffer_limit_is_checked_after_all_callbacks() {
    let buffer = ByteBuffer::from(vec![7]);
    for total in [u16::MAX as usize, u16::MAX as usize + 1] {
        let root = node(
            1,
            vec![buffer.clone(); total - 1],
            vec![node(2, vec![buffer.clone()], vec![])],
        );
        let (session, calls) = session(None, None, None);
        let ctx = ArrayContext::empty();
        let actual = ArrayNodeFlatBuffer::try_new(&ctx, &session, &root);
        assert_eq!(*calls.lock(), vec![1, 2]);
        calls.lock().clear();
        let old_ctx = ArrayContext::empty();
        let expected = reference::ArrayNodeFlatBuffer::try_new(&old_ctx, &session, &root);
        assert_eq!(*calls.lock(), vec![1, 2]);
        if total > u16::MAX as usize {
            assert_eq!(
                actual.err().unwrap().to_string(),
                expected.err().unwrap().to_string()
            );
        } else {
            let mut builder = FlatBufferBuilder::new();
            let mut old_builder = FlatBufferBuilder::new();
            let offset = actual.unwrap().try_write_flatbuffer(&mut builder).unwrap();
            let old_offset = expected
                .unwrap()
                .try_write_flatbuffer(&mut old_builder)
                .unwrap();
            builder.finish_minimal(offset);
            old_builder.finish_minimal(old_offset);
            assert_eq!(builder.finished_data(), old_builder.finished_data());
        }
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn deep_serialization_and_failed_cleanup_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_ARRAY_SERIALIZATION";
    const COMPLETED: &str = "deep array serialization completed";
    if env::var_os(SELECTOR).is_some() {
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                for fail in [None, Some(0)] {
                    let drops = Arc::new(AtomicUsize::new(0));
                    let mut root = node(0, vec![], vec![])
                        .with_retained_owner(Arc::new(DropCount(Arc::clone(&drops))))
                        .unwrap();
                    for depth in 1..=8192 {
                        root = node(depth, vec![], vec![root])
                            .with_retained_owner(Arc::new(DropCount(Arc::clone(&drops))))
                            .unwrap();
                    }
                    let (session, calls) = session(fail, None, None);
                    let result = root.serialize(
                        &ArrayContext::empty(),
                        &session,
                        &SerializeOptions::default(),
                    );
                    assert_eq!(result.is_ok(), fail.is_none());
                    assert_eq!(*calls.lock(), (0..=8192).rev().collect::<Vec<_>>());
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    drop(root);
                    assert_eq!(drops.load(Ordering::SeqCst), 8193);
                    drop(result);
                }
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{COMPLETED}");
        return;
    }
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "serde::serialization::tests::deep_serialization_and_failed_cleanup_on_small_stack",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(SELECTOR, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line == COMPLETED)
    );
}
