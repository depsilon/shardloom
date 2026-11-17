//! Complete codec values and callback order on the optional native decoder.
//!
//! These are provider-mechanism checks. They do not lift the public depth bound
//! or claim that every provider object has native structural memory admission.

use std::cell::RefCell;
use std::sync::Arc;

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayContext, ArrayDeserialization, ArrayRef, VortexSessionExecute as _,
        buffer::BufferHandle,
        dtype::DType,
        memory::HostMemorySessionExt as _,
        serde::{ArrayChildren, SerializeOptions, SerializedArray, finish_deserialize},
        session::ArraySessionExt as _,
    },
    buffer::ByteBufferMut,
    error::{VortexResult, vortex_bail},
    session::{VortexSession, registry::ReadContext},
};

#[path = "native_provider_decode_fixtures.rs"]
mod fixtures;

fn session(memory: &LiveMemoryPool) -> VortexSession {
    let session = VortexSession::default().with_host_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    super::install(&session, memory.clone());
    session
}

fn wire(array: &ArrayRef, session: &VortexSession) -> (SerializedArray, ReadContext) {
    let context = ArrayContext::empty();
    let bytes = array
        .serialize(
            &context,
            session,
            &SerializeOptions {
                offset: 0,
                include_padding: true,
            },
        )
        .unwrap()
        .into_iter()
        .flat_map(IntoIterator::into_iter)
        .collect::<ByteBufferMut>()
        .freeze();
    (
        SerializedArray::try_from(bytes).unwrap(),
        ReadContext::new(context.to_ids()),
    )
}

fn same_values(label: &str, actual: &ArrayRef, expected: &ArrayRef, session: &VortexSession) {
    assert_eq!(actual.dtype(), expected.dtype(), "{label}");
    assert_eq!(actual.len(), expected.len(), "{label}");
    let mut context = session.create_execution_ctx();
    for index in 0..actual.len() {
        assert_eq!(
            actual.execute_scalar(index, &mut context).unwrap(),
            expected.execute_scalar(index, &mut context).unwrap(),
            "{label}, row {index}"
        );
    }
}

fn same_graph(label: &str, actual: &ArrayRef, expected: &ArrayRef) {
    let mut pending = vec![(actual.clone(), expected.clone())];
    while let Some((actual, expected)) = pending.pop() {
        assert_eq!(actual.dtype(), expected.dtype(), "{label}");
        assert_eq!(actual.len(), expected.len(), "{label}");
        assert_eq!(actual.encoding_id(), expected.encoding_id(), "{label}");
        let actual_buffers = actual.buffers();
        let expected_buffers = expected.buffers();
        assert_eq!(actual_buffers.len(), expected_buffers.len(), "{label}");
        for (actual, expected) in actual_buffers.into_iter().zip(expected_buffers) {
            assert_eq!(actual.as_slice(), expected.as_slice(), "{label}");
            assert_eq!(actual.alignment(), expected.alignment(), "{label}");
        }
        let actual_children = actual.children();
        let expected_children = expected.children();
        assert_eq!(actual_children.len(), expected_children.len(), "{label}");
        pending.extend(actual_children.into_iter().zip(expected_children));
    }
}

#[test]
fn native_registered_codec_continuations_preserve_complete_values_and_wire_graphs() {
    let memory = LiveMemoryPool::new(1 << 30).unwrap();
    {
        let session = session(&memory);
        let fixtures = fixtures::arrays(&session);
        assert_eq!(fixtures.len(), 19);
        for fixtures::Fixture { name: label, array } in fixtures {
            let (wire, context) = wire(&array, &session);
            let expected = wire
                .decode(array.dtype(), array.len(), &context, &session)
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            let actual = wire
                .decode_iterative(array.dtype(), array.len(), &context, &session)
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            same_values(label, &actual, &array, &session);
            same_values(label, &actual, &expected, &session);
            same_graph(label, &actual, &expected);
        }
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

struct RecordingChildren<'a> {
    arrays: &'a [ArrayRef],
    calls: RefCell<Vec<(usize, DType, usize)>>,
    fail: Option<usize>,
}

impl ArrayChildren for RecordingChildren<'_> {
    fn get(&self, index: usize, dtype: &DType, len: usize) -> VortexResult<ArrayRef> {
        self.calls.borrow_mut().push((index, dtype.clone(), len));
        if self.fail == Some(index) {
            vortex_bail!("injected native codec child failure at {index}");
        }
        let array = self.arrays[index].clone();
        assert_eq!(array.dtype(), dtype);
        assert_eq!(array.len(), len);
        Ok(array)
    }

    fn len(&self) -> usize {
        self.arrays.len()
    }
}

#[test]
fn native_registered_codec_continuations_preserve_child_failures_and_metadata_errors() {
    let memory = LiveMemoryPool::new(1 << 30).unwrap();
    {
        let session = session(&memory);
        for fixtures::Fixture { name: label, array } in fixtures::arrays(&session) {
            let serialized = session.array_serialize(&array).unwrap().unwrap();
            let plugin = session
                .arrays()
                .registry()
                .get(&serialized.serialized_id)
                .unwrap();
            let buffers = serialized
                .buffers
                .iter()
                .cloned()
                .map(BufferHandle::new_host)
                .collect::<Vec<_>>();
            let mut cases = vec![(serialized.metadata.clone(), None)];
            for index in 0..serialized.children.len() {
                cases.push((serialized.metadata.clone(), Some(index)));
            }
            if !serialized.metadata.is_empty() {
                cases.push((
                    serialized.metadata[..serialized.metadata.len() - 1].to_vec(),
                    None,
                ));
            }
            cases.push((vec![0x80], None));
            for (metadata, fail) in cases {
                let children = RecordingChildren {
                    arrays: &serialized.children,
                    calls: RefCell::new(Vec::new()),
                    fail,
                };
                let parts = || {
                    ArrayDeserialization::new(
                        serialized.serialized_id,
                        array.dtype(),
                        array.len(),
                        &metadata,
                        &buffers,
                        &children,
                    )
                };
                let expected = plugin.deserialize(parts(), &session);
                let trace = children.calls.take();
                let actual = finish_deserialize(plugin.deserialize_async(parts(), &session));
                assert_eq!(*children.calls.borrow(), trace, "{label}, failure {fail:?}");
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => {
                        same_values(label, &actual, &expected, &session);
                        same_graph(label, &actual, &expected);
                    }
                    (Err(actual), Err(expected)) => {
                        assert_eq!(actual.to_string(), expected.to_string(), "{label}");
                    }
                    _ => panic!("codec decode outcome differs: {label}"),
                }
            }
        }
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
