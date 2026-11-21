// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::cell::RefCell;

use vortex_array::ArrayContext;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::VarBinArray;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::serde::ArrayChildren;
use vortex_array::serde::SerializeOptions;
use vortex_array::serde::SerializedArray;
use vortex_array::serde::finish_deserialize;
use vortex_array::session::ArraySessionExt;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

use crate::DEFAULT_CONFIG;
use crate::OnPair;
use crate::onpair_compress;

struct Children<'a> {
    arrays: &'a [ArrayRef],
    trace: RefCell<Vec<(usize, DType, usize)>>,
    fail: Option<usize>,
}

impl ArrayChildren for Children<'_> {
    fn get(&self, index: usize, dtype: &DType, len: usize) -> VortexResult<ArrayRef> {
        self.trace.borrow_mut().push((index, dtype.clone(), len));
        if self.fail == Some(index) {
            vortex_bail!("injected OnPair child failure at {index}");
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
fn optional_continuation_preserves_nullable_values_and_child_failures() -> VortexResult<()> {
    let session = VortexSession::empty();
    crate::initialize(&session);
    let mut ctx = session.create_execution_ctx();
    let input = VarBinArray::from_iter(
        [
            Some("http://one.example/a"),
            None,
            Some(""),
            Some("http://one.example/b"),
        ],
        DType::Utf8(Nullability::Nullable),
    )
    .into_array();
    let encoded = onpair_compress(&input, DEFAULT_CONFIG, &mut ctx)?;
    assert!(encoded.is::<OnPair>());
    let context = ArrayContext::empty();
    let payload = encoded
        .serialize(&context, &session, &SerializeOptions::default())?
        .into_iter()
        .flat_map(|buffer| buffer.into_iter())
        .collect::<ByteBufferMut>()
        .freeze();
    let serialized = SerializedArray::try_from(payload)?;
    let actual = serialized.decode_iterative(
        encoded.dtype(),
        encoded.len(),
        &ReadContext::new(context.to_ids()),
        &session,
    )?;
    assert_arrays_eq!(actual, input, &mut ctx);
    let parts = session.array_serialize(&encoded)?.unwrap();
    let plugin = session
        .arrays()
        .registry()
        .get(&parts.serialized_id)
        .unwrap();
    let buffers = parts
        .buffers
        .iter()
        .cloned()
        .map(BufferHandle::new_host)
        .collect::<Vec<_>>();
    assert_eq!(
        parts.children.len(),
        5,
        "four physical children plus validity"
    );
    let mut cases = vec![(parts.metadata.clone(), None)];
    cases.extend((0..parts.children.len()).map(|index| (parts.metadata.clone(), Some(index))));
    cases.push((vec![0x80], None));
    for (metadata, fail) in cases {
        let children = Children {
            arrays: &parts.children,
            trace: RefCell::new(Vec::new()),
            fail,
        };
        let args = || {
            ArrayDeserialization::new(
                parts.serialized_id,
                encoded.dtype(),
                encoded.len(),
                &metadata,
                &buffers,
                &children,
            )
        };
        let expected = plugin.deserialize(args(), &session);
        let trace = children.trace.take();
        let actual = finish_deserialize(plugin.deserialize_async(args(), &session));
        assert_eq!(children.trace.take(), trace);
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => assert_arrays_eq!(actual, expected, &mut ctx),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            _ => panic!("OnPair continuation changed its result"),
        }
    }
    Ok(())
}
