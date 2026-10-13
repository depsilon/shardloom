use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;

fn frame_array(body: &[u8], names: &[&str], rows: u32, version: u8) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let names = names
        .iter()
        .map(|name| builder.create_string(name))
        .collect::<Vec<_>>();
    let names = builder.create_vector(&names);
    let array = fm::ArrayMessage::create(
        &mut builder,
        &fm::ArrayMessageArgs {
            row_count: rows,
            encodings: Some(names),
        },
    );
    let message = fm::Message::create(
        &mut builder,
        &fm::MessageArgs {
            version: fm::MessageVersion(version),
            header_type: fm::MessageHeader::ArrayMessage,
            header: Some(array.as_union_value()),
            body_size: body.len() as u64,
        },
    );
    builder.finish_minimal(message);
    let header = builder.finished_data();
    let mut output = u32::try_from(header.len()).unwrap().to_le_bytes().to_vec();
    output.extend_from_slice(header);
    output.extend_from_slice(body);
    output
}

fn synthetic_body(depth: usize, buffer_index: u16, alignment: u8, buffer_len: u32) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let indices = builder.create_vector(&[buffer_index]);
    let mut child = fa::ArrayNode::create(
        &mut builder,
        &fa::ArrayNodeArgs {
            buffers: Some(indices),
            ..Default::default()
        },
    );
    for _ in 0..depth {
        let children = builder.create_vector(&[child]);
        child = fa::ArrayNode::create(
            &mut builder,
            &fa::ArrayNodeArgs {
                children: Some(children),
                ..Default::default()
            },
        );
    }
    let buffers = builder.create_vector(&[fa::Buffer::new(
        0,
        alignment,
        fa::Compression::None,
        buffer_len,
    )]);
    let root = fa::Array::create(
        &mut builder,
        &fa::ArrayArgs {
            root: Some(child),
            buffers: Some(buffers),
        },
    );
    builder.finish_minimal(root);
    let metadata = builder.finished_data();
    let mut output = vec![0u8; 8];
    output.extend_from_slice(metadata);
    output.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
    output
}

fn primitive_messages() -> (Vec<u8>, Vec<u8>) {
    use vortex::{
        VortexSessionDefault as _,
        array::{
            IntoArray as _,
            arrays::{PrimitiveArray, StructArray},
        },
        ipc::messages::{EncoderMessage, MessageEncoder},
        session::VortexSession,
    };
    let array = StructArray::from_fields(&[(
        "key",
        PrimitiveArray::from_iter([0i64, 1, 2, 3]).into_array(),
    )])
    .unwrap()
    .into_array();
    let mut encoder = MessageEncoder::new(VortexSession::default());
    let dtype = encoder
        .encode(EncoderMessage::DType(array.dtype()))
        .unwrap()
        .iter()
        .flat_map(|buffer| buffer.to_vec())
        .collect();
    let body = encoder
        .encode(EncoderMessage::Array(&array))
        .unwrap()
        .iter()
        .flat_map(|buffer| buffer.to_vec())
        .collect();
    (dtype, body)
}

#[test]
fn ipc_rejects_versions_alignment_buffer_ranges_depth_and_trailing_data() {
    let (dtype, original) = primitive_messages();
    for (depth, index, alignment, len, version) in [
        (0, 0, 3, 8, 1),
        (0, 0, 255, 8, 0),
        (0, 0, 3, 9, 0),
        (0, 1, 3, 8, 0),
        (MAX_DEPTH + 1, 0, 3, 8, 0),
    ] {
        let bytes = frame_array(
            &synthetic_body(depth, index, alignment, len),
            &["vortex.primitive"],
            1,
            version,
        );
        assert!(
            inspect(&dtype, &bytes).is_err(),
            "case {depth}/{index}/{alignment}/{len}/{version}"
        );
    }
    let mut trailing = original.clone();
    trailing.push(0);
    assert!(inspect(&dtype, &trailing).is_err());
    let body = synthetic_body(0, 0, 3, 8);
    for names in [
        &["vortex.primitive", "vortex.primitive"][..],
        &["vortex.primitive", "untrusted.encoding"][..],
    ] {
        assert!(inspect(&dtype, &frame_array(&body, names, 1, 0)).is_err());
    }
    let mut huge_header = original;
    huge_header[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(inspect(&dtype, &huge_header).is_err());
}

fn with_false_statistics(dtype: &[u8], bytes: &[u8]) -> Vec<u8> {
    fn copy<'a>(
        builder: &mut flatbuffers::FlatBufferBuilder<'a>,
        node: fa::ArrayNode<'_>,
    ) -> flatbuffers::WIPOffset<fa::ArrayNode<'a>> {
        let child_nodes = node
            .children()
            .unwrap_or_default()
            .iter()
            .map(|child| copy(builder, child))
            .collect::<Vec<_>>();
        let children = builder.create_vector(&child_nodes);
        let indices = node
            .buffers()
            .unwrap_or_default()
            .iter()
            .collect::<Vec<_>>();
        let buffers = builder.create_vector(&indices);
        let metadata =
            builder.create_vector(node.metadata().map_or(&[][..], |bytes| bytes.bytes()));
        // Malformed scalar protobuf must remain unused as well as false flags.
        let bad_scalar = builder.create_vector(&[255u8, 255, 255]);
        let stats = fa::ArrayStats::create(
            builder,
            &fa::ArrayStatsArgs {
                min: Some(bad_scalar),
                is_constant: Some(true),
                is_sorted: Some(true),
                null_count: Some(4),
                ..Default::default()
            },
        );
        fa::ArrayNode::create(
            builder,
            &fa::ArrayNodeArgs {
                encoding: node.encoding(),
                metadata: Some(metadata),
                children: Some(children),
                buffers: Some(buffers),
                stats: Some(stats),
            },
        )
    }
    let inspected = inspect(dtype, bytes).unwrap();
    let suffix = inspected.body.len() - 4;
    let metadata_len = u32::from_le_bytes(inspected.body[suffix..].try_into().unwrap()) as usize;
    let mut output = inspected.body[..suffix - metadata_len].to_vec();
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let root = copy(&mut builder, inspected.array.root().unwrap());
    let buffers = inspected
        .array
        .buffers()
        .unwrap_or_default()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let buffers = builder.create_vector(&buffers);
    let array = fa::Array::create(
        &mut builder,
        &fa::ArrayArgs {
            root: Some(root),
            buffers: Some(buffers),
        },
    );
    builder.finish_minimal(array);
    let metadata = builder.finished_data();
    output.extend_from_slice(metadata);
    output.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
    let names = inspected
        .message
        .encodings()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    frame_array(&output, &names, inspected.message.row_count(), 0)
}

#[test]
fn ipc_never_installs_wire_statistics_or_parses_their_scalar_payloads() {
    use vortex::{
        array::{VortexSessionExecute as _, arrays::struct_::StructArraySlotsExt as _},
        expr::stats::{Stat, StatsProvider as _},
    };
    let (dtype, bytes) = primitive_messages();
    let bytes = with_false_statistics(&dtype, &bytes);
    let session = ResidentVortexSession::new(4 << 20, 1).unwrap();
    let copied = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            receive(&dtype, &bytes, context)
        })
        .unwrap();
    let field = copied.arrays.values[0]
        .0
        .as_::<vortex::array::arrays::Struct>()
        .fields()[0]
        .clone();
    assert!(field.statistics().get(Stat::IsConstant).is_absent());
    assert!(field.statistics().get(Stat::NullCount).is_absent());
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..4 {
                assert_eq!(
                    i64::try_from(&field.execute_scalar(row, &mut execution).unwrap()).unwrap(),
                    i64::try_from(row).unwrap()
                );
            }
            Ok(())
        })
        .unwrap();
    drop((field, copied));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

fn buffer_range(dtype: &[u8], bytes: &[u8], index: usize) -> std::ops::Range<usize> {
    let inspected = inspect(dtype, bytes).unwrap();
    let header_len = 4 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let mut offset = header_len;
    for (i, buffer) in inspected
        .array
        .buffers()
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let start = offset + buffer.padding() as usize;
        offset = start + buffer.length() as usize;
        if i == index {
            return start..offset;
        }
    }
    panic!("fixture buffer index missing")
}

#[test]
fn ipc_rejects_invalid_dictionary_codes_and_run_end_coordinates() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/independent-vortex-0.85");
    let dtype = std::fs::read(root.join("encoded.dtype.vxipc")).unwrap();
    let bytes = std::fs::read(root.join("encoded.array.vxipc")).unwrap();
    for field in [1, 2] {
        let inspected = inspect(&dtype, &bytes).unwrap();
        let node = inspected
            .array
            .root()
            .unwrap()
            .children()
            .unwrap()
            .get(field)
            .children()
            .unwrap()
            .get(0);
        let buffer = node.buffers().unwrap().get(0) as usize;
        let range = buffer_range(&dtype, &bytes, buffer);
        let mut broken = bytes.clone();
        if field == 1 {
            broken[range.start..range.start + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        } else {
            broken[range.start..range.start + 8].fill(0);
        }
        let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                receive(&dtype, &broken, context)
            })
            .err()
            .unwrap();
        assert!(error.to_string().contains("coordinate"), "{error}");
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn ipc_rejects_invalid_string_offsets_utf8_and_schema_nesting() {
    use std::sync::Arc;
    use vortex::{
        VortexSessionDefault as _,
        array::{
            IntoArray as _,
            arrays::{ConstantArray, StructArray},
            dtype::{DType, Nullability},
            scalar::Scalar,
        },
        ipc::messages::{EncoderMessage, MessageEncoder},
        session::VortexSession,
    };
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/independent-vortex-0.85");
    let dtype = std::fs::read(root.join("mixed.dtype.vxipc")).unwrap();
    let bytes = std::fs::read(root.join("mixed.array.vxipc")).unwrap();
    let inspected = inspect(&dtype, &bytes).unwrap();
    let text = inspected.array.root().unwrap().children().unwrap().get(3);
    let offset_index = text.children().unwrap().get(0).buffers().unwrap().get(0) as usize;
    let range = buffer_range(&dtype, &bytes, offset_index);
    let width = range.len() / (4099 + 1);
    assert!([1, 2, 4, 8].contains(&width));
    let mut bad_offsets = bytes.clone();
    bad_offsets[range.start..range.start + width].copy_from_slice(&u64::MAX.to_le_bytes()[..width]);
    let mut bad_utf8 = bytes.clone();
    let value_range = buffer_range(&dtype, &bytes, text.buffers().unwrap().get(0) as usize);
    bad_utf8[value_range.start] = 255;
    for broken in [&bad_offsets, &bad_utf8] {
        let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
        assert!(
            session
                .with_native_execution_context(&CancellationToken::default(), |context| receive(
                    &dtype, broken, context
                ))
                .is_err()
        );
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
    let mut dtype = DType::Bool(Nullability::Nullable);
    for _ in 0..=MAX_DEPTH {
        dtype = DType::List(Arc::new(dtype), Nullability::Nullable);
    }
    let array = StructArray::from_fields(&[(
        "deep",
        ConstantArray::new(Scalar::null(dtype), 1).into_array(),
    )])
    .unwrap()
    .into_array();
    let mut encoder = MessageEncoder::new(VortexSession::default());
    let dtype = encoder
        .encode(EncoderMessage::DType(array.dtype()))
        .unwrap()
        .iter()
        .flat_map(|buffer| buffer.to_vec())
        .collect::<Vec<_>>();
    let array = encoder
        .encode(EncoderMessage::Array(&array))
        .unwrap()
        .iter()
        .flat_map(|buffer| buffer.to_vec())
        .collect::<Vec<_>>();
    assert!(inspect(&dtype, &array).is_err());
}

#[test]
fn ipc_transport_and_alias_amplification_are_bounded_before_construction() {
    let (dtype, _) = primitive_messages();
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let leaf = fa::ArrayNode::create(&mut builder, &fa::ArrayNodeArgs::default());
    let children = builder.create_vector(&vec![leaf; MAX_NODES]);
    let root = fa::ArrayNode::create(
        &mut builder,
        &fa::ArrayNodeArgs {
            children: Some(children),
            ..Default::default()
        },
    );
    let array = fa::Array::create(
        &mut builder,
        &fa::ArrayArgs {
            root: Some(root),
            buffers: None,
        },
    );
    builder.finish_minimal(array);
    let metadata = builder.finished_data();
    let mut body = metadata.to_vec();
    body.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
    let aliased = frame_array(&body, &["vortex.primitive"], 4, 0);
    let oversized = vec![0u8; MAX_TRANSPORT + 1];
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    for bytes in [&aliased, &oversized] {
        assert!(
            session
                .with_native_execution_context(&CancellationToken::default(), |context| receive(
                    &dtype, bytes, context
                ))
                .is_err()
        );
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}
