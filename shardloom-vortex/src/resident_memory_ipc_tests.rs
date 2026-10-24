use super::*;
use crate::resident_session::ResidentVortexSession;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    VortexSessionDefault as _,
    array::{
        VortexSessionExecute as _,
        arrays::struct_::StructArraySlotsExt as _,
        arrays::{ConstantArray, PrimitiveArray, StructArray, VarBinArray},
        dtype::Nullability,
        scalar::Scalar,
    },
    ipc::messages::{EncoderMessage, MessageEncoder},
    session::VortexSession,
};

fn messages(array: &ArrayRef) -> (Vec<u8>, Vec<u8>) {
    let mut encoder = MessageEncoder::new(VortexSession::default());
    let mut encode = |message| {
        encoder
            .encode(message)
            .unwrap()
            .into_iter()
            .flat_map(|buffer| buffer.to_vec())
            .collect()
    };
    (
        encode(EncoderMessage::DType(array.dtype())),
        encode(EncoderMessage::Array(array)),
    )
}

fn input() -> ArrayRef {
    StructArray::from_fields(&[
        (
            "key",
            PrimitiveArray::from_iter([1i64, 2, 3, 4]).into_array(),
        ),
        ("constant", ConstantArray::new(19i64, 4).into_array()),
        (
            "text",
            VarBinArray::from_iter(
                [Some("λ\0猫"), None, Some(""), Some("last")],
                DType::Utf8(Nullability::Nullable),
            )
            .into_array(),
        ),
    ])
    .unwrap()
    .into_array()
}

fn scalars(array: &ArrayRef) -> Vec<Scalar> {
    let mut context = VortexSession::default().create_execution_ctx();
    (0..array.len())
        .map(|index| array.execute_scalar(index, &mut context).unwrap())
        .collect()
}

#[test]
fn ipc_receiving_values_and_ownership_survive_transport_release() {
    let input = input();
    let expected = scalars(&input);
    let (dtype, bytes) = messages(&input);
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let copied = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            receive(&dtype, &bytes, context)
        })
        .unwrap();
    let array = copied.arrays.values[0].0.clone();
    drop(copied);
    drop(input);
    drop(dtype);
    drop(bytes);
    assert_eq!(scalars(&array), expected);
    assert!(session.memory().snapshot().reserved_bytes > 0);
    drop(array);
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

#[test]
fn public_ipc_intake_accepts_native_record_chunks_without_materializing_fields() {
    let chunk = input();
    let input = vortex::array::arrays::ChunkedArray::try_new(
        [chunk.clone(), chunk],
        input().dtype().clone(),
    )
    .unwrap()
    .into_array();
    let expected = scalars(&input);
    let (dtype, bytes) = messages(&input);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = super::super::ResidentMemorySource::from_vortex_ipc(
        &session,
        &dtype,
        &bytes,
        &CancellationToken::default(),
    )
    .unwrap();
    drop((input, dtype, bytes));
    assert_eq!(scalars(&source.0.array), expected);
    assert_eq!(source.intake_transfer_report().materialized_batches, 0);
    drop(source);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn ipc_null_fields_and_nested_null_elements_keep_receiving_ownership() {
    use vortex::array::{
        arrays::{ChunkedArray, FixedSizeListArray, ListArray, NullArray},
        validity::Validity,
    };
    for rows in [0, 3] {
        let nulls = NullArray::new(rows).into_array();
        let elements = NullArray::new(rows * 2).into_array();
        let input = StructArray::from_fields(&[
            ("null", nulls.clone()),
            (
                "nested",
                StructArray::from_fields(&[("null", nulls)])
                    .unwrap()
                    .into_array(),
            ),
            (
                "list",
                ListArray::try_new(
                    elements.clone(),
                    PrimitiveArray::from_iter((0..=rows).map(|row| (row * 2) as u64)).into_array(),
                    Validity::NonNullable,
                    &mut vortex::array::VortexSessionExecute::create_execution_ctx(
                        &vortex::array::array_session(),
                    ),
                )
                .unwrap()
                .into_array(),
            ),
            (
                "fixed",
                FixedSizeListArray::try_new(elements, 2, Validity::NonNullable, rows)
                    .unwrap()
                    .into_array(),
            ),
        ])
        .unwrap()
        .into_array();
        let chunked = ChunkedArray::try_new([input.clone()], input.dtype().clone())
            .unwrap()
            .into_array();
        for input in [input, chunked] {
            let expected = scalars(&input);
            let expected_dtype = input.dtype().clone();
            let (dtype, bytes) = messages(&input);
            let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
            let source = super::super::ResidentMemorySource::from_vortex_ipc(
                &session,
                &dtype,
                &bytes,
                &CancellationToken::default(),
            )
            .unwrap();
            assert_eq!(source.intake_transfer_report().materialized_batches, 0);
            let retained = source.0.array.clone();
            drop((source, input, dtype, bytes));
            assert_eq!(retained.dtype(), &expected_dtype);
            assert_eq!(scalars(&retained), expected);
            assert!(session.snapshot().memory.reserved_bytes > 0);
            drop(retained);
            assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        }
    }
}

#[test]
fn ipc_truncations_unknown_registry_and_denial_leave_no_receiving_credit() {
    let (dtype, bytes) = messages(&input());
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    for end in 0..dtype.len() {
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                receive(&dtype[..end], &bytes, context)
            });
        assert!(error.is_err(), "dtype prefix {end} must be rejected");
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
    for end in 0..bytes.len() {
        let error = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                receive(&dtype, &bytes[..end], context)
            });
        assert!(error.is_err(), "array prefix {end} must be rejected");
        assert_eq!(session.memory().snapshot().reserved_bytes, 0);
    }
    let mut unknown = bytes.clone();
    let start = unknown
        .windows(b"vortex.primitive".len())
        .position(|window| window == b"vortex.primitive")
        .unwrap();
    unknown[start..start + b"vortex.primitive".len()].copy_from_slice(b"unknown.encoding");
    assert!(
        inspect(&dtype, &unknown)
            .err()
            .unwrap()
            .to_string()
            .contains("registry")
    );
    let denied = ResidentVortexSession::new(8192, 1).unwrap();
    assert!(
        denied
            .with_native_execution_context(&CancellationToken::default(), |context| receive(
                &dtype, &bytes, context
            ))
            .is_err()
    );
    assert_eq!(denied.memory().snapshot().reserved_bytes, 0);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(
        session
            .with_native_execution_context(&cancellation, |context| receive(
                &dtype, &bytes, context
            ))
            .is_err()
    );
    assert_eq!(session.memory().snapshot().reserved_bytes, 0);
}

fn fixture_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/independent-vortex-0.85")
}

fn expected_field(cohort: &str, name: &str, row: usize) -> Scalar {
    use vortex::array::dtype::PType;
    let nullable = Nullability::Nullable;
    let required = Nullability::NonNullable;
    match (cohort, name) {
        ("mixed", "key") => Scalar::from((1i64 << 52) + i64::try_from(row).unwrap()),
        ("mixed", "amount") => {
            if row.is_multiple_of(7) {
                Scalar::null(DType::Primitive(PType::F64, nullable))
            } else {
                Scalar::primitive(
                    -10.5 + f64::from(u32::try_from(row).unwrap()) / 4.0,
                    nullable,
                )
            }
        }
        ("mixed", "flag") => {
            if row.is_multiple_of(11) {
                Scalar::null(DType::Bool(nullable))
            } else {
                Scalar::bool(row.is_multiple_of(2), nullable)
            }
        }
        ("mixed", "text") => {
            if row.is_multiple_of(17) {
                Scalar::null(DType::Utf8(nullable))
            } else if row.is_multiple_of(13) {
                Scalar::utf8("", nullable)
            } else {
                Scalar::utf8(format!("row-{row}-λ\0猫"), nullable)
            }
        }
        ("encoded", "constant") => Scalar::from(11i64),
        ("encoded", "run_end") => Scalar::from(i64::try_from(row / 97).unwrap()),
        ("encoded", "packed") => Scalar::from(i32::try_from(row % 31).unwrap()),
        ("encoded", "dictionary") => match row % 4 {
            0 => Scalar::utf8("", nullable),
            1 => Scalar::utf8("λ\0猫", nullable),
            2 => Scalar::null(DType::Utf8(nullable)),
            _ => Scalar::utf8("shared dictionary payload λ ".repeat(128), nullable),
        },
        ("nested", "key") => Scalar::from(i64::try_from(row).unwrap()),
        ("nested", "items") => Scalar::list(
            DType::Primitive(PType::I64, nullable),
            (2 * row..2 * row + 2)
                .map(|item| {
                    if item.is_multiple_of(5) {
                        Scalar::null(DType::Primitive(PType::I64, nullable))
                    } else {
                        Scalar::primitive(i64::try_from(item).unwrap() - 200, nullable)
                    }
                })
                .collect(),
            required,
        ),
        _ => panic!("unexpected fixture field {cohort}.{name}"),
    }
}

fn verify_fixture(cohort: &str, arrays: &[ArrayRef], rows: usize) {
    let mut context = VortexSession::default().create_execution_ctx();
    let mut offset = 0;
    for array in arrays {
        let names = array.dtype().as_struct_fields().names();
        for row in 0..array.len() {
            let value = array.execute_scalar(row, &mut context).unwrap();
            for name in names.iter() {
                assert_eq!(
                    value.as_struct().field(name).unwrap(),
                    expected_field(cohort, name.as_ref(), offset + row),
                    "{cohort}.{name} row {}",
                    offset + row
                );
            }
        }
        offset += array.len();
    }
    assert_eq!(offset, rows);
}

#[test]
fn independent_ipc_fixtures_preserve_all_values_encodings_and_reusable_sources() {
    use crate::resident_memory_source::ResidentMemorySource;
    for (name, rows) in [
        ("mixed", 4099),
        ("encoded", 65_539),
        ("nested", 257),
        ("empty", 0),
    ] {
        let dtype = std::fs::read(fixture_root().join(format!("{name}.dtype.vxipc"))).unwrap();
        let bytes = std::fs::read(fixture_root().join(format!("{name}.array.vxipc"))).unwrap();
        let session = ResidentVortexSession::new(256 << 20, 1).unwrap();
        let copied = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                receive(&dtype, &bytes, context)
            })
            .unwrap();
        let array = copied.arrays.values[0].0.clone();
        assert_eq!(array.len(), rows);
        if name == "encoded" {
            let structure = array.as_::<vortex::array::arrays::Struct>();
            let ids = structure
                .fields()
                .iter()
                .map(|field| field.encoding_id().as_str().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                ids,
                [
                    "vortex.constant",
                    "vortex.dict",
                    "vortex.runend",
                    "fastlanes.bitpacked.v2"
                ]
            );
            assert!(copied.report.encoded_buffer_bytes_copied < 256 << 10);
        }
        drop((array, copied));
        let source = ResidentMemorySource::from_vortex_ipc(
            &session,
            &dtype,
            &bytes,
            &CancellationToken::default(),
        )
        .unwrap();
        let receipt = source.intake_transfer_report();
        assert_eq!(
            receipt.ipc_transport_bytes,
            (dtype.len() + bytes.len()) as u64
        );
        assert!(receipt.ipc_buffer_bytes_copied > 0);
        assert!(receipt.encoded_array_nodes_copied > 0);
        assert_eq!(receipt.materialized_batches, 0);
        if name == "encoded" {
            assert!(receipt.ipc_coordinate_arrays_executed >= 3);
            assert!(receipt.ipc_coordinate_values_checked >= rows as u64);
        }
        assert_eq!(
            source.intake_columnar_buffer_bytes_copied(),
            receipt.ipc_buffer_bytes_copied + receipt.encoded_buffer_bytes_copied
        );
        drop((dtype, bytes));
        verify_fixture(name, std::slice::from_ref(&source.0.array), rows);
        let before = session.snapshot().completed_executions;
        let names = source.dtype().as_struct_fields().names();
        let first = source
            .prepare_projection(&[names[0].as_ref()], None, Some(rows.min(17)))
            .unwrap();
        let second = source
            .prepare_projection(&[names[names.len() - 1].as_ref()], None, Some(rows.min(23)))
            .unwrap();
        for (prepared, limit) in [
            (&first, rows.min(17)),
            (&second, rows.min(23)),
            (&first, rows.min(17)),
        ] {
            let result = prepared.execute_arrays().unwrap();
            verify_fixture(name, result.arrays(), limit);
        }
        assert_eq!(session.snapshot().completed_executions, before + 3);
        assert_eq!(source.intake_transfer_report(), receipt);
        drop((first, second, source));
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn independent_native_files_preserve_complete_values_schema_and_later_execution() {
    for (name, rows) in [
        ("mixed", 4099),
        ("encoded", 65_539),
        ("nested", 257),
        ("empty", 0),
    ] {
        let session = ResidentVortexSession::new(256 << 20, 1).unwrap();
        let source = session
            .prepare_file(fixture_root().join(format!("{name}.vortex")))
            .unwrap();
        assert_eq!(source.prepare_count().execute().unwrap(), rows as u64);
        let names = source
            .dtype()
            .as_struct_fields()
            .names()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>();
        let all = source
            .prepare_projection(&names, rows.max(1) as u64, 256 << 20)
            .unwrap();
        let result = all.execute().unwrap();
        assert_eq!(result.dtype(), source.dtype());
        verify_fixture(name, result.arrays(), rows);
        drop(result);
        let later = source
            .prepare_projection(&names[..1], rows.clamp(1, 19) as u64, 1 << 20)
            .unwrap();
        let result = later.execute().unwrap();
        verify_fixture(name, result.arrays(), rows.min(19));
        drop((result, later, all, source));
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn ipc_rebuilds_sliced_packed_patches_without_trusting_the_wire_cache() {
    use vortex::{
        array::{Array, ArrayParts, patches::Patches},
        encodings::fastlanes::{BitPacked, BitPackedArrayExt as _, BitPackedData, BitPackedSlots},
    };
    let positions = [10, 100, 1030, 1190, 1510, 2160];
    let integers = PrimitiveArray::from_iter(
        (0..3000).map(|row| if positions.contains(&row) { 127i32 } else { 1 }),
    )
    .into_array();
    let mut execution = VortexSession::default().create_execution_ctx();
    let packed = BitPackedData::encode(&integers, 2, &mut execution).unwrap();
    let original_patches = packed.patches().unwrap();
    let patches = Patches::new(
        3000,
        0,
        original_patches.indices().clone(),
        original_patches.values().clone(),
        Some(PrimitiveArray::from_iter([0u64, 2, 5]).into_array()),
        &mut vortex::array::VortexSessionExecute::create_execution_ctx(
            &vortex::array::array_session(),
        ),
    )
    .unwrap()
    .slice(50..2250, &mut execution)
    .unwrap()
    .unwrap();
    assert_eq!(patches.offset_within_chunk(), Some(1));
    let data =
        BitPackedData::try_new(packed.packed().clone(), Some(patches.clone()), 2, 50).unwrap();
    let slots = BitPackedSlots {
        patch_indices: Some(patches.indices().clone()),
        patch_values: Some(patches.values().clone()),
        patch_chunk_offsets: patches.chunk_offsets().clone(),
        validity_child: None,
        block_offsets: None,
    }
    .into_slots();
    let packed = Array::<BitPacked>::try_from_parts(ArrayParts::new(
        BitPacked,
        integers.dtype().clone(),
        2200,
        data,
        slots,
    ))
    .unwrap()
    .into_array();
    let input = StructArray::from_fields(&[("packed", packed)])
        .unwrap()
        .into_array();
    let (dtype, bytes) = messages(&input);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let copied = session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            receive(&dtype, &bytes, context)
        })
        .unwrap();
    let structure = copied.arrays.values[0]
        .0
        .as_::<vortex::array::arrays::Struct>();
    let child = &structure.fields()[0];
    assert!(
        child
            .as_::<BitPacked>()
            .patches()
            .unwrap()
            .chunk_offsets()
            .is_none()
    );
    for row in 0..2200 {
        let expected = if positions.contains(&(row + 50)) {
            127i32
        } else {
            1
        };
        assert_eq!(
            i32::try_from(&child.execute_scalar(row, &mut execution).unwrap()).unwrap(),
            expected
        );
    }
    drop(copied);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

fn layout(array: &ArrayRef, depth: usize, output: &mut String) {
    use std::fmt::Write as _;
    let buffers: Vec<_> = array
        .buffer_handles()
        .iter()
        .map(|buffer| buffer.as_host_opt().map(vortex::buffer::Buffer::len))
        .collect();
    writeln!(
        output,
        "{}{} rows={} buffers={buffers:?}",
        "  ".repeat(depth),
        array.encoding_id(),
        array.len()
    )
    .unwrap();
    for child in array.children_iter() {
        layout(child, depth + 1, output);
    }
}

#[test]
fn independent_completed_file_columns_preserve_encoded_transfer() {
    fn encodings(array: &ArrayRef, found: &mut std::collections::BTreeSet<String>) {
        found.insert(array.encoding_id().as_str().to_owned());
        for child in array.children_iter() {
            encodings(child, found);
        }
    }
    let mut expanded = Vec::new();
    for (column, rows, maximum_copied_bytes) in [
        ("constant", 65_539, 0),
        ("dictionary", 65_539, 1 << 20),
        ("run_end", 65_539, 65_539),
        ("packed", 65_539, 65_539),
        ("packed", 17, 68),
    ] {
        let producer = ResidentVortexSession::new(256 << 20, 1).unwrap();
        let receiving = ResidentVortexSession::new(256 << 20, 1).unwrap();
        let file = producer
            .prepare_file(fixture_root().join("encoded.vortex"))
            .unwrap();
        let plan = file.prepare_projection(&[column], rows, 256 << 20).unwrap();
        let result = plan.execute().unwrap();
        let mut physical = std::collections::BTreeSet::new();
        for array in result.arrays() {
            encodings(array, &mut physical);
        }
        let source =
            crate::resident_memory_source::ResidentMemorySource::from_completed_result_in_session(
                &receiving,
                &result,
                &CancellationToken::default(),
            )
            .unwrap();
        let report = source.intake_transfer_report();
        if (rows > 17 && report.materialized_rows != 0)
            || source.intake_columnar_buffer_bytes_copied() > maximum_copied_bytes
        {
            let mut details = String::new();
            if let Some(first) = result.arrays().first() {
                layout(first, 0, &mut details);
            }
            expanded.push(format!(
                "{column}/{rows}: {physical:?}, {report:?}\n{details}"
            ));
        }
        drop((result, plan, file));
        assert_eq!(producer.snapshot().memory.reserved_bytes, 0);
        verify_fixture(
            "encoded",
            std::slice::from_ref(&source.0.array),
            usize::try_from(rows).unwrap(),
        );
        drop(source);
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    }
    assert!(
        expanded.is_empty(),
        "encoded native file transfer expanded: {expanded:?}"
    );
}

#[test]
fn independent_relational_file_columns_preserve_compact_transfer() {
    use crate::{
        VortexLocalPrimitiveExecutionPolicy,
        local_primitives::prepared_relational::prepare_relational_with_schema_in_session,
        relational_query::{
            VortexRelationalLimit, VortexRelationalPlan as Plan, VortexRelationalProject,
            VortexRelationalScan,
        },
    };
    use shardloom_core::{ColumnRef, DatasetUri, ExprId, Expression, ExpressionKind};
    let mut expanded = Vec::new();
    for (column, rows, maximum_copied_bytes) in [
        ("constant", 65_539_usize, 0),
        ("dictionary", 65_539, 1 << 20),
        ("run_end", 65_539, 65_539),
        ("packed", 65_539, 65_539),
        ("packed", 17, 68),
    ] {
        let producer = ResidentVortexSession::new(256 << 20, 1).unwrap();
        let receiving = ResidentVortexSession::new(256 << 20, 1).unwrap();
        let field = ColumnRef::new(column).unwrap();
        let scan = Plan::Scan(VortexRelationalScan {
            source_uri: DatasetUri::new(
                fixture_root().join("encoded.vortex").display().to_string(),
            )
            .unwrap(),
            projection: shardloom_plan::ProjectionRequest::Columns(vec![field.clone()]),
            predicate: None,
        });
        let projected = Plan::Project(Box::new(VortexRelationalProject {
            input: scan,
            expressions: vec![(
                column.to_owned(),
                Expression::new(ExprId::new("copy").unwrap(), ExpressionKind::Column(field)),
            )],
        }));
        let request = Plan::Limit(Box::new(VortexRelationalLimit {
            input: projected,
            offset: 0,
            count: rows,
        }));
        let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
        policy.resource_envelope.memory_budget_bytes = 256 << 20;
        let prepared =
            prepare_relational_with_schema_in_session(policy, &producer, |_| Ok(request)).unwrap();
        let completed = prepared
            .execute_native(&CancellationToken::default())
            .unwrap();
        let source =
            crate::resident_memory_source::ResidentMemorySource::from_completed_result_in_session(
                &receiving,
                &completed.result,
                &CancellationToken::default(),
            )
            .unwrap();
        let report = source.intake_transfer_report();
        if (rows > 17 && report.materialized_rows != 0)
            || source.intake_columnar_buffer_bytes_copied() > maximum_copied_bytes
        {
            let mut details = String::new();
            if let Some(first) = completed.result.arrays().first() {
                layout(first, 0, &mut details);
            }
            expanded.push(format!("{column}/{rows}: {report:?}\n{details}"));
        }
        drop((completed, prepared));
        assert_eq!(producer.snapshot().memory.reserved_bytes, 0);
        verify_fixture("encoded", std::slice::from_ref(&source.0.array), rows);
        drop(source);
        assert_eq!(receiving.snapshot().memory.reserved_bytes, 0);
    }
    assert!(
        expanded.is_empty(),
        "excessive physical copies: {expanded:?}"
    );
}
