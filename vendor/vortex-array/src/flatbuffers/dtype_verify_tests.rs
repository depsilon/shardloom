// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

use std::env;
use std::process::Command;
use std::sync::Arc;
use std::thread;

use flatbuffers::{FlatBufferBuilder, Verifiable, VerifierOptions, WIPOffset};
use vortex_session::registry::CachedId;

use super::*;
use crate::dtype::extension::ForeignExtDType;
use crate::dtype::{DType, DecimalDType, Nullability, PType, StructFields, UnionVariants};
use crate::flatbuffers::{WriteFlatBuffer, WriteFlatBufferExt};

#[path = "dtype_verify_reference.rs"]
mod reference;

const KINDS: [&str; 7] = [
    "list",
    "fixed_list",
    "map_key",
    "map_value",
    "struct",
    "union",
    "extension",
];

fn wrap(kind: &str, child: DType) -> DType {
    let nullable = Nullability::NonNullable;
    match kind {
        "list" => DType::List(Arc::new(child), nullable),
        "fixed_list" => DType::FixedSizeList(Arc::new(child), 7, nullable),
        "map_key" => DType::map(child, DType::Bool(nullable), true, nullable).unwrap(),
        "map_value" => DType::map(DType::Bool(nullable), child, false, nullable).unwrap(),
        "struct" => DType::Struct(StructFields::from_iter([("naïve\"", child)]), nullable),
        "union" => DType::Union(
            UnionVariants::try_new(["child"].into(), vec![child], vec![255]).unwrap(),
            nullable,
        ),
        "extension" => {
            static ID: CachedId = CachedId::new("test.physical_dtype_verification");
            DType::Extension(ForeignExtDType::from_parts(*ID, vec![0, 255, 7], child).unwrap())
        }
        _ => panic!("unknown verifier fixture"),
    }
}

fn chain(kind: &str, depth: usize) -> DType {
    (0..depth).fold(DType::Bool(Nullability::NonNullable), |child, index| {
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

fn finish<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    tag: fb::Type,
    payload: WIPOffset<flatbuffers::UnionWIPOffset>,
) -> Vec<u8> {
    let root = fb::DType::create(
        builder,
        &fb::DTypeArgs {
            type_type: tag,
            type_: Some(payload),
        },
    );
    builder.finish_minimal(root);
    builder.finished_data().to_vec()
}

fn fixtures() -> Vec<Vec<u8>> {
    let mut dtypes = vec![
        DType::Null,
        DType::Struct(StructFields::empty(), Nullability::NonNullable),
    ];
    for nullable in [Nullability::NonNullable, Nullability::Nullable] {
        dtypes.extend([
            DType::Bool(nullable),
            DType::Binary(nullable),
            DType::Utf8(nullable),
            DType::Variant(nullable),
            DType::Decimal(DecimalDType::try_new(38, -4).unwrap(), nullable),
        ]);
        for ptype in [
            PType::U8,
            PType::U16,
            PType::U32,
            PType::U64,
            PType::I8,
            PType::I16,
            PType::I32,
            PType::I64,
            PType::F16,
            PType::F32,
            PType::F64,
        ] {
            dtypes.push(DType::Primitive(ptype, nullable));
        }
        for kind in KINDS.into_iter().chain(["mixed"]) {
            dtypes.push(chain(kind, 4).with_nullability(nullable));
        }
    }
    dtypes.push(DType::Struct(
        StructFields::from_iter([
            ("first", chain("mixed", 3)),
            ("second", chain("extension", 3)),
            ("last", DType::Null),
        ]),
        Nullability::Nullable,
    ));
    dtypes.push(DType::Union(
        UnionVariants::try_new(
            ["one", "two", "three"].into(),
            vec![
                DType::Null,
                chain("mixed", 4),
                DType::Variant(Nullability::Nullable),
            ],
            vec![0, 255, 7],
        )
        .unwrap(),
        Nullability::Nullable,
    ));
    let mut fixtures = Vec::new();
    for dtype in dtypes {
        for defaults in [false, true] {
            let mut builder = FlatBufferBuilder::new();
            builder.force_defaults(defaults);
            let offset = dtype.write_flatbuffer(&mut builder).unwrap();
            builder.finish_minimal(offset);
            fixtures.push(builder.finished_data().to_vec());
        }
    }
    // Every optional compound field may be absent even when logical parsing
    // would later refuse the declaration.
    for tag in [
        fb::Type::List,
        fb::Type::FixedSizeList,
        fb::Type::Map,
        fb::Type::Struct_,
        fb::Type::Union,
        fb::Type::Extension,
    ] {
        let mut builder = FlatBufferBuilder::new();
        let payload = match tag {
            fb::Type::List => {
                fb::List::create(&mut builder, &fb::ListArgs::default()).as_union_value()
            }
            fb::Type::FixedSizeList => {
                fb::FixedSizeList::create(&mut builder, &fb::FixedSizeListArgs::default())
                    .as_union_value()
            }
            fb::Type::Map => {
                fb::Map::create(&mut builder, &fb::MapArgs::default()).as_union_value()
            }
            fb::Type::Struct_ => {
                fb::Struct_::create(&mut builder, &fb::Struct_Args::default()).as_union_value()
            }
            fb::Type::Union => {
                fb::Union::create(&mut builder, &fb::UnionArgs::default()).as_union_value()
            }
            fb::Type::Extension => {
                fb::Extension::create(&mut builder, &fb::ExtensionArgs::default()).as_union_value()
            }
            _ => unreachable!(),
        };
        fixtures.push(finish(&mut builder, tag, payload));
    }
    for tag in [fb::Type::NONE, fb::Type::Null, fb::Type(255)] {
        for present in [false, true] {
            let mut builder = FlatBufferBuilder::new();
            let payload =
                present.then(|| fb::Null::create(&mut builder, &fb::NullArgs {}).as_union_value());
            let root = fb::DType::create(
                &mut builder,
                &fb::DTypeArgs {
                    type_type: tag,
                    type_: payload,
                },
            );
            builder.finish_minimal(root);
            fixtures.push(builder.finished_data().to_vec());
        }
    }
    fixtures
}

fn outcome(bytes: &[u8], options: &VerifierOptions, original: bool) -> Result<()> {
    let mut verifier = Verifier::new(options, bytes);
    if original {
        reference::verify_root(&mut verifier).map_err(reference::normalize_marker_alignment)
    } else {
        <ForwardsUOffset<fb::DType>>::run_verifier(&mut verifier, 0)
    }
}

fn compare(bytes: &[u8], options: &VerifierOptions) {
    assert_eq!(
        outcome(bytes, options, false),
        outcome(bytes, options, true)
    );
}

#[test]
fn physical_verifier_matches_original_errors_for_every_mutated_and_truncated_fixture() {
    let fixtures = fixtures();
    assert!(fixtures.len() > 100);
    for (index, bytes) in fixtures.iter().enumerate() {
        let options = VerifierOptions::default();
        compare(bytes, &options);
        for end in 0..bytes.len() {
            compare(&bytes[..end], &options);
        }
        for position in 0..bytes.len() {
            for replacement in [0, 1, 0x7f, 0x80, 0xff] {
                let mut changed = bytes.clone();
                changed[position] = replacement;
                let expected = outcome(&changed, &options, true);
                assert_eq!(
                    outcome(&changed, &options, false),
                    expected,
                    "fixture {index} byte {position} = {replacement}"
                );
                compare(
                    &changed,
                    &VerifierOptions {
                        ignore_missing_null_terminator: true,
                        ..options.clone()
                    },
                );
            }
        }
    }
}

fn limit(options: &mut VerifierOptions, axis: usize, value: usize) {
    match axis {
        0 => options.max_depth = value,
        1 => options.max_tables = value,
        2 => options.max_apparent_size = value,
        _ => unreachable!(),
    }
}

#[test]
fn physical_verifier_preserves_exact_configured_budget_thresholds() {
    for bytes in fixtures() {
        if outcome(&bytes, &VerifierOptions::default(), true).is_err() {
            continue;
        }
        for axis in 0..3 {
            let mut low = 0;
            let mut high = match axis {
                0 => 64,
                1 => 1_000_000,
                _ => 1 << 31,
            };
            while low < high {
                let middle = low + (high - low) / 2;
                let mut options = VerifierOptions::default();
                limit(&mut options, axis, middle);
                compare(&bytes, &options);
                if outcome(&bytes, &options, true).is_ok() {
                    high = middle;
                } else {
                    low = middle + 1;
                }
            }
            for value in [0, low.saturating_sub(1), low, low + 1] {
                let mut options = VerifierOptions::default();
                limit(&mut options, axis, value);
                compare(&bytes, &options);
                assert_eq!(outcome(&bytes, &options, false).is_ok(), value >= low);
            }
        }
    }
}

#[test]
fn repeated_child_offsets_consume_expanded_work_credit() {
    let mut builder = FlatBufferBuilder::new();
    let mut child = DType::Bool(Nullability::NonNullable)
        .write_flatbuffer(&mut builder)
        .unwrap();
    let first = builder.create_string("first");
    let second = builder.create_string("second");
    let names = builder.create_vector(&[first, second]);
    for _ in 0..10 {
        let dtypes = builder.create_vector(&[child, child]);
        let payload = fb::Struct_::create(
            &mut builder,
            &fb::Struct_Args {
                names: Some(names),
                dtypes: Some(dtypes),
                nullable: false,
            },
        );
        child = fb::DType::create(
            &mut builder,
            &fb::DTypeArgs {
                type_type: fb::Type::Struct_,
                type_: Some(payload.as_union_value()),
            },
        );
    }
    builder.finish_minimal(child);
    let bytes = builder.finished_data();
    compare(bytes, &VerifierOptions::default());
    assert!(outcome(bytes, &VerifierOptions::default(), false).is_ok());
    for (options, expected) in [
        (
            VerifierOptions {
                max_tables: 100,
                ..VerifierOptions::default()
            },
            InvalidFlatbuffer::TooManyTables,
        ),
        (
            VerifierOptions {
                max_apparent_size: 4096,
                ..VerifierOptions::default()
            },
            InvalidFlatbuffer::ApparentSizeTooLarge,
        ),
    ] {
        compare(bytes, &options);
        assert_eq!(outcome(bytes, &options, false), Err(expected));
    }
}

fn descendant<'a>(dtype: fb::DType<'a>, kind: &str) -> fb::DType<'a> {
    match dtype.type_type() {
        fb::Type::List => dtype.type__as_list().unwrap().element_type().unwrap(),
        fb::Type::FixedSizeList => dtype
            .type__as_fixed_size_list()
            .unwrap()
            .element_type()
            .unwrap(),
        fb::Type::Extension => dtype.type__as_extension().unwrap().storage_dtype().unwrap(),
        fb::Type::Struct_ => dtype.type__as_struct_().unwrap().dtypes().unwrap().get(0),
        fb::Type::Union => dtype.type__as_union().unwrap().dtypes().unwrap().get(0),
        fb::Type::Map => {
            let map = dtype.type__as_map().unwrap();
            if kind == "map_key" {
                map.key_type().unwrap()
            } else {
                map.value_type().unwrap()
            }
        }
        _ => panic!("not a compound fixture"),
    }
}

#[test]
fn deep_physical_verifier_on_small_stack() {
    const SELECTOR: &str = "VORTEX_TEST_DEEP_PHYSICAL_VERIFIER";
    const COMPLETED: &str = "deep physical verifier completed";
    if let Ok(kind) = env::var(SELECTOR) {
        let marker = format!("{COMPLETED} {kind}");
        thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || {
                let dtype = chain(&kind, 8192);
                let bytes = dtype.write_flatbuffer_bytes().unwrap();
                assert_eq!(
                    outcome(&bytes, &VerifierOptions::default(), false),
                    Err(InvalidFlatbuffer::DepthLimitReached)
                );
                let options = VerifierOptions {
                    max_depth: 32_768,
                    ..VerifierOptions::default()
                };
                let mut current =
                    flatbuffers::root_with_opts::<fb::DType>(&options, &bytes).unwrap();
                for remaining in (0..8192).rev() {
                    let child_kind = if kind == "mixed" {
                        KINDS[remaining % KINDS.len()]
                    } else {
                        &kind
                    };
                    current = descendant(current, child_kind);
                }
                assert_eq!(current.type_type(), fb::Type::Bool);
                let location = current._tab.loc();
                let mut corrupt = bytes.to_vec();
                corrupt[location..location + 4].copy_from_slice(&i32::MIN.to_le_bytes());
                let error = outcome(&corrupt, &options, false).unwrap_err();
                match error {
                    InvalidFlatbuffer::SignedOffsetOutOfBounds { error_trace, .. } => {
                        assert!(error_trace.as_ref().len() >= 8192 * 3)
                    }
                    _ => panic!("wrong deep physical corruption error"),
                }
                drop(dtype);
            })
            .unwrap()
            .join()
            .unwrap();
        println!("\n{marker}");
        return;
    }
    for kind in KINDS.into_iter().chain(["mixed"]) {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "flatbuffers::dtype_verify::tests::deep_physical_verifier_on_small_stack",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(SELECTOR, kind)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "physical verifier {kind}: {}\n{}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == format!("{COMPLETED} {kind}"))
        );
    }
}
