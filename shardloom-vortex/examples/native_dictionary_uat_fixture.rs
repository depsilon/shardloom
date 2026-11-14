//! Bounded explicit dictionary fixtures for public text-consumer verification.

use std::{fs::OpenOptions, path::Path, sync::Arc};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::layouts::{chunked::writer::ChunkedLayoutStrategy, flat::writer::FlatLayoutStrategy},
    session::VortexSession,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const PROFILES: [&str; 6] = [
    "dictionary",
    "sparse",
    "all-null",
    "unique",
    "flat",
    "empty",
];

fn domain(
    profile: &str,
    chunk: usize,
    start: usize,
    rows: usize,
    public: bool,
) -> Vec<Option<String>> {
    if profile == "unique" {
        return (start..start + rows)
            .map(|row| Some(format!(" unique_{row:08}_éΣ ")))
            .collect();
    }
    if profile == "sparse" {
        let mut values = vec![Some("unused".to_owned()); if public { 16 } else { 128 }];
        values[0] = Some(format!(" {} ", ["A", "B", "C"][chunk]));
        values[1] = Some("UNREFERENCED İΣΟΣ ".repeat(if public { 128 } else { 4096 }));
        let indices = if public { [5, 11, 15] } else { [17, 63, 127] };
        values[indices[0]] = Some("a".into());
        values[indices[1]] = None;
        values[indices[2]] = Some("\u{2003}東京\0\u{2003}".into());
        return values;
    }
    vec![
        Some(format!(" {} ", ["A", "B", "C"][chunk])),
        Some("a".into()),
        Some(" \u{2003}ΣΟΣİß東京\0 e\u{301} Case \u{2003}".repeat(8)),
        None,
        Some(String::new()),
        Some(" \u{2003}Λ\u{2003} ".into()),
        Some("İßΣΟΣ".into()),
        Some("unreferenced valid text".into()),
    ]
}

fn code(profile: &str, local: usize, row: usize, public: bool) -> Option<u16> {
    if row.is_multiple_of(11) {
        return None;
    }
    Some(match profile {
        "sparse" => {
            (if public {
                [0, 5, 11, 15]
            } else {
                [0, 17, 63, 127]
            })[local % 4]
        }
        "all-null" => 3,
        "unique" => u16::try_from(local).expect("bounded per-chunk domain"),
        _ => u16::try_from(local % 7).expect("seven-entry observed domain"),
    })
}

fn batch(profile: &str, chunk: usize, start: usize, rows: usize, public: bool) -> Result<ArrayRef> {
    let values = domain(profile, chunk, start, rows, public);
    let codes = (0..rows)
        .map(|local| code(profile, local, start + local, public))
        .collect::<Vec<_>>();
    let text = if profile == "flat" {
        VarBinViewArray::from_iter_nullable_str(
            codes
                .iter()
                .map(|code| code.and_then(|index| values[usize::from(index)].as_deref())),
        )
        .into_array()
    } else {
        DictArray::try_new(
            PrimitiveArray::from_option_iter(codes).into_array(),
            VarBinViewArray::from_iter_nullable_str(values.iter().map(Option::as_deref))
                .into_array(),
        )?
        .into_array()
    };
    Ok(StructArray::from_fields(&[
        (
            "row_id",
            PrimitiveArray::from_iter(
                (start..start + rows)
                    .map(|row| i64::try_from(row).expect("bounded row identifier")),
            )
            .into_array(),
        ),
        ("value", text),
    ])?
    .into_array())
}

fn write(root: &Path, profile: &str, public: bool) -> Result<()> {
    let lengths = if profile == "empty" {
        vec![0]
    } else if public {
        vec![23, 24, 24]
    } else {
        vec![4097; 3]
    };
    // There are at most three leaves. Flat serialization preserves the supplied
    // arrays; no compressor or source-dictionary preservation heuristic runs.
    let strategy = Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let first = batch(profile, 0, 0, lengths[0], public)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{profile}.vortex")))?;
    let mut writer = session
        .write_options()
        .with_strategy(strategy)
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, first.dtype().clone());
    writer.push(first)?;
    let mut start = lengths[0];
    for (chunk, rows) in lengths.into_iter().enumerate().skip(1) {
        writer.push(batch(profile, chunk, start, rows, public)?)?;
        start += rows;
    }
    if writer.finish()?.row_count() != start as u64 {
        return Err("dictionary fixture row count differs".into());
    }
    output.sync_all()?;
    println!("{profile}: {start} rows");
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err(
            "usage: native_dictionary_uat_fixture <guarded-existing-directory> <public|work>"
                .into(),
        );
    }
    let root = Path::new(&args[0]);
    if !root.is_dir() {
        return Err("fixture directory must exist under the UAT storage guard".into());
    }
    let public = match args[1].to_str() {
        Some("public") => true,
        Some("work") => false,
        _ => return Err("fixture size must be public or work".into()),
    };
    for profile in PROFILES {
        write(root, profile, public)?;
    }
    Ok(())
}
