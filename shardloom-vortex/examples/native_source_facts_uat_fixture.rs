//! Bounded native files with explicit float/integer encodings and footer controls.

use std::{fs::OpenOptions, path::Path, sync::Arc};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{PrimitiveArray, StructArray},
        expr::stats::Stat,
    },
    encodings::{
        alp::alp_encode,
        fastlanes::{BitPacked, FoRData},
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
const PROFILES: [&str; 12] = [
    "alp-f32",
    "alp-f64",
    "no-stats-f32",
    "no-stats-f64",
    "flat-f32",
    "flat-f64",
    "all-null-f32",
    "all-null-f64",
    "empty-f32",
    "empty-f64",
    "bitpacked-u64",
    "for-u64",
];

#[allow(clippy::cast_precision_loss)] // The modular numerator is at most 2047.
fn floating(row: usize, all_null: bool) -> Option<f64> {
    if all_null || row.is_multiple_of(17) {
        return None;
    }
    if row % 31 == 1 {
        return Some(-0.0);
    }
    if row % 29 == 1 {
        return Some(0.0);
    }
    // Explicit finite patches exercise ALP's exact patch correction.
    if row % 97 == 1 {
        return Some(std::f64::consts::PI);
    }
    Some((((row * 73) % 2048) as f64 - 1024.0) / 8.0)
}

#[allow(clippy::cast_possible_truncation)] // F32 fixture values intentionally use the native F32 domain.
fn batch(profile: &str, start: usize, rows: usize, session: &VortexSession) -> Result<ArrayRef> {
    let mut ctx = session.create_execution_ctx();
    let value = if profile.ends_with("u64") {
        let primitive = PrimitiveArray::from_option_iter(
            (start..start + rows)
                .map(|row| (!row.is_multiple_of(17)).then_some(((row * 73) % 2048) as u64)),
        );
        if profile == "bitpacked-u64" {
            BitPacked::encode(&primitive.into_array(), 11, &mut ctx)?.into_array()
        } else {
            FoRData::encode(primitive, &mut ctx)?.into_array()
        }
    } else {
        let values =
            (start..start + rows).map(|row| floating(row, profile.starts_with("all-null")));
        let primitive = if profile.ends_with("f32") {
            PrimitiveArray::from_option_iter(values.map(|value| value.map(|value| value as f32)))
        } else {
            PrimitiveArray::from_option_iter(values)
        };
        if profile.starts_with("alp-") || profile.starts_with("no-stats-") {
            alp_encode(primitive.as_view(), None, &mut ctx)?.into_array()
        } else {
            primitive.into_array()
        }
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
        ("value", value),
    ])?
    .into_array())
}

fn write(root: &Path, profile: &str, public: bool) -> Result<()> {
    let lengths: &[usize] = if profile.starts_with("empty-") {
        &[0]
    } else if public {
        &[23, 28, 28]
    } else {
        &[4097; 3]
    };
    // Preserve explicit arrays through flat serialization: this is an encoding
    // exposure fixture, not a compression heuristic or a production codec default.
    let strategy = Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let first = batch(profile, 0, lengths[0], &session)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{profile}.vortex")))?;
    let statistics = if profile.starts_with("no-stats-") {
        vec![]
    } else {
        [Stat::Min, Stat::Max, Stat::NullCount, Stat::NaNCount]
            .into_iter()
            .filter_map(|stat| stat.aggregate_fn())
            .collect()
    };
    let mut writer = session
        .write_options()
        .with_strategy(strategy)
        .with_file_statistics(statistics)
        .blocking(&runtime)
        .writer(&mut output, first.dtype().clone());
    writer.push(first)?;
    let mut start = lengths[0];
    for rows in &lengths[1..] {
        writer.push(batch(profile, start, *rows, &session)?)?;
        start += rows;
    }
    if writer.finish()?.row_count() != start as u64 {
        return Err("source-fact fixture row count differs".into());
    }
    output.sync_all()?;
    println!("{profile}: {start} rows");
    Ok(())
}

fn main() -> Result<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err(
            "usage: native_source_facts_uat_fixture <guarded-existing-directory> <public|work>"
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
