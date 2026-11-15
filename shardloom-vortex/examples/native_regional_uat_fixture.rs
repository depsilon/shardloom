//! Bounded native zoned fixtures; query validation uses the public entry points.

use std::{fs::OpenOptions, num::NonZeroUsize, path::Path, sync::Arc};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        aggregate_fn::{
            AggregateFnVTableExt as _, EmptyOptions, NumericalAggregateOpts,
            fns::{max::Max, min::Min, null_count::NullCount},
        },
        arrays::{PrimitiveArray, StructArray},
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::{
        LayoutStrategy,
        layouts::{
            chunked::writer::ChunkedLayoutStrategy,
            flat::writer::FlatLayoutStrategy,
            repartition::{RepartitionStrategy, RepartitionWriterOptions},
            struct_::StructStrategy,
            zoned::writer::{ZonedLayoutOptions, ZonedStrategy},
        },
    },
    session::VortexSession,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn zoned(length: usize, missing_min: bool) -> Arc<dyn LayoutStrategy> {
    let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let mut functions = vec![
        NullCount.bind(EmptyOptions),
        Max.bind(NumericalAggregateOpts::skip_nans()),
    ];
    if !missing_min {
        functions.push(Min.bind(NumericalAggregateOpts::skip_nans()));
    }
    Arc::new(RepartitionStrategy::new(
        ZonedStrategy::new(
            ChunkedLayoutStrategy::new(flat.clone()),
            flat,
            ZonedLayoutOptions {
                block_size: NonZeroUsize::new(length).unwrap(),
                concurrency: NonZeroUsize::new(1).unwrap(),
                aggregate_fns: Some(functions.into()),
            },
        ),
        RepartitionWriterOptions {
            block_size_minimum: 0,
            block_len_multiple: length,
            block_size_target: None,
            canonicalize: false,
        },
    ))
}

fn write(root: &Path, name: &str, rows: usize, lengths: [usize; 3]) -> Result<()> {
    let row_count = if name == "empty" { 0 } else { rows };
    let values = StructArray::try_new(
        ["selector", "signed_value", "unsigned_value"].into(),
        vec![
            PrimitiveArray::from_option_iter((0..row_count).map(|row| {
                if name == "nullable-predicate" && row < lengths[0] {
                    None
                } else {
                    Some(i64::try_from(row).expect("bounded fixture index"))
                }
            }))
            .into_array(),
            PrimitiveArray::from_option_iter((0..row_count).map(|row| {
                if name == "all-null" || row % 7 == 0 {
                    None
                } else {
                    Some(match row % 4 {
                        0 => i64::MIN,
                        1 => i64::MAX,
                        2 => 9_007_199_254_740_993,
                        _ => -i64::try_from(row).expect("bounded fixture index"),
                    })
                }
            }))
            .into_array(),
            PrimitiveArray::from_option_iter(
                (0..row_count).map(|row| (row % 5 != 0).then_some(u64::MAX - row as u64)),
            )
            .into_array(),
        ],
        row_count,
        Validity::NonNullable,
    )?
    .into_array();
    let flat: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let strategy: Arc<dyn LayoutStrategy> = if name == "flat" {
        Arc::new(StructStrategy::new(flat.clone(), flat))
    } else {
        Arc::new(
            StructStrategy::new(flat.clone(), flat)
                .with_field_writer("selector", zoned(lengths[0], false))
                .with_field_writer("signed_value", zoned(lengths[1], name == "missing-min"))
                .with_field_writer("unsigned_value", zoned(lengths[2], false)),
        )
    };
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join(format!("{name}.vortex")))?;
    let mut writer = session
        .write_options()
        .with_strategy(strategy)
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, values.dtype().clone());
    writer.push(values)?;
    if writer.finish()?.row_count() != row_count as u64 {
        return Err("fixture writer row count differs".into());
    }
    output.sync_all()?;
    println!("{name}: {row_count} rows, zone lengths {lengths:?}");
    Ok(())
}

fn main() -> Result<()> {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    if arguments.len() != 2 {
        return Err(
            "usage: native_regional_uat_fixture <guarded-existing-directory> <public|work>".into(),
        );
    }
    let root = Path::new(&arguments[0]);
    if !root.is_dir() {
        return Err("fixture directory must already exist under the UAT storage guard".into());
    }
    let (rows, lengths, independent) = match arguments[1].to_str() {
        Some("public") => (71, [8, 4, 8], [8, 4, 6]),
        Some("work") => (1_048_577, [16384, 8192, 16384], [16384, 8192, 12288]),
        _ => return Err("fixture size must be public or work".into()),
    };
    for name in [
        "complete",
        "all-null",
        "nullable-predicate",
        "missing-min",
        "flat",
        "empty",
    ] {
        write(root, name, rows, lengths)?;
    }
    write(root, "independent", rows, independent)?;
    Ok(())
}
