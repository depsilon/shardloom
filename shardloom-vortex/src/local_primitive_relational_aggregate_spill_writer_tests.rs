//! File-backed aggregates share the same spilled execution with every writer.

use super::*;
use crate::local_primitives::{
    VortexLocalPrimitiveRowExportFormat as Format, native_relational_spill::BEFORE_RUN_OPEN,
};
use std::{cell::Cell, fmt::Write as _, rc::Rc};

fn prepared_file(fixture: &Fixture, plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    let workspace = fixture.0.join("runs");
    fs::create_dir(&workspace).unwrap();
    let mut policy = policy();
    // This deliberately sliced VarBin file has repeated wide physical buffers.
    // Its reader and terminal writer share 64 MiB; separate streaming pressure
    // tests establish the 16 MiB grant with detached, bounded input batches.
    policy.resource_envelope.memory_budget_bytes = 64 << 20;
    prepare_relational(plan, policy)
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(workspace, 256 << 20, 1 << 20).unwrap())
        .unwrap()
}

fn fixture_and_expected() -> (Fixture, VortexRelationalPlan, Vec<Value>, String) {
    let total: usize = 8 * 1024 + 7;
    let strings = (0..total)
        .map(|row| format!("{row:09}{}", "x".repeat(503)))
        .collect::<Vec<_>>();
    let array = StructArray::try_new(
        FieldNames::from(["group", "number", "word"]),
        vec![
            PrimitiveArray::from_iter((0..total).map(|row| i64::try_from(row % 3).unwrap()))
                .into_array(),
            PrimitiveArray::from_option_iter(
                (0..total)
                    .map(|row| (!row.is_multiple_of(31)).then(|| i64::try_from(row).unwrap())),
            )
            .into_array(),
            VarBinArray::from_iter(
                strings.iter().map(|value| Some(value.as_str())),
                DType::Utf8(Nullability::NonNullable),
            )
            .into_array(),
        ],
        total,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let fixture = Fixture::new(array, 1024);
    let plan = aggregate(
        fixture.scan(),
        &["group"],
        vec![
            measure("count", None, "rows"),
            measure("count", Some("number"), "present"),
            measure("count_distinct", Some("word"), "words"),
            measure("min", Some("number"), "min"),
            measure("max", Some("number"), "max"),
        ],
    );
    let mut csv = "group,rows,present,words,min,max\n".to_owned();
    let expected = (0..3)
        .map(|group| {
            let rows = (0..total)
                .filter(|row| row % 3 == group)
                .collect::<Vec<_>>();
            let numbers = rows
                .iter()
                .copied()
                .filter(|row| !row.is_multiple_of(31))
                .collect::<Vec<_>>();
            let (count, present, min, max) = (
                rows.len(),
                numbers.len(),
                numbers[0],
                numbers[numbers.len() - 1],
            );
            writeln!(csv, "{group},{count},{present},{count},{min},{max}").unwrap();
            json!({"group":group,"rows":count,"present":present,"words":count,"min":min,"max":max})
        })
        .collect();
    (fixture, plan, expected, csv)
}

#[test]
fn ordered_aggregate_spilled_all_eight_writers_reopen_every_group_and_type() {
    let (fixture, plan, expected, csv) = fixture_and_expected();
    let prepared = prepared_file(&fixture, &plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for (index, format) in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Orc,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ]
    .into_iter()
    .enumerate()
    {
        let path = fixture.0.join(format!("aggregate.{}", format.as_str()));
        let result = prepared
            .write(&path, format, false)
            .unwrap_or_else(|error| panic!("{format:?}: {error}"));
        assert_eq!(result.output.rows_written, 3);
        assert_eq!(result.execution.ordered_aggregate_stages, 1);
        assert_eq!(result.execution.ordered_aggregate_input_rows, 8 * 1024 + 7);
        assert!(result.execution.runtime.memory.peak_reserved_bytes <= 64 << 20);
        let spill = result.execution.spill.as_ref().unwrap();
        assert!(
            spill.runs_written > 3 && spill.merge_passes > 1 && spill.owned_cleanup_completed,
            "{format:?}: {spill:?}"
        );
        if format == Format::Csv {
            assert_eq!(fs::read_to_string(&path).unwrap(), csv);
        } else {
            assert_eq!(
                super::super::super::writer_tests::read_rows(
                    &path,
                    format,
                    &prepared.output_dtype().unwrap()
                ),
                expected,
                "{format:?}"
            );
        }
        drop(result);
        assert_eq!(prepared.snapshot().completed_executions, (index + 1) as u64);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(fixture.0.join("runs")).unwrap().count(), 0);
    }
}

#[test]
fn ordered_aggregate_spilled_source_change_prevents_publication_and_refunds() {
    let (fixture, plan, _, _) = fixture_and_expected();
    let prepared = prepared_file(&fixture, &plan);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let changed = Rc::new(Cell::new(false));
    let captured = Rc::clone(&changed);
    let source = fixture.path();
    BEFORE_RUN_OPEN.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |run| {
            assert!(fs::metadata(run).unwrap().len() > 0);
            let file = fs::OpenOptions::new().write(true).open(&source).unwrap();
            file.set_len(file.metadata().unwrap().len() + 1).unwrap();
            captured.set(true);
        }));
    });
    let target = fixture.0.join("changed-source.vortex");
    let error = prepared
        .write(&target, Format::Vortex, false)
        .err()
        .expect("changed aggregate source must not publish")
        .to_string();
    assert!(error.contains("changed"), "{error}");
    assert!(changed.get());
    assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
    assert!(!target.exists());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(fixture.0.join("runs")).unwrap().count(), 0);
}
