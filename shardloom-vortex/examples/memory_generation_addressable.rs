//! Bounded owned/borrowed intake and addressable-generation measurement.
//! Native-array return clocks exclude independent scalar verification and drop.
//! This example writes no files and claims no cold-cache/device-I/O measurement.

#[cfg(unix)]
mod native {
    use serde_json::{Value, json};
    use shardloom_vortex::{
        memory_file_generation::{
            MemoryFileGenerationBounds, MemoryFileGenerationLayout, MemoryFileSegmentEvidence,
        },
        resident_memory_source::{
            MemoryColumn, MemoryColumnValues, MemorySourceBounds, OwnedMemoryColumn,
            ResidentMemorySource,
        },
        resident_session::{OwnedVortexResultBatch, ResidentVortexSession},
    };
    use std::{hint::black_box, ops::Range, time::Instant};
    use vortex::{
        VortexSessionDefault as _,
        array::{
            VortexSessionExecute as _,
            dtype::{DType, Nullability, PType},
            scalar::Scalar,
        },
        session::VortexSession,
    };

    type Error = Box<dyn std::error::Error>;
    const BASE: i64 = 1_i64 << 60;
    const COLUMNS: [&str; 3] = ["renamed_text", "exact_identifier", "nullable_measure"];
    const OUTPUT_BYTES: u64 = 32 << 20;

    fn label(row: usize) -> Option<String> {
        (!row.is_multiple_of(7)).then(|| format!("λ-東京-{row:08}-\"native\""))
    }
    fn measure(row: usize) -> Option<f64> {
        (!row.is_multiple_of(5)).then(|| f64::from(u32::try_from(row).expect("bounded rows")) / 4.0)
    }
    fn nanos(started: Instant) -> Result<u64, Error> {
        Ok(u64::try_from(started.elapsed().as_nanos())?)
    }

    struct Fixture {
        ids: Vec<i64>,
        measures: Vec<Option<f64>>,
        texts: Vec<Option<String>>,
    }
    impl Fixture {
        fn new(rows: usize) -> Self {
            Self {
                ids: (0..rows)
                    .map(|row| BASE + i64::try_from(row).expect("bounded rows"))
                    .collect(),
                measures: (0..rows).map(measure).collect(),
                texts: (0..rows).map(label).collect(),
            }
        }
    }

    fn intake(
        session: &ResidentVortexSession,
        rows: usize,
        owned: bool,
    ) -> Result<(ResidentMemorySource, u64, u64), Error> {
        let preparation = Instant::now();
        let fixture = Fixture::new(rows);
        if owned {
            let mut bytes = Vec::new();
            let mut offsets = Vec::with_capacity(rows + 1);
            let mut text_validity = Vec::with_capacity(rows);
            offsets.push(0);
            for text in &fixture.texts {
                text_validity.push(text.is_some());
                if let Some(text) = text {
                    bytes.extend_from_slice(text.as_bytes());
                }
                offsets.push(u64::try_from(bytes.len())?);
            }
            let values = fixture
                .measures
                .iter()
                .map(|value| value.unwrap_or_default())
                .collect::<Vec<_>>();
            let validity = fixture
                .measures
                .iter()
                .map(Option::is_some)
                .collect::<Vec<_>>();
            let prepared = nanos(preparation)?;
            let started = Instant::now();
            let columns = vec![
                OwnedMemoryColumn::int64(session, "exact_identifier", fixture.ids, None)?,
                OwnedMemoryColumn::float64(session, "nullable_measure", values, Some(validity))?,
                OwnedMemoryColumn::utf8(
                    session,
                    "renamed_text",
                    offsets,
                    bytes,
                    Some(text_validity),
                )?,
            ];
            let source = ResidentMemorySource::from_owned_columns(
                session,
                columns,
                MemorySourceBounds::default(),
            )?;
            Ok((source, prepared, nanos(started)?))
        } else {
            let texts = fixture
                .texts
                .iter()
                .map(Option::as_deref)
                .collect::<Vec<_>>();
            let prepared = nanos(preparation)?;
            let started = Instant::now();
            let source = ResidentMemorySource::from_columns(
                session,
                &[
                    MemoryColumn {
                        name: "exact_identifier",
                        values: MemoryColumnValues::Int64NonNullable(&fixture.ids),
                    },
                    MemoryColumn {
                        name: "nullable_measure",
                        values: MemoryColumnValues::Float64(&fixture.measures),
                    },
                    MemoryColumn {
                        name: "renamed_text",
                        values: MemoryColumnValues::Utf8(&texts),
                    },
                ],
                MemorySourceBounds::default(),
            )?;
            Ok((source, prepared, nanos(started)?))
        }
    }

    fn verify(result: &OwnedVortexResultBatch, range: Range<usize>) -> Result<u64, Error> {
        let started = Instant::now();
        if result.row_count() != u64::try_from(range.len())? {
            return Err("complete native result row count mismatch".into());
        }
        // This separate validation session is outside query clocks and the
        // measured session's allocation scope. Literal Rust values are the oracle.
        let native = VortexSession::default();
        let mut context = native.create_execution_ctx();
        let mut source_row = range.start;
        for array in result.arrays() {
            let fields = array
                .dtype()
                .as_struct_fields_opt()
                .ok_or("native result lost Struct dtype")?;
            if fields.nfields() != COLUMNS.len()
                || fields
                    .names()
                    .iter()
                    .zip(COLUMNS)
                    .any(|(name, expected)| name.as_ref() != expected)
            {
                return Err("native projection schema mismatch".into());
            }
            for row in 0..array.len() {
                let scalar = array.execute_scalar(row, &mut context)?;
                let fields = scalar.as_struct();
                let expected_id = Scalar::from(BASE + i64::try_from(source_row)?);
                let expected_text = label(source_row).map_or_else(
                    || Scalar::null(DType::Utf8(Nullability::Nullable)),
                    |text| Scalar::utf8(text, Nullability::Nullable),
                );
                let expected_measure = measure(source_row).map_or_else(
                    || Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
                    |value| Scalar::primitive(value, Nullability::Nullable),
                );
                if fields.field("exact_identifier") != Some(expected_id)
                    || fields.field("renamed_text") != Some(expected_text)
                    || fields.field("nullable_measure") != Some(expected_measure)
                {
                    return Err(
                        format!("complete scalar mismatch at source row {source_row}").into(),
                    );
                }
                source_row += 1;
            }
        }
        if source_row != range.end {
            return Err("complete native result ended early".into());
        }
        nanos(started)
    }

    #[derive(Default)]
    struct Samples {
        warmup: u64,
        values: Vec<u64>,
    }
    impl Samples {
        fn report(&self) -> Value {
            let mut sorted = self.values.clone();
            sorted.sort_unstable();
            let percentile = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100) - 1];
            json!({"warmup_nanos": self.warmup, "raw_nanos": self.values, "p50_nanos": percentile(50),
                "p95_nanos": percentile(95), "p99_nanos": percentile(99), "percentile_method":"nearest_rank"})
        }
    }

    fn segments(values: &[MemoryFileSegmentEvidence]) -> Value {
        json!(values.iter().map(|segment| json!({"id":segment.segment_id,"column":segment.column_index,
            "row_start":segment.row_start,"rows":segment.rows,"serialized_bytes":segment.serialized_bytes,
            "requests":segment.requests,"returned_bytes":segment.returned_bytes})).collect::<Vec<_>>())
    }

    #[allow(clippy::too_many_lines)] // Keep the measured lifecycle and complete owner-release report together.
    fn variant(
        rows: usize,
        iterations: usize,
        row_group_rows: usize,
        owned: bool,
    ) -> Result<Value, Error> {
        let setup = Instant::now();
        let session = ResidentVortexSession::new(128 << 20, 1)?;
        let session_setup_nanos = nanos(setup)?;
        let (source, source_preparation_nanos, typed_intake_nanos) = intake(&session, rows, owned)?;
        let native_intake_live_bytes = session.snapshot().memory.reserved_bytes;
        let width = rows.min(7);
        let middle = (rows - width) / 2;
        // Complete direct execution occurs before any generation is requested.
        let direct = source.prepare_projection(&COLUMNS, None, Some(width))?;
        let started = Instant::now();
        let first = direct.execute_arrays()?;
        let direct_before_generation_nanos = nanos(started)?;
        let mut validation_nanos = verify(&first, 0..width)?;
        drop(first);
        let started = Instant::now();
        let generation = source.file_generation_with_layout(
            MemoryFileGenerationBounds::default(),
            MemoryFileGenerationLayout {
                row_group_rows,
                max_segments: 4096,
            },
            None,
        )?;
        let generation_construction_nanos = nanos(started)?;
        let construction = generation.evidence();
        let started = Instant::now();
        let prefix = generation.prepare_projection_range(
            &COLUMNS,
            None,
            0..u64::try_from(width)?,
            u64::try_from(width)?,
            OUTPUT_BYTES,
        )?;
        let interior = generation.prepare_projection_range(
            &COLUMNS,
            None,
            u64::try_from(middle)?..u64::try_from(middle + width)?,
            u64::try_from(width)?,
            OUTPUT_BYTES,
        )?;
        let file_query_preparation_nanos = nanos(started)?;
        let before_queries = generation.segment_evidence();
        let mut samples: [Samples; 3] = std::array::from_fn(|_| Samples::default());
        for iteration in 0..=iterations {
            for position in 0..3 {
                let case = (iteration + position) % 3;
                let started = Instant::now();
                let result = black_box(match case {
                    0 => direct.execute_arrays()?,
                    1 => prefix.execute()?,
                    _ => interior.execute()?,
                });
                let elapsed = nanos(started)?;
                if iteration == 0 {
                    samples[case].warmup = elapsed;
                } else {
                    samples[case].values.push(elapsed);
                }
                validation_nanos += verify(
                    &result,
                    if case == 2 {
                        middle..middle + width
                    } else {
                        0..width
                    },
                )?;
            }
        }
        let after_queries = generation.segment_evidence();
        let complete = generation
            .prepare_projection(&COLUMNS, None, u64::try_from(rows)?, OUTPUT_BYTES)?
            .execute()?;
        validation_nanos += verify(&complete, 0..rows)?;
        drop(complete);
        let after_complete_validation = generation.segment_evidence();
        let end = generation.evidence();
        if construction.array_serializer_calls != end.array_serializer_calls
            || construction.construction_footer_serializer_calls
                != end.construction_footer_serializer_calls
            || end.source_file_opens != 0
            || session.snapshot().prepared_source_opens != 0
        {
            return Err("queries unexpectedly serialized/opened a generation".into());
        }
        drop(interior);
        drop(prefix);
        drop(direct);
        drop(generation);
        drop(source);
        let snapshot = session.snapshot();
        if snapshot.memory.reserved_bytes != 0 || snapshot.memory.denied_reservations != 0 {
            return Err(
                "native owned buffers remained after complete source/result release".into(),
            );
        }
        Ok(json!({
            "intake": if owned {"owned_vectors"} else {"borrowed_copy"}, "rows": rows,
            "source_preparation_nanos": source_preparation_nanos, "session_setup_nanos":session_setup_nanos,
            "typed_intake_nanos":typed_intake_nanos, "intake_live_owned_bytes":native_intake_live_bytes,
            "direct_native_array_before_generation_nanos":direct_before_generation_nanos,
            "direct_extra_preconstruction_probe_calls":1,
            "generation_construction_nanos":generation_construction_nanos,
            "file_query_preparation_nanos":file_query_preparation_nanos,
            "direct_prefix":samples[0].report(), "file_prefix":samples[1].report(), "file_interior_range":samples[2].report(),
            "query_rows":width,"interior_row_start":middle,"independent_scalar_validation_nanos":validation_nanos,
            "complete_values_verified":true,"fallback_attempted":false,"source_file_opens":0,
            "generation":{"columns":end.columns,"row_groups":end.row_groups,"row_group_rows":end.row_group_rows,
                "input_logical_bytes":end.input_logical_bytes,"intake_payload_bytes_copied":end.intake_payload_bytes_copied,
                "array_serializer_calls":end.array_serializer_calls,"construction_footer_serializer_calls":end.construction_footer_serializer_calls,
                "construction_footer_bytes":end.construction_footer_bytes,"row_group_offset_bytes_built":end.row_group_offset_bytes_built,
                "segment_assembly_bytes_copied":end.segment_assembly_bytes_copied,
                "segments_before_queries":segments(&before_queries),"segments_after_queries":segments(&after_queries),
                "segments_after_complete_validation":segments(&after_complete_validation)},
            "memory":{"peak_owned_bytes":snapshot.memory.peak_reserved_bytes,"live_owned_bytes_after_drop":snapshot.memory.reserved_bytes,
                "denied_reservations":snapshot.memory.denied_reservations},
        }))
    }

    pub fn run() -> Result<(), Error> {
        let args = std::env::args().skip(1).take(4).collect::<Vec<_>>();
        if args.len() != 3 {
            return Err("usage: memory_generation_addressable ROWS ITERATIONS ROW_GROUP_ROWS; rows 1..65536, iterations 1..1000, at most 4096 segments".into());
        }
        let rows = args[0].parse::<usize>()?;
        let iterations = args[1].parse::<usize>()?;
        let row_group_rows = args[2].parse::<usize>()?;
        if !(1..=65_536).contains(&rows)
            || !(1..=1000).contains(&iterations)
            || !(1..=65_536).contains(&row_group_rows)
            || rows.div_ceil(row_group_rows) * COLUMNS.len() > 4096
        {
            return Err("fixture exceeds explicit row/iteration/segment bounds".into());
        }
        let report = json!({"schema":"memory_generation_addressable.v1","provider_version":shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
            "comparison_scope":"within-process current implementation: borrowed versus owned intake and direct versus file-backed memory-provider queries; no historical generation implementation baseline or generation speedup claim",
            "iterations":iterations,"warmups_per_case":1,"case_order":"rotating direct-prefix/file-prefix/file-interior; sequential",
            "warmup_scope":"reused providers, no cold-cache control; direct path also has one separately reported preconstruction correctness probe",
            "timing_scope":"native-array return only; prepared-operation bind, scalar verification and result drop excluded; no total scalar-query claim",
            "intake_scope":"source preparation includes caller vector/offset/reference assembly; intake starts after that preparation; owned intake consumes full capacity, borrowed intake copies native payloads",
            "memory_scope":"measured-session allocation owners and imported vector capacity; caller preparation, validation session, provider metadata/scratch and RSS excluded",
            "segment_scope":"memory-provider requests and returned bytes, not filesystem/device reads; ordinary direct query precedes explicit once-per-generation serialization",
            "filesystem_writes":false,"variants":[variant(rows, iterations, row_group_rows, false)?,variant(rows, iterations, row_group_rows, true)?]});
        println!("{}", serde_json::to_string_pretty(&report)?);
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn uneven_groups_both_intakes_preserve_every_value_and_release_all_owned_bytes() {
            for owned in [false, true] {
                let report = variant(257, 1, 32, owned).unwrap();
                assert_eq!(report["complete_values_verified"], true);
                assert_eq!(report["generation"]["row_groups"], 9);
                assert_eq!(report["generation"]["array_serializer_calls"], 27);
                assert_eq!(report["memory"]["live_owned_bytes_after_drop"], 0);
                if owned {
                    assert_eq!(report["generation"]["intake_payload_bytes_copied"], 0);
                }
            }
        }
    }
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    native::run()
}
#[cfg(not(unix))]
fn main() {
    eprintln!("addressable immutable memory generations currently require Unix");
}
