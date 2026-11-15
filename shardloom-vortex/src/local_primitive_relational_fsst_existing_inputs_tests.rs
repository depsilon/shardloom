//! Explicit finite diagnostic against immutable, independently verified inputs.

use super::*;
use crate::{
    local_primitives::{
        VortexLocalPrimitiveExecutionPolicy, prepared_relational::prepare_relational,
    },
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalPlan,
        VortexRelationalProject, VortexRelationalScan,
    },
};
use serde_json::{Value as Json, json};
use sha2::{Digest as _, Sha256};
use shardloom_core::{
    ColumnRef, DatasetUri, ExprId, Expression as PublicExpression, ExpressionKind, PredicateExpr,
    ScalarValue,
};
use std::{collections::BTreeMap, fmt::Write as _, fs, io::Read as _, path::Path};
use vortex::array::arrays::struct_::StructArrayExt as _;

fn write_record(output: &mut impl std::io::Write, value: &Json) {
    serde_json::to_writer(&mut *output, value).unwrap();
    output.write_all(b"\n").unwrap();
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    let mut output = String::new();
    for byte in bytes.as_ref() {
        write!(output, "{byte:02x}").unwrap();
    }
    output
}

fn hash(path: &Path) -> String {
    let mut file = fs::File::open(path).unwrap();
    let mut digest = Sha256::new();
    let mut block = [0u8; 8192];
    loop {
        let read = file.read(&mut block).unwrap();
        if read == 0 {
            break;
        }
        digest.update(&block[..read]);
    }
    hex(digest.finalize())
}

fn bits(values: &[Option<bool>]) -> String {
    values
        .iter()
        .map(|v| match v {
            Some(true) => 't',
            Some(false) => 'f',
            None => 'n',
        })
        .collect()
}

fn reference_values(reference: &Json) -> Vec<Option<bool>> {
    reference["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            if v.is_null() {
                None
            } else {
                Some(v.as_bool().unwrap())
            }
        })
        .collect()
}

fn expr(kind: ExpressionKind) -> PublicExpression {
    PublicExpression::new(ExprId::new("fsst_admission").unwrap(), kind)
}
fn column(name: &str) -> PublicExpression {
    expr(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn predicate(needle: &str, prefix: bool) -> PublicExpression {
    expr(ExpressionKind::FunctionCall {
        name: if prefix { "starts_with" } else { "contains" }.into(),
        args: vec![
            column("value"),
            expr(ExpressionKind::Literal(ScalarValue::Utf8(needle.into()))),
        ],
    })
}
fn scan(path: &str, legacy: Option<&str>) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(path).unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: legacy.map(|needle| PredicateExpr::StringContains {
            column: ColumnRef::new("value").unwrap(),
            needle: needle.into(),
            negated: false,
        }),
    })
}
fn project(
    input: VortexRelationalPlan,
    name: &str,
    expression: PublicExpression,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: vec![(name.into(), expression)],
    }))
}

fn request_plan(input: &Json, reference: &Json, consumer: &str) -> VortexRelationalPlan {
    let prefix = reference["operation"] == "startswith";
    let needle = reference["pattern"].as_str().unwrap();
    let path = input["input"]["path"].as_str().unwrap();
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: scan(path, None),
        predicate: predicate(needle, prefix),
    }));
    match consumer {
        "project" => project(scan(path, None), "matched", predicate(needle, prefix)),
        "filter" => project(filtered, "row_id", column("row_id")),
        "count" => VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
            input: filtered,
            group_by: vec![],
            grouping_sets: None,
            measures: vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".into(),
            )],
        })),
        "legacy_scan" => project(scan(path, Some(needle)), "row_id", column("row_id")),
        _ => unreachable!(),
    }
}

fn expected_request_rows(expected: &[Option<bool>], consumer: &str) -> Vec<Json> {
    if consumer == "project" {
        expected
            .iter()
            .map(|value| json!({"matched":value}))
            .collect()
    } else if consumer == "count" {
        vec![json!({"rows":expected.iter().filter(|v| **v == Some(true)).count()})]
    } else {
        expected
            .iter()
            .enumerate()
            .filter(|(_, value)| **value == Some(true))
            .map(|(row, _)| json!({"row_id":row}))
            .collect()
    }
}

fn complete_request(
    input: &Json,
    reference: &Json,
    consumer: &str,
    records: &mut impl std::io::Write,
    observe_upstream: bool,
) {
    let expected = reference_values(reference);
    let path = input["input"]["path"].as_str().unwrap();
    let plan = request_plan(input, reference, consumer);
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 1).unwrap();
    policy.resource_envelope.memory_budget_bytes = 128 << 20;
    let prepared = prepare_relational(&plan, policy).unwrap();
    let upstream = vortex::encodings::fsst::like_observation::LikeObservation::default();
    if observe_upstream {
        prepared
            .session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                context.native_session().register(upstream.clone());
                Ok(())
            })
            .unwrap();
    }
    let memory = prepared.session.memory().clone();
    let before = prepared.snapshot().reserved_without_source_segment_cache();
    let observation = Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let work = observation.finish();
    let upstream_work = upstream.snapshot();
    assert!(!upstream_work.overflowed);
    let actual = result
        .result_jsonl
        .value()
        .lines()
        .map(|line| serde_json::from_str::<Json>(line).unwrap())
        .collect::<Vec<_>>();
    let expected_rows = expected_request_rows(&expected, consumer);
    assert_eq!(actual, expected_rows, "{path}: {consumer}");
    assert_eq!(
        result.execution.provider_selection_reason,
        "native_operator_pipeline"
    );
    assert!(!result.execution.native_io_certificate.fallback_attempted);
    let output = if consumer == "project" {
        json!({"boolean_cells":bits(&actual.iter().map(|v|v["matched"].as_bool()).collect::<Vec<_>>())})
    } else {
        json!({"rows":actual})
    };
    let certificate = &result.execution.native_io_certificate;
    let reads = certificate
        .file_scans
        .sources
        .iter()
        .map(|source| {
            json!({
        "generation":source.generation_fingerprint,"requests":source.completed_read_requests,
        "bytes":source.completed_read_bytes,"segment_requests":source.segment_requests,
        "segment_reuse_hits":source.segment_reuse.hits})
        })
        .collect::<Vec<_>>();
    write_record(
        records,
        &json!({"case":"complete_file_request", "input":input["input"]["sha256"],
        "size":input["size"],"profile":input["profile"],"encoding":input["encoding"],
        "operation":reference["operation"],"pattern":reference["pattern_id"],"consumer":consumer,
        "actual":output,"actual_schema":result.result_schema_json.value(),"complete_equal":true,
        "provider":result.execution.provider_selection_reason,"fallback_attempted":false,
        "scan_batches":result.execution.scan_batches,"scan_rows":result.execution.scan_rows_delivered,
        "output_rows":result.execution.output_rows,"variable_domains":work.variable_domains,
        "variable_domain_values":work.variable_domain_values,"variable_fsst_domains":work.variable_fsst_domains,
        "variable_fsst_domain_values":work.variable_fsst_domain_values,
        "legacy_fsst_like_calls":work.legacy_fsst_like_calls,"legacy_fsst_like_rows":work.legacy_fsst_like_rows,
        "native_io_certificate":{"schema_version":certificate.schema_version,
            "certificate_id":certificate.certificate_id,"path_id":certificate.path_id,
            "proof_basis":certificate.source_pushdown_report.proof_basis,
            "representation_transitions":format!("{:?}",certificate.representation_transitions),
            "materialization_boundaries":format!("{:?}",certificate.materialization_boundaries),
            "fallback_attempted":certificate.fallback_attempted},"physical_reads":reads}),
    );
    drop(result);
    let retained = prepared.snapshot().reserved_without_source_segment_cache();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    write_record(
        records,
        &json!({"case":"complete_request_refund","input":input["input"]["sha256"],
        "operation":reference["operation"],"pattern":reference["pattern_id"],"consumer":consumer,
        "prepared_reserved_before":before,"prepared_reserved_after_result_drop":retained,
        "all_owners_dropped_reserved_bytes":0}),
    );
    if observe_upstream {
        assert_eq!(upstream.snapshot(), upstream_work);
        write_record(
            records,
            &json!({"case":"upstream_like_dispatch", "input":input["input"]["sha256"],
                "size":input["size"],"profile":input["profile"],"encoding":input["encoding"],
                "operation":reference["operation"],"pattern":reference["pattern_id"],"consumer":consumer,
                "successful_calls":upstream_work.calls,"logical_operand_rows":upstream_work.rows,
                "eligible_contains_calls":upstream_work.eligible_contains_calls,
                "prefilter_calls":upstream_work.prefilter_calls,"prefilter_rows":upstream_work.prefilter_rows,
                "rejected_rows":upstream_work.rejected_rows,"exact_rows":upstream_work.exact_rows,
                "matcher_constructions":upstream_work.matcher_constructions,"block_rejections":upstream_work.block_rejections,
                "overflowed":upstream_work.overflowed,"engine_timing":null}),
        );
    }
}

fn physical_and_shared(input: &Json, references: &[&Json], records: &mut impl std::io::Write) {
    let session = ResidentVortexSession::new(128 << 20, 1).unwrap();
    let memory = session.memory().clone();
    let source = session
        .prepare_file(input["input"]["path"].as_str().unwrap())
        .unwrap();
    let mut actual = vec![Vec::new(); references.len()];
    let mut direct_parts = vec![Vec::new(); references.len()];
    let mut physical = BTreeMap::<String, usize>::new();
    let mut calls = vec![0usize; references.len()];
    let before = session.snapshot().reserved_without_source_segment_cache();
    session
        .with_native_execution_context(&CancellationToken::default(), |context| {
            source.with_admitted_native_execution(context, |file, context| {
                if file.row_count() == 0 {
                    return Ok(());
                }
                let batches = file
                    .scan()
                    .map_err(vortex_error)?
                    .with_ordered(true)
                    .with_concurrency(1)
                    .into_array_iter(context.runtime())
                    .map_err(vortex_error)?;
                for batch in batches {
                    let batch = batch.map_err(vortex_error)?;
                    let batch = batch
                        .execute::<vortex::array::arrays::StructArray>(
                            &mut context.native_session().create_execution_ctx(),
                        )
                        .map_err(vortex_error)?;
                    let column = batch.unmasked_field(1).clone();
                    *physical
                        .entry(column.encoding_id().to_string())
                        .or_default() += 1;
                    for (index, reference) in references.iter().enumerate() {
                        let needle = reference["pattern"].as_str().unwrap();
                        let prefix = reference["operation"] == "startswith";
                        let values = booleans(shared(&column, needle, prefix, context)?, context)?;
                        let offset = actual[index].len();
                        if let Some(result) = direct(&column, needle, prefix, context)? {
                            calls[index] += 1;
                            let matched = booleans(result, context)?;
                            assert_eq!(matched, values);
                            direct_parts[index]
                                .push(json!({"start":offset,"values":bits(&matched)}));
                        }
                        actual[index].extend(values);
                    }
                }
                Ok(())
            })
        })
        .unwrap();
    let retained = session.snapshot().reserved_without_source_segment_cache();
    drop(source);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for (index, reference) in references.iter().enumerate() {
        assert_eq!(actual[index], reference_values(reference));
        write_record(
            records,
            &json!({"case":"physical_file_consumer", "input":input["input"]["sha256"],
            "size":input["size"],"profile":input["profile"],"encoding":input["encoding"],
            "operation":reference["operation"],"pattern":reference["pattern_id"],
            "complete_shared_values":bits(&actual[index]),"direct_kernel_parts":direct_parts[index],
            "direct_kernel_calls":calls[index],"physical_input_batches":physical,
            "complete_equal":true,"engine_timing":null,"native_reservations_refunded":true,
            "prepared_reserved_before":before,"prepared_reserved_after_observation":retained,
            "all_owners_dropped_reserved_bytes":0}),
        );
    }
}

#[test]
#[ignore = "requires the frozen existing-input manifest and isolated diagnostic execution"]
fn fsst_admission_existing_inputs() {
    let observe_upstream = match std::env::var("SHARDLOOM_FSST_ADMISSION_TRACE") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "1" => true,
        _ => panic!("optional FSST trace flag must be 1"),
    };
    let manifest_path =
        std::env::var("SHARDLOOM_FSST_ADMISSION_MANIFEST").expect("frozen input manifest required");
    let manifest_path = Path::new(&manifest_path);
    let output_path = std::env::var("SHARDLOOM_FSST_ADMISSION_OUTPUT")
        .expect("unique diagnostic output required");
    let output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output_path)
        .unwrap();
    let mut records = flate2::write::GzEncoder::new(output, flate2::Compression::fast());
    let manifest: Json = serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
    let reference_path = manifest_path
        .parent()
        .unwrap()
        .join(manifest["boolean_references"]["path"].as_str().unwrap());
    assert_eq!(
        hash(&reference_path),
        manifest["boolean_references"]["gzip_sha256"]
            .as_str()
            .unwrap()
    );
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(fs::File::open(&reference_path).unwrap())
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(
        hex(Sha256::digest(&decoded)),
        manifest["boolean_references"]["uncompressed_json_sha256"]
            .as_str()
            .unwrap()
    );
    let reference_document: Json = serde_json::from_slice(&decoded).unwrap();
    let inputs = manifest["inputs"].as_array().unwrap();
    assert_eq!(inputs.len(), 54);
    // Full file hashing occurs outside the active query observations.
    for input in inputs {
        assert_eq!(
            hash(Path::new(input["input"]["path"].as_str().unwrap())),
            input["input"]["sha256"].as_str().unwrap()
        );
    }
    for input in inputs {
        let references = reference_document["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["size"] == input["size"] && r["profile"] == input["profile"])
            .collect::<Vec<_>>();
        assert_eq!(references.len(), 16);
        physical_and_shared(input, &references, &mut records);
        for reference in references {
            let chosen = if reference["operation"] == "contains" {
                "needle"
            } else {
                "short_ab"
            };
            if reference["pattern_id"] != chosen {
                continue;
            }
            for consumer in ["project", "filter", "count"] {
                complete_request(input, reference, consumer, &mut records, observe_upstream);
            }
            if reference["operation"] == "contains" {
                complete_request(
                    input,
                    reference,
                    "legacy_scan",
                    &mut records,
                    observe_upstream,
                );
            }
        }
    }
    for input in inputs {
        assert_eq!(
            hash(Path::new(input["input"]["path"].as_str().unwrap())),
            input["input"]["sha256"].as_str().unwrap()
        );
    }
    let completion = json!({"case":"existing_input_completion","inputs":54,"physical_shared_declarations":864,
        "complete_public_core_requests":378,"engine_timing":null});
    write_record(&mut records, &completion);
    records.finish().unwrap().sync_all().unwrap();
    emit(&completion);
}

#[test]
#[ignore = "requires frozen inputs and an isolated, debug-only upstream dispatch diagnostic"]
fn fsst_admission_source_dispatch() {
    let manifest_path = std::env::var("SHARDLOOM_FSST_ADMISSION_MANIFEST").unwrap();
    let manifest_path = Path::new(&manifest_path);
    let manifest: Json = serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
    let references_path = manifest_path
        .parent()
        .unwrap()
        .join(manifest["boolean_references"]["path"].as_str().unwrap());
    assert_eq!(
        hash(&references_path),
        manifest["boolean_references"]["gzip_sha256"]
    );
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(fs::File::open(references_path).unwrap())
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(
        hex(Sha256::digest(&decoded)),
        manifest["boolean_references"]["uncompressed_json_sha256"]
    );
    let references: Json = serde_json::from_slice(&decoded).unwrap();
    let inputs = manifest["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|input| {
            input["profile"] == "selective"
                && (input["encoding"] == "fsst" || input["encoding"] == "flat")
        })
        .collect::<Vec<_>>();
    assert_eq!(inputs.len(), 4);
    for input in &inputs {
        assert_eq!(
            hash(Path::new(input["input"]["path"].as_str().unwrap())),
            input["input"]["sha256"]
        );
    }
    let output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("SHARDLOOM_FSST_ADMISSION_OUTPUT").unwrap())
        .unwrap();
    let mut records = flate2::write::GzEncoder::new(output, flate2::Compression::fast());
    let mut requests = 0;
    for input in &inputs {
        let chosen = references["vectors"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|reference| {
                reference["size"] == input["size"]
                    && reference["profile"] == input["profile"]
                    && ((reference["operation"] == "contains"
                        && reference["pattern_id"] == "needle")
                        || (reference["operation"] == "startswith"
                            && reference["pattern_id"] == "short_ab"))
            })
            .collect::<Vec<_>>();
        assert_eq!(chosen.len(), 2);
        for reference in chosen {
            for consumer in ["project", "filter", "count"] {
                complete_request(input, reference, consumer, &mut records, true);
                requests += 1;
            }
            if reference["operation"] == "contains" {
                complete_request(input, reference, "legacy_scan", &mut records, true);
                requests += 1;
            }
        }
    }
    for input in &inputs {
        assert_eq!(
            hash(Path::new(input["input"]["path"].as_str().unwrap())),
            input["input"]["sha256"]
        );
    }
    assert_eq!(requests, 28);
    let completion = json!({"case":"source_dispatch_completion","inputs":4,"complete_requests":requests,"engine_timing":null});
    write_record(&mut records, &completion);
    records.finish().unwrap().sync_all().unwrap();
    emit(&completion);
}
