use super::*;
use crate::{
    VortexLocalPrimitiveExecutionPolicy,
    local_primitives::prepared_relational::prepare_relational_with_schema,
    relational_query::{VortexRelationalPlan, VortexRelationalScan},
    resident_memory_source::MemoryColumnValues,
};
use shardloom_core::DatasetUri;
use shardloom_plan::ProjectionRequest;
use vortex::array::dtype::{Nullability, PType};

fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 128 << 20;
    policy
}

#[test]
fn native_batch_source_complete_values_cross_collection_and_input_boundaries() {
    let uri = DatasetUri::new("memory://batches").unwrap();
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_memory_source(uri.clone(), |session| {
            let mut builder = MemoryBatchSourceBuilder::new(session, CancellationToken::default())?;
            for start in (0..65_539).step_by(2048) {
                let values = (start..(start + 2048).min(65_539))
                    .map(|n| {
                        if n % 17 == 0 {
                            None
                        } else {
                            Some(i64::from(n) - 32_768)
                        }
                    })
                    .collect::<Vec<_>>();
                let text = values
                    .iter()
                    .map(|v| v.map(|_| "λ\"\n"))
                    .collect::<Vec<_>>();
                builder.push_columns(&[
                    MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Int64(&values),
                    },
                    MemoryColumn {
                        name: "s",
                        values: MemoryColumnValues::Utf8(&text),
                    },
                ])?;
            }
            builder.finish()
        })?;
        Ok(VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: uri,
            projection: ProjectionRequest::All,
            predicate: None,
        }))
    })
    .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    let mut next = 0_i64;
    let mut batches = 0_u64;
    let result = prepared
        .for_each_json_batch(&CancellationToken::default(), 1024, 1 << 20, |batch| {
            let rows: Vec<serde_json::Value> =
                serde_json::from_str(batch.values_json.value()).unwrap();
            assert_eq!(rows.len(), batch.rows);
            assert!(batch.rows <= 1024);
            for row in rows {
                let n = (next % 17 != 0).then_some(next - 32_768);
                assert_eq!(row, serde_json::json!({"n":n,"s":n.map(|_| "λ\"\n")}));
                next += 1;
            }
            let schema: serde_json::Value =
                serde_json::from_str(batch.result_schema_json.value()).unwrap();
            let expected = DType::struct_(
                vec![
                    ("n", DType::Primitive(PType::I64, Nullability::Nullable)),
                    ("s", DType::Utf8(Nullability::Nullable)),
                ],
                Nullability::NonNullable,
            );
            assert_eq!(schema, serde_json::to_value(expected).unwrap());
            batches += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(next, 65_539);
    assert_eq!(result.output_rows, 65_539);
    assert_eq!(result.output_batches, batches);
    assert_eq!(result.runtime.prepared_source_opens, 0);
    assert_eq!(result.runtime.completed_executions, 1);
    assert!(result.native_io_certificate.is_certified());
    assert!(
        result
            .native_io_certificate
            .sink_requirement_report
            .supports_streaming
    );
}

#[test]
fn native_batch_source_width_empty_schema_and_retained_credit() {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let names = (0..128).map(|i| format!("c{i}")).collect::<Vec<_>>();
    let columns = names
        .iter()
        .map(|name| MemoryColumn {
            name,
            values: MemoryColumnValues::Int64(&[]),
        })
        .collect::<Vec<_>>();
    let mut builder =
        MemoryBatchSourceBuilder::new(&session, CancellationToken::default()).unwrap();
    builder.push_columns(&columns).unwrap();
    builder.push_columns(&columns).unwrap();
    let source = builder.finish().unwrap();
    assert_eq!(source.row_count(), 0);
    assert_eq!(
        source.dtype().as_struct_fields_opt().unwrap().nfields(),
        128
    );
    let retained = source.0.array.clone();
    drop(source);
    assert!(session.snapshot().memory.reserved_bytes > 0);
    drop(retained);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_batch_source_denials_drop_every_owned_buffer() {
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let cancel = CancellationToken::default();
    let mut builder = MemoryBatchSourceBuilder::new(&session, cancel.clone()).unwrap();
    let data = [Some(1_i64), None, Some(i64::MAX)];
    builder
        .push_columns(&[MemoryColumn {
            name: "n",
            values: MemoryColumnValues::Int64(&data),
        }])
        .unwrap();
    for (name, values) in [("renamed", data.to_vec()), ("n", vec![Some(1); 2049])] {
        assert!(
            builder
                .push_columns(&[MemoryColumn {
                    name,
                    values: MemoryColumnValues::Int64(&values)
                }])
                .is_err()
        );
    }
    assert!(builder.reserve_scratch(8 << 20).is_err());
    cancel.cancel();
    assert!(builder.finish().is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    assert!(MemoryBatchSourceBuilder::new(&session, cancel).is_err());
    let tiny = ResidentVortexSession::new(1024, 1).unwrap();
    assert!(MemoryBatchSourceBuilder::new(&tiny, CancellationToken::default()).is_err());
    assert_eq!(tiny.snapshot().memory.reserved_bytes, 0);
}
