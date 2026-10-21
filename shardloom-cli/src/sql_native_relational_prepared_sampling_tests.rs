//! Ordinary SQL reaches prepared sampling only after exact native normalization.

use super::*;
use shardloom_vortex::{
    resident_memory_source::{
        MemoryColumn, MemoryColumnValues, MemorySourceBounds, ResidentMemorySource,
    },
    resident_session::ResidentVortexSession,
};

const ROWS: usize = 4096;
const URI: &str = "memory://sql-prepared-sampling";
const OPTIONS: &str = r#"{"n":4096,"seed":7,"replace":true,"weights":"weight","columns":"id"}"#;

#[allow(clippy::cast_precision_loss)] // Independent reference matches the specified Float64 RNG conversion.
fn expected() -> Vec<Value> {
    let mut total = 0.0;
    let cumulative = (0..ROWS)
        .map(|row| {
            total += f64::from(u32::try_from(1 + row % 16).unwrap());
            total
        })
        .collect::<Vec<_>>();
    (0..ROWS)
        .map(|draw| {
            let mut value = (7_u64 ^ 0xa076_1d64_78bd_642f)
                ^ (u64::try_from(draw)
                    .unwrap()
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15));
            value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^= value >> 31;
            let unit = ((value as f64 + 0.5) / (u64::MAX as f64 + 1.0))
                .clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
            let index = cumulative
                .partition_point(|weight| *weight < unit * total)
                .min(ROWS - 1);
            json!({"id":index})
        })
        .collect()
}

fn prepare_sample(statement: &str, session: &ResidentVortexSession) -> PreparedVortexRelational {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap();
    policy.resource_envelope.memory_budget_bytes = 64 << 20;
    prepare_in_session(
        statement,
        policy,
        session,
        |schemas| {
            schemas.register_memory_source(DatasetUri::new(URI)?, |owner| {
                let ids = (0..ROWS)
                    .map(|row| i64::try_from(row).unwrap())
                    .collect::<Vec<_>>();
                let weights = (0..ROWS)
                    .map(|row| Some(f64::from(u32::try_from(1 + row % 16).unwrap())))
                    .collect::<Vec<_>>();
                ResidentMemorySource::from_columns(
                    owner,
                    &[
                        MemoryColumn {
                            name: "id",
                            values: MemoryColumnValues::Int64NonNullable(&ids),
                        },
                        MemoryColumn {
                            name: "weight",
                            values: MemoryColumnValues::Float64(&weights),
                        },
                        // Deliberately not alphabetical: pruning must preserve SELECT * order.
                        MemoryColumn {
                            name: "other",
                            values: MemoryColumnValues::Int64NonNullable(&ids),
                        },
                    ],
                    MemorySourceBounds::default(),
                )
            })
        },
        |_, _| Ok(vec![DatasetUri::new(URI)?]),
        &CancellationToken::default(),
    )
    .unwrap()
}

#[test]
fn native_relational_sql_prepared_sampling_activates_for_identity_and_qualified_projections() {
    for input in [
        format!("SELECT * FROM '{URI}'"),
        format!("SELECT i.id AS id,i.weight AS weight,i.other AS other FROM '{URI}' AS i"),
    ] {
        let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let statement = format!("SELECT s.id AS id FROM SAMPLE(({input}), '{OPTIONS}') AS s");
        let prepared = prepare_sample(&statement, &session);
        let before = prepared.snapshot().memory.reserved_bytes;
        let mut retained = None;
        for _ in 0..3 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let rows = result
                .result_jsonl
                .value()
                .lines()
                .map(|row| serde_json::from_str::<Value>(row).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(rows, expected());
            assert!(result.execution.native_io_certificate.is_certified());
            assert!(
                !result
                    .execution
                    .native_io_certificate
                    .side_effects
                    .fallback_attempted
            );
            drop(result);
            let delta = prepared.snapshot().memory.reserved_bytes - before;
            assert!(
                (1..=128).contains(&delta),
                "scalar preparation must actually be retained"
            );
            assert_eq!(*retained.get_or_insert(delta), delta);
        }
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
        assert_eq!(session.snapshot().memory.denied_reservations, 0);
    }
}

#[test]
fn native_relational_sql_prepared_sampling_keeps_real_intervening_operators() {
    for input in [
        format!("SELECT weight,id,other FROM '{URI}'"),
        format!("SELECT * FROM '{URI}' WHERE id >= 0"),
        format!("SELECT * FROM '{URI}' LIMIT {ROWS}"),
        format!("SELECT id,weight + 0.0 AS weight,other FROM '{URI}'"),
    ] {
        let session = ResidentVortexSession::new(64 << 20, 1).unwrap();
        let prepared = prepare_sample(
            &format!("SELECT * FROM SAMPLE(({input}), '{OPTIONS}') AS s"),
            &session,
        );
        let before = prepared.snapshot().memory.reserved_bytes;
        for _ in 0..2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let rows = result
                .result_jsonl
                .value()
                .lines()
                .map(|row| serde_json::from_str::<Value>(row).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(rows, expected());
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        }
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}
