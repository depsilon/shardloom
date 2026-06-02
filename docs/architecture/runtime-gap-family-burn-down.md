# Runtime Gap Family Burn-Down

Schema version: `shardloom.runtime_gap_family_burn_down.v1`

## Purpose

`GAR-RUNTIME-IMPL-6D:gap-family-burn-down` converts the 38 unchecked
`docs/architecture/global-architecture-review.md` rows into family-owned runtime slices. This is a
runtime-safety gate: it prevents broad blocker prose from being mistaken for a runnable user
workflow, and it prevents true runtime gaps from disappearing into docs-only cleanup.

The deterministic validator is:

```bash
python3 scripts/check_runtime_gap_family_burn_down.py --output target/runtime-gap-family-burn-down.json
```

The report maps every unchecked global-review row to one family, one or more phase items, user
surfaces, owning modules, required evidence, validators, no-fallback invariants, claim boundaries,
and the next action.

## Claim Boundary

Passing this gate means the blocker inventory is split and machine-checkable. It does not mean any
listed family is runtime-supported, production-ready, performance-ready, package-ready, or
Spark-replacement-ready. Family rows stay `claim_gate_status=not_claim_grade` until their owning
implementation slices attach correctness, runtime, Native I/O, release, and benchmark evidence.

## Fallback Boundary

No row in this burn-down map authorizes fallback execution. DuckDB, Polars, Spark, DataFusion,
Velox, external databases, Vortex query-engine integrations, and managed platforms remain baselines,
test oracles, report-only handles, or explicit external boundaries only. ShardLoom runtime closure
must preserve `fallback_attempted=false` and `external_engine_invoked=false`.

## Runtime Families

Work the families in this order unless a release, security, claim-integrity, or dependency blocker
forces a narrower reorder.

| Family id | Runtime focus | Primary phase owner |
| --- | --- | --- |
| `language_front_door_runtime` | SQL/Python/DataFrame grammar, helpers, notebook ergonomics, and deterministic unsupported diagnostics. | `GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar`; `GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth` |
| `native_vortex_operator_runtime` | Native Vortex source/sink/operator coverage, encoded execution, Source/Split proof, and operator blocker matrices. | `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down` |
| `object_store_table_lakehouse_runtime` | Object-store providers, table/catalog metadata/data I/O, commits, rollback, recovery, and lakehouse boundaries. | `GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime` |
| `output_sink_runtime` | Output/fanout/sink promotion, metadata preservation/loss, replay, and local/native output proof. | `GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime`; `GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime` |
| `performance_claim_evidence` | Claim-grade correctness/benchmark evidence, comparative reruns, Spark-displacement boundaries, and replacement sufficiency. | `GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication` |
| `effects_extensions_runtime` | Extension execution, arbitrary UDFs, LLM/API calls, embeddings, vector search, external writes, credentials, and sandboxing. | `GAR-RUNTIME-IMPL-6D:last_order.effectful_operations` |
| `streaming_live_hybrid_runtime` | Streaming, live/hybrid engines, CDC, broker/state-store, REST/remote runtime, and freshness/snapshot contracts. | `GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime` |
| `spill_fault_tolerance_runtime` | Spill, OOM, runtime filters, adaptive execution, skew, retry, cancellation, commit, and cleanup. | `GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime` |
| `observability_runtime` | Live profiling, profile artifacts, debug bundles, metrics exporters, trace exporters, and runtime introspection. | `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down` |
| `plan_interop_harness_runtime` | Substrait-compatible direction, plan import/export, universal harness execution, and dependency/provenance gates. | `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down` |
| `release_package_platform_readiness` | Public package/release publication, signing, attestations, final release rehearsal, Foundry/package channels, and platform packs. | `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down` |
| `result_envelope_migration` | Typed result-envelope migration, legacy flat field mirrors, Python accessors, and agent contract compatibility. | `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down` |
| `io_reuse_fanout_followthrough` | I/O reuse, cross-format fanout follow-through, prepared-state reuse, route packets, and benchmark handoff. | `GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime` |

## Closure Rule

Close a family row only when the owning implementation slice either:

- lands runtime behavior with tests, evidence fields, and no-fallback proof;
- lands deterministic unsupported/admission diagnostics that make the boundary explicit; or
- reclassifies a row out of runtime scope with validator coverage and claim-boundary evidence.

Do not close a family because an artifact exists, a docs page says planned, a command is
discoverable, or a placeholder payload has the right schema.
