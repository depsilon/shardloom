# Production Certification Gate

`shardloom.production_certification_gate.v1` is the common fail-closed production workload gate.
It evaluates declared workload profiles in
[`production-certification-workloads.json`](production-certification-workloads.json) and keeps
production claims separate from local v1 readiness.

## Local-engine preview exit criteria

The current public posture is **published local engine; operational hardening in progress**.
The `local_file_etl_v1_candidate` declaration describes that local engine. Selected v0.4.0
package channels have [verified publication evidence](v0.4.0-publication-verification.md);
package availability is no longer a missing production prerequisite. Passing the default gate
means the declarations and blocked claims are consistent. It does not certify production use.

A stable local-engine support promise can be narrower than the complete product roadmap.
Before changing its maturity label or `production_claim_allowed`, accept the following for
one explicitly declared local workload and bind the evidence to the release source and binary:

| Requirement | Existing evidence | Remaining acceptance |
| --- | --- | --- |
| Supported workload and platforms | Published package platform contracts, local format/type admission, public SQL/Python/CLI tests and deterministic blockers. | Freeze the supported OS/architecture, workload shapes, types, formats, resource/storage conditions and unsupported edges. Specify support and upgrade obligations for that envelope. |
| Complete and correct results | Typed/nested relational workflows, decimal reductions, analytic frames, native output/readback and complete-result regression evidence. | Run the declared envelope on the release candidate with independent expected results, empty/null/overflow cases, skew, wide rows, and complete output above small-collection limits. |
| Resource and pressure behavior | Shared reservations, bounded result batches, specialized spill and real multi-key ordering spill with slow/failing consumers. | Account for admitted reader/codec scratch, retained operator state, queued batches and writers; prove bounded retention or deterministic denial across the whole workload. Measure RSS separately and declare unaccounted provider allocations. General aggregate/join/window spill is required only for shapes promised to run beyond resident limits; otherwise fail before unsafe growth. |
| Failure, cancellation and recovery | Quota, corrupt/truncated/replaced runs, source mutation, cancellation, owned cleanup and failed-publication tests. | Exercise the supported workload end to end under pressure, interruption and storage failure; prove no successful partial output, credit/handle cleanup, ownership-safe recovery and repeat-call behavior. Document whether recovery means safe cleanup/restart or resumable execution. |
| Reproducible operating evidence | Fresh local UAT and exact-source/channel receipts exist. | Accept workload-specific scale, latency and memory observations with exact provenance and timing boundaries. Production acceptance does not require a speedup or competitor ranking; broader superiority claims retain their independent CG-5/CG-6 gates. |
| Security, API and release support | Local security gates, diagnostic/schema contracts and four verified package channels. | Review the supported envelope, compatibility policy, known issues, installation/upgrade/rollback and support instructions together; approve the release against all required evidence keys. |

The [current resource contract](../architecture/native-relational-resources-2026-10-02.md)
and [local resource gate](../architecture/v1-local-resource-safety.md) distinguish live execution
evidence from older fixture checks. Native Vortex remains the execution and highest-fidelity
persistence boundary. Unsupported work must keep deterministic no-fallback diagnostics.

Cloud connectors, table transactions, distributed/live/hybrid operation and full SQL/DataFrame
parity retain their own gates. They are not blanket prerequisites for a scoped stable local
engine. This checklist defines acceptance; it does not assert that the remaining work has passed.

## Gate behavior

Default mode is claim-safe:

```text
production_certification_status=blocked_not_production_ready
production_claim_allowed=false
performance_claim_allowed=false
public_release_claim_allowed=false
public_package_claim_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

The validator checks:

- workload name, environment, scale, formats, statefulness, effects, security posture, and
  unsupported edge boundary;
- the scoped `object_store_local_emulator_runtime_v1_candidate` profile when present, including
  local-emulator-only effects, provider admission status, request-signing boundary,
  no-network/no-credential/no-provider-probe posture, live-provider blocked diagnostics, and
  blocked benchmark/backpressure evidence until claim-grade proof exists;
- required evidence keys for runtime execution, correctness, Native I/O, execution certificates,
  fault tolerance, memory/backpressure, benchmarks, security/governance, release/API stability,
  and unsupported diagnostics;
- ShardLoom technique review for PulseWeave, capillary work units, dynamic admission/work shaping,
  metadata-first execution, timing-surface separation, and evidence-tier controls;
- deterministic unsupported diagnostics with `fallback_attempted=false` and
  `external_engine_invoked=false`;
- public claim surfaces in README, status docs, package metadata, and the benchmark handoff page.

Run:

```powershell
python scripts\check_production_certification_gate.py
```

Future maintainer-approved production release commands can use strict mode:

```powershell
python scripts\check_production_certification_gate.py --require-production-ready-workload
```

Strict mode fails until at least one declared workload has every required evidence key passed. The
gate does not publish packages, create tags, upload artifacts, use secrets, or allow Spark,
DataFusion, DuckDB, Polars, pandas, Dask, Ray, Velox, Trino, or another external engine as
ShardLoom execution evidence.
