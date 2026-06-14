# Production Certification Gate

`shardloom.production_certification_gate.v1` is the common fail-closed production workload gate.
It evaluates declared workload profiles in
[`production-certification-workloads.json`](production-certification-workloads.json) and keeps
production claims separate from local v1 readiness.

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
- public claim surfaces in README, status docs, package metadata, and benchmark manifest.

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
