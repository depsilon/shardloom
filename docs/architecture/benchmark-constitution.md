# Benchmark Constitution

Schema: `shardloom.benchmark_constitution_validation.v1`

CLI: `shardloom benchmark-constitution [foundation|traditional-analytics] --format json`

Validator: `python scripts/check_benchmark_constitution.py`

## Purpose

The benchmark constitution is the fail-closed row validator for benchmark evidence. It verifies that
claim-bearing rows carry the evidence needed to interpret a timing row safely:

- benchmark result row identity
- dataset/source admission
- preparation route
- execution route
- output route
- correctness proof
- hardware profile
- build profile
- cold/warm cache state
- stage timings
- cost/unit fields where available
- no-fallback proof
- external-baseline boundary

The validator does not run benchmarks, invoke external engines, read datasets, publish artifacts, or
authorize performance, superiority, replacement, package, or production claims.

## Claim Boundary

Rows with `claim_gate_status=not_claim_grade`, `blocked`, `fixture_smoke_only`, or
`external_baseline_only` may be incomplete as long as they remain visibly blocked. Rows that claim
`claim_grade`, `ready_to_publish`, `ready_for_claim_review`, or
`performance_claim_allowed=true` are rejected unless every required field is present.

External engines remain baselines or correctness oracles only. They cannot become ShardLoom
execution evidence or fallback execution.

## Release Gate

Release readiness now checks that the validator exists, that the website benchmark manifest declares
the constitution schema and required field order, and that benchmark artifacts keep
`performance_claim_allowed=false` until claim-grade evidence is attached.

Current remaining gaps are measured result rows, complete source/preparation/execution/output route
metadata for every claim-bearing row, correctness proof attachment, reproducible hardware/build
metadata, cold/warm rerun attribution, stage timing completeness, and per-row no-fallback proof.
