<!-- SPDX-License-Identifier: Apache-2.0 -->

# Use Case Atlas

The Use Case Atlas is the non-expert map for ShardLoom. It answers four questions without requiring
readers to understand the phase plan, RFCs, or benchmark internals:

- Can ShardLoom do my thing?
- How do I try it?
- What evidence do I get?
- What is not supported yet?

ShardLoom is still a pre-release, Vortex-first, no-fallback local compute project. These use cases
are not production, platform, performance, Spark-replacement, broad SQL/DataFrame, object-store,
lakehouse, or Foundry production claims.

## Status Vocabulary

Use cases use the same small status vocabulary everywhere:

- `ready_local`: a local source-checkout path is documented and expected to run without fallback.
- `smoke_supported`: a scoped local smoke or fixture path exists, with claim boundaries.
- `report_only`: users can inspect posture, diagnostics, or plans, but the runtime path is not
  supported.
- `planned`: the use case is an intended roadmap item and must not be treated as supported.
- `blocked`: ShardLoom intentionally blocks the path until required evidence exists.
- `unsupported`: the path is not supported and has no current implementation promise.

## Capability Families Covered

The machine-readable index in `docs/use-cases/use-case-index.yml` maps every current capability
family to at least one use case:

1. onboarding / first 10 minutes
2. local file ETL
3. compatibility import certified
4. prepared/native Vortex
5. Python wrapper/client
6. SQL/DataFrame/report-only surfaces
7. source-free generated output
8. messy data / dirty CSV / nested JSON / CDC
9. query scenario cookbook
10. output and fanout
11. object-store/S3/GCS/ADLS boundaries
12. table/lakehouse boundaries
13. Foundry dev-stack and local proof
14. evidence/audit/claim gates
15. benchmark interpretation
16. package/release/install channels

## How To Read A Use Case

Each indexed use case declares:

- audience and status
- execution mode and engine mode
- inputs and outputs
- evidence fields
- claim boundary
- runnable example or blocked explanation
- expected output/evidence
- common mistakes
- references
- related use cases

If a use case is `planned`, `blocked`, `unsupported`, or `report_only`, the blocked explanation is
part of the product surface. It prevents unsupported paths from being mistaken for hidden runtime
support.

## Local Validation

Use the atlas checks after editing the index:

```powershell
python scripts\check_use_case_index.py
python scripts\check_use_case_coverage.py
python scripts\check_use_case_glossary.py
python scripts\check_use_case_backlinks.py
python scripts\check_workflow_recipes.py
```

The checks ensure every capability family is represented, every use case has references, supported
or smoke-supported use cases include a runnable example, planned/blocked use cases explain the
blocker, and every use case declares a claim boundary.
