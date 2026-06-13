<!-- SPDX-License-Identifier: Apache-2.0 -->

# Per-Claim Evidence Attachment Matrix

Status: GAR-0041-A fail-closed release claim gate. This document describes the release-domain
surface emitted as `shardloom.per_claim_evidence_attachment_matrix.v1`.

The matrix binds every public claim family to explicit evidence attachments. It does not run tests,
rerun benchmarks, publish packages, resolve secrets, probe networks, invoke external engines, or
authorize fallback execution.

The current v1 framing is ShardLoom-scoped: required rows describe supported ShardLoom surfaces,
while broad replacement/superiority/platform rows remain explicitly out-of-v1 and blocked.

## Shared Release Fields

```text
per_claim_evidence_attachment_matrix_schema_version=shardloom.per_claim_evidence_attachment_matrix.v1
per_claim_evidence_attachment_matrix_report_id=gar-0041-a.per_claim_evidence_attachment_matrix
per_claim_evidence_attachment_matrix_support_status=blocked
per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade
per_claim_evidence_attachment_matrix_row_count=15
per_claim_evidence_attachment_matrix_blocking_row_count=15
per_claim_evidence_attachment_matrix_missing_attachment_count=135
per_claim_evidence_attachment_matrix_required_v1_row_count=7
per_claim_evidence_attachment_matrix_release_row_count=2
per_claim_evidence_attachment_matrix_out_of_v1_row_count=6
per_claim_evidence_attachment_matrix_all_required_categories_named=true
per_claim_evidence_attachment_matrix_all_claims_blocked=true
per_claim_evidence_attachment_matrix_public_release_claim_allowed=false
per_claim_evidence_attachment_matrix_public_package_claim_allowed=false
per_claim_evidence_attachment_matrix_performance_claim_allowed=false
per_claim_evidence_attachment_matrix_performance_superiority_claim_allowed=false
per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false
per_claim_evidence_attachment_matrix_engine_replacement_claim_allowed=false
per_claim_evidence_attachment_matrix_production_claim_allowed=false
per_claim_evidence_attachment_matrix_drop_in_replacement_claim_allowed=false
per_claim_evidence_attachment_matrix_production_platform_claim_allowed=false
per_claim_evidence_attachment_matrix_external_baseline_context_allowed=true
per_claim_evidence_attachment_matrix_package_publication_performed=false
per_claim_evidence_attachment_matrix_runtime_execution_performed=false
per_claim_evidence_attachment_matrix_benchmark_rerun_performed=false
per_claim_evidence_attachment_matrix_fallback_attempted=false
per_claim_evidence_attachment_matrix_external_engine_invoked=false
```

Every row names these required categories:

```text
required_test_evidence
required_benchmark_evidence
required_certificate_evidence
required_native_io_evidence
required_security_evidence
required_provenance_evidence
required_unsupported_path_evidence
required_no_fallback_evidence
required_release_approval
```

## V1 Claim Rows

| Claim row | Current status | Required attachment boundary |
| --- | --- | --- |
| `local_runtime_product_claim` | blocked | Source-built local runtime proof, CLI/Python route support, unsupported diagnostics, certificates, Native I/O, no-fallback fields, and user docs. |
| `api_schema_stability_claim` | blocked | API/schema compatibility window, package identity, migration notes, public schema report, and release approval. |
| `supported_front_door_scope_claim` | blocked | Supported CLI, Python, and SQL/DataFrame-style front-door matrix; examples; route capability evidence; unsupported diagnostics; and no-fallback proof. |
| `supported_vortex_route_claim` | blocked | Vortex-native input/preparation/query/output evidence, timing surface, evidence tier, certificates, Native I/O, and route admission proof. |
| `supported_output_sink_claim` | blocked | Supported local output/sink proof for Vortex, JSONL/CSV, evidence artifacts, overwrite/digest behavior, replay where applicable, and no-fallback evidence. |
| `security_supply_chain_claim` | blocked | Security gate, dependency provenance, SBOM/checksum evidence, signing/OIDC posture, known unsupported paths, and maintainer approval. |
| `external_baseline_comparison_claim` | blocked | Benchmark profile, correctness/reference status, baseline labels, timing-surface labels, no-fallback evidence, and no superiority/replacement wording. |

## Release Channel Rows

| Claim row | Current status | Required attachment boundary |
| --- | --- | --- |
| `public_release_claim` | blocked | Workspace validation, benchmark smoke, release/certificate/Native I/O/security/provenance/unsupported-path/no-fallback evidence and approval. |
| `public_package_claim` | blocked | Channel install, uninstall, smoke, SBOM/checksum/provenance, rollback/yank/delete/deprecate, Trusted Publisher/OIDC posture, and approval. |

## Out-of-V1 Rows

These rows remain blocked or historical. They document what is not a finished-product v1 claim.

| Claim row | Current status | Required attachment boundary before any future promotion |
| --- | --- | --- |
| `performance_superiority_claim` | blocked | Claim-grade benchmark profile, complete competitor lanes, correctness, certificates, Native I/O, environment, provenance, unavailable-lane reasons, no-fallback evidence, and claim-language approval. |
| `spark_displacement_claim` | blocked | Full local plus Spark evidence, scale/shuffle/spill/commit proof, object-store/table proof, Native I/O, Spark baseline-only labels, and claim-language approval. |
| `engine_replacement_claim` | blocked | Broad runtime, SQL/DataFrame, adapter, output, session, benchmark, certificate, Native I/O, release, and no-fallback evidence. |
| `production_sql_dataframe_claim` | blocked | Parser, binder, planner, optimizer, operator, type/null, API, benchmark, certificate, Native I/O, API stability, unsupported diagnostics, and release evidence. |
| `object_store_lakehouse_claim` | blocked | Credential, network, byte-range, write, commit, retry, table, scale, certificate, Native I/O, redaction, and unsupported operation evidence. |
| `foundry_platform_claim` | blocked | Real Foundry/dev-stack transform, package resolution, output dataset, evidence dataset, governance, platform fingerprint, no Spark fallback, and approval evidence. |

## Release Rule

Any missing row fails the claim gate. The hard release gate must keep public release/package claims
blocked while this matrix reports:

```text
per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade
```

Evidence that exists elsewhere in the repository is not enough by itself. A public claim requires an
explicit row-level attachment that connects the claim language to passing evidence, exact reference
files or artifacts, and a claim boundary.

## Non-Goals

- No new public superiority, replacement, or production platform claim.
- No package publication.
- No tag creation.
- No benchmark rerun.
- No runtime expansion.
- No external engine invocation.
- No fallback execution.
