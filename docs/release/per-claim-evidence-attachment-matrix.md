<!-- SPDX-License-Identifier: Apache-2.0 -->

# Per-Claim Evidence Attachment Matrix

Status: GAR-0041-A fail-closed release claim gate. This document describes the release-domain
surface emitted as `shardloom.per_claim_evidence_attachment_matrix.v1`.

The matrix binds every public claim family to explicit evidence attachments. It does not run tests,
rerun benchmarks, publish packages, resolve secrets, probe networks, invoke external engines, or
authorize fallback execution.

## Shared Release Fields

```text
per_claim_evidence_attachment_matrix_schema_version=shardloom.per_claim_evidence_attachment_matrix.v1
per_claim_evidence_attachment_matrix_report_id=gar-0041-a.per_claim_evidence_attachment_matrix
per_claim_evidence_attachment_matrix_support_status=blocked
per_claim_evidence_attachment_matrix_claim_gate_status=not_claim_grade
per_claim_evidence_attachment_matrix_row_count=8
per_claim_evidence_attachment_matrix_blocking_row_count=8
per_claim_evidence_attachment_matrix_missing_attachment_count=72
per_claim_evidence_attachment_matrix_all_required_categories_named=true
per_claim_evidence_attachment_matrix_all_claims_blocked=true
per_claim_evidence_attachment_matrix_public_release_claim_allowed=false
per_claim_evidence_attachment_matrix_public_package_claim_allowed=false
per_claim_evidence_attachment_matrix_performance_claim_allowed=false
per_claim_evidence_attachment_matrix_superiority_claim_allowed=false
per_claim_evidence_attachment_matrix_spark_displacement_claim_allowed=false
per_claim_evidence_attachment_matrix_production_claim_allowed=false
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

## Claim Rows

| Claim row | Current status | Required attachment boundary |
| --- | --- | --- |
| `public_release_claim` | blocked | Workspace validation, benchmark smoke, release/certificate/Native I/O/security/provenance/unsupported-path/no-fallback evidence and approval. |
| `public_package_claim` | blocked | Channel install, uninstall, smoke, SBOM/checksum/provenance, rollback/yank/delete/deprecate, Trusted Publisher/OIDC posture, and approval. |
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

- No new public claim.
- No package publication.
- No tag creation.
- No benchmark rerun.
- No runtime expansion.
- No external engine invocation.
- No fallback execution.
