<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 API Schema Stability

Status: local v1 schema-contract evidence for `PROD-V1-2A`.

Schema marker: `shardloom.v1_api_schema_stability_matrix.v1`.

Validate with:

```bash
python scripts/check_v1_api_schema_stability.py
```

This contract defines the stable v1 machine-readable surfaces that users and agents may depend on
inside source-built/local ShardLoom workflows. It is narrower than public package/release approval:
it does not publish packages, create tags, sign artifacts, upload attestations, run workloads,
invoke external engines, or authorize fallback execution.

Stable surface count: 12.

Diagnostic code count: 22.

Stable surfaces:

- `output_envelope`
- `diagnostic`
- `fallback_status`
- `route_fields`
- `evidence_summary`
- `claim_summary`
- `execution_certificate`
- `native_io_certificate`
- `capability_report`
- `package_release_report`
- `doctor_report`
- `support_bundle`

Compatibility window: additive v1. Existing stable fields stay available for v1; field removal,
semantic rename, or type narrowing requires migration notes, compatibility tests, and explicit
breaking-change approval.

Legacy flat-field policy: flat field aliases used by current CLI, Python, benchmark, and release
reports are stable aliases for v1. They may enter a documented deprecation window only after the
typed replacement exists, tests cover both names, and migration notes state the change.

No-fallback status:

```text
runtime_execution=false
fallback_attempted=false
external_engine_invoked=false
public_release_claim_allowed=false
public_package_claim_allowed=false
package_publication_performed=false
tag_created=false
signing_key_used=false
```

Diagnostic-code stability policy: `docs/release/diagnostic-code-stability.md`.

## Relationship To Publication Gate

`docs/release/publication-api-schema-stability-gate.md` remains the public-release fail-closed gate.
The matrix here supplies local schema stability evidence for API/schema fields. Package identity,
signing, checksum/SBOM publication grade, and channel proof remain blocked until their own release
items close.
