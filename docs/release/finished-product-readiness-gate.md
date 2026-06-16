# Finished Product Readiness Gate

`shardloom.finished_product_readiness_report.v1` is the final no-publication readiness aggregator
for v1 local product evidence.

It has two modes:

- Default mode: local v1 product evidence must pass, while public package/release blockers are
  reported separately under `public_release_blockers`.
- `--require-public-release-ready`: local evidence and all publication evidence must pass,
  including package-channel readiness, benchmark publication freshness, hard-release readiness, and
  post-release verification.

Local product evidence includes `target/v1-release-boundary-report.json`, which keeps public docs,
package metadata, generated support surfaces, package dry-run proof, and unsupported
production-family boundaries fail-closed before this final aggregator can pass. It also includes
`target/production-certification-gate.json`, which proves declared production workload profiles are
schema-valid and claim-safe while current production evidence blockers remain explicit.

Default mode is allowed to pass with:

```text
finished_product_readiness_status=local_v1_ready_publication_blocked
public_release_ready=false
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
package_upload_attempted=false
fallback_attempted=false
external_engine_invoked=false
```

Public-release mode fails unless those publication blockers are cleared by maintainer-approved
release evidence. The gate does not publish packages, create tags, upload attestations, use secrets,
or authorize performance, production, Spark-replacement, object-store, lakehouse, Foundry, broad
SQL, or broad DataFrame claims.

The public-release side also consumes
`target/final-release-approval-post-release-verification-report.json`, generated from
`docs/release/final-release-approval-post-release-verification.json`. That report keeps package
install/uninstall, first-10-minutes, golden workflow, no-fallback smoke, docs-link, and website
support-matrix verification blocked until a maintainer-approved public release has channel proof.

Run:

```powershell
python scripts\check_v1_release_boundary.py
python scripts\check_production_certification_gate.py
python scripts\check_final_release_approval.py
python scripts\check_finished_product_readiness.py
python scripts\check_finished_product_readiness.py --require-public-release-ready
```

The first command is the CI release-readiness contract. The second command is reserved for a
channel-proofed public release command.
