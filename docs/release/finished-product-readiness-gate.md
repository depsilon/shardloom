# Finished Product Readiness Gate

`shardloom.finished_product_readiness_report.v1` is the final no-publication readiness aggregator
for v1 local product evidence.

It has two modes:

- Default mode: local v1 product evidence must pass, while public package/release blockers are
  reported separately under `public_release_blockers`.
- `--require-public-release-ready`: local evidence and all publication evidence must pass,
  including package-channel readiness, benchmark publication freshness, hard-release readiness, and
  human publication approval.

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

Run:

```powershell
python scripts\check_finished_product_readiness.py
python scripts\check_finished_product_readiness.py --require-public-release-ready
```

The first command is the CI release-readiness contract. The second command is reserved for a future
human-approved public release command.
