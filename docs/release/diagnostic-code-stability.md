<!-- SPDX-License-Identifier: Apache-2.0 -->

# Diagnostic Code Stability

Status: local v1 diagnostic-code compatibility policy for `PROD-V1-2A`.

Validate with:

```bash
python scripts/check_v1_api_schema_stability.py
```

Diagnostic codes are stable machine-readable fields for agents, tests, release gates, and user
support. Human text may improve over time, but a v1 diagnostic code must not be removed, renamed,
or assigned a new meaning without migration notes, compatibility tests, and explicit
breaking-change approval.

Compatibility window: additive v1.

Migration policy:

- New diagnostic codes may be added when they describe a new deterministic condition.
- Existing diagnostic codes keep their current semantic meaning for v1.
- Renaming a diagnostic code requires preserving the old code as an alias for a documented
  deprecation window.
- Removing a diagnostic code requires migration notes, fixture updates, compatibility tests, and
  breaking-change approval.
- Severity or category changes require migration notes when agents could take different action.
- Unsupported-path diagnostics must continue to report `fallback_attempted=false`.

Stable v1 diagnostic code set:

- `SL_INVALID_INPUT`
- `SL_CONFIGURATION_ERROR`
- `SL_NOT_IMPLEMENTED`
- `SL_UNSUPPORTED_ENCODING`
- `SL_UNSUPPORTED_DTYPE`
- `SL_UNSUPPORTED_SQL`
- `SL_UNSUPPORTED_UDF`
- `SL_UNSUPPORTED_EFFECT`
- `SL_UNSUPPORTED_OUTPUT_FORMAT`
- `SL_MISSING_STATISTICS`
- `SL_PRUNING_INCONCLUSIVE`
- `SL_METADATA_LOSS`
- `SL_MATERIALIZATION_REQUIRED`
- `SL_EXTERNAL_EFFECT_DISABLED`
- `SL_LLM_CALL_DISABLED`
- `SL_API_CALL_DISABLED`
- `SL_EMBEDDING_MODEL_UNCONFIGURED`
- `SL_VECTOR_INDEX_UNAVAILABLE`
- `SL_OBJECT_STORE_UNSUPPORTED`
- `SL_COMMIT_NOT_ATOMIC`
- `SL_RESOURCE_BUDGET_EXCEEDED`
- `SL_NO_FALLBACK_EXECUTION`

Claim boundary: this policy stabilizes diagnostic identifiers and migration rules. It does not
claim that every planned runtime feature is implemented, production-ready, or package-published.

Fallback boundary: diagnostics explain unsupported or blocked behavior; they must not invoke
Spark, DataFusion, DuckDB, Polars, Velox, or another fallback execution engine.
