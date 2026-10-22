<!-- SPDX-License-Identifier: Apache-2.0 -->

Historical design record. The [final decision](native-multiway-join-2026-10-20.md)
does not retain this experiment; production code is restored. The prospective
protocol and observations below remain unchanged as provenance.

# Multiway public projection admission

The first normal CLI test, `multiway-public-cli-tests-820`, failed during SQL
parsing, before execution. Its flat three-table join is outside the current
parser. The parser supports a derived two-input relation joined with a third
input, but that supported form inserts a projection between the two native join
nodes. The initial integration consequently does not prove public selection.
The failed test, exact sources and ordinary release build remain evidence.

Before another implementation attempt, admit exactly one intervening projection
whose complete bound expression list consists only of column references. Resolve
its ordered output aliases back to the lower join's existing output mapping.
Borrow the bound names and expressions; do not allocate or clone another plan,
evaluate expressions, change SQL parsing, or broaden source admission. Preserve
the ordinary binder's complete name, type and expression validation. The lower
join's root is nonnullable, and these projections only select or rename existing
columns. Computed, cast, literal, filtered and limited intermediate nodes remain
ineligible, as do all existing source, kind, key, parameter and spill exclusions.

This mapping is part of the shared native probe. Public SQL, Python, DataFrame
and CLI paths keep their existing lowering and source handling. Final payload
gathering follows the projected alias mapping without materializing the lower
join. Duplicate columns, field ordering and both upper key orders remain exact.

Keep the original eight fixtures, sample counts, one/four-call workflows, both
cohorts, ordinary controls and quantitative thresholds unchanged. Repeat the
integrated comparison against the changed runtime before acceptance. Add direct
boundary tests for alias/reorder/duplicate projections and computed/literal
exclusions. Correct public tests to the supported derived-relation syntax and
require actual selected-stage evidence for materialized integer declarations;
chunked Python sources remain ordinary controls. Repeat the core gates, normal
release builds, installed Python, Public39, Full43 and modular acceptance.

The previous integrated measurements remain historical evidence, not acceptance
of this extension. Do not claim general multiway planning, general projection
fusion, flat chained SQL syntax, broader encoded inputs or release readiness.
