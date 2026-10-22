<!-- SPDX-License-Identifier: Apache-2.0 -->

Historical design record. The [final decision](native-multiway-join-2026-10-20.md)
does not retain this experiment; production code is restored. The prospective
protocol and observations below remain unchanged as provenance.

# Three-input constraint probe: integration gate

The prototype under `multiway-probe-817-v1` passed both complete frozen
cohorts, all 80 preflight queries and six boundary tests. Across preflight,
warmups and scored samples, all 688 queries passed complete ordered value,
schema, activation, native-credit and certificate checks. This permits an
integration attempt; it does not yet admit a production optimization.

Promote the same three-input probe into the shared prepared native dispatcher
under the existing native-primitives, native-write and Unix feature boundary.
Keep the exact plan, integer representation, source, parameter and spill
restrictions. Require a nonnullable canonical source struct as well as the
already materialized integer fields and validity. This conservative source
check does not materialize or inspect values. No new user option, alternate
frontend, dependency or external-engine execution is introduced.

Production admission is automatic for the bounded shape. A test-only ordinary
strategy override remains for comparison; it can disable the optimization but
cannot bypass admission. Ordinary public execution has no thread-local selector.
Expose `multiway_join_stages` in the native execution report and certificate,
and `relational_multiway_join_stages` through the existing public envelope.
One stage means one completed three-input constraint probe replacing two binary
joins. A zero value makes no claim about why a plan was ineligible. Failed
executions never produce a completed report. Per-pair test observations remain
excluded from production and disabled during scored samples.

Repeat the original eight-cell, one/four-call, two-cohort protocol with all
original gates, exact source formulas, order, warmups and controls unchanged.
First repeat all six boundary tests, add direct default-selection/report
checks and check a public typed workflow. The integrated comparison must
validate both the test activation count and actual execution-report evidence.
Preserve prototype and integration identities and results separately. Do not
promote an unchanged failed integrated candidate by selecting favorable rows.

Positive integration requires the workspace format/lint/test gates, native
feature tests, installed Python/public workflow acceptance, Public39, Full43
and modular acceptance for the resulting runtime. Ordinary source/type/operator,
writer, failure and non-admission tests remain binding. The comparison concerns
the finite memory-source shape; it does not claim general encoded joins,
worst-case optimality, spill, remote inputs or broader multiway planning.

Preserve all source snapshots, controls and failed attempts in a portable
packet. Retire redundant generated data and obsolete executables only after
readback and replay. Release publication and broader roadmap gates remain held.
