# Public documentation and API acceptance — October 18, 2026

The public-guide and context/result/discovery consolidation is implemented and
locally validated at `dba85f1c43aeb83a2640c81654ac2b9ce8b15d98`. The base is
`c87f893483ce8c985c4aa27da792bb4e1ed64062`, which retains the six CPU investigations
after PR #1575 merged at `f13da73db2e7550c4c9273faa567e55e9ebaef29`.
The [implementation contract](public-documentation-and-api-consolidation.md)
records dispositions and ownership; the [migration reference](../reference/python-api-migration.md)
describes deliberate behavior changes. This acceptance does not publish a package,
deploy a website or close a broader PERF owner or competitive gate.

## Delivered behavior

- The first workflow has one authored guide and one maintained three-order fixture.
  Python/DataFrame, SQL and CLI examples return both paid rows, IDs 1 and 3, with
  explicit resources and write/reopen continuation. Root/package readmes project
  the same example; `/start/` redirects to the canonical guide with useful legacy
  fragments preserved. Release rehearsals remain contributor material.
- Narrative pages are ordinary MDX. Reusable components project version, platform,
  capability and benchmark facts from existing authorities. Task navigation,
  diagrams, responsive behavior and search remain intact. Product search excludes
  contributor inventories and raw evidence; obsolete generated output is pruned.
- The default guide describes the verified published **v0.6.0** package. A visible
  development annotation and migration page distinguish later source behavior.
  The new local wheel keeps the held version for testing; it is a different,
  unpublished artifact and does not replace the immutable published package.
- `sl.context` chooses an installation backend once, owns resources and close/cancel,
  and shares the native plan/provider and result owners used by existing execution.
  Linux/macOS native installations default to in-process execution; explicit CLI
  configuration and Windows select process execution. Execution failure does not
  switch backends or replay a one-shot request.
- Collection uses `Result`, with schema, explicit value conversions and typed
  details. `from_result` recognizes ShardLoom owners directly; native chaining
  needs no Python-row or Arrow export. Process admission uses a typed bounded
  conversion. Completed values remain valid after context close. Writers preserve
  fidelity and publication contracts, including existing destination restrictions.
- Ordinary execution errors have a consistent typed hierarchy and attached
  diagnostics. `check=False` remains explicit report handling. Context/session
  sharing, `run`, collection, profiling and compatibility names have documented
  roles; the old result class name is an identity alias. Prepared reuse no longer
  closes a native client before executing.
- One lazy export authority controls application, diagnostics and testing
  namespaces, type stubs and reference generation. The executable CLI registry
  generates **203** commands, partitioned into **14** application and **189**
  maintainer commands. Human help and completion foreground applications while
  unfiltered machine metadata retains the complete inventory and JSON envelope.

## Verification

The [portable evidence index](../benchmarks/evidence/public-documentation-api-2026-10-18.json)
identifies complete logs, reports, source hashes, fixtures, comparison outputs and
screenshots. Paths in this table are archive members under `evidence/`.

| Boundary | Evidence and outcome |
| --- | --- |
| Required Rust gates | `rust-fmt-716a2.json`, `rust-clippy-715a3.json`, `rust-tests-716a1.json`: formatting, strict workspace/all-target Clippy and workspace/all-target tests pass; 3,235 tests, zero ignored. |
| Native provider features | `native-feature-checks-714a1.json`: 1,087 tests pass with `release-user-surfaces` across the CLI library/binary and Vortex preparation, public memory/workflow and resident-worker targets. `native-feature-clippy-715a1.json` passes strict CLI/Vortex all-target Clippy. Production inputs are unchanged since these checks. |
| Python source suite | `python-all-tests-715a1.json`: 829 discovered, zero failures/errors, six skips. Three require absent optional NumPy/pandas/fastavro integrations; three installed-only cases pass separately below. |
| Actual installed local wheels | `wheel-current-715/report.json` and `wheel-prior-715/report.json`: build/install/load/uninstall proof and 100 binding tests each, zero skips. Both are macOS arm64 native wheels, exercised on Python 3.12. |
| Minimal installed application API | `installed-minimal-public-715.json`: 38 tests, zero skips, isolated installed imports without repository `PYTHONPATH` or Arrow/pandas/NumPy/Polars/DuckDB. Both default-native and explicit-process examples return exact values after their contexts close. |
| Published-package tutorial | `published-examples-709a3.json`, `published-quickstart-probe.json`, `published-cli-quickstart-result.json`: clean PyPI v0.6.0 installation, exact Python/DataFrame/SQL/CLI values, native/process execution and write/reopen. No optional integration is needed for the tutorial. |
| Documentation generation and validators | `docs-generation-checks-715a3.json`: both full generator/build passes produce the same 141 file hashes. Astro reports zero errors/warnings; existing static-asset, readiness, productization, public-status, reference and stub checks pass. The 169 release/quickstart tests pass with two installed opt-ins covered separately; all ten link tests pass. |
| Rendered UI | `ui-715/qa.json` and `ui-715/development-final-716.json`: desktop 1280×720 and phone 390×844 navigation/readability, published/development labels, five relevant paid-orders search results, no horizontal page overflow and no warning/error console entries. The final development example title is `context-result.py`. Temporary viewport overrides are reset. |
| Complete workflow comparison | `workflow-comparison-715a2/report.json`: all 80 samples pass exact full-value/schema checks across prior/current defaults and matched explicit native/process configurations. Full Arrow results and synthetic Vortex writes are retained. |
| Final documentation closeout | `final-checks-716b/report.json`: 174 release/roadmap regression tests, current stubs/reference, contribution, scope, runtime-gap and documentation checks pass. The architecture tracker retains its blocked posture for the open roadmap. Bash and Zsh parse the generated completion scripts; Fish is unavailable locally, while its executable text/JSON agreement is covered by Rust tests. |

The principal behavioral regressions are exercised in
`python/tests/test_public_context.py`, `test_batches.py`, `test_cli_client.py`,
`test_public_exports.py`, `test_public_api_reference.py` and
`shardloom-cli/tests/public_discovery.rs`. Coverage includes:

- Missing/malformed resource grants before extension or dataset access; missing
  native modules and unsupported-platform selection; imports without workers,
  optional integrations or implicit allocations.
- Actual SQL/DataFrame `native.Plan` construction and Vortex 0.85 provider evidence,
  with subprocess launch rejected during native execution.
- Empty, nullable and nested values, Unicode, binary values and full-width unsigned
  integers; native chaining with row/Arrow conversion deliberately disabled;
  installed process chaining of completed native and process results.
- Prepared-source generation invalidation, shared facade close, `reuse=False`,
  retained owners after close, paused/unopened batch cancellation, cancellation
  while a producer is active, late errors and final-completion requirements.
- Existing writer fidelity/non-overwrite/publication contracts, one-shot producers,
  typed errors and aliases, command JSON/audience partitioning, and generated
  discovery rejecting missing, duplicate or stale metadata.

## Comparison and identity

The prior wheel is built from the clean accepted base; its production runtime is
the same as merged PR #1575 because the intervening CPU queue commit changes docs.
The current wheel is built from this implementation. The retained source manifest
has 931 current inputs. The final source bridge verifies 930 unchanged inputs;
the only later difference is a test assertion in `public_discovery.rs`, covered
by final strict Clippy and the full workspace test pass. No production difference
is inferred away. The implementation commit and final documentation identities
are recorded separately.

After the two matching documentation builds, the source-checkout guide's example
link was corrected to the maintained development fixture; its former README
target now provides an embedding summary rather than the described example.
The final bridge records this single authored-source change among the 141 hashed
paths; generated website output remains identical. Acceptance/ledger/phase updates
are separate documentation-only closeout changes.

Five alternating repetitions cover four workloads in four configurations:
101 short calls; a three-collection retained chain over 32,769 input rows;
131,073 rows delivered in columnar batches; and a one-shot 70,017-row producer
written to Vortex and fully reopened. Every arm uses explicit 1 GiB/two-lane
resources and checks its actual transport, installed package and binary identities.
The process baseline uses an explicit non-nullable schema so type comparison is
equivalent. Complete values, schemas, publication and no-fallback evidence decide
acceptance; the producer runs once.

Engine timing is unavailable in these high-level receipts and remains null for
all 80 samples. Separately labeled complete-workflow diagnostic clocks are retained
for examining dispatch/materialization changes; they are not engine times or a
kernel-speed claim. This task changes no Full43 method and requires no new
full-size ingestion or Full43 pass.

The full release-boundary gate was also attempted and failed closed because this
checkout has neither its separate source-rehearsal transcript nor a package-channel
readiness report at the default generated paths. Its regression tests pass, and
the fresh installed-wheel proofs above cover this change's package acceptance.
No release-readiness result is inferred from that failed gate or from an older
rehearsal. The final closeout checks retain this missing prerequisite explicitly.

The first baseline build accidentally reused a current artifact from the shared
Cargo target. Identity checks caught it before measurement. Both baseline artifacts
were rebuilt and independently identified; no comparison used the discarded pair.
The first complete comparison preserved correct values but failed aggregate schema
equality because its old-process fixture defaulted to nullable. Both the failed
cohort and the corrected full rerun remain retained. Earlier test/build failures,
the interrupted documentation build, a formatter launch PATH error, and recovery
from a stale browser tab are diagnostic history, not passing evidence.

## Remaining boundaries

This is local macOS runtime and generated-site acceptance. Changed-package Linux
and Windows runtime lanes, hosted PR checks and deployment verification require
the later integration step; they are not inferred from local builds. A fresh origin
check confirmed the existing published v0.6.0 site. Older cached extraction was
not used to claim a deployment defect. The new site is locally built and rendered.

Process batch pipes still require POSIX; Windows uses bounded collection and
writers. Remaining cross-platform batch delivery belongs to PERF-10.contracts and
PERF-11.envelope. Small computed collection retains its existing 65,536-row/8 MiB
boundary, distinct from admitted native columnar result paths. Larger results use
existing batches or writers. Process cancellation can leave publication uncertain
and reports that fact. These are explicit existing contracts, not parity claims.

PERF-03/07/10/11/12 remain open for their broader resource, ownership, composition,
platform and workflow obligations. The [phase plan](phased-execution-plan.md)
resumes the existing CPU/breadth sequence. CG-1 through CG-23, native Vortex
input/output, no external-engine fallback, the version hold and publication
authorization remain unchanged.

## Hosted integration follow-up — October 19

[PR #1577](https://github.com/depsilon/shardloom/pull/1577) publishes this source
change for review. The first hosted core shard, job
[117992326766](https://github.com/depsilon/shardloom/actions/runs/39232121032/job/117992326766),
reported four errors that were reproduced locally with the exact shard entry
point and no selected native extension. The prior discovery-based acceptance
did not exercise that package-qualified test import path.

The context test now imports its shared native helper in both package and
discovery modes. The resource-admission test uses its existing recording client
for a successful construction assertion; missing-resource checks still reject
before data access. The source-only parity checker reads the same static method
rows directly instead of probing an installed backend. A regression test rejects
any backend invocation from that metadata check. Runtime transport selection and
missing-native-extension errors are unchanged.

The corrected `python scripts/run_python_test_shard.py --shard core` passes
locally: 661 discovered cases, 551 executed, zero failures/errors and 16 reported
skips, including native classes unavailable in that source-only environment.
The difference between discovered and executed counts includes whole-class skips;
it is not additional passing runtime coverage. Hosted checks remain separate.
These integration changes affect tests and source-validation tooling, not the
frozen Rust or Python runtime implementation. The four preceding trailing-blank-line
removals and regenerated API source identities are also behavior-preserving.

The later package smoke job
[117995447125](https://github.com/depsilon/shardloom/actions/runs/39232909840/job/117995447125)
built and installed the CLI-bundled wheel successfully, then failed its generated
row and range output commands because they implicitly selected the in-process
transport without an installed native extension. Those two commands now select
`transport="subprocess"` explicitly, matching this package proof's bundled CLI.
The separate native-wheel gate continues to prove the in-process package; neither
the application default nor installation-error behavior changes.

The strengthened output-proof test fails against the original commands. All 168
output-proof and release-script tests pass after the correction on both PR source
stacks. The two corrected commands also pass through the public Python wrappers
and the retained native CLI with the native Python extension absent, validating
all committed values and no-fallback fields. This local reproducer does not claim
a rebuilt installed-wheel pass; the new hosted package job supplies that evidence.

The subsequent package and native-wheel jobs passed. The release-readiness job
[118016969493](https://github.com/depsilon/shardloom/actions/runs/39235412363/job/118016969493)
then reported one aggregate blocker: `example_replay: docs_marker_source_count=3`.
The consolidated documentation producer correctly validates its three current
sources, while the conformance matrix and two readiness consumers still expected
six. Their expected count now matches the current source inventory; the hard
readiness consumers share the conformance constant.

A new producer-to-consumer test reproduces that exact blocker before the fix. It
passes the real documentation-marker summary through the conformance and hard
readiness gates, and rejects absent, incomplete, extra and stale counts. All 180
release-script, conformance and producer-contract tests pass locally. This fixes
source-evidence accounting and does not change runtime behavior or claim that
the pending hosted release-readiness run has passed.

The next release-readiness job
[118038464603](https://github.com/depsilon/shardloom/actions/runs/39242475270/job/118038464603)
passed that accounting boundary, then found the benchmark guide missing the
required `production readiness` claim marker. The guide now uses that term in
its existing statement that a complete regression suite does not establish
production readiness. Generated pages and search assets carry the same wording.

A new test applies the production-certification checker to the actual current
claim surfaces and reproduces the missing marker before the wording correction.
All 181 release-script, conformance and producer-contract tests pass locally
afterward. This restores the existing claim boundary without relaxing the
checker or asserting hosted readiness; the updated PR still requires its own
hosted result.
