# Public documentation and API consolidation

Status: implementation contract accepted from the October 18, 2026 maintainer
brief. This records the migration, not a claim that the target API has shipped.
Work stays in the [phase plan](phased-execution-plan.md) under RFC 0044 and
PERF-03/07/10/11/12. It is not another roadmap.

The brief reviewed `6780e7df564c55f3e256822be1d0383cfabff0c9`. Implementation
starts after PR #1575, merged as `f13da73db2e7550c4c9273faa567e55e9ebaef29`.
Its final code diff changes a `cfg(test)` pressure hook; production code is
unchanged. The six CPU investigations in PR #1576 retain their existing owners.
The version bump and package publication remain held.

## Decision

Configure resources → declare input → SQL or DataFrame plan → native execution
→ consume or write results.

Use `sl.context(...)` as the ordinary Python entry point. Keep the CLI as an
application entry point and native Python/Rust embedding as advanced integration
interfaces. SQL expresses work; formats describe I/O boundaries. Preserve exact
values/types, explicit resources, source identity, retained owners/credits,
cancellation, failure and publication guarantees.

The default guide describes the verified selected release from
`docs/release/package-channel-readiness-matrix.json`. Development changes have
explicit availability annotations. Documentation cannot change an immutable
package's behavior or authorize a release.

## Interface migration map

Grounding: `python/src/shardloom/{context,session,client,query,_native_backend,
_batches,native}.py`, `shardloom-python/src/{session,plan,result,stream}.rs` and
their focused tests. This inventories the listed boundaries, not every method.

| Existing boundary | Classification | Contract |
| --- | --- | --- |
| `ShardLoomContext`, `context()` | Canonical configuration facade with lifecycle gaps | Own close/cancel and shared source/prepared/result lifecycle; construction and fluent planning do not execute producers. |
| `ShardLoomSession` and its frame/workflow wrappers | Useful prepared-artifact/reuse reporting plus redundant facade | Delegate shared execution and result behavior. Preserve fingerprint invalidation and reuse evidence; do not declare aliases before lifetime behavior matches. |
| `ShardLoomClient` transports | Useful deployment distinction | Select once at construction. Preserve explicit process isolation and Windows package requirements; never replay execution through another backend. |
| `shardloom.native.Session`, typed `Relation`/`Plan` | Useful advanced integration | Retain typed embedding; ordinary Python reaches the same admitted runtime/owners without requiring this interface. |
| `LazyFrame`, `SqlWorkflow` | Useful input-language distinction | Preserve expressions and SQL parsing while sharing binding, native providers, resources and sinks for equivalent semantics. Common crate membership is insufficient proof. |
| `VortexWorkflowExecutionReport` | Completed data/native owner with a report-oriented name | Reuse this owner for the ordinary result, with schema, explicit conversions and typed execution details; preserve existing data/evidence access deliberately. |
| `SessionSqlResult` | Redundant wrapper around completed data | Converge collection on the same result contract while preserving useful reuse fields through documented compatibility; never rerun or implicitly export rows. |
| Native/process batch wrappers | Useful delivery implementations | One consumption contract with explicit close/cancel, provisional batches and final completion; process buffers are not zero-copy. |
| `from_arrow_table(result)` | Native-result naming/integration gap | Recognize trusted ShardLoom objects directly through the existing owner-admission path. Arrow conversion remains optional. |
| `run`, `collect`, `profile` | Distinct current roles with confusing discovery | Collection returns data; report execution and bounded profiling have explicit roles. Compatibility never executes twice. |
| Smoke/fixture/V1/release exports | Maintainer discovery mixed into application discovery | One lazy export authority, curated `__all__`/`__dir__` and reference, explicit maintainer namespace and small compatibility lookup. |
| CLI registry | Shared authority without audience classification | Minimal audience metadata/filtering in existing discovery/help; generate reference/completion/counts, preserving parsing and JSON envelopes. |

Observed lifecycle gaps to fix before changing defaults: context lacks close and
cancel; session `reuse=False` calls `client.close()` before executing, permanently
closing a native client. Native and process close/cancel must be validated. These
are implementation obligations, not permanent exclusions.
The existing process batch protocol requires POSIX. Cross-platform batch delivery
remains an implementation obligation under PERF-10.contracts and PERF-11.envelope;
Windows collection and writers remain its explicit supported delivery boundaries.

## Plan, result and error contract

| Intent | Behavior |
| --- | --- |
| Declare/transform | Return a plan. Explicit input admission and metadata inspection retain their documented boundaries. |
| Inspect/explain | Return useful plan/capability information without consuming a producer or implying query execution. |
| Collect | Complete once under the resource grant and retain schema/values and actual owners. Python row allocation is an explicit conversion. |
| Iterate batches | Use the same plan/execution with bounded delivery and close/cancel. Full drain and final success establish completion. |
| Write | Use the native writer; report fidelity and publication. Preserve current non-overwrite and multi-destination failure contracts. |
| Convert | Explicit Python/Arrow/pandas conversion with allocation/fidelity documented; optional packages stay optional. |
| Inspect evidence | Typed details attached to data, with raw certificates and stable diagnostics available. |

Ordinary errors use a consistent typed exception with the structured diagnostic
attached. Non-throwing inspection/reporting is explicit. Close revokes future
work but preserves already promised independent result lifetime. Resource
validation precedes dataset access; import neither starts workers nor allocates
resources. No backend switch or one-shot replay follows execution failure.

## Public page dispositions and ownership

The inventory covers these public sources and their producers/validators, not
uninspected internal files. Keep Astro/Starlight, current styling, reusable
diagrams, search and responsive navigation. Author narrative MDX directly.

| Source or route | Disposition |
| --- | --- |
| `README.md` | Keep overview, one shared tested example, canonical guide links and separate contributor links. |
| `python/README.md` | Package summary, same snippet and package-index-safe links; no parallel manual. |
| `/start` | Redirect to canonical workflow; preserve useful fragments. |
| `field-guide/start-local-proof.mdx` | Canonical installation and paid-orders workflow; retain URL. |
| `docs/getting-started/install.md` | Short canonical-guide link stub. |
| `package-user-install.md` | Merge tutorial; retain one verified package/platform reference. |
| `first-10-minutes.md` | Canonical-guide stub; move release rehearsal instructions to contributor/release tooling. |
| `source-checkout-install.md` | Keep advanced builds; no install commands for unpublished crates. |
| `examples.md`, `certified-local-workload.md` | Separate user examples from maintainer fixture/proof inventories; retain useful link stubs. |
| `troubleshooting-support.md` | Keep focused setup/error help and stable diagnostic links. |
| `v1-supported-unsupported.md` and generator | Replace onboarding dump with version-capability link; retain internal classifications in existing maintainer sources. Change the generator too. |
| `field-guide/index.mdx` | Task-oriented guide overview. |
| `python-surface.mdx` | Task guide for installed-release methods; mark later source-only behavior. |
| `runtime-and-io.mdx` | Split operations, types/formats, resources and delivery; preserve useful anchors and semantic caveats. |
| `execution-model.mdx`, `compute-flow.mdx` | Keep visual explanations; update terms/links and distinguish roadmap mechanisms. |
| `execution-routes.mdx` | Keep Data lifecycle explanation and stable slug. |
| `limitations.mdx` | Concrete version-specific issues and support details. |
| Benchmark guide/landing | One measurement explanation plus summaries/links to exact evidence, without chronological duplication. |
| User-surface index Markdown/JSON | Application discovery distinct from maintainer inventory; export/command facts generated from code. |
| Status matrices/release evidence/native embedding | Retain intentionally reachable advanced material outside ordinary onboarding/search priority. |
| `sync-content.mjs` | Data/reference/use-case/redirect synchronization only; remove narrative templates and guide pruning together. |

Navigation: Start; Work with data; Understand the engine; Reference; Versions
and help. No page-per-phrase requirement. Superseded prose belongs in Git history,
not another searchable archive. Evidence stays in its existing location. Moved
headings/routes need tested redirects/stubs and inbound-link updates.

Use the existing three-order CSV values (`1,paid`, `2,pending`, `3,paid`) in one
maintained executable example. SQL and DataFrame examples show both complete
paid rows. Verify write/reopen in the named installed release before including
it. Declare paths, memory and parallelism; illustrative allocations are not
defaults. Project installation facts from verified release metadata and runtime
capabilities from existing sources, without a second support database. Keep
availability, correctness, operational support and performance as separate facts.

## Existing owners and acceptance

| Owner | Obligation |
| --- | --- |
| PERF-10.contracts | Context/result/discovery migration and actual common planner/provider use. |
| PERF-03 | Explicit grants/ceilings, validation before data, retained credits. |
| PERF-07.transfer_lifetime | Result admission/chaining, close lifetime, unchanged writer/publication guarantees. |
| PERF-11.envelope | Context/session cancellation, batch drain, late producer failures and process/native semantics. |
| PERF-12.workflows/support | Installed examples, generation, search/links/mobile, compatibility and package/platform acceptance. |

Editorial/generator and runtime/default-path units have different review and
validation boundaries; both stay under these owners. Run generators twice to
stable output, remove obsolete generated pages, and replace old-prose validators
with canonical-link, snippet-identity, capability and audience checks. Preserve
independent correctness/safety tests. Execute published examples in a clean
installation without repository `PYTHONPATH`; check canonical URLs, old anchors,
versions, actual search hits and desktop/mobile reading/navigation.

Runtime acceptance covers installed native and supported process configurations,
missing-native/platform diagnostics, invalid/missing resources, retained chaining,
empty/nullable/nested values, invalidation, large batches, cancellation, late
errors, close lifetimes and streamed writes. Compare complete representative
workflows with the accepted implementation; wrapper changes do not establish a
kernel speedup. Run required Rust gates and applicable Python/native matrices.
Editorial changes alone do not require a full-size ingest or Full43 rerun.
Routine timing communication remains engine-only.

Vortex-first decision: `use_vortex_native_provider` through the existing admitted
`shardloom-vortex` and `shardloom-python` result/source owners, plans, batches and
sinks. No new encoding, query engine or provider dependency is introduced.
Native reuse preserves owners directly; rows/Arrow are explicit boundaries.
No-fallback certificates, source validation, resource credits and all CG-1
through CG-23 gates remain intact.
