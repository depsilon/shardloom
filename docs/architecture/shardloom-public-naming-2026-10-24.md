<!-- SPDX-License-Identifier: Apache-2.0 -->

# ShardLoom public naming and terminology

Status: accepted October 24 maintainer direction, queued after
[user-surface execution parity](native-user-surface-execution-parity-2026-10-24.md)
and [warm source reuse](native-warm-source-reuse-2026-10-24.md), before
[full-ingest ownership/preparation](native-ingest-ownership-preparation-2026-10-24.md)
and [universal I/O](native-universal-io-performance-coverage-2026-10-24.md).
This records the naming decision and implementation scope; public copy and
interfaces have not yet been changed by this handoff. RFCs 0010/0012, the
[canonical terminology](canonical-terminology.md) and existing workflow,
resource, no-fallback and Vortex input/output contracts govern the pass.
CG-1 through CG-23 remain independent evidence gates.

## Product identity

Use one product name: **ShardLoom**.

Primary description: **A unified data processing engine.**

Supporting copy: Read, transform, query, and write data using SQL and DataFrame
workflows, with multiple input and output formats and one shared execution engine.

Short headline option: **One engine. Many formats. Familiar interfaces.**

Explain ordinary use as SQL or DataFrames in Python, queries from the command
line, or embedding ShardLoom in an application. Python hosts both SQL and
DataFrame interfaces; these are not separate execution products. Introduce
Vortex and other credited dependencies in the architecture explanation, where
their representations, file capabilities and limitations matter. Keep actual
supported operations, platforms and formats visible; the description does not
make planned capabilities available.

The public computation diagram is:

```text
Input formats -> ShardLoom engine -> Output formats
```

SQL and DataFrame interfaces describe work. Readers and writers handle formats.
ShardLoom plans and executes computation. "Shared middle" and blended labels
such as "prepared native Vortex engine" do not belong in product names.

## Scope and pre-adoption compatibility decision

This is a medium, coordinated terminology pass across authored copy, display
labels and their tests, not an engine rewrite. Doing it after parity and warm
reuse lets the lifecycle explanation describe the settled behavior. It precedes
ingestion so subsequent reader/writer work inherits the same public vocabulary.

The maintainer confirms ShardLoom is not yet in public use. Do not preserve
awkward public names through speculative compatibility layers or mandatory
deprecation periods for nonexistent consumers. Prefer the clearer name when a
real callable/parameter/command improvement is justified, and update its known
callers, reference generation, tests and examples together. A concise inventory
of affected interfaces is the compatibility plan; the default remains a naming
pass rather than wholesale API restructuring.

Published package/version records already exist in the repository. Absence of
external adoption does not authorize rewriting those records or immutable
benchmark evidence. Machine codes and schema fields have actual internal
consumers too. Prefer human display labels when an identifier already describes
a precise technical contract; change a machine identifier deliberately only
when useful, updating its real consumers and version contract. No alias is
required solely because a string previously existed. Preserve exact diagnostic
classification, fallback fields and artifact identity checks.

## Contextual naming map

| Context | Public wording | Precision to retain |
| --- | --- | --- |
| Product/computation | ShardLoom; the ShardLoom engine | One execution system across supported interfaces |
| Interfaces | Python API, SQL, DataFrame API, CLI, embedding API | SQL and DataFrames can both be used in Python |
| I/O | Input/output formats, readers, writers, connectors | Actual format/type support, conversion cost and metadata loss |
| Vortex output | A Vortex file written by ShardLoom | Actual layout/encoding and any required extensions |
| Persisted additions | ShardLoom metadata stored with the dataset | Exactly which metadata exists and which reader can interpret it |
| Prepared artifact | Loaded dataset, data file or reusable prepared data | Distinguish durable data from a retained query preparation |
| Source admission | Input validation; resource allocation separately | Validation and memory authorization are different operations |
| Resource envelope/credits | Memory and parallelism settings; memory reservations | A reservation is not a complete process-memory ceiling |
| Capillary work units | Execution tasks or bounded work units | Use the subsystem term within technical explanation when useful |
| Execution certificates | Execution reports or validation details | Preserve the underlying structured proof contract |
| Native result owner | Result; native-backed result for lifetime discussion | Preserve ownership, explicit conversion and release behavior |
| Full43 | 43-query ClickBench workload on first mention | Actual run order, repetition/cache policy and clock |
| No-fallback execution | Executed by ShardLoom | Explain the architectural prohibition separately; keep it enforced |

These are contextual translations, not global replacements. "Prepared query"
and "memory reservation" remain useful precise terms. Use "native" only when
it identifies compiled execution, retained engine-owned results, a benchmark's
normal storage classification or another explicit boundary. Do not rename
Vortex, invent a "ShardLoom file format" or add a "ShardLoom Storage" product.
Calling other formats supported inputs/outputs does not erase fidelity loss or
the internal distinction between execution and translation.

Introduce distinctive subsystem names by function, such as "Work scheduling —
PulseWeave", in architecture material. Initial navigation and first-run examples
should not require users to learn subsystem or evidence-policy vocabulary.

## Lifecycle, reports and benchmarks

- **Read:** declare a source through the normal API. Preserve laziness where it
  exists; constructing a file query is not importing its contents.
- **Load/ingest:** explicitly process or import source data when that work occurs.
- **Prepare a query:** bind and retain eligible query preparation, separately
  from dataset import.
- **Execute:** compute a fresh answer. **Collect, iterate or write:** select how
  to consume it. **Reuse:** identify the valid source/query preparation retained.

Cold and warm remain defined measurement conditions. Do not introduce product
modes named Cold, Warm, Prepared or Native. "First execution" and "repeated
execution in the same session" state the lifecycle without inventing cache
control. Equal execution capability across formats is not equal elapsed time
for parsing, decoding or writing physically different representations.

Human reports should lead with the actual input format, whether source
preparation was performed or reused, ShardLoom execution and output format.
Detailed provider/state identifiers remain available in advanced diagnostics.
Errors lead with the problem and an actionable recovery step, followed by the
diagnostic code and technical details. Retain deterministic error classes and
explicit no-fallback facts without making policy jargon the first sentence.

Use **ShardLoom** as the ordinary benchmark display name. Configuration details
belong alongside the measurement: version/build, hardware, interface, loading,
stored representation, resources, cache state, output and clock. Qualifiers such
as **ShardLoom (Parquet)**, **ShardLoom (in-memory)**, **ShardLoom (DataFrame)** or
**ShardLoom (tuned)** require genuinely separate measured configurations. These
are naming rules, not claims that those configurations are implemented.

Loading Parquet into a stored Vortex dataset is still the ordinary ShardLoom
configuration: disclose Parquet as source, the import and resulting Vortex
storage, plus load time and storage size where required. A direct-Parquet label
must not conceal a prior import. The [ClickBench methodology](https://github.com/ClickHouse/ClickBench)
requires reproducible load/query procedures and separate load/size observations.
Internal engine totals retain their existing measurement description; changing
a display name cannot turn them into official cold/hot or end-to-end scores.
Keep historical packet bytes, labels and observations unchanged, with a clear
presentation mapping where needed.

## Source-grounded implementation inventory

The October 24 source review confirms these current targets:

- [README](../../README.md): substrate-led opening, runtime badge and
  differentiators including "One Vortex-native middle", artifact intelligence,
  capillary work and execution certificates.
- [Homepage](../../website-src/src/pages/index.astro),
  [About](../../website-src/src/pages/about.astro),
  [layout](../../website-src/src/layouts/SiteLayout.astro) and
  [navigation](../../website-src/src/lib/data.ts): metadata, hero, chapter labels,
  diagrams, footer and subsystem-first navigation. Inspect interactive component
  labels and accessible descriptions as well as visible prose.
- [Field-guide introduction](../../website-src/src/content/docs/field-guide/index.mdx),
  [execution model](../../website-src/src/content/docs/field-guide/execution-model.mdx)
  and [lifecycle](../../website-src/src/content/docs/field-guide/execution-routes.mdx):
  move the diagnostic label dictionary into reference material and explain normal
  read/query/result actions. Keep guides for Python/DataFrames, SQL, reading and
  writing, memory/parallelism, execution and benchmarks easy to find.
- [Guide metadata](../../website-src/src/data/field-guide.json) and
  [sync script](../../website-src/scripts/sync-content.mjs): distinguish vocabulary
  and backlink metadata from the hand-authored guide. Refresh related generated
  projections through their source authority rather than editing output copies.
- [Benchmark guide](../../website-src/src/content/docs/field-guide/benchmark-methodology.mdx)
  and [summary component](../../website-src/src/components/BenchmarkSummary.astro):
  make the product name clear while retaining exact timing/cache/setup distinctions.
- [Diagnostic model](../../shardloom-core/src/diagnostics.rs),
  [Python errors](../../python/src/shardloom/errors.py) and actual user-facing
  messages such as [workflow preparation](../../shardloom-cli/src/native_runtime_workflow.rs):
  distinguish message/display changes from code/schema changes. The inventory
  is bounded; search remaining public help, API docstrings and examples during
  implementation rather than assuming these few files exhaust the surface.

## Acceptance

Keep one contextual terminology map in the canonical vocabulary and apply it
across the complete public pass. Review the first-run experience, human error
recovery and benchmark table independently of the technical reference. No
unsupported capability or performance claim may be introduced by simpler copy.

Use the [website source workflow](../../website-src/README.md): edit
`website-src`, shared source data and `website-public` where appropriate, then
regenerate `website`. Check rendered desktop/mobile pages, navigation, keyboard
and accessible labels, search, anchors and identical compatibility-page output.
Run the existing website build/check, link, readiness and static-asset checks.
Run executable examples and focused CLI/Python/diagnostic tests for any changed
public behavior or contract; run required workspace/native checks when runtime
code changes. A source terminology scan is a completeness aid, not proof that
every remaining technical occurrence should be removed. No benchmark rerun is
needed solely for presentation changes. Publishing packages or creating releases
is outside this queued pass.
