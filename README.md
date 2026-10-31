# ShardLoom

[![CI](https://github.com/depsilon/shardloom/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/depsilon/shardloom/actions/workflows/ci.yml?query=branch%3Amain)
[![PyPI](https://img.shields.io/pypi/v/shardloom?label=PyPI)](https://pypi.org/project/shardloom/)
[![Python client versions](https://img.shields.io/pypi/pyversions/shardloom?label=Python%20client)](https://shardloom.io/field-guide/start-local-proof/)
[![Homebrew](https://img.shields.io/badge/Homebrew-depsilon%2Ftap%2Fshardloom-2f4f4f)](https://github.com/depsilon/homebrew-tap)
[![Website](https://img.shields.io/badge/website-shardloom.io-0f766e)](https://shardloom.io/)
[![Field Guide](https://img.shields.io/badge/docs-field_guide-2563eb)](https://shardloom.io/field-guide/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Patent Pending](https://img.shields.io/badge/patent--pending-designs-7c3aed)](#license)

**A unified data processing engine.**

Read, transform, query, and write data using SQL and DataFrame workflows, with multiple
input and output formats and one shared execution engine. Use SQL or DataFrames in
Python, run queries from the command line, or embed ShardLoom in an application.

ShardLoom reduces unnecessary reads, decoding, and intermediate data movement while
keeping memory and parallelism explicit. The published technical preview supports
specific local analytical workflows; broader capabilities remain under development.

**[Explore the website](https://shardloom.io/) · [Read the field guide](https://shardloom.io/field-guide/) · [Install and run](https://shardloom.io/field-guide/start-local-proof/)**

## First query

<!-- BEGIN SHARDLOOM RELEASE -->
Published **1.0.0**. [Install and platform requirements](https://shardloom.io/field-guide/start-local-proof).

```sh
python -m pip install shardloom==1.0.0
```

- macOS 26 or newer, Apple silicon; CPython 3.10 or newer; native or subprocess.

- Linux x86_64, glibc 2.39 or newer; CPython 3.10 or newer; native or subprocess.

- Windows x86_64; CPython 3.13; subprocess.

The example selects native execution. Use `transport="subprocess"` for the Windows CLI-backed package.
<!-- END SHARDLOOM RELEASE -->

<!-- BEGIN SHARDLOOM QUICKSTART -->
```python
from pathlib import Path
from tempfile import TemporaryDirectory
import shardloom as sl

workspace = TemporaryDirectory(prefix="shardloom-orders-")
orders = Path(workspace.name) / "orders.csv"
orders.write_text("order_id,status\n1,paid\n2,pending\n3,paid\n", encoding="utf-8")

# Illustrative caller choices: 256 MiB and two execution lanes.
ctx = sl.context(transport="native", memory_bytes=256 << 20, max_parallelism=2)

paid = ctx.read_csv(orders).filter(sl.col("status") == "paid").sort("order_id")
result = paid.collect(check=True)
print(result.to_rows())

ctx.close()
workspace.cleanup()
```

Expected values:

```python
({'order_id': 1, 'status': 'paid'}, {'order_id': 3, 'status': 'paid'})
```
<!-- END SHARDLOOM QUICKSTART -->

The DataFrame is lazy: reading, filtering, and sorting describe the work; `collect()`
executes it. The example creates its own input and cleans its temporary directory.
Memory and parallelism are illustrative caller choices. Continue with
[SQL and writing results](https://shardloom.io/field-guide/start-local-proof).

## What Makes ShardLoom Different

ShardLoom combines work avoidance, computation over compressed data, explicit resource
ownership, and inspectable execution in one engine. These choices connect the whole data path:

**Query interfaces → planning → source access and preparation → native computation → results and storage**

Scheduling, memory management, and execution evidence support that path throughout. The
sections below explain the engine and its extension contracts; executable combinations and
release availability follow the [support matrix](docs/release/public-status-matrix.md).

### Planning and interfaces: one shared engine

SQL, DataFrame operations, the command line, and embedded applications describe work for
the same native execution engine. Readers and writers connect formats to that engine.

- **Shared operators.** Grouping, joins, expressions, ordering, and result ownership serve
  multiple interfaces and input formats. Each adapter supplies the metadata, selected reads,
  row identity, and buffer ownership it can actually support.
- **Demand before preparation.** Planning identifies visible output columns and hidden
  dependencies such as join keys, filters, and ordering expressions before expensive source
  work. Supported plans carry demand through joins to exclude unused intermediate payloads.
- **An explicit execution boundary.** Unsupported requests produce actionable diagnostics.
  ShardLoom never delegates their execution to Spark, DataFusion, DuckDB, Polars, or another
  query engine.

> **ShardLoom design:** a common execution contract connecting interfaces, source capabilities,
> native operators, and evidence.

### Source access and storage: avoid reading work into existence

The first optimization opportunity is often before an operator receives any values.

- **Answer from exact metadata.** Supported requests use established counts or statistics
  without scanning the corresponding payload.
- **Prune irrelevant data.** Segment and row-group statistics exclude regions that cannot
  contribute to a result.
- **Read the required columns.** Source demand reaches adapters that support selected reads.
- **Rank before fetching wide payloads.** Eligible bounded-ordering queries read ranking
  dependencies first, identify winning source positions, and retrieve payload from the source
  regions containing those winners. Losing regions need not contribute their large text,
  decimal, or nested payloads.
- **Keep source identity intact.** Ranking, retrieval, reuse, and completion checks remain
  tied to the same source generation.

Read granularity remains explicit: an adapter may retrieve whole column chunks from selected
row groups even when only a few rows survive.

For prepared local workflows, reusable structure stays with the data in a `.vortex` artifact:
native layouts, statistics, and supported derived metadata. Embedded helpers such as string
lengths or time buckets support later queries without appearing in ordinary `SELECT *` output.

> **Foundation:** Vortex's encoded layouts and metadata, combined with ShardLoom's source-demand
> and identity contracts.

### Preparation and reuse: retain useful work at the right scope

ShardLoom distinguishes three kinds of reuse:

- **Prepared data and query setup.** Supported sessions retain source handles, validated
  identities, and eligible query preparation. Repeated execution avoids repeated setup while
  computing a fresh result. Detected source replacement or mutation invalidates reuse.
- **Completed expressions.** Within a supported execution scope, an identical completed
  expression supplies another consumer. Matching includes types, function options, and other
  semantic parameters; reuse preserves conditional domains and observable errors.
- **Explicit materialized relations.** A declared materialized intermediate computes its
  complete input once per action and supplies that completed native relation to subsequent
  references. It uses existing memory and optional spill machinery. A later action recomputes it.

Merely naming a query does not create a persistent answer cache. Partial or failed
intermediates cannot become reusable completed relations.

> **Inspiration:** compiler common-subexpression elimination and explicit dataflow reuse.

### Native computation: use the representation the data already has

ShardLoom tries to compute against encoded values before expanding them into a more expensive
representation.

- **Repetition becomes computational structure.** Supported kernels consume dictionary codes,
  constant values, and run values with repetition counts. Repeated values contribute their
  multiplicity without independent reconstruction for every row.
- **Grouping and membership share exact keys.** Grouping, DISTINCT, joins, and related
  operations share native key identity and equality. Group ordinals can be established once
  per batch and reused across measures. Hash matches still require equality checks;
  dictionary codes remain tied to their value domains.
- **Typed execution removes repeated interpretation.** Numeric operations bind supported
  physical types and operations before entering batch loops. Compatible aggregate updates
  reuse prepared work. Prepared string kernels reuse batch-level access, temporal extraction
  computes the requested component, and eligible arithmetic keeps literal operands scalar.
- **Proven dependencies remove redundant work.** Where a derived grouping value can be
  reconstructed from another key without changing semantics, it need not remain an independent
  physical key.

These optimizations preserve exact DISTINCT, complete grouping equality, NULL behavior,
numeric overflow rules, and declared floating-point semantics.

> **Inspiration:** compressed database execution, weighted algebra, and compiler dependency analysis.

The same native operator foundation supports richer analytical composition:

- As-of joins with explicit direction, tolerance, and duplicate handling.
- Elapsed-time and calendar windows, including daylight-saving and month-end policies.
- Resampling, gap filling, and interpolation.
- Decimal precision, scale, and rounding after exact accumulation where supported.
- Strict and extended floating-point policies with defined NaN, infinity, and signed-zero behavior.
- Grouping sets, filtered aggregates, ordered collections, and recursive queries.

These capabilities share native value, memory, cancellation, and spill contracts.

### Ordering and reduction: narrow candidates before expensive final work

When a query needs a small ordered result, supported strategies retain a bounded candidate
set. Ordering keys and row locations can remain separate from wide payloads until selected
rows need gathering. Rich-value paths extend this to decimal and nested payloads while
preserving validation and independent output ownership.

Selected grouped top-K strategies also separate candidate discovery from expensive final
work. Conservative bounds identify possible winners; exact recount or deferred measure
evaluation completes the answer. Exclusion requires evidence that a candidate cannot affect
the result, including ordering and tie rules.

Late materialization therefore connects source access, ordering, grouping, and output construction.

> **Inspiration:** information-retrieval ranking and candidate reduction followed by exact evaluation.

### Scheduling and runtime control: bounded work with inspectable decisions

ShardLoom's runtime-control model connects work shape to resources and execution evidence.

**Capillary work units** describe bounded, typed pieces of supported work. Their contracts
carry source ranges, projection and filter references, artifact identity, ownership, and
resource information, giving scheduling and completion checks a concrete unit to reason about.

> **ShardLoom design:** connect each work unit's data requirements, resource obligations, and evidence.

**PulseWeave** organizes automatic control into four cooperating components:

- **FlowInventory — control unfinished work.** Bound ready and in-flight work using memory,
  parallelism, and downstream capacity.

  *Inspiration: Kanban and work-in-progress limits.*

- **ScarcityLedger — make resource pressure comparable.** Represent memory, queue capacity,
  decoding, sink pressure, and spill constraints as deterministic decision inputs.

  *Inspiration: resource pricing and electricity-market allocation.*

- **EndoPulse — adjust from observed pressure.** Use a completed local window's feedback to
  inform the next window's work shape and concurrency, with decisions local to the run.

  *Inspiration: endocrine regulation and slow feedback control.*

- **ProofBound — gate automatic application.** Require the relevant route, resource,
  correctness, and execution evidence before a policy can be treated as applied.

  *ShardLoom design: make evidence requirements part of runtime control.*

Dynamic work shaping uses these concepts to reason about splitting oversized work, combining
small units, or holding work for memory or downstream capacity. Application is scoped to
supported local routes; reports distinguish applied controls from advisory decisions and
blocked capabilities. See the [control model](docs/architecture/pulseweave-runtime-control.md).

### Memory and data movement: ownership follows the work

Buffers, retained operator state, and results carry explicit ownership and memory reservations
through supported execution boundaries.

- **Controlled buffer transfer.** Proven private reader buffers can transfer into native
  ownership without another copy. Eligibility depends on provenance, lifetime, and retained capacity.
- **Bounded retention.** Operators retain required state; queues and output buffering limit
  accumulated unfinished work.
- **Independent result lifetimes.** Supported results remain valid after source or producer
  closure, with reservations attached to the surviving data.
- **Native spill.** Supported stateful operations move state into ShardLoom-managed temporary
  runs under explicit spill policy.
- **Defined cleanup.** Cancellation, allocation refusal, producer failure, and unsuccessful
  publication release task-owned resources through their established lifecycle.

Reservations cover accounted engine resources, rather than total process memory. Spill
support is specific to the operator and value domain.

> **ShardLoom design:** carry resource responsibility with data and state across operator boundaries.

### Formats, datasets, and integrations: explicit boundaries around the engine

The adapter architecture separates file representation, dataset semantics, and transport.
This keeps shared computation independent of the source and destination format.

| Boundary | Families and responsibilities |
| --- | --- |
| Native storage | Vortex arrays, files, and dataset metadata, preserving the most information. |
| Columnar and structured files | Parquet, Arrow IPC/Feather, ORC, and Avro, with explicit logical-type, encoding, framing, and metadata contracts. |
| Text files | CSV and the TSV/delimited family; JSON, JSONL, and NDJSON, with explicit schemas, quoting, framing, and rich-value representations. |
| Dataset organization | Directories, globs, manifests, partitions, and multi-file collections, with membership, schema, ordering, and generation identity. |
| Enterprise format extensions | Workbook contracts such as XLSX, including worksheet selection, cached formula values, dates, precision, and format limits. |
| Table integration contracts | Iceberg, Delta, and Hudi require their own snapshot, schema, delete, log, and publication semantics beyond reading the underlying files. |
| Transport integration contracts | S3, ADLS/Azure Blob, GCS, and HTTP own authentication, selected-range access, retries, and publication while reusing file adapters and native computation. |

The [I/O contract](docs/architecture/native-universal-io-performance-coverage-2026-10-24.md)
also defines shared demand across adapters, mixed-format queries, explicit one-computation
multi-output requests, format-aware writer layout, and independent readback. Individual
adapter capabilities determine which combinations execute and which require diagnostics.

Platform integration follows the same separation. The optional
[Foundry integration contract](docs/architecture/native-foundry-integration-2026-10-25.md)
places dataset access, managed Iceberg, incremental changes, and safe output publication
around ordinary ShardLoom SQL/DataFrame computation. Platform hosting does not change the
engine that performs the query.

> **ShardLoom design:** separate format, dataset, table, transport, and platform contracts around
> one native execution engine.

### Rich data and application workflows: extend through declared contracts

The extension architecture gives richer workloads explicit places in the same data lifecycle:

- **Application batch functions** declare input/output schemas, NULL and determinism rules,
  resource ownership, cancellation, and external effects at the application-code boundary.
- **Rich-data lifecycle contracts** cover nested values, binary payloads, vectors, embeddings,
  and tensors through retain, select/filter/join, stream/spill, write, reopen, and export.
  Managed external binary objects add their own ownership and lazy-access requirements.
- **Model-data interfaces** define typed batches, streaming/indexed loaders, sample order,
  duplicates, epoch identity, and worker partitioning for consumers such as PyTorch and Hugging Face.
- **Feature and retrieval contracts** cover persisted fitted state, point-in-time features,
  exact filtered vector search, and lexical/hybrid retrieval with explicit quality semantics.
- **Model and API calls** belong at declared effectful boundaries, with permissions and result
  provenance visible to the surrounding native workflow.

These are [extension](docs/rfcs/0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md),
[rich-data](docs/architecture/native-rich-data-lifecycle-2026-10-24.md), and
[AI-data](docs/architecture/native-ai-data-workflows-2026-10-24.md) contracts; a type declaration
or available upstream provider alone does not grant executable support.

Post-1.0.0 development source admits the scoped
[application batch-function API](docs/reference/application-batch-functions.md)
for trusted, deterministic, row-preserving Rust/Python code inside native plans.
It validates declared schemas and output ownership, exposes conversions, and
preserves cancellation and action failures. Plugin loading, sandboxed UDFs and
portable callback execution through the stock CLI remain separate contracts.

> **Design principle:** keep native computation composable while making application code,
> model execution, and external effects explicit.

### Results and writers: preserve useful native structure

A result can remain executable columnar data. Supported results retain native arrays,
logical types, validity, and ownership for further computation, batch delivery, or writing,
avoiding reconstruction through Python rows or JSON when the consumer can use native data.

Vortex is the highest-fidelity persistence target. Local compatibility writers cover
**Parquet, Arrow IPC, Avro, ORC, CSV, JSON, and JSONL/NDJSON** within their schema and
representation contracts. Output statistics describe actual result values; format-specific
type and metadata limits remain explicit. Large outputs use bounded batch delivery and
writer-owned state under the applicable resource contract.

- **Zero-decode:** an operation consumes an encoded representation.
- **Zero-copy:** a boundary transfers or shares buffers without copying them.

A route may provide either, both, or neither. Execution evidence makes these properties
and their boundaries inspectable.

> **Foundation:** columnar interoperability, extended with ShardLoom's ownership and completion contracts.

### Execution evidence: explain the work to people and software

Structured evidence connects source admission, preparation, execution, and output. Reports
identify available observations of providers, reads, pruning, representation changes,
materialization, resource decisions, and completion checks. Planned capabilities remain
distinct from performed work.

Execution certificates connect those observations to the source, operation, and output.
Deterministic diagnostics and machine-readable reports give developers and agents a stable
way to inspect behavior, including `fallback_attempted=false` and `external_engine_invoked=false`.

Correctness checks and reproducible benchmarks establish their own evidence. An available
optimization alone does not establish a performance improvement; the
[benchmark methodology](https://shardloom.io/field-guide/benchmark-methodology) defines
the comparison boundary.

> **ShardLoom design:** connect execution decisions and their evidence throughout the data lifecycle.

## Architecture

**Input formats → ShardLoom engine → Output formats**

ShardLoom uses Vortex's columnar representations, encodings, and file-format
capabilities within its execution and storage architecture. Its planning, execution,
resource management, and user interfaces form one ShardLoom system. Vortex is the
highest-fidelity persistence target; other formats have explicit type and metadata
limits. A Vortex file written by ShardLoom remains a Vortex file, with any additional
metadata or required extensions documented separately.

The architecture guide explains [work scheduling — PulseWeave — and bounded execution
tasks](https://shardloom.io/field-guide/compute-flow/#where-the-differentiators-apply),
including which controls are active and which are only reported as available.

## Autonomous development and shared reuse

The autonomous development framework used to build ShardLoom was translated into the
sister project [Noetloom](https://github.com/depsilon/noetloom) for shared reuse across
projects. Noetloom carries forward project-specific guidance, phased implementation,
verification, repair, and continuity across sessions as a portable development method.

## Explore the Field Guide

| Start with | What you will find |
| --- | --- |
| [Install and run](https://shardloom.io/field-guide/start-local-proof/) | Packages, source setup, and your first local query |
| [Python and DataFrames](https://shardloom.io/field-guide/python-surface/) | Queries, preparation reuse, batch input, and results |
| [SQL](https://shardloom.io/field-guide/sql/) | Supported queries and SQL through Python or the CLI |
| [Reading and writing data](https://shardloom.io/field-guide/runtime-and-io/) | Supported operators, formats, and writers |
| [How ShardLoom executes queries](https://shardloom.io/field-guide/execution-model/) | How the engine avoids work and carries data to its next use |
| [Query walkthroughs](https://shardloom.io/field-guide/compute-flow/) | The architecture from source to result |
| [Performance and benchmarks](https://shardloom.io/field-guide/benchmark-methodology/) | Workload evidence and measurement boundaries |
| [Support and limitations](https://shardloom.io/field-guide/limitations/) | Current coverage and remaining work |

For repository work, see [contributing](CONTRIBUTING.md), [agent and development instructions](AGENTS.md),
the [phase plan](docs/architecture/phased-execution-plan.md),
the [public support matrix](docs/release/public-status-matrix.md), and the
[API index](docs/reference/shardloom-user-surface-index.md) ([machine-readable](docs/reference/shardloom-user-surface-index.json)).

## License

ShardLoom is licensed under [Apache-2.0](LICENSE). PulseWeave, capillary work units, dynamic
work shaping, and related route/evidence/certificate machinery include patent-pending designs.
This notice does not expand the project's support claims.

ShardLoom is independent of, and not endorsed by, Vortex.
