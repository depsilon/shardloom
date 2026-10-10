# ShardLoom

[![CI](https://github.com/depsilon/shardloom/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/depsilon/shardloom/actions/workflows/ci.yml?query=branch%3Amain)
[![Release](https://img.shields.io/github/v/release/depsilon/shardloom?include_prereleases&label=release)](https://github.com/depsilon/shardloom/releases)
[![PyPI](https://img.shields.io/pypi/v/shardloom?label=PyPI)](https://pypi.org/project/shardloom/)
[![Python client versions](https://img.shields.io/pypi/pyversions/shardloom?label=Python%20client)](https://shardloom.io/field-guide/start-local-proof/)
[![Homebrew](https://img.shields.io/badge/Homebrew-depsilon%2Ftap%2Fshardloom-2f4f4f)](https://github.com/depsilon/homebrew-tap)
[![Website](https://img.shields.io/badge/website-shardloom.io-0f766e)](https://shardloom.io/)
[![Field Guide](https://img.shields.io/badge/docs-field_guide-2563eb)](https://shardloom.io/field-guide/)
[![Runtime](https://img.shields.io/badge/runtime-Vortex--native-0f766e)](#what-makes-shardloom-different)
[![No Fallback](https://img.shields.io/badge/policy-no%20external%20fallback-991b1b)](#what-makes-shardloom-different)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Patent Pending](https://img.shields.io/badge/patent--pending-designs-7c3aed)](#license)

**Vortex-native compute. Make less work.**

ShardLoom is an encoded-columnar compute engine for reading, transforming, querying,
and writing data through one native pipeline. Python, SQL, DataFrame-style calls, and
the CLI provide familiar interfaces; Vortex supplies the native data representation.
Today's published engine supports local analytical workflows, with broader data-processing
capabilities under active development.

**[Explore the website](https://shardloom.io/) · [Read the field guide](https://shardloom.io/field-guide/) · [Install and run](https://shardloom.io/field-guide/start-local-proof/)**

## What Makes ShardLoom Different

- **[Metadata-first work avoidance](https://shardloom.io/field-guide/execution-model/#avoid-work-first).**
  Answer from metadata, prune segments, and defer decoding and payload materialization.
- **[Encoded aggregation](https://shardloom.io/field-guide/runtime-and-io/#native-execution).**
  Use dictionary codes and repetition counts, exact DISTINCT, and late measure evaluation
  on supported routes.
- **[One Vortex-native middle](https://shardloom.io/field-guide/execution-model/#one-native-pipeline).**
  User interfaces, native operators, and format adapters share one execution pipeline.
  Vortex remains the highest-fidelity input and persistence format.
- **[Reusable intelligence in the artifact](https://shardloom.io/field-guide/execution-routes/#prepare-once).**
  Keep layouts, statistics, and derived metadata in one `.vortex` artifact; validate source,
  schema, and artifact identities before reuse.
- **[Native result ownership](https://shardloom.io/field-guide/runtime-and-io/#preparation-and-result-handoffs).**
  Supported results retain Vortex arrays, validity, and memory credits through further
  execution or output, avoiding a row/JSON reconstruction roundtrip.
- **[PulseWeave](https://shardloom.io/field-guide/compute-flow/#where-the-differentiators-apply).**
  Connect work inventory, resource pressure, and run-local feedback to explicit policy
  decisions. Evidence distinguishes applied control from readiness-only reporting.
- **[Capillary work units](https://shardloom.io/field-guide/compute-flow/#where-the-differentiators-apply).**
  Represent admitted work as bounded, typed units with source ranges, ownership, and
  execution evidence.
- **[Native execution under pressure](https://shardloom.io/field-guide/runtime-and-io/#resources-and-recovery).**
  Supported stateful operators share memory admission and explicit native spill policies,
  with quota, cancellation, and owned-cleanup contracts.
- **[Inspectable execution certificates](https://shardloom.io/field-guide/execution-model/#execution-evidence).**
  Connect source identity, execution, materialization, and output evidence for developers
  and agents. Correctness and performance claims retain separate proof requirements.
- **[No hidden execution fallback](https://shardloom.io/field-guide/execution-model/#no-fallback).**
  Unsupported requests fail with deterministic diagnostics. ShardLoom never delegates
  them to Spark, DataFusion, DuckDB, Polars, or another query engine.

## Explore the Field Guide

| Start with | What you will find |
| --- | --- |
| [Install and run](https://shardloom.io/field-guide/start-local-proof/) | Packages, source setup, and your first local query |
| [Python](https://shardloom.io/field-guide/python-surface/) | Queries, preparation reuse, batch input, and result delivery |
| [Execution model](https://shardloom.io/field-guide/execution-model/) | How the engine avoids work and preserves native data |
| [Compute flow](https://shardloom.io/field-guide/compute-flow/) | The architecture from source to result |
| [Runtime and I/O](https://shardloom.io/field-guide/runtime-and-io/) | Supported operators, formats, writers, and resource contracts |
| [Benchmarks](https://shardloom.io/field-guide/benchmark-methodology/) | Workload evidence and timing boundaries |
| [Support and limitations](https://shardloom.io/field-guide/limitations/) | Current coverage and remaining work |

## Current Support Posture

**Published local engine; operational hardening in progress.** Package availability,
supported workflows, and newer source changes are tracked in the
[public support matrix](docs/release/public-status-matrix.md). Production support, broad
SQL/DataFrame parity, and performance superiority are not claimed.

For repository work, see [contributing](CONTRIBUTING.md), [agent and development instructions](AGENTS.md),
the [phase plan](docs/architecture/phased-execution-plan.md), and the
[API index](docs/reference/shardloom-user-surface-index.md) ([machine-readable](docs/reference/shardloom-user-surface-index.json)).

## License

ShardLoom is licensed under [Apache-2.0](LICENSE). PulseWeave, capillary work units, dynamic
work shaping, and related route/evidence/certificate machinery include patent-pending designs.
This notice does not expand the project's support claims.

ShardLoom is independent of, and not endorsed by, Vortex.
