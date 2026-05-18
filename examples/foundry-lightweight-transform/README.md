<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Lightweight Transform Example

This is a local Foundry-style smoke example. It shows the shape of a future
Foundry Python code repository transform without importing Foundry packages,
calling Foundry services, or treating Foundry compute as ShardLoom execution.

Run from a source checkout after building the local CLI:

```powershell
cargo build -p shardloom-cli --bin shardloom
python examples\foundry-lightweight-transform\run.py --repo-root .
```

The script resolves the local ShardLoom CLI, runs no-dataset smoke and
capability checks, records an explicit staged input path, and writes a local
certificate-style JSON file under `target/`.

For the fuller local dev-stack starter workflow, see
`docs/foundry/dev-stack-starter-kit.md`.

Files in this example:

- `environment.yml`: minimal future transform environment shape.
- `fixtures/staged_input.csv`: small staged local input fixture.
- `expected-output.json`: expected output fields from the local dry run.
- `expected-certificate-fields.json`: expected certificate/policy fields.
- `known-limitations.md`: current boundaries and non-goals.

Foundry Spark, Snowflake, Databricks, BigQuery, virtual tables, and external
compute remain external boundaries or baselines, not ShardLoom-native execution.
Foundry output APIs, result datasets, evidence datasets, direct S3/object-store writes, and
Marketplace/package availability are not claimed by this local example.
