# GitHub Actions Dependency Review

## Purpose

This document records CI action-version dependency posture for ShardLoom release validation. It
does not authorize package publication, benchmark publication, secrets usage, or fallback
execution.

## Artifact Download Action 8

- Source: Dependabot PR <https://github.com/depsilon/shardloom/pull/1149>.
- Updated action: `actions/download-artifact@v8`.
- Previous action: `actions/download-artifact@v7`.
- Scope: `.github/workflows/ci.yml` evidence-reuse steps only.
- Release-note boundary: v8 is an ESM action and its digest-mismatch default is error.
- ShardLoom does not override the secure digest behavior in this update.

## CI And Security Action Pin Refresh

- Source: Dependabot PRs <https://github.com/depsilon/shardloom/pull/1397>,
  <https://github.com/depsilon/shardloom/pull/1398>,
  <https://github.com/depsilon/shardloom/pull/1399>,
  <https://github.com/depsilon/shardloom/pull/1400>, and
  <https://github.com/depsilon/shardloom/pull/1401> were closed and absorbed into one cohesive
  dependency-intake PR rather than merged as individual slivers. Follow-up Dependabot PR
  <https://github.com/depsilon/shardloom/pull/1407> is absorbed into the current dependency hygiene
  pass for the same action family.
- Updated action pins:
  - `github/codeql-action/init@v4` pinned to commit
    `cdf488f595d80d6e07e03d4674febd5ab45fa938`.
  - `github/codeql-action/analyze@v4` pinned to commit
    `cdf488f595d80d6e07e03d4674febd5ab45fa938`.
  - `github/codeql-action/upload-sarif@v4` pinned to commit
    `cdf488f595d80d6e07e03d4674febd5ab45fa938`.
  - `ossf/scorecard-action@v2.4.4` pinned to commit
    `2d1146689b8cda280b9bc96326124645441f03bc`.
  - `actions/setup-python@v7.0.0` pinned to commit
    `5fda3b95a4ea91299a34e894583c3862153e4b97`.
  - `actions/checkout@v7.0.1` pinned to commit
    `3d3c42e5aac5ba805825da76410c181273ba90b1`.
  - `actions/setup-node@v7.0.0` pinned to commit
    `820762786026740c76f36085b0efc47a31fe5020`.
  - `Swatinem/rust-cache@v2.9.2` pinned to commit
    `f0d9c3887740aee45f6153b24b3a6b815192ec16`.
  - `pypa/gh-action-pypi-publish@v1.14.2` pinned to commit
    `dc37677b2e1c63e2034f94d8a5b11f265b73ba33`.
- Scope: CI setup, CodeQL analysis, OpenSSF Scorecard, SARIF upload, and PyPI draft workflow setup
  only.
- Dependabot grouping now batches GitHub Actions updates as `ci-security-actions` so action pin
  changes and validator marker updates stay in one reviewable PR.

## No-Fallback Posture

- The action downloads GitHub Actions artifacts produced by earlier CI jobs.
- It is not an execution engine and cannot become a Spark, DataFusion, DuckDB, Polars, Velox, or
  Vortex query-engine fallback.
- It introduces no fallback execution path.
- It does not alter ShardLoom runtime dependencies, benchmark rows, or package publication policy.
