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
  dependency-intake PR rather than merged as individual slivers.
- Updated action pins:
  - `github/codeql-action/init@v4` pinned to commit
    `e4fba868fa4b1b91e1fdab776edc8cfbe6e9fb81`.
  - `github/codeql-action/analyze@v4` pinned to commit
    `e4fba868fa4b1b91e1fdab776edc8cfbe6e9fb81`.
  - `github/codeql-action/upload-sarif@v4` pinned to commit
    `e4fba868fa4b1b91e1fdab776edc8cfbe6e9fb81`.
  - `ossf/scorecard-action@v2.4.4` pinned to commit
    `2d1146689b8cda280b9bc96326124645441f03bc`.
  - `actions/setup-python@v7.0.0` pinned to commit
    `5fda3b95a4ea91299a34e894583c3862153e4b97`.
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
