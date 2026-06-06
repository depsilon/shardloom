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

## No-Fallback Posture

- The action downloads GitHub Actions artifacts produced by earlier CI jobs.
- It is not an execution engine and cannot become a Spark, DataFusion, DuckDB, Polars, Velox, or
  Vortex query-engine fallback.
- It introduces no fallback execution path.
- It does not alter ShardLoom runtime dependencies, benchmark rows, or package publication policy.
