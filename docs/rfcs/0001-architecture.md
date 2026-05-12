# RFC 0001: ShardLoom Architecture

## Summary

ShardLoom is a standalone encoded-columnar distributed execution engine designed to compute directly
over Vortex-native layouts, produce Vortex-native and lakehouse-compatible outputs, and eliminate
Spark dependency for massive object-store workloads.

## Goals

- Standalone execution with no Spark or DataFusion fallback
- Vortex-native input and output
- Encoded segment execution
- Late materialization
- Segment-level pruning
- Object-store-native planning
- Modular storage translation
- Reproducible performance benchmarks

## Non-goals

- ShardLoom is not a new file format.
- ShardLoom is not a Spark plugin.
- ShardLoom is not a DataFusion wrapper.
- ShardLoom is not initially a full lakehouse table format.
- ShardLoom is not initially a BI tool or application framework.

## Core architecture

```text
SQL / DataFrame / API
        ↓
Logical Planner
        ↓
Vortex-Native Optimizer
        ↓
Encoded Physical Plan
        ↓
Standalone Execution Runtime
        ↓
Columnar Translation Layer
        ↓
Vortex / Parquet / Arrow IPC / Lakehouse-Compatible Outputs
