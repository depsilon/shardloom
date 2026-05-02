# ShardLoom

ShardLoom is a standalone encoded-columnar execution engine designed to compute directly over Vortex-native layouts, produce Vortex and lakehouse-compatible outputs, and eliminate Spark dependency for massive object-store workloads.

## Mission

Compute less. Decode later. Weave at scale.

## Status

ShardLoom is in early design and implementation planning.

The initial focus is:

- Vortex-native input and output
- Encoded segment execution
- Late materialization
- Segment-level pruning
- Object-store-native planning
- Modular translation to Vortex, Parquet, Arrow IPC, and lakehouse-compatible outputs
- Standalone execution with no Spark or DataFusion fallback

## License

ShardLoom is licensed under the Apache License 2.0.
