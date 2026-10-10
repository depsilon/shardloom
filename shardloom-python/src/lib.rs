//! Optional in-process Python interface to existing `ShardLoom` native owners.
//! The lean workspace does not link Python. Vortex remains the execution and
//! persistence substrate; Arrow capsules are an explicit interchange boundary.

#[cfg(all(feature = "python-native", unix))]
mod binding;
