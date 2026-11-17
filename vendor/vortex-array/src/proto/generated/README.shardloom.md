# Root protobuf trait overrides

The generated wire schema, field types, oneofs and Message derives are unchanged.
The `DType` and `ScalarValue` root structs alone omit derived `Clone` and
`PartialEq`, and add `#[prost(skip_debug)]`. Their handwritten implementations
live in `../structural.rs`; intrinsic cleanup lives in `../owned_release.rs`.
Every recursive generated model cycle crosses one of those two roots.

This vendored subset does not include the upstream xtask. When regenerating in
the complete upstream source with `cargo run -p xtask -- generate-proto`, reapply
exactly those two derive changes and attributes after copying the generated
files here. Do not remove the Message or oneof attributes or change model fields.
Review any added variants against the exhaustive traversal matches. Run the
provider structural reference tests, complete provider tests/lint, native
workspace gates and provenance/license checks before accepting regenerated code.

The immutable `../reference/` files are original generated source from ShardLoom
commit `59ffad352`. They are test-only compatibility oracles and must not be
rewritten to match the new implementation. Their source identity is checked in
the source-reference evidence. The pinned Prost derive retains default decode behavior.
