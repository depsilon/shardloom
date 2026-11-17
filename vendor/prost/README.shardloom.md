# Prost checked decode continuations

This is the published Apache-2.0 **0.14.4** crate, with its license and source
identities recorded in `upstream-provenance.json`. It remains the existing native
Vortex codec dependency; the independent older Prost graphs are unchanged.

The local extension supplies explicit bounded `DecodeContext` construction,
read-only limit checks, checked child entry and `message::DecodeContinuation`.
Default decoding retains the original limit of 100. The optional upstream
`no-recursion-limit` feature retains an unlimited default, while explicitly
bounded contexts remain bounded even when that feature is unified downstream.
ShardLoom does not enable that feature. Primitive codecs and generated message
merge implementations remain unchanged.

Unknown-group skipping uses ordered continuations, preserving original key
consumption, end-tag checks, depth checks and errors. Non-group fields require
no continuation allocation. Delimited message continuations preserve wire,
depth and length check order and check overrun after complete field consumption.
Consumers must retain the same buffer and own admission for all model and
continuation allocation. These APIs add no unsafe operations or execution engine.

Strict standalone lint keeps one named `too_many_arguments` exception on the
unchanged upstream map codec callback function. This preserves its published
signature; the new continuation code and tests retain all strict checks.

The [RFC 0044 amendment](../../docs/rfcs/0044-resident-runtime-resource-ownership.md#protobuf-decode-continuation-amendment-november-17-2026)
defines the acceptance boundary. Provider tests compare the original group
helper and ordinary message merge, including malformed input and feature-unified
explicit limits. Native deep-data support remains a separate full-workflow and
resource obligation. Remove the patch when an approved upstream API provides
the same checked ordered continuation contract and passes those checks.
