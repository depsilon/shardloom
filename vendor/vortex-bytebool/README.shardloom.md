# ShardLoom local vortex-bytebool patch

This is the existing Apache-2.0 vortex-bytebool 0.93.0 provider, preserved from its
checksum-verified registry archive. `upstream-provenance.json` records every
original member and the published source commit and dirty marker. The crate
archive omits the workspace license; `LICENSE` preserves the verified Vortex
license from the same source commit. Local publication is disabled.

## Ordered optional array decoding

The RFC 0044 logical-array decode amendment replaces recursive child requests
in this provider with the pinned array provider's optional deserialization
continuations. The existing synchronous entrypoint uses an immediate-child
adapter to run the same body. Each callback, metadata check, conditional child,
legacy wire transformation and error stays in its original order. No callback
is replayed and no child is decoded before it is requested.

This changes no dependency version, wire format, encoding registration or
execution feature. Existing rejecting VTable entrypoints remain rejecting when
a dedicated plugin owns their wire format. Existing unsafe constructor operations
are unchanged; no new unsafe operation or lifetime extension is introduced.

Provider tests and strict lint use the reviewed standalone recipe and committed
lockfile selected by `scripts/ci_provider_tests.py`. Native complete-value tests
exercise the optional decoder through the ordinary registered codec set.
These checks do not grant deeper public admission: native memory ownership,
cancellation and complete-workflow acceptance remain required separately.
