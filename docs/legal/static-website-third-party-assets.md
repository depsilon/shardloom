# Static Website Third-Party Assets

ShardLoom's project code remains Apache-2.0. The static website also commits a generated Pagefind
search bundle under `website/pagefind/` so `shardloom.io` can search Field Guide, use-case, status,
telemetry, compute-flow, and rendered documentation pages without a runtime search service.

## Pagefind

- Package: `pagefind_bin_extended`
- Version used for the committed bundle: `1.5.2`
- License reported by local package metadata: MIT
- Scope: generated static website search assets only
- Runtime boundary: no ShardLoom runtime code, no benchmark execution, no fallback engine, and no
  external search service

Pagefind is an independent static-search project. Its generated website bundle is served as a
first-party static asset from `website/pagefind/`; it is not ShardLoom execution logic.

## MIT License Text

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT
OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
