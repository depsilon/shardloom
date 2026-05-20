# Static Website Third-Party Assets

Status: current public website has no committed third-party runtime asset bundle.

The previous generated atlas website committed a Pagefind static-search bundle under
`website/pagefind/`. That bundle was retired during the public website reset and is not part of the
current `GAR-WEB-REDESIGN-2` website.

Current posture:

- `website/pagefind/` must not exist.
- `scripts/check_website_readiness.py` and `website/validate_static_assets.js` reject Pagefind runtime references.
- The current public site uses committed first-party static HTML, CSS, JavaScript, logo assets,
  benchmark artifacts, and generated data snapshots.
- External GitHub links may appear as normal anchor links to source files, but runtime `raw.githubusercontent.com` content fetches remain forbidden.

If Pagefind, Astro, Starlight, or another third-party website dependency is reintroduced later, the
phase plan must include a new dependency/license review item and this document must be updated with
the package name, version, license, scope, served artifact paths, and non-runtime boundary.
