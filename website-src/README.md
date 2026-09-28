# ShardLoom Website Source

This directory contains the Astro/Starlight source for `shardloom.io`.

The public website uses a parallax ShardLoom homepage for `/` and claim-safe Starlight documentation for the rest of the public surface. It is an interpretation layer for the current repository evidence, not a replacement for the canonical architecture, release, benchmark, and phase-plan documents.

Build shape:

- `website-src/` is the source tree and website-only Node toolchain.
- `website-public/` contains static assets copied into the build.
- `website/` is the committed static output served by Cloudflare Workers Static Assets.

Public surface:

- `/`: parallax ShardLoom homepage experience from the productionized source-of-truth HTML.
- `/about`: shipped differentiators, technical-preview support, and evidence pointers.
- `/start`: package installation, a small CSV example, and a first local query.
- `/field-guide`: Starlight docs for installation, Python, runtime and I/O, benchmark methodology, limitations, and vocabulary.
- `/benchmarks`: ClickBench handoff and claim-safe public comparison posture.
- `/compute-engine-flow`: human-readable route translation.

Detailed RFCs, phase history, recipes, and source-of-truth docs remain in the repository under `docs/`.

Common commands:

```powershell
npm install
npm run build
npm run check
```

The build must not run ShardLoom benchmarks, fetch runtime GitHub/raw content, publish packages, or
expand support claims. The benchmark page links to ClickBench instead of rendering committed local
artifact rows as a public leaderboard. `npm run sync-content` copies canonical compute-flow content
into Astro import data before each build, and it keeps repository use-case records under
`docs/use-cases/generated/` for source-of-truth evidence instead of publishing a generated use-case
browser.

Edit Field Guide content in `scripts/sync-content.mjs` or `src/data/field-guide.json`; its MDX files
are generated. Each `.html` compatibility page must be byte-identical to its canonical directory
page after the build. Old bookmarks must never select separate page content. Validate both
`website/validate_static_assets.js` and `scripts/check_website_readiness.py` after regeneration.
