# ShardLoom Website Source

This directory contains the Astro/Starlight source for `shardloom.io`.

The public website uses a parallax ShardLoom homepage for `/`, Starlight documentation for `/field-guide`, and Astro pages for the supporting public routes. It is an interpretation layer for the current repository evidence, not a replacement for the canonical architecture, release, benchmark, and phase-plan documents.

Build shape:

- `website-src/` is the source tree and website-only Node toolchain.
- `website-public/` contains static assets copied into the build.
- `website/` is the committed static output served by Cloudflare Workers Static Assets.

Public surface:

- `/`: parallax ShardLoom homepage experience from the productionized source-of-truth HTML.
- `/about`: shipped differentiators, technical-preview support, and evidence pointers.
- `/start`: package installation, a small CSV example, and a first local query.
- `/field-guide`: Starlight docs for installation, Python, execution, runtime and I/O, benchmarks, and current support boundaries.
- `/field-guide/benchmark-methodology`: ClickBench handoff, current engineering evidence, and measurement boundaries. `/benchmarks` redirects here.
- `/field-guide/compute-flow`: interactive query-path illustrations. `/compute-engine-flow` redirects here.

Detailed RFCs, phase history, recipes, and source-of-truth docs remain in the repository under `docs/`.

Common commands:

```powershell
npm install
npm run build
npm run check
```

The build must not run ShardLoom benchmarks, fetch runtime GitHub/raw content, publish packages, or
expand support claims. The benchmark page links to ClickBench instead of rendering committed local
artifact rows as a public leaderboard. `npm run sync-content` copies the canonical compute-flow reference
into website assets, generates repository use-case records under `docs/use-cases/generated/` for
source-of-truth evidence instead of publishing a generated use-case browser, and refreshes redirects
from retired Field Guide vocabulary. `website-src/src/data/field-guide.json` remains vocabulary and
backlink metadata; it is not the owner of guide copy.

The public Field Guide pages are hand-authored MDX under
`website-src/src/content/docs/field-guide/`. Edit those files directly; `sync-content.mjs` does not
overwrite or prune guide pages. It removes only the specifically retired
`website-src/src/content/docs/docs.mdx` Starlight source, which would reintroduce the old `/docs`
route that the clean-slate IA redirects to `/field-guide/`. Each `.html` compatibility page must be
byte-identical to its canonical directory page after the build. Old bookmarks must never select
separate page content. From the repository root, validate with:

```bash
python -m unittest discover -s scripts -p test_website_links.py
python scripts/check_website_readiness.py
node scripts/validate_static_assets.cjs
```
