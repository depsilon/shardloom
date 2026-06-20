# ShardLoom Website Source

This directory contains the Astro/Starlight source for `shardloom.io`.

The public website is light-mode and evidence-console oriented. It is an interpretation layer for the current repository evidence, not a replacement for the canonical architecture, release, benchmark, and phase-plan documents.

Build shape:

- `website-src/` is the source tree and website-only Node toolchain.
- `website-public/` contains static assets copied into the build.
- `website/` is the committed static output served by Cloudflare Workers Static Assets.

Public surface:

- `/`: route/evidence console overview.
- `/about`: concise claim-safe project overview and evidence pointers.
- `/start`: first local proof entry point.
- `/field-guide`: Starlight docs shell for local proof, Python route shape, benchmark methodology, limitations, and vocabulary.
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
