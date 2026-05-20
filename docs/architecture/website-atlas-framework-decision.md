# Website Atlas Framework Decision

Status: superseded historical decision.

This file previously recorded the `GAR-WEB-ATLAS-1H` decision for the old generated atlas and
Pagefind-backed public website. That site shape was later retired by the minimal public surface
reset and then replaced by `GAR-WEB-REDESIGN-2`.

Current framework guidance lives in:

- `docs/architecture/website-redesign-framework-decision.md`

Historical takeaway retained from the old decision:

- Astro and Starlight were plausible future options.
- The current generator was preferred until a concrete migration need and explicit approval existed.
- Static deployment, committed artifacts, validator parity, claim safety, and no runtime external
  fetches were non-negotiable.

Do not use this historical document to justify Pagefind, Astro, Starlight, or another website
framework dependency in the current public site.
