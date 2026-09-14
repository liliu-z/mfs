# Documentation guide

The current contract lives in [design.md](design.md), with callable details in the [interface reference](reference.md). Historical reports describe the checkout and experiments at their recorded dates; their findings, line numbers, host observations, and test counts are not assertions about the present checkout.

## Current documentation

| Document | Read it for |
| --- | --- |
| [Project README](../README.md) | Installation and runnable first examples |
| [Design](design.md) | Ownership, state transitions, visibility, execution, and recovery |
| [Interface reference](reference.md) | Method defaults, filters, adapter requirements, and errors |
| [Domain language](../CONTEXT.md) | Shared terminology |
| [Decision log](decision-log.md) | Accepted choices and their rationale |
| [Backlog](backlog.md) | Open work and dated verification records |
| [StashBase integration](stashbase-integration.md) | Proposed host mapping and application acceptance requirements |

## Historical reviews and proposals

| Record | Scope and disposition |
| --- | --- |
| [September 12 review](review-2026-09-12.md) | Recovery, source validation, process supervision, and host seams; library fixes completed |
| [September 12 follow-up](review-2026-09-12-followup.md) | Design and host integration discussion; later concurrency work supersedes the single-worker model |
| [Concurrent lifecycle proposal](proposals/concurrent-lifecycle.md) | Implemented architecture proposal; its all-members-success publication policy was superseded by partial publication |
| [September 13 review](review-2026-09-13.md) | Search, configuration, source reads, and concurrency findings; library fixes completed |
| [September 13 independent review](review-2026-09-13-independent.md) | Four independently reproduced defects and their verification; all four closed |
| [September 13 liveness review](review-2026-09-13-liveness.md) | Transaction recovery, rules, claims, configuration, and backend scoring; library fixes completed except the backend limitation |
| [September 13 boundary audit](review-2026-09-13-boundary-audit.md) | Cleanup, bootstrap, alias waits, maintenance, partial publication, and host gaps |
| [September 14 boundary fixes](review-2026-09-14-boundary-fixes.md) | Implemented fixes and validation for the preceding audit |
| [September 14 documentation audit](review-2026-09-14-documentation.md) | Corrections and verification of this documentation cleanup |

Reproduction scripts and JSONL outputs remain beside their reports. They contain synthetic experimental observations, not live application data. Some older reports also name temporary scripts that were not retained in this repository; those paths are historical provenance, not runnable checkout assets. Use `uv run pytest -q` for the maintained regression suite.
