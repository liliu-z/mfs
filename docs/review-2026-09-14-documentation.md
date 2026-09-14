# Documentation audit: 2026-09-14

This audit checked repository documentation against the current public interface, lifecycle/configuration/query implementation, regression tests, dependency metadata, and CI configuration. It did not modify StashBase or call cloud models. Historical review evidence was translated and consolidated, with its original date, baselines, defect mechanisms, verification outcomes, and limits retained.

## Findings and corrections

| Finding | Correction |
| --- | --- |
| Current contracts, superseded proposals, and per-round implementation notes were mixed together | Added a documentation guide; organized the design by invariant and lifecycle; labeled historical reviews/proposal; moved open work to the front of backlog |
| README was a long accumulation of changes without an installation path or clear initial workflow | Rewrote it in English with Python/dependency requirements, complete Internal/External examples, result shapes, and links to focused references |
| The create/reopen example checked whether any namespace existed | Check the intended namespace by name, so an unrelated namespace does not select a failing reopen branch |
| README said configuration failure/cancellation retained the old generation, conflicting with implemented partial publication | Specify terminal-member promotion, retained failure/cancellation, the nonempty zero-success safeguard, and why failed wait does not imply rollback |
| Older timeout text said Milvus received remaining RPC time, contradicting actual handler-retirement protection | Explain outer caller/watchdog deadlines and actual backend ownership; distinguish asynchronous maintenance deadlines from synchronous open/create/sync/read |
| Query prerequisites and return types were underspecified | Add an interface reference covering namespace scope, External-only UnderPath query filtering, AND/AnyOf behavior, selections, UTF-8 offsets, budgets, query defaults, and errors |
| Text references sounded like a streaming input replacement | Clarify that ProcessedDocument.text remains required and must match text_path, plus alternate grep/read views and borrowed-output lifetime |
| Strong queries, current-work wait, index pause, processing pause, cancellation, and quiescence were easy to conflate | Add explicit completion/control tables and host source-operation/recovery ordering |
| Integration text still described fully serial StashBase dispatch and swallowed search errors | Use dated host observations from the newer reports, preserve the remaining bind-barrier finding, and avoid claiming an external checkout was re-audited |
| Historical documents depended on links into a sibling StashBase checkout or stale source line fragments | Retain external source paths/old line numbers as historical provenance; use portable local documentation/source links |
| Chinese remained in documentation and Unicode test literals | Rewrite all documentation in English and use Unicode escapes in four test modules; preserve the same runtime strings and parsed test behavior |

The core design already had useful separation between source ownership, durable current targets, actual execution, publication, and cleanup. This pass clarified those contracts and corrected documentation drift; it introduced no production behavior change or new public interface.

## Verification

The full suite ran with unhandled background-thread warnings promoted to errors:

```sh
.venv/bin/python -m pytest -q -W error::pytest.PytestUnhandledThreadExceptionWarning --tb=short --show-capture=no
```

Result: **241 passed, 3 skipped, 1 xfailed in 613.65 seconds**. Native Windows tests skipped on this platform; the strict expected failure remains the pinned Milvus Lite BM25 flush/segment ranking issue. Seven PDF/SWIG deprecation warnings occurred, with no unhandled background-thread warnings.

Additional checks passed:

- Ruff lint and format checks for `src`/`tests`, strict Pyright (zero errors/warnings), and `git diff --check`.
- All 17 Markdown files: balanced fences, 120 local links/anchors, and syntax of all three Python example blocks.
- Actual README examples against real temporary SQLite/Milvus state: fresh Internal namespace, repeated reopen, an existing unrelated namespace, and External sync/grep.
- The reference rule example: an explicitly included descendant remains searchable beneath an excluded parent.
- Repository-wide Han-character scan: no matches outside ignored/generated state. Test literals use escapes preserving the original Unicode values.
- AST comparison of all four modified test modules against HEAD: identical parsed behavior after escaping and formatting.

## Remaining limits

Backend BM25 ranking, metadata/text-only startup on backend failure, directory-level strong queries, production-scale latency, and host migration/evaluation remain explicitly tracked in [backlog](backlog.md). Documentation consistency and library regressions do not establish real cloud-model quality, actual Windows/frozen packaging, arbitrary native-hang recovery, or StashBase end-to-end acceptance.
