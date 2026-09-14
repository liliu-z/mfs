# Boundary fixes and acceptance: 2026-09-14

This is the implementation record for discussion items 1, 2, 3, 4, 6, and 7 from [the preceding audit](review-2026-09-13-boundary-audit.md). Item 8, independent metadata/text startup when the backend cannot open, remained deferred; other host integration work was outside this implementation. [Post-fix JSONL](review-2026-09-14-boundary-audit-results.jsonl) preserves the repeated original probes. The [design](design.md) defines the current contract.

## Resulting behavior

| Concern | Implemented result |
| --- | --- |
| Exhausted cleanup debt survived collection drop and polluted recreation | Wait for actual old creation/query/file execution, drop the collection, and settle exact incarnation/generation debt in the completion transaction. Reopen also settles older successful drops; new same-name file waits/status do not inherit old debt. |
| Interrupted first initialization could not reopen | Persist `.mfs-initializing` before locks/catalog and remove it after schema commit. Recover recognized pre-lock/catalog/schema exceptions or SIGKILL while preserving unknown files/schemas. |
| Slow maintenance affected every namespace | Two bounded maintenance threads fairly schedule namespaces, one owner per namespace and a minimum pass interval. Per-collection backend mutexes allow independent collections to overlap while retaining same-collection read/write exclusion. |
| Management timeout confused caller return with retirement | Supervise asynchronous create/load/retirement/snapshot cleanup with `stage_timeout`; report failure and reject late publication while retaining actual ownership until return. Drop/close still wait for real users. |
| Alias sync reports completed too early | Canonical `path` plus out-of-scope alias `wait_paths`; unchanged observations wait too, and root redirection reports the full namespace. |
| One unhealthy member blocked new capability | After current members reach succeeded/failed/blocked/cancelled and execution retires, atomically publish successful members. Preserve errors/cancellation for repair, including after model replacement/reopen. Protect prior publication when a nonempty candidate has zero successes, except when disabling indexing. |
| Pending did not explain admission | `DocumentStatus.blocking_reason` identifies binding, pauses, resources, retirement, quiescence, configuration, index availability, and retry backoff. Missing-binding waits fail as blocked and recover after binding. |

Partial publication does not imply namespace readiness. Eventual retrieval can use successful members; dependent wait/strong still report unhealthy members. Configuration changes, reopen, and retrying another file do not clear user cancellation.

## Backend evidence and concurrency checks

Pinned dependencies were pymilvus 3.0.1 and Milvus Lite 3.2.1. Inspection of installed `milvus_lite/adapter/grpc/server.py`, `servicer.py`, and `db.py` confirmed threaded requests, a per-collection single-writer requirement, and independent collection storage. MFS retained collection mutexes and actual execution/query leases.

Lite handlers did not stop on gRPC cancellation. A wire timeout could return while the server still wrote, so it could not justify releasing the collection lock or permitting drop. Outer MFS deadlines ended caller wait or revoked commit authority; actual backend invocation remained owned until the handler returned. A real-server regression blocked a's `create_collection`, verified a's timeout retained ownership, allowed b's search, and required a's drop to wait for actual return.

Another real-backend regression used four collections and eight writers with interleaved insert/flush/search, checking single-writer safety, independent-collection concurrency, and complete data after reopen. The existing BM25 segment scoring xfail remained.

A maintenance no-progress regression originally measured 853 passes in 0.2 seconds. Per-namespace spacing and notifications only on real deadline expiry removed the mutual-wakeup spin. Candidate claim checks waited to adopt newly prepared compatible G0 text instead of processing it again, preserving one checkpoint resume. Runtime binding status no longer depended on worker startup timing; resource-wait diagnostics retired with their targets.

Tests kept their original behavioral purpose while adopting new semantics: missing binding immediately reports blocked; fault injection follows maintenance thread names/namespace parameters; GC eventual collection is checked under a finite retry budget because a busy return is valid.

## Recorded verification

The completed implementation ran:

```sh
.venv/bin/python -m pytest -q -W error::pytest.PytestUnhandledThreadExceptionWarning --tb=short --show-capture=no
```

Result: **241 passed, 3 skipped, 1 xfailed in 620.24 seconds**. Skips required native Windows handles. The xfail was existing Milvus BM25 segment scoring. Seven PDF/SWIG deprecation warnings occurred, without unhandled background-thread exceptions.

Twenty-three new regressions included three real SIGKILL windows, two bootstrap exception windows, preservation of unrelated files/schemas, aliases/root redirection, old write/drop/recreation interleavings, older successful-drop debt settlement, failed/cancelled partial-model-change retry after reopen, actual backend concurrency/deadlines, maintenance pass frequency, and bindings before worker startup. Existing checkpoint tests constrained candidate reuse.

Ruff lint/format, strict Pyright, `git diff --check`, and local Markdown link checks passed. Original probes for alias/binding waits, initialization faults/SIGKILL, cleanup/drop/recreation, and first dense enablement with a failed member were rerun into the linked JSONL. This implementation did not modify StashBase or call cloud models. These are historical implementation results; the separate documentation-cleanup rerun is in [its audit](review-2026-09-14-documentation.md).
