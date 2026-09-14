# Design decisions

The current contract is in [design.md](design.md); terms are in [CONTEXT.md](../CONTEXT.md). This log records accepted choices and reasons. The [proposal](proposals/concurrent-lifecycle.md) and dated reviews retain earlier alternatives; superseded choices are explicitly identified here.

## Core choices

| Decision | Rationale |
| --- | --- |
| Keep one MFS instance with namespace-owned business configuration | Share database/thread lifetime while allowing independent models, rules, and indexing policies. |
| Share the SQLite catalog; isolate Milvus collections by namespace and configuration generation | Metadata has one shape, but namespaces can use different embedding dimensions/spaces. |
| Borrow External originals; own Internal originals | Ownership determines copying, deletion, and recovery responsibility. |
| Create derived text only when conversion requires it | Direct text needs no copy; applications may already own usable extraction files. |
| Revoke old source eligibility on acceptance; clean physically in the background | A replacement PDF that fails processing must not expose the old PDF as current. |
| Keep only the latest desired file target and a durable active run | Coalesce repeated updates without losing actual execution identity or precise cleanup responsibility. |
| Separate grep/read from ranked search; remove public query | Text/path matching, known-document reading, and BM25/vector ranking have different contracts. |
| Require one explicit namespace for grep/search | The library does not define routing, authorization, deduplication, or ranking across independently configured collections. |
| Allow equal or nested External roots across namespaces | Stable host Folder identities can overlap physically while remaining independent; only state/source overlap is rejected. |
| Use current file/scope waits instead of permanent operation history | Hosts need latest work state; unchanged sync should not accumulate receipts. Explicit idempotency still deduplicates acceptance. |
| Keep existing status interfaces instead of adding another readiness system | Hosts can inspect document/configuration state and choose fallback without duplicating the lifecycle model. |
| Leave playback, source operations, product visibility, and permissions with the host | MFS supplies file search, not application policy or a filesystem transaction manager. |

## Execution, recovery, and queries

| Decision | Rationale |
| --- | --- |
| Lifecycle owns state transactions; execution modules return typed results | Central revision/attempt checks prevent late work from overwriting newer intent. |
| Same-content observation refreshes stat; callbacks merge current target state | Avoid repeated hashing after mtime changes and prevent old execution copies from erasing new observations. |
| Default queries to eventual with a five-second total caller budget | Interactive callers can use valid current results and bound queueing/execution wait. |
| Checkpoint yield requires durable resume data and actual invocation retirement | Cancellation, replacement, and priority changes cannot authorize overlapping same-file work. |
| Query leases span embedding through backend exit | A caller timeout is not evidence that a collection can be destroyed. |
| Cache complete validated vectors separately from search rows, under a bounded LRU | Renames can reuse calculations while old document visibility is revoked and physical rows are cleaned. |
| Reconcile uncertain commits against durable state | Memory authorization must follow SQLite; stop publication when the outcome cannot be established. |
| Validate borrowed source identity around checkpoints and results | Zero-copy must not populate old content caches with changed input; it still does not provide an atomic external snapshot. |
| Keep a POSIX supervisor holding process ownership through descendant retirement | Hard host termination must not let reopened work overlap old native execution and source users. |
| Separate temporary ScopeLease from persistent user cancellation | Host file operations need retirement without changing user intent. |
| Persist processing pause before importing legacy cancellation/failure | Restart must not start expensive preparation before migration finishes; import replay must not undo later explicit retry. |

## Concurrent lifecycle decisions: 2026-09-13

- Preserve `active_run_id`, `stage/state`, `attempts`, and `attempt_token`; reducing fields must not weaken identity checks.
- Use a bounded file-worker pool with atomic Processor/Chunker resource admission. Grep chunking shares that adapter's admission. Embedder calls use worker/query capacities only; implementations own thread safety and service limits.
- Accept configuration changes together and build a private candidate while the active generation serves valid sources. The original all-members-success promotion rule was superseded by the September 14 partial-publication decision below.
- Separate source input identity from configuration generation. A captured old-generation query may finish after a same-input configuration change, while source deletion is filtered immediately.
- Keep physical cleanup as exact durable debt. Strong readiness does not wait for already deleted sources' physical cleanup; current-work waits do.
- Install a startup gate before execution/GC so the host can recover its own disk journal. The gate does not replace that journal.
- Give grep and ranked search separate bounded pools and total deadlines. Per-file read failures become explicit partial grep results.
- Separate candidate declaration from bounded member reconciliation. Repeated maintenance recovers missing members and lost commit acknowledgements; the same configuration request can retry maintenance failure.
- Preserve snapshot identity until visibility filtering and report normal SQLite GC contention as busy.

## Boundary decisions: 2026-09-14

- Persist bootstrap ownership before lock/catalog creation, remove it after schema commit, and recover only recognized owned initialization state.
- Settle collection deletion and its exact cleanup debt together; file waits remain isolated by namespace incarnation.
- Use two maintenance owners, one per namespace at a time, and serialize backend calls per collection. This retains the pinned backend's single-writer requirement while allowing independent collections to overlap.
- Do not infer Milvus handler retirement from an RPC timeout. The pinned Lite handler ignores cancellation; outer query/maintenance deadlines end waits or revoke commit authority while the real call retains ownership.
- Preserve canonical sync scope and additional alias targets in reports; do not reintroduce historical wait receipts.
- Publish successful candidate members atomically after all current members are terminal and execution retires. Retain failure/cancellation and permit explicit repair in the new generation. If a nonempty candidate has zero successes and the active generation has publications, keep the active generation unless disabling indexing. Never mix embedding spaces. This replaces the earlier all-members-success rule and its limited no-text cancellation exception.
- Explain admission with computed `blocking_reason`, including actionable missing-binding failures, without another task/readiness state machine.

Rejected or superseded proposals include External stable copies, global model inheritance, per-stage independent queues, SQLite document bodies, single-worker-only execution, and mandatory host-issued resource grants. Historical operation receipt interfaces were discussed but not selected. Host admission sharing remains optional.
