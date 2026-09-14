# Implementation status

The current contract is [design.md](design.md). This file tracks gaps and dated implementation/validation evidence. Historical counts below belong to their recorded checkouts, not a claim that those exact runs were repeated today. Review IDs remain stable for traceability.

## Open work and explicit limits

| ID | Status | Remaining work |
| --- | --- | --- |
| **BACKEND-005** | Open, upstream | The pinned Milvus Lite 3.2.1 BM25 implementation uses segment-local IDF/average document length, so flush grouping can change ranking. Keep the strict xfail in [test_backend_conformance.py](../tests/test_backend_conformance.py); revalidate on upgrades. MFS will not implement a second BM25 engine to mask it. |
| **INTEGRATION-001** | Open, host | Migrate StashBase's daemon to this MFS interface, adapt actual processors, import state and rules, implement host source-operation/recovery handling, and validate packaging. Shared cross-process admission is optional and needed only if the host requires a common global budget. See [integration guide](stashbase-integration.md). |
| **EVAL-001** | Open, host | Run representative-corpus retrieval evaluation, initial indexing/interactive latency measurements, and end-to-end recovery checks in StashBase. Library tests do not establish application equivalence. |
| **STARTUP-FALLBACK-008** | Deferred | Allow metadata/prepared-text access when Milvus cannot open. Runtime grep has separate execution slots, but `MFS.open` still opens the backend. This was boundary-audit D3/discussion item 8. |
| **STRONG-SCOPE** | Deferred | Directory-level strong queries. Current strong retrieval waits for the entire selected namespace; separate `wait(namespace, path=...)` already exists. |
| **HOST-NOTIFICATION** | Unselected alternative | If a host prepares outputs outside MFS, define durable incarnation/revision-scoped completion notification without clearing cancellation. The full Processor execution approach does not require this protocol. |

Arbitrary Python/native calls cannot be force-stopped safely. Query/close deadlines end caller waits while actual slots, leases, and ownership remain until exit. Synchronous open/create/sync/read and final reindex statistics have no universal caller deadline. Large promotion transactions and production corpus throughput need measurement; they are not newly confirmed deadlocks.

Native Windows handle behavior, actual frozen sidecars, cloud models, and real OCR/transcription workflows require their own environments. No extra readiness subscription system or permanent operation-receipt queue is planned.

## Latest completed fixes: 2026-09-14

The [boundary audit](review-2026-09-13-boundary-audit.md) preserves pre-fix evidence; the [fix report](review-2026-09-14-boundary-fixes.md) records resulting behavior and acceptance.

| ID | Completed behavior |
| --- | --- |
| **CLEANUP-008** | Successful collection drop, after actual old execution retirement, settles exact incarnation/generation cleanup debt including exhausted records. Reopen settles older completed drops; same-name recreation does not inherit old file wait errors. |
| **BOOTSTRAP-008** | Recover recognized interrupted initialization before catalog/schema commit, including real process termination; preserve unrelated directories and unknown schemas. |
| **MAINTENANCE-008** | Two bounded maintenance threads schedule namespaces fairly. Per-collection backend serialization allows independent collections to overlap. Stage deadlines supervise asynchronous create/load/retirement/snapshot cleanup without releasing still-running calls. Polling is rate-limited. |
| **SYNC-WAIT-008** | Sync reports canonical scope and additional alias targets; unchanged alias scans wait correctly, and root redirection reports widened observation. |
| **CONFIG-PARTIAL-008** | Atomically publish successful candidate members after current members are terminal and actual execution retires; retain failure/cancellation for retry. Preserve an existing publication when a nonempty candidate has zero successes, except when disabling indexing. Supersedes the earlier all-members-success policy. |
| **BLOCKING-008** | Expose binding, pause, resources, retirement, quiescence, configuration, index availability, and backoff reasons. Missing-binding dependent waits report blocked and recover after binding. |

Concurrency follow-ups made candidate claims wait for reusable active text and sourced binding status directly from the runtime table, avoiding duplicate checkpoint resume and false blocked status before worker startup. The maintenance spin regression originally measured 853 passes in 0.2 seconds; the fix enforces per-namespace spacing.

Discussion items mapped to these fixes: 1=cleanup, 2=bootstrap, 3=maintenance, 4=alias waits, 6=partial publication, 7=blocking diagnostics. Item 8 remains deferred; item 9 retains the actual-retirement constraint. StashBase dispatch/transactions/packaging and other host items remain outside this library change. Item 13 described the optional host-preparation alternative, not an existing MFS defect.

## Earlier completed work

| IDs | Implemented work and subsequent changes |
| --- | --- |
| **EXT-001, VIS-001, NS-001** | External zero-copy references, immediate stale-source invalidation, durable precise cleanup, independent namespace manifests/collections/index controls. |
| **IGNORE-001, API-002** | Ordered namespace rules with optimistic updates; `grep`/`read` replace public `query` and Query types. |
| **REF-001** | Central lifecycle transactions and separated execution/read responsibilities. Initially one worker plus GC; superseded by EXECUTION-004. |
| **ROOT-001, ROOT-002** | Keep state separate from source roots; ROOT-002 removed the original ban on overlap between different External namespaces. |
| **PROCESS-001, VERIFY-001** | UTF-8/PDF plus basic DOCX, application text references, behavioral/real-backend/recovery/type checks. |
| **SEARCH-DEFAULT-001, TEXT-002** | Eventual ranked search and five-second total caller timeout; CRLF/BOM-preserving UTF-8 offsets. The original remaining-RPC-time approach was superseded by MAINTENANCE-008's actual-handler retirement policy. |
| **RECEIPT-002** | Latest file/scope waits replace permanent operation history; remove `operation_id`, retain explicit idempotency keys and deletion responsibility. First migrated schema 6; current schema is 8. |
| **SYNC-002, SYNC-003** | Remove repeated parent enumeration; same-content observations refresh stat without resetting revision/progress or allowing callbacks to overwrite newer observations. |
| **REF-002, REF-003** | Typed execution permits/results and explicit module dependencies; move duplicated facade mutation logic into Lifecycle. |
| **CONFIG-001** | Read-only persisted namespace configuration without loading adapters. |
| **SEARCH-SCOPE-001** | Explicit single-namespace grep/search; remove `ByNamespace` and cross-namespace routing/ranking; reject filters outside the selected namespace. |
| **REUSE-002** | Independent durable checksummed vector cache with a 32 MiB logical LRU bound, isolated by incarnation/epoch/model/hash. Reindex advances the epoch; drop clears its incarnation. First introduced with schema 7. |
| **SCHED-001** | Durable checkpoint yield/resume, actual retirement, admission/priority/aging, late-result rejection, retained failure budgets, collection and file leases. Initially implemented with one worker; now used by the bounded pool. |
| **RECOVERY-003, SOURCE-003, PROCESS-003** | Reconcile uncertain state commits; guard borrowed source/checkpoint/cache identity and prepared text hashes; supervise POSIX descendants across host death and frozen startup. |
| **REBUILD-003, WAIT-003, GC-003, SYNC-004** | Preserve cancellation but reset obsolete index plans on rebuild; fail strong waits on terminal errors; manage work-directory grep outputs; isolate failed scan prefixes. |
| **QUIESCE-001, MIGRATION-003** | Incarnation/path ScopeLease with actual source-user retirement; persistent processing pause and revision-checked import of initial failure/cancellation. |
| **STATE-004, EXECUTION-004, ADMISSION-004** | Latest targets plus durable active runs/tokens; four file workers and independent queries; atomic resource admission; durable Internal originals and GC pins outside long locks. Embedder admission later changed under EMBED-006. |
| **CONFIG-004** | Unified configuration acceptance, stage reuse, private candidate generations, bounded retirement, generation-specific rebinding. Its original all-members-success promotion was superseded by CONFIG-PARTIAL-008. |
| **SYNC-005, VIS-004** | Remove remaining quadratic protected/nonmember scans, allow cross-namespace scans, revalidate captured identity; separate text/index readiness from physical deletion cleanup and preserve precise debt. |
| **INTEGRATION-004 (library)** | Extend quiescence to real source users/control work; provide `start_paused`/`resume_background` for host disk-journal recovery. Application integration remains INTEGRATION-001. |
| **LIVENESS-005, CONFIG-005, STORAGE-005, TIMEOUT-005** | Move root filesystem work outside the lifecycle lock; converge index toggles on the latest request; bound SQLite connections to eight; supervise stages and bound close caller wait. |
| **CACHE-006, PAUSE-006, SCAN-006, EMBED-006** | Move processing-cache I/O outside the lock; make candidates obey index pause; interrupt scans cooperatively on close; remove Embedder object/resource gates in favor of worker/query capacity and adapter-owned limits. |
| **CLAIM-007, STATUS-007, RULES-007, STATE-007** | Bound claim persistence failures with shared backoff; indexed per-file cleanup status; recheck persisted Internal rules and normalize legacy excluded targets; prevent stale cached tasks from overwriting SQLite commands. |
| **CONFIG-007** | Initially allowed cancelled members with no valid text in either generation to be absent from promotion. CONFIG-PARTIAL-008 broadens this while preserving cancellation. |

The September 13 search/concurrency fixes additionally preserve snapshot identity through deduplication, put backend lock waiting under the query deadline, provide total grep deadlines and partial failures, safely open borrowed text, reconcile candidate membership in bounded batches, expose maintenance retries, classify GC contention as busy, and cap background aging at active-folder priority. The [independent review](review-2026-09-13-independent.md) verified fixes for sniff/FIFO locking, transient-text Processor admission, promotion retry counting, and obsolete runtime bindings.

The three [September 12 follow-up findings](review-2026-09-12-followup.md) were also fixed: cleanup no longer restores stale cancellation state, reindex waits use shared storage-fault handling, and unchanged scans no longer enumerate all protected paths per file. INTEGRATION-002 was withdrawn because actual extraction belongs inside the supplied Processor; READY-001 did not require a new readiness system.

## Recorded validation

| Date / checkpoint | Recorded result |
| --- | --- |
| Before September 11 implementation | 65 passed, 3 skipped |
| September 11 | 79 passed, 3 skipped; final namespace/contract subset 14 passed |
| September 12, current-work waits and text fixes | 94 passed, 3 skipped; final text/recovery/search-timeout subset 20 passed |
| September 12, before scheduling changes | 98 passed, 3 skipped, 216.70 seconds |
| September 12, checkpoint scheduling | 119 passed, 3 skipped, 276.38 seconds; 21 new tests |
| September 12, recovery/quiescence/migration | 146 passed, 3 skipped, 328.61 seconds; 27 new tests; final relevant subset 19 passed |
| September 13, concurrent lifecycle/configuration | 162 passed, 3 skipped, 388.88 seconds; final concurrent/wait/lease subset 28 passed, 79.41 seconds |
| September 13, search and independent review fixes | 196 passed, 3 skipped, 1 xfailed across four independent test processes, each module once; 25 targeted regressions and four independently repeated defect mechanisms passed |
| September 13, liveness baseline | 205 passed, 3 skipped, 1 xfailed, 525.32 seconds |
| September 13, liveness fixes | 218 passed, 3 skipped, 1 xfailed, 563.39 seconds; 13 new regressions |
| September 14, boundary fixes | 241 passed, 3 skipped, 1 xfailed, 620.24 seconds; 23 new regressions |
| September 14, documentation cleanup | 241 passed, 3 skipped, 1 xfailed, 613.65 seconds; unhandled background-thread warnings treated as errors. See [documentation audit](review-2026-09-14-documentation.md) for the remaining checks. |

Recorded completed implementation checks include Ruff lint/format, strict Pyright, and `git diff --check`. The three skips require native Windows handles. The strict xfail is BACKEND-005. Seven PDF/SWIG deprecation warnings occurred in the recent full runs; the documentation-cleanup run had no unhandled background-thread warnings.

Additional historical measurements: 3,000 accepted, processing-paused files took 0.00148 seconds to accept a new configuration after batched reconciliation, with a maximum 0.01406-second status call among 52 samples in the next 0.2 seconds. These synthetic local timings do not predict million-file promotion or network filesystem behavior. A deterministic real-Milvus probe exercised BM25 to 2D model-a to 3D model-b. Older StashBase CRLF ingest was also checked with a deterministic local Embedder; neither probe established cloud-model quality or completed host migration.
