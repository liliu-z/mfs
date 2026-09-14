# Coalesced targets, concurrent execution, and configuration replacement

**Historical proposal, implemented in stages beginning 2026-09-13.** The authoritative current contract is [the design](../design.md); [backlog](../backlog.md) records acceptance. This document retains the proposed alternatives, rationale, and integration concerns. It does not override later decisions.

Notable changes since the proposal: the worker pool/active-run/configuration architecture is implemented; the strict all-members-success promotion policy below was superseded by [partial publication](../design.md#configuration-and-indexing-control); maintenance now has two bounded threads and per-collection backend locks; Embedder uses worker/query pool limits without Processor/Chunker resource/object gates. Permanent historical receipts and host-prepared completion notifications were not selected. Shared host admission is optional, not a prerequisite for standalone MFS.

## 1. Interface alternatives

MFS already offered file cancel/retry/reprocess, namespace reprocessing, and reindex with replacement chunker/embedder. The problem was not an absent model-change method. Cancellation already persisted intent and returned before actual retirement; wait already followed latest file/scope work rather than historical operation success. Execution initially used one file worker.

Three interface shapes were considered:

| Alternative | Tradeoff |
| --- | --- |
| Generic `reconcile`, `configure`, and `control` commands | Fewer method names, but callers still learn the same commands and result semantics |
| Extensible execution platform with injected admission/version/scheduling | Covers shared playback/transcription resources, but exposes too much execution configuration to ordinary callers |
| Keep file operations and unify configuration changes | Preserve sync/upsert/remove/cancel/retry/reprocess; hide coalescing, execution, and recovery inside Lifecycle |

The selected approach was the third, using an internal resource-admission seam from the second. Local and host-shared admission are real alternate adapters; there is no public task DAG or host-managed stage/file retirement. Lifecycle owns execution state, indexing owns generation mechanics, and callers declare intent and inspect status.

The recorded StashBase UI already had Cancel for document preparation and transcription via its own source-based endpoint. It was not yet mapped to MFS. One physical source can map to several namespace-specific DocumentIds; host cancellation/source operations must account for those mappings.

## 2. Latest desired target and actual active run

Conceptually each `(namespace incarnation, DocumentId)` has:

| Record | Contents and purpose |
| --- | --- |
| Desired | Latest target/input version or deletion, sequence, user cancellation; coalescible |
| Active | Captured input/configuration, logical run identity, stage/state, checkpoints, retry information |
| Invocation identity | Attempt token and actual execution lease; distinguishes calls within one stage/run |
| Publication | Generation-specific text/index identity and source input version eligible for reading |
| Cleanup debt | Exact files, collection/generation/snapshot side effects requiring retirement |

The explicit decision was to preserve `active_run_id`, `stage/state`, `attempts`, and `attempt_token`. A current-stage field alone cannot distinguish versions or late invocations. The actual lease outlives lost commit authority until execution exits. Source input identity is distinct from processing configuration so a model change does not invalidate unchanged-source G0 text merely by comparing revision strings.

### Coalescing and handoff

| Sequence | Desired state and execution |
| --- | --- |
| A1 added, A2 updated before execution | Only A2 needs processing |
| A1 added then deleted before execution | Desired becomes deletion; retire immediately if no outputs/users/debt remain |
| A1 executing while A2/A3 arrive | Active retains A1; desired retains only A3 and marks A1 obsolete |
| A1 returns while A3 is desired | Discard A1 publication authority and skip its remaining stages; retire, then execute A3 |
| Delete while A1 runs | Revoke visibility at acceptance; retain actual A1 use and clean its exact effects afterward |
| A1 failed/backing off when A2 arrives | Once no invocation runs, retire obsolete A1 and start A2 without waiting for old success/backoff |

A successor waits for actual invocation retirement, not success of the whole obsolete chain. Same-file execution stays serial; different files can overlap. Physical source operations spanning namespaces remain a host responsibility.

Cancellation is independent intent, retained across cleanup, sync including new bytes, and ordinary configuration changes. Explicit retry/reprocess clears it. It neither deletes the source nor authorizes removal of a still-valid publication. Referenced originals/prepared text/checkpoints remain; unreferenced abandoned work becomes collectible after retirement. Partial/stale backend writes remain invisible and retain precise cleanup debt. Cleanup must restore current cancellation eligibility, fixing the then-reproduced stale cleanup-restore bug.

### External input and failed old calls

An active run captures an observation, not preserved V2 bytes. MFS does not copy, hardlink, or reflink External originals to stabilize them. SourceGuard rejects detectable mismatches, but cannot provide atomic snapshots against arbitrary writers.

Reading V4 during a V2 attempt must never cache V4 output under V2's hash. Reject and reobserve changed input, or any explicitly chosen processing-time observation variant must record the actual verified identity. A V2 Processor error belongs to that invocation; if V4 is now desired, it must not fail V4 or consume V4's budget. Uncleanable old side effects remain exact debt; general storage unavailability still stops publication. A stuck call requires cooperation/supervision, not a false retirement on thread timeout.

### Internal acceptance order

The proposed ordering, subsequently implemented and tested, was unique staging copy/hash, file fsync, rename to a unique originals version, directory fsync, then a short SQLite transaction storing target/input references and invalidating prior eligibility. Never overwrite an old original in place.

SQLite commit is the logical acceptance point, not a cross-system atomic filesystem/SQLite transaction. Before the file is durable there is no new target; after file persistence but failed transaction, the old target remains and GC may reclaim an unreferenced new file. Lost acknowledgements require readback/idempotency rather than deleting possibly accepted bytes.

Concurrency acceptance had to add GC pins for copying/pending originals, move rename/fsync outside long state locks, persist newly created parent entries as supported, and revalidate incarnation and admission order afterward. Old originals retire only after active/checkpoint/read references release. No claim of testing every platform's physical power-loss behavior was made.

## 3. Stages and resource admission

Start with a configurable four-worker pool, separate query capacity, and low-priority GC. This does not promise four simultaneous local models or a fixed OS-thread count. One work unit is one file-stage invocation: Processor, Chunker, Embedder, or index write/publication.

1. Choose a runnable stage.
2. Nonblockingly acquire its complete resource/object grant.
3. In a short transaction validate target/run/cancellation/attempt and claim execution.
4. Execute outside state locks, persist outputs, then commit progress/next stage or error.
5. Return actual invocation resources; retain the logical active run until complete or safely retired.

Resource waiting must not occupy execution threads or hold SQLite/lifecycle/backend locks. Processor/Chunker objects are serial by default, with explicit concrete-class concurrency declarations. Embedder ultimately uses only worker/query capacities and owns provider/model limits itself.

The recorded host used two light/one heavy plus classifier capacity. A product that shares playback and preparation capacity requires one actual owner, with asynchronous/prefetched host grants if crossing processes. Temporary transcription yield for playback must not set user cancellation. A timed-out/disconnected RPC does not prove execution ended and cannot authorize granting the same capacity twice.

Stage boundaries naturally return resources. Long processors can checkpoint their own page/audio units to reduce restart and handoff latency. Checkpoints remain optional durable recovery, not automatic media partitioning or forced native-thread interruption.

## 4. Publication and crash recovery

SQLite and Milvus cannot share a transaction. A logical file lock alone cannot make their commits atomic. The protocol therefore requires:

1. Persist ownership/cleanup responsibility for the run's outputs and backend writes.
2. Produce durable private files and write stable row IDs to the exact collection generation.
3. After backend queryability, validate incarnation, input version, configuration, run, and attempt in a short SQLite transaction.
4. Publish only while still authorized; obsolete work contributes only exact cleanup responsibility.
5. Filter query candidates by persistent eligibility, so physical rows without publication remain invisible.

Rows distinguish incarnation, collection generation, DocumentId, snapshot, and chunk ordinal. Retrying a write uses stable IDs, and cleanup targets exact versions rather than every other snapshot of a file.

| Failure point | Expected recovery |
| --- | --- |
| Acceptance did not commit | Do not report accepted; preserve previous target |
| Files persisted, stage did not commit | Validate/reuse or collect from registered responsibility; do not report success prematurely |
| Backend write succeeded, publication did not | Rows stay invisible; replay stable IDs and retry publication |
| Publication committed, acknowledgement lost | Adopt durable state without inventing another revision |
| Obsolete invocation returns | Reject publication; retain cleanup of its effects |
| Transient stage failure | Persist error/count/backoff; do not retain a worker during backoff |
| Permanent failure or exhausted budget | Report failure; other files continue; only still-valid old outputs can remain eligible |
| Missing capability | Report blocked with a reason, not hot retries |
| User cancellation | Preserve cancellation plus separate actual execution status; keep ownership until retirement |
| Persistent SQLite fault | Stop scheduling/publication and make every dependent wait report storage failure |

The initial retry policy used a five-failure stage budget; jitter was a possible future refinement, not an implemented promise. Yield must not reset a failure budget; a new source has an independent budget. Reopen first acquires exclusive state/process ownership and retires old native users, then reconstructs persistent targets with new invocation tokens and explicitly rebound adapters.

## 5. Acceptance, readiness, and historical receipts

Three outcomes must stay separate: intent durably accepted, requested result available, and actual execution/cleanup retired. The host can render queued/processing/indexing/retrying/blocked/failed/cancelling/cancelled/ready from current state and `executing`; these are UI labels, not an additional public task-state enum.

An accepted HTTP/RPC response cannot be retroactively changed by later processing failure. Expose that failure and retry through status. Delete acceptance revokes search even while `cleanup_pending` remains true. Physical file operations require quiescence, not a UI cancellation acknowledgement.

A separate historical receipt design was discussed **but not selected**: bounded-lived per-operation target associations with pending/succeeded/superseded/cancelled/failed outcomes, explicit expired/unknown status, and tentative `operation_status`/`await_operation` methods. Such receipts would have been observations, not another FIFO. They would need to distinguish A1 superseded before execution from A1 completed before A2 arrived.

No such methods were added. Existing `wait(file/scope/report)` continues to follow current work and cleanup; explicit idempotency keys deduplicate acceptance only. Incomplete observation remains an error rather than a complete receipt.

## 6. Unified configuration and generations

The proposed single `configure_namespace` accepts Processor, Chunker, Embedder, and indexing changes together. Compare stable declarations, not object addresses or dimensions alone.

| Change | Earliest required stage |
| --- | --- |
| Compatible credentials/connection only | Rebind; no rebuild |
| Embedding space | Reuse valid text/compatible chunks, rebuild vectors/index |
| Chunker | Reuse valid text, rechunk and rebuild |
| Processor output compatibility | Reprocess affected formats and later stages |
| Processor and Embedder together | One desired configuration and necessary chain |
| Disable indexing or remove/exclude source | Revoke corresponding eligibility immediately, clean asynchronously |

Keep explicit retry/reprocess and same-configuration index repair. Moving all incidental configuration duties away from legacy reindex/reprocess methods was a possible later simplification, not a completed interface removal.

G0 holds serving text/collection and matching query Embedder; G1 builds private text/index data. Processor changes must not silently mix G1 text and G0 semantics. A pure configuration change can preserve G0; source changes/deletion/exclusion revoke stale input in both generations. Sequential same-file work can maintain G0/G1 and reuse compatible stages; distinct models may need both vector calculations while building.

### Original strict-promotion proposal: superseded

The original recommendation required all current participating G1 members to succeed before switching. Failed/cancelled required members kept G0, with an explicit possible exception for cancelled files that never had valid text. Membership followed current accepted input; deletion removed requirements and newer input invalidated old completion. A short transaction verified the latest desired generation and membership before switching. Old queries retained G0 leases through actual exit.

**This is historical rationale, not the current policy.** The implemented September 14 policy allows successful-member partial publication while preserving unhealthy states, with a safeguard for a nonempty zero-success candidate replacing existing publications. Consult [current promotion conditions](../design.md#configuration-and-indexing-control).

Continuous writes can prevent a candidate from catching up; unlimited admission, exact latest membership, and fixed-time promotion cannot all be guaranteed. A fixed-baseline switch was considered but not selected. Repeated G1/G2/G3 changes retain only the latest candidate, stop obsolete work cooperatively, and bound retiring collections/models. Reopen binds G0 and G1 by configuration revision; missing G1 must not force its Embedder onto G0's collection.

## 7. Observation and path work

The then-current quadratic loop checked all protected paths for each old file. The selected correction used canonical sets and bounded ancestors, reducing that work to file count times path depth. Lightweight parent/path directory records were considered for future scale; the implementation uses existing SQLite path keys/indexes and does not maintain a second full tree.

Full sync still enumerates files. Parent-directory mtime cannot prove child contents unchanged. Keep stat fast paths versus explicit content verification and avoid rewriting every unchanged file merely for last-seen bookkeeping. Successful direct-child coverage can establish deletion; failed prefixes and inaccessible roots cannot.

Serialize scans within each namespace, allow independent namespaces to overlap, and keep traversal/hash outside the instance mutation lock. Acceptance rechecks captured incarnation/root/rules/configuration and input version. An old scan cannot delete a newer accepted file or infer missing state using obsolete scope.

## 8. Derived outputs, nested roots, and consistency

Managed Internal originals and both namespace kinds' derived/work files live under the state directory. External originals stay outside. Explicit `text_path`/`grep_path` outputs outside the attempt work directory can remain borrowed; their availability belongs to the host. Managed artifacts can be shared with readers through handles that pin their lifetime.

Namespace nesting does not create lifecycle inheritance. Dropping parent membership affects only its metadata/collection/owned files. Deleting a physical parent directory can remove a child's originals; direct grep may fail while prepared text/index remains until observed invalidation. A missing entire child root is incomplete observation, not proof of all-file deletion; the host may explicitly drop membership.

| Event | Eventual retrieval | Strong retrieval |
| --- | --- | --- |
| Addition | May omit unpublished work | Wait for required text/index; report failure |
| Source update | Old input immediately ineligible; possible gap | Wait for current input's required stages |
| Accepted deletion | Immediately filter despite physical rows | Same filtering; do not wait for deleted-source cleanup |
| Configuration build | Serve still-valid G0 | Wait for relevant target text/index capability |

Strong grep can use prepared candidate text without waiting for vectors; eventual grep stays on active text until promotion. Neither freezes External bytes or guarantees physical cleanup. Final eligibility checks cannot retract results already returned to a caller.

## 9. Host source transactions

A durable active record freezes metadata, not source bytes or other programs' reads. The host-controlled operation sequence remains path lock and replayable journal, quiesce all affected scopes/host users, disk rename/delete, complete durable sync of old/new ranges inside the lease, then release and allow background indexing.

Do not wait for indexing while quiesced. Later embedding failure must not automatically roll back an already committed disk rename. Compensate before release when needed; if already released, reacquire all scopes and resync. ScopeLease is process-local, not a durable journal or automatic crash recovery. Restart with the startup gate before restoring host disk steps and execution. Request dispatch must keep status/cancellation/admission callbacks runnable alongside long work.

## 10. Implementation and acceptance record

The proposed order was to fix cleanup cancellation, fatal-storage reindex wait, and quadratic unchanged sync; introduce desired/active/publication/debt invariants and migration under one worker; verify handoff before enabling concurrency; tighten backend identity and resource/read leases; add unified configuration/generations; then migrate host playback, outputs, and transactions.

Deterministic acceptance covered pre-execution coalescing; update/delete/cancel during execution; new targets during retry; cancellation during cleanup/reopen; crash around backend/publication commits; stale attempts; same-file exclusion and cross-file concurrency; per-stage resources versus logical run; late timed-out queries protecting collections; candidate source/member/model changes; separate generation rebinding; local scan failure and scaling; immediate deletion with delayed cleanup; and host death while leases/native helpers were active.

Library-side targets, durable active runs, optional admission, candidate publication, precise cleanup, observation optimization, text/index readiness, and startup gates were subsequently implemented. Host daemon conversion, actual format/model adapters, optional shared grants, disk journals, Viewer lifetimes, and full application/corpus validation remain distinct integration work.
