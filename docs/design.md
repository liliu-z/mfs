# MFS design

This is the authoritative description of the implemented MFS contract, reviewed against this checkout on 2026-09-14. [CONTEXT.md](../CONTEXT.md) defines domain terms; the [interface reference](reference.md) records callable details. [Backlog](backlog.md) tracks remaining work. Dated reviews and [proposals](proposals/concurrent-lifecycle.md) preserve historical reasoning and do not override this document.

## Purpose and scope

MFS is an embedded Python 3.13 library that accepts or observes source files, prepares search text, and manages processing and indexing through replacement, cancellation, failure, and restart. It owns a durable lifecycle behind a small `MFS` interface and three application-supplied adapter interfaces: Processor, Chunker, and Embedder.

The host owns authorization, user-facing file operations, playback/viewer lifetimes, model credentials, and product decisions. MFS provides neither a filesystem watcher nor a daemon/RPC server. Queries select one namespace; cross-namespace fan-out, deduplication, and ranking belong to the host. A namespace is a configuration and identity scope, not an access-control tenant or independently portable database.

## Invariants

1. **Source ownership is explicit.** External originals are borrowed and never copied, modified, or deleted by MFS. Internal originals are owned and durably stored before acceptance.
2. **Acceptance and readiness are separate.** SQLite records the latest desired target before a write returns. Preparation and index publication happen later.
3. **Source invalidation is immediate.** Accepting replacement, deletion, or exclusion revokes old text/index eligibility. Failure, restart, or a late result cannot restore the old source.
4. **One file has at most one actual stage invocation.** New targets can be accepted while an older invocation runs, but the next invocation waits for actual retirement.
5. **Publication requires matching identities.** Input version, namespace incarnation, configuration generation, active run, and attempt token must still authorize the result.
6. **Timeout and cancellation do not prove retirement.** Running calls retain the resources, file pins, and collection leases they use until they exit.
7. **Cleanup survives target replacement.** Physical deletion responsibility is durable and independent of the newest file target.
8. **Configuration generations do not mix embedding spaces.** Each ranked query captures one collection and its matching Embedder. Partial publication exposes only successful members of the new generation.

## Instance, identity, and storage

One instance exclusively owns a state directory, a SQLite catalog, a Milvus Lite database, and bounded execution pools. Business configuration belongs to namespaces; there are no global Processor, Chunker, Embedder, rule, or indexing defaults inherited across them.

`DocumentId(namespace, doc_id)` is the public identity. Identical content at different paths remains distinct documents. Internal IDs are opaque strings; External IDs are canonical root-relative POSIX paths. An External path cannot contain empty, `.` or `..` segments; `.` denotes an entire observation/wait scope. Namespace names allow up to 255 UTF-8 bytes and document IDs up to 2,048 bytes. Original display spelling and canonical filesystem identity must not be confused by the host.

A namespace's private, non-reusable **incarnation** distinguishes deletion and recreation under the same public name. Its configuration generations distinguish active, building, and retiring collections. A source input version is separate from configuration revision and execution attempt; changing the model does not mean the source bytes changed.

| Storage | Responsibility |
| --- | --- |
| SQLite catalog | Namespace manifests, rules, observations, current targets, prepared references and SourceMaps, active runs, candidate membership, cleanup debt, explicit idempotency records, and bounded vector cache |
| Milvus Lite | One active collection per namespace, at most one candidate, and bounded retiring collections; chunk text, optional vectors, source metadata, snapshot identity, and location |
| Managed files | Internal originals, necessary derived text, artifacts, checkpoint files, and temporary work |
| Borrowed files | External originals and application-owned derived files; the host owns their continued availability |

The following is a conceptual layout, not a stable file-management interface:

```text
mfs-state/
  LOCK
  PROCESS_LOCK
  catalog.sqlite          # SQLite may also create WAL/SHM files
  milvus.db               # Backend-managed storage path
  namespaces/
    <storage-id>/
      originals/
      derived/
      work/
```

Top-level compatibility and staging directories can also exist. Namespace directories cannot be moved independently of the shared databases. Applications should not edit managed state files.

External roots cannot overlap the state directory in either direction. Roots in different namespaces may be identical or nested; observation, rules, identity, and deletion remain independent. Dropping a parent namespace neither deletes its directory nor drops child namespaces.

Initialization creates and persists an empty `.mfs-initializing` ownership marker before lock/catalog files and removes it after schema commit. Reopening can recover recognized interrupted bootstrap state; unrelated files or unknown schemas produce explicit errors rather than being overwritten. Only one instance/process may open the state at a time.

Opening a modern instance does not construct application adapters. It does open the backend and validate/load collections: text-only startup when Milvus itself cannot open is **not implemented**. Once open, configuration/status reads, deletion, supported-suffix observation, and available saved text can work without a bound model. Missing execution bindings block the affected work, independently of other namespaces.

## Adapter interfaces and text

The host supplies adapter objects when creating or binding a namespace. MFS persists compatibility declarations, never Python objects, functions, connections, or secrets.

| Adapter | Persistent compatibility |
| --- | --- |
| Processor | `id`, `version`, output-affecting `options`, media types, and suffix routing |
| Chunker | `id`, `version`, and chunk-affecting `options` |
| Embedder | `embedding_space` and `dimension`; the dense index currently uses fixed `COSINE` similarity |

Equal dimensions do not imply equal embedding spaces. Incompatible declarations fail binding with `NamespaceCompatibilityError`; missing/incompatible collections require explicit repair. Vector batches are checked for count, dimension, and finite values before use or caching. Treat declarations as immutable while bound. Credential rotation within the same declared space can rebind without rebuilding. In `off`/`bm25`, an unused Embedder may be omitted on reopen; if supplied, its declaration is still checked.

Processor routing uses an explicit media type, registered suffix, or `sniff` result. One Processor is selected for a format. Unsupported binary files are skipped/rejected; a formerly supported source becoming unsupported still loses its old eligibility. Built-in processors cover UTF-8 TXT/Markdown, PDF extraction, and basic DOCX. OCR, enhanced document conversion, HTML, and transcription are application adapters.

A Processor returns `ProcessedDocument(text, source_map, ...)`. `text` remains required even when `text_path` is supplied, and that UTF-8 file must decode to the returned text. This is a reference-oriented persistence interface, not streaming extraction. `grep_path` may expose a separate textual view, such as original HTML, while in-memory extracted text feeds indexing. Outputs inside the attempt's `work_dir` are copied to managed storage before that directory can retire. Other borrowed outputs remain the host's responsibility. Artifacts are optional immutable attachments, opened through `open_artifact`; they do not automatically become search text.

| Source/view | Grep/read text | Index text |
| --- | --- | --- |
| External TXT/Markdown | Borrowed original | Original text observed during processing |
| Internal TXT/Markdown | Managed original | Original text |
| PDF/DOCX/image/audio/video | Necessary extracted text or a borrowed existing output | Extracted text |
| Processor with `grep_path` | Explicit alternate text view | Returned `text` |

SQLite stores references and metadata, not document bodies. Milvus stores chunk text because BM25 requires it. Processing plans retain ranges/hashes and recovery information without duplicating bodies in JSON.

UTF-8 decoding preserves CRLF and removes an optional UTF-8 BOM. Text offsets are half-open **UTF-8 byte ranges in decoded text**, not Python character offsets or original binary offsets. SourceMap version 1 spans are ordered, non-overlapping, and within the text; source payloads are JSON locations such as pages, lines, or time ranges. Chunk ranges cover the entire nonempty text without gaps, increase by start offset, align to UTF-8 code points, and may overlap. Each range is at most 65,535 bytes. Empty text yields no chunks. `DefaultChunker` uses 4,096-byte windows with 512-byte overlap and prefers nearby separators.

Direct-text grep derives locations from the text actually read. A borrowed extraction's saved SourceMap is used only while its text hash matches; otherwise grep/read fall back to current line locations. Two syncs do not delimit a frozen source snapshot: grep may read newer bytes while the index still represents the previous observation.

## Acceptance and observation

```text
sync / Internal upsert
        |
observe, hash, route, and deduplicate
        |
SQLite accepts latest target ------> return report
        |
file workers: process -> chunk -> embed -> publish
        |                 (embed only for hybrid)
short transactions validate and persist each stage

independent maintenance: snapshot cleanup, generation creation/promotion/retirement, GC
```

An Internal upsert prepares the original outside the lifecycle lock: copy/hash, file fsync, rename to a unique originals path, and directory fsync. GC pins protect copies and pending originals. Under the lock, final acceptance rechecks the persisted namespace incarnation, rules, and request ordering. An older slow copy cannot overwrite a later accepted upsert/remove/reprocess. A failed commit preserves the previous target and may leave an unreferenced file for GC. A lost commit acknowledgement is reconciled before deleting any possibly accepted original.

An External sync saves a pointer, hash/stat observation, and target; it never creates staging copies, hardlinks/reflinks, mirrors, or body caches of originals. `verify="stat"` can skip hashing unchanged observations; `verify="content"` rehashes them. Equal content with changed stat refreshes the observation while preserving revision and progress. Unchanged sync does not restart failed work or append historical receipts. New content creates a new target with its own failure budget, while preserving any user cancellation gate.

Scans serialize within a namespace and can overlap across namespaces. Traversal, root resolution/stat, hashing, and `sniff` run outside the lifecycle lock. An already safely opened file supplies the sniff head; acceptance rechecks routing declarations without reopening it. Normal-file/no-follow/nonblocking opens prevent link and FIFO replacement races from entering blocking reads under the lock.

A scan captures incarnation, root, binding, rules, and configuration, then validates them at acceptance. Missing-file inference applies only to successfully observed ranges and targets known at scan start whose revisions have not changed. Failed subtrees preserve their previous targets without suppressing confirmed deletions elsewhere. An inaccessible root is not a complete empty directory.

`SyncReport.path` describes the actual canonical observation scope; `wait_paths` adds targets reached through aliases outside that scope. Root redirection that triggers a full scan reports `.`. Even an unchanged alias scan waits for those current targets. During close, scans check stopping between entries/files/hash blocks and return `complete=False` with `Closed` failures; accepted changes remain, unobserved files are not inferred missing. A blocked individual OS call must still return before it can retire.

SourceGuard validates borrowed source identity, hash, and change metadata during preparation, checkpointing, and result publication. Indexing also verifies prepared text hashes. Changed input cannot populate an old content cache or use an old SourceMap for new text. Even a change followed by restoration of bytes/mtime is checked against change metadata. These checks are not atomic with arbitrary external writers; hosts must quiesce their own writes, and adapters needing stronger reads must provide stable-input semantics themselves.

## Current work, cancellation, and retries

Each file has one latest target and a durable active run. `active_run_id` names the logical processing chain; `stage`, `state`, and `attempts` describe its progress; `attempt_token` names an actual invocation. Stage completion releases execution resources, but the active run may persist for the next stage or checkpoint resume.

Pending changes coalesce. While V2 executes, V3 and V4 can be accepted; only V4 remains desired. V2 keeps its resources until it returns, then loses any obsolete subsequent stages. A V2 error cannot fail V4. Deletion replaces the desired target and revokes visibility immediately while retaining independent cleanup responsibility.

| State | Meaning |
| --- | --- |
| `pending` | Awaiting a runnable stage and its admission conditions |
| `running` | A stage has been claimed; actual retirement is separately tracked |
| `retry_wait` | Waiting for bounded retry backoff |
| `succeeded` | This target's required stages have completed |
| `failed` | Permanent failure, retry exhaustion, or execution timeout |
| `blocked` | Required capability is unavailable |
| `cancelled` | User intent prevents further processing until explicitly resumed |

`cancel(DocumentId)` accepts durable user cancellation; it does not prove actual exit, delete sources, or revoke a completed publication that remains valid. Ordinary sync, configuration changes, cleanup, and restart preserve the gate. `retry` or `reprocess` explicitly clears it. Full `reprocess_namespace` is also an explicit reprocessing request and can clear member cancellation. Internal supersession, quiescence, and shutdown stop executions without creating user cancellation.

Retryable stage errors use exponential backoff with a bounded five-failure budget. Permanent failures fail immediately; missing capabilities block. An execution timeout is not automatically retried. Explicit retry still waits for the previous invocation to retire. Same-content sync does not reset failure budgets.

`DocumentStatus` exposes revision, text/index revisions, configuration revision, stage/state, attempts, batch progress, errors, `executing`, active run/token, and `cleanup_pending`. Its computed `blocking_reason` distinguishes `binding`, `background_paused`, `processing_paused`, `indexing_paused`, `resources`, `retiring`, `quiescence`, `configuration`, `index_unavailable`, and `retry_backoff`. `resources` reflects the last failed admission attempt, not a second persistent task state. Missing active bindings cause dependent wait/strong-text readiness to raise `OperationFailed(state="blocked")`; rebinding lets work resume. Pauses and transient queues remain waitable within the caller's timeout.

## Configuration and indexing control

`configure_namespace` durably accepts a desired manifest and returns `ConfigurationReport`. Unspecified adapters retain the latest bound candidate's values, or the active binding when no candidate exists. Equal configurations replace runtime bindings without rebuilding. Use `indexing="bm25"` or `"off"` to stop using dense embedding; `embedder=None` is not a model-removal operation.

Changes reuse the earliest valid stage: an Embedder change reuses valid text and compatible chunk plans, a Chunker change rechunks, and a Processor change reprocesses affected formats. `reprocess_namespace` forces preparation; blocking `reindex` forces index rebuilding and cannot change Processors. None of these observes unsynced External files.

The active generation G0 serves valid results while G1 builds private text/index data. Candidate membership is reconciled from SQLite in batches of at most 32 differences per maintenance pass, releasing the lifecycle lock between members. Source changes, deletions, cancellation, and reusable preparation update candidate expectations. Targets and prepared references are persisted together, and lost acknowledgements are reconciled. A candidate waits to adopt compatible active text rather than invoking the same Processor again.

Promotion requires an initialized collection, a bound candidate, matching current membership, no relevant quiescence, and actual file execution retirement. It atomically switches the manifest, collection, and publications:

| Candidate outcome | Promotion behavior |
| --- | --- |
| Any member still pending/running/retrying, or membership incomplete | Keep building; G0 continues to serve valid input |
| All current members are `succeeded`, `failed`, `blocked`, or `cancelled`, with successful members | Publish successful G1 members together; retain other members' errors, stage data, and cancellation for explicit recovery |
| Nonempty candidate has no successful members and G0 has published results; mode is not `off` | Retain G0 and the pending candidate until repair or a new configuration request |
| No prior published results, no members, or switching to `off` | The zero-success safeguard does not prevent promotion once the other conditions hold |

A failed `wait(change)` therefore does **not** mean the active configuration stayed unchanged. Inspect `namespace_configuration` and member status: partial publication may already have occurred or may still be completing retirement. Successful members never borrow vectors from G0. Failed/blocked/cancelled members can be repaired in the new active generation. Strong grep can use prepared candidate text without waiting for embedding; eventual grep stays with active-generation text until promotion.

Maintenance errors are exposed as `pending_error`, `pending_failures`, and `pending_retry_at`. Five failures stop automatic retry; resubmitting the same manifest resets the maintenance budget and keeps its candidate revision. Execution timeout also stops automatic maintenance retry. A newer request supersedes the previous candidate; old calls retain their leases until actual exit. Retiring generations are bounded, and creation waits if they cannot retire.

`namespace_configuration` exposes both manifests, `active_revision`/`pending_revision`, modes, pauses, source-size limit, and cleanup/maintenance diagnostics without runtime objects or credentials. On restart, bind G0 and G1 separately with `open_namespace(..., configuration_revision=...)`. A generation change during binding raises `NamespaceCompatibilityError`; reread configuration before retrying. An unbound G1 must not query G0 with the wrong Embedder. MFS releases obsolete bindings but never calls a host-shared adapter's close method.

| Control | Effect |
| --- | --- |
| `indexing="off"` | Preserve observation/preparation/grep, immediately suppress ranked results, asynchronously remove index data; no embedding |
| `indexing="bm25"` | Build BM25 only; no embedding |
| `indexing="hybrid"` | Build BM25 and dense together; requires a compatible Embedder |
| `configure_index(paused=True)` | Pause new indexing, including an `off` to enabled candidate; keep observation, necessary text processing, invalidation, and cleanup |
| `configure_processing(paused=True)` | Persistently stop preparation and new indexing admission; request active file calls to stop cooperatively; cleanup/control work can continue |

`configure_index` compares the newest candidate, falling back to active. A pause-only change preserves the requested mode. Repeated mode changes converge on the last request. Enabling indexing catches up accepted inputs without an implicit sync. Turning indexing off and its cleanup are not blocked by index pause. Choosing `search(..., mode="bm25")` affects only that query and does not stop background embedding.

## Rules and eligibility

Each namespace has one ordered rule set, without implicit `.gitignore` inheritance. Rules have stable IDs, include/exclude actions, and patterns; the last matching rule wins. Updates can atomically add, remove, replace, and reorder rules. `expected_revision` is an optimistic conflict check and raises `RuleConflict` immediately on mismatch.

Patterns without `/` match names at any depth. Patterns containing `/` are relative to the root; a leading `/` anchors at the root. Directory rules cover descendants. `*`, `?`, character classes, and `**` are supported. An included descendant can override an excluded parent, so traversal must not prune such parents prematurely.

Acceptance, task commit, and reads share eligibility rules. Internal acceptance rereads current persisted rules after copying; if a rule commits first, excluded input is rejected, and if acceptance commits first, the rule revokes it and schedules deletion. Reopening normalizes excluded targets left by older versions. Re-included External files must be synced again; removed Internal originals must be supplied again. Application-specific sibling/generated-file relationships should be converted to explicit rules by the host.

## Publication, cleanup, and reuse

A worker writes complete rows for an exact collection/snapshot, then SQLite validates authorization and publishes them. Rows whose publication did not commit are invisible and can be replayed using stable IDs after recovery. Source replacement, deletion, and exclusion revoke eligibility in the acceptance transaction, before physical cleanup.

Cleanup debt is keyed by incarnation, collection generation, document identity, and snapshot. It has independent errors/backoff and cannot mark a newer source task failed or delete its rows with an overly broad predicate. `retry(DocumentId)` can restart related cleanup. Successful collection drop, after old calls retire, settles its exact debt in the completion transaction, including exhausted records. Reopen also settles debt left by older successful drops. A recreated same-name namespace does not inherit old-incarnation file wait/status debt; explicit drop waits retain deletion semantics. Retirement errors appear in namespace configuration and can delay the next generation without stopping unrelated preparation.

Queries capture the active collection, matching Embedder, publications, and input versions. Results are checked again before return; source deletion/exclusion is filtered immediately. An already started query may finish against G0 after a same-input configuration switch, but cannot mix it with G1's model. Its collection is retained until actual query exit, even after caller timeout. Filtering stale hits can trigger bounded candidate expansion.

Managed originals, derived text, work files, checkpoints, cache references, and open artifact handles have durable references or in-memory pins. GC deletes only unreferenced managed files under its budgets. It never deletes borrowed originals/outputs. SQLite contention yields `GCReport.busy`; it is not automatically evidence of corrupted storage. `GCPolicy(enabled=False)` disables automatic collection; hosts can call `collect_garbage`.

Processing-cache lookup pins metadata under the lifecycle lock, performs I/O/parsing outside it, then revalidates the entry and its referenced files before reuse. Replacement/GC races fall back to recomputation; an old failed read cannot delete a newer entry. Cross-path processing reuse requires explicit `cache_scope="content"` and valid references.

The separate dense-vector cache stores checksummed binary values in SQLite, never chunk bodies. Its key includes incarnation, index epoch, full dense configuration, and chunk hash. Only a completely validated batch is inserted, and late obsolete attempts cannot repopulate a cleared epoch. The cache has a per-instance 32 MiB logical LRU limit, excluding SQLite page/WAL overhead. Deleting old search rows does not discard reusable calculations or restore eligibility. Explicit reindex advances the epoch; drop clears the incarnation. Eviction, corruption, configuration changes, or an empty upgraded cache cause recomputation. There is no unlimited or cross-namespace zero-embedding rename guarantee.

## Search, reads, and waits

The public retrieval methods are `grep`, `read`, and `search`; there is no `query`, public Query type, or `ByNamespace`. Grep/search require an existing namespace as their first argument. Structured filters narrow it; mismatched namespaces inside `ByDocumentId`, `UnderPath`, or nested `AnyOf` raise `InvalidFilter`.

Top-level filters combine with AND. `AnyOf` combines structured metadata alternatives with OR and cannot contain `TextMatch`. `TextMatch` belongs only to grep. `UnderPath` query filters require an External namespace; Internal IDs support identity and path/name-affix filters. Source extension/media type and path filters are applied before ranked backend top-k.

Grep streams documents with document, file-byte, total-byte, and match budgets. Path-only `select="doc_id"` filtering does not open bodies. `select="doc"` returns budgeted text without loading Internal originals; chunk selection invokes the configured Chunker and shares its admission limits. Source mapping uses ordered-span lookup and computes offsets only at match boundaries. An unreadable or temporarily unavailable file produces `GrepFailure(id, TaskError)` plus `truncated=True`, while other results remain available. Invalid input, corrupt persistent state, and storage faults fail the call. Known-document `read` returns `None` when no current prepared document is available and raises read errors directly; it is unbounded and includes Internal original bytes.

Ranked search supports BM25, vector, and hybrid modes. Hybrid uses reciprocal rank fusion within one namespace's collection; query embedding is computed once. `select="doc_id"` deduplicates by document; chunks retain occurrence/snapshot identity. Candidate retrieval starts at `min(1000, max(100, limit * 10))` and can expand to 1,000 candidates per channel. `truncated` means the requested result set is bounded, not an exact total-hit count. `indexing="off"` returns no ranked results. Vector/hybrid retrieval requires a bound dense capability when retrieving published documents; the query default remains `hybrid` even for BM25 namespaces.

| Operation | Completion condition | Includes physical deletion cleanup? |
| --- | --- | --- |
| Eventual grep/search | Read currently eligible text/publications immediately | No |
| Strong grep | Current text in the selected namespace, including candidate preparation | No; later embedding failure need not block prepared text |
| Strong search | Current index work and configuration in the selected namespace | No for already deleted sources |
| `wait(DocumentId)` | Current file work plus relevant namespace control work | Yes |
| `wait(namespace, path=...)` | Current work in the path scope plus relevant namespace control work | Yes |
| `wait(report)` | Current file/scope encoded by the report; sync aliases add `wait_paths` | Yes |
| `wait_ready()` | Instance-wide index readiness | No for already deleted sources |

Strong search/grep wait for the whole selected namespace even when filters narrow results. Directory-level strong is not implemented; `wait(namespace, path=...)` is available separately and provides no search snapshot isolation. Updates accepted during a current-work wait are included. `ConfigurationReport.revision` identifies the accepted configuration, but waiting on that report follows current namespace work rather than a historical generation. Explicit idempotency keys deduplicate acceptance and do not freeze subsequent wait semantics. An incomplete SyncReport fails waiting with `OperationFailed(state="incomplete")`.

Terminal failed/blocked/cancelled work makes dependent waits raise `OperationFailed`. Scope readiness does not hide unhealthy members after partial configuration publication. Storage that cannot be reconciled raises `StorageFailed`. A wait timeout only stops waiting; it neither cancels nor rolls back accepted work.

## Concurrency and deadlines

Default `ExecutionPolicy` provides four file workers, four ranked-query slots, four separate grep slots, and local resources `heavy=1`, `light=2`. Two maintenance threads fairly schedule namespaces, with one owner per namespace and a minimum interval between passes. A deadline watchdog and optional GC thread are separate. These capacities do not limit all native/backend OS threads.

Processor/Chunker admission is nonblocking and all-or-nothing before durable stage claim. Resource waiting consumes neither a worker invocation nor a file execution. Undeclared adapters request one heavy resource; built-in text, DOCX, and chunking are light, PDF is heavy. Adapters can declare `workload` or a `resources` mapping, including `{}` for no local compute requirement. The same Processor/Chunker object defaults to serial execution. Only a concrete class's explicit `concurrency` declaration allows more; subclasses must renew that promise. Per-file mutable state belongs in locals or an independent context/work directory. `sniff` must be fast and stateless because it can overlap `process` outside its grants.

Embedder calls use only their worker/query pool bounds. MFS does not read an Embedder's `concurrency`, `workload`, or `resources`. A shared Embedder can receive four background and four query calls under defaults; the adapter/host owns thread safety, local-model constraints, and provider rate limits.

`LocalAdmission` can be shared with other in-process host work. A custom `Admission.try_acquire` must return immediately with every requested resource or none. Cross-process implementations need prefetched/asynchronously delivered grants; they cannot perform RPC under the lifecycle lock. Grants return only after actual invocation retirement, including on timeout or disconnection.

`set_active_scopes` and aging prioritize runnable work. Aging can reach active-folder priority but cannot outrank explicit interactive work. Processor checkpoints first persist resume state/files, then may cooperatively yield to more urgent runnable work. The invocation and its subprocesses must exit before capacity is reassigned. MFS does not automatically split media into processing units; adapters checkpoint at their own bounded units and must not swallow cancellation/yield exceptions.

| Deadline | Default | What it bounds |
| --- | --- | --- |
| `search` / `grep` | 5 seconds | Total caller wait: queue, consistency, adapters, reads/matching, backend lock/calls, expansion and result assembly |
| Background stage and asynchronous maintenance | 300 seconds | From claim; watchdog persists failure and revokes late commit authority |
| `close` | 30 seconds | Caller wait for actual shutdown; cleanup continues after timeout |
| `wait`, `wait_ready`, `quiesce`, blocking `reindex` wait | No deadline | Caller-supplied optional wait budget |

Query deadlines use one monotonic clock, checked at waits and stage/lock/backend boundaries. `None` removes a query/wait deadline; `0` makes search/grep immediately time out. Stage timeout must be finite and positive. Synchronous `open`, `create_namespace`, `sync`, and `read` do not expose a total caller timeout; `stage_timeout` is not a universal timeout for all public methods.

The pinned Milvus Lite 3.2.1 in-process handlers do not cooperate with gRPC cancellation. MFS therefore does not use a wire timeout to infer retirement: its outer query wait/watchdog can expire, while the actual backend call retains its collection mutex and lease until return. Calls on different collections can overlap; calls on one collection, including reads/lazy loading, serialize for backend safety. A hung call can continue to consume its bounded slot and affect users of the same collection. Independent pools/collections improve isolation without guaranteeing recovery from arbitrary native hangs.

`ExecutionTimeout` marks a background target failed and prevents late publication; retry is explicit and waits for retirement. A Processor can pass `context.cancellation.remaining()` to its internal calls. Search `WaitTimeout` discards late results and prevents subsequent stages, but does not free a running slot. Four stuck ranked calls can exhaust that pool while grep retains its separate pool.

`close` stops admission, cooperatively stops work, drains foreground calls, workers, queries, and read leases, then closes storage and releases instance ownership. One cleanup execution continues after a caller timeout; subsequent close calls wait on it. MFS cannot safely force-stop arbitrary Python/native threads. A daemon host can terminate its process after its shutdown budget and reopen through recovery only after old process ownership retires.

## Host file operations and recovery

`quiesce(scopes: Sequence[UnderPath], timeout=None)` requires at least one explicit scope and returns a `ScopeLease`. The lease captures namespace incarnation, blocks matching new file execution and source-text reads plus relevant namespace control work, requests cooperative retirement, and waits for actual source users. Overlapping leases release independently; an old incarnation's lease cannot restrict a recreated namespace. Timeout/close abandons only the unsuccessful acquisition. A lease can be closed on another thread or after instance close and does not itself keep a foreground call open.

Inside a lease, new `read` raises `CapabilityUnavailable` and text-reading grep reports partial per-file failures. Metadata grep, existing ranked queries, and managed artifact handles do not need a source open and can remain available. Releasing a lease resumes valid work from its durable stage/checkpoint without changing user cancellation. The lease does not control external editors, scan reads, playback, or viewer handles.

The host's source-operation sequence is:

1. Acquire host path locks and determine every namespace/range sharing the source.
2. Persist a replayable host transaction describing intended disk steps and compensation.
3. Quiesce all affected MFS scopes and retire host playback/viewer users.
4. Apply the disk operation; sync old and new ranges while the lease remains held.
5. Verify every scan is complete and durably record observation acceptance.
6. Release leases/path locks, then optionally wait for indexing.

The host serializes disk steps and sync; MFS does not create a distributed filesystem/SQLite transaction. A pre-acceptance failure may require compensation and resync inside the lease. A later embedding failure means indexing is behind; it must not automatically undo a committed source rename. A whole missing root needs an explicit host decision, not a fabricated empty scan.

`MFS.open(..., start_paused=True)` installs a startup gate before workers, maintenance, or GC can execute. Configuration/status and sync are allowed; reads are temporarily unavailable. Recover the host's durable disk journal, bind adapters, quiesce/reconcile affected ranges, check complete sync acceptance, release leases, then call `resume_background()`. This process-local gate is not itself a journal: every restart must choose it again if host recovery remains incomplete.

For importing old per-file state, create a namespace with `processing_paused=True`, install rules, and observe sources. `restore_document_state(id, expected_revision=..., state="failed" | "cancelled", error=...)` accepts only a matching, not-yet-executed upsert; failure requires `TaskError`. Identical imports replay without undoing later explicit retry/cancel, while conflicting declarations or revisions fail. Restore all intent before releasing the persistent processing pause. Index pause alone still allows preparation and is insufficient for this migration.

`ProcessingContext.run_process` supervises child execution. On POSIX a separate supervisor watches a host-liveness pipe and inherits the `PROCESS_LOCK` flock descriptor. It retires the managed process group before releasing ownership, including after host SIGKILL. It retains the unreaped leader identity to avoid PID reuse, ignores zombies as source users, and keeps retrying with the lock if retirement cannot be confirmed. It depends on POSIX `waitid` and `/bin/ps`; commands must not daemonize or escape their managed session. Windows uses a Job Object. Frozen executables must call public `run_process_supervisor()` before application initialization and package the private supervisor module; normal Python launches it directly.

## Internal modules and durable recovery

| Module | Responsibility and implementation |
| --- | --- |
| MFS | Public argument handling and instance lifetime; [_core.py](../src/mfs/_core.py) |
| Lifecycle / ReadView | Sole authority for targets, permits, state transactions, cancellation, waits, and read eligibility; [_lifecycle.py](../src/mfs/_lifecycle.py) |
| Worker / Preparation | Execute permits, processors, checkpoints, and prepared references; [_worker.py](../src/mfs/_worker.py), [_preparation.py](../src/mfs/_preparation.py) |
| Indexing | Chunk/embed/publication stages; [_indexing.py](../src/mfs/_indexing.py) |
| Configuration / Maintenance / IndexCleanup | Candidate reconciliation, promotion, retirement, and exact cleanup debt; [_configuration.py](../src/mfs/_configuration.py), [_maintenance.py](../src/mfs/_maintenance.py), [_cleanup.py](../src/mfs/_cleanup.py) |
| NamespaceRuntime / ChunkIndex | Bindings, adapter admission, collection routing, backend serialization; [_runtime.py](../src/mfs/_runtime.py), [_index.py](../src/mfs/_index.py) |
| Reader / ArtifactStore | Bounded retrieval, source locations, owned files, pins, and GC; [_reader.py](../src/mfs/_reader.py), [_artifacts.py](../src/mfs/_artifacts.py) |
| Catalog | Short SQLite queries/transactions and schema migration; [_catalog.py](../src/mfs/_catalog.py) |

Execution modules return typed results to Lifecycle rather than maintaining separate queues or mutating one another's state. `ExecutionPermit` captures identity, input/configuration versions, active run, attempt, cancellation, and resource lease. `finish_execution` commits or rejects results, then retires actual ownership. Slow adapter/file/backend calls run outside the lifecycle lock; commits are short and revalidate persisted targets.

SQLite is the durable source of truth. Uncertain command commits are reconciled from persistent namespace/target/document state before releasing the lifecycle lock. Adapter binding follows the manifest actually committed. If reconciliation itself fails, MFS stops scheduling and publication and raises `StorageFailed`; reopening after storage recovery reconstructs accepted work. Memory refresh updates caches, cancellation signals, and scheduling hints without writing candidate membership. An old cached target cannot overwrite a newer durable command.

Claim persistence failures share one worker-wide consecutive budget with monotonic backoffs of 0.25 and 0.5 seconds. Notifications cannot shorten backoff. Three consecutive failures stop scheduling, mark status dirty, and fail dependent waits with `StorageFailed`. One successful claim clears the budget; an acknowledged-as-failed but durably committed claim is adopted instead of counted again. Candidate maintenance has its separate five-failure budget, including promotion transactions.

The catalog pool has at most eight connections, borrowed per query/transaction rather than retained per caller thread. Nested transactions reuse the current lease; only the outer transaction commits/rolls back. Transactions cannot call adapters, access external files, or acquire the lifecycle lock in reverse order. Document enumeration uses 128-row keyset pages and releases connections before invoking reader/Chunker code. Status uses indexed per-file cleanup lookup rather than decoding the whole queue per row.

Current catalog schema is **8**. Structural migrations retain latest targets, active runs, candidate state, cancellation, references, vector cache, and cleanup responsibility; interrupted running work is recoverable. Historical `runs`, `wait_operations`, `wait_target_sets`, and `run_dependencies` tables stay removed. Old operation IDs are not wait tokens. Explicit idempotency-key acceptance records remain.

Catalog schema upgrades do not silently choose processing models. Legacy namespaces lacking modern manifests require `migrate_namespace` with explicit adapters and indexing mode. Migration invalidates legacy prepared/index results and rebuilds from available originals, without replaying body caches. Missing originals or capabilities fail explicitly. Old managed layouts become collectible after references retire. This migration does not import an application's separate database.

## Acceptance and remaining limits

The maintained suite must exercise public behavior as well as fault injection: External zero-copy, byte/source mapping, immediate invalidation, late results, same-name recreation, crash recovery, cancellation, rules, independent configurations/dimensions, alias scopes, concurrent resources, partial publication, timeout/retirement, and GC protection. Real Milvus conformance checks accompany lifecycle tests; native Windows tests require Windows.

Remaining limitations are tracked in [backlog.md](backlog.md): segment-dependent BM25 scores in the pinned backend, no metadata/text-only startup when the backend fails, no directory-level strong query, and incomplete host integration acceptance. MFS tests do not demonstrate StashBase migration, real OCR/transcription/model quality, arbitrary blocking filesystem recovery, frozen application packaging, or production-scale latency. Those need their own host/corpus acceptance.
