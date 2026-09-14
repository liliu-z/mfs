# StashBase integration

This is a host integration contract, not a claim that StashBase has migrated. MFS behavior is defined in [design.md](design.md), callable details in [reference.md](reference.md), and unfinished work in [backlog.md](backlog.md).

StashBase implementation observations come from the dated [September 13 liveness review](review-2026-09-13-liveness.md) and [boundary audit](review-2026-09-13-boundary-audit.md), most recently at StashBase `7a6ac737`. They were not re-audited in this documentation cleanup. That snapshot still used `mfs-cli`, but already had bounded request dispatch and propagated search errors. Older reports describing a fully serial dispatcher or swallowed search errors are historical; the later audit identified a remaining bind-barrier queueing issue.

## Instance and source identity

Reuse the existing Node-to-Python daemon chain: one Python process exclusively owns one MFS state directory. Give every registered Library Folder a persistent namespace ID, including nested folders. For example, `/work` can map to `folder-A`, and `/work/project` to `folder-B`. Each independently observes, configures, searches, and drops its membership.

The same physical source can be `(folder-A, project/a.md)` and `(folder-B, a.md)`. Do not merge roots or move one Folder's identity into another. The host determines search-result ownership/deduplication; the recorded keyword path's deepest-Folder ownership is one possible product rule. Source operations and scans must cover every registered namespace sharing the affected paths. A temporary subdirectory selection uses `UnderPath`, not another namespace.

External roots must be outside the MFS state directory. MFS borrows originals and accepts only `sync` for External changes. The host performs disk writes, moves, and deletes. Direct TXT/Markdown grep reads the original; PDF and other binary formats require extracted text. There is no requirement that every file acquire a new Markdown copy.

The recorded application scans on startup, Folder open/switch, window focus, Agent turn end, manual Sync, and MCP reindex. Preserve the relevant application events; MFS installs no filesystem watcher and does not propagate observations between namespaces.

## Responsibility split

| StashBase owns | MFS owns |
| --- | --- |
| Source operations, Folder membership, access control, product visibility | Observation, accepted targets, processing/indexing, invalidation, and recovery |
| Actual enhanced PDF/HTML/OCR/transcription implementations and playback conversion | Invoking Processor, chunking, vector reuse, BM25/dense publication |
| User decisions about starting large indexing batches | `off`/`bm25`/`hybrid`, index pause, and processing pause |
| Fallback selection and UI/MCP error/partial-result presentation | Grep/read/search and document/configuration diagnostics |
| Sibling/generated-file relationships and old-rule import | Ordered namespace rules and consistent eligibility |
| Viewer/playback handles and host disk/transaction journals | Managed text/artifact references, execution leases, ScopeLease, and startup gate |

For a generated playable video, the host can exclude the original and sync the generated file, or choose to search the original instead. It owns generated-input update/deletion relationships. Playback files and presentation JSON do not automatically belong in MFS; use `ProcessedDocument.artifacts` only for attachments whose lifetime should follow prepared search text.

Keep integration orchestration in one host adapter around Folder binding, observation, retrieval, status, and source retirement. HTTP/MCP handlers should not coordinate MFS private fields, configuration generations, or cleanup records individually.

## Processor mapping

The recorded old daemon commonly accepted application-extracted text and then called the old Chunker/Embedder/store, even though `mfs-cli` also had generic converters. The new interface should run actual extraction inside a supplied Processor:

```text
sync(folder namespace)
  -> MFS accepts report.pdf
  -> StashPdfProcessor.process(path, media_type, context) performs extraction
  -> ProcessedDocument(text, source_map, ...)
  -> MFS chunks, embeds, and publishes
```

There is no second extraction step after `process` returns. Reuse Python functions or scripts inside that implementation; use `context.run_process` only when a managed subprocess is needed. Avoid making the Processor wait on another independently owned long-running preparation queue.

| Format / concern | Mapping |
| --- | --- |
| TXT/Markdown | Built-in `Utf8TextProcessor` routes `.txt` and `.md`. |
| JSON/HTML | Supply explicit application processors. HTML may return extracted index text and a `grep_path` for original HTML. |
| PDF/DOCX | Use the built-in basic processors or the application's enhanced extraction, one Processor per route. |
| OCR | Preserve actual completion markers, source locations, and the valid empty-text-success case. |
| Audio/video | Adapt transcription and time mapping; MFS checkpoints do not automatically split media into ten-minute units. |
| Existing derived outputs | Validate source hash, conversion configuration, and completion state; retain immutable borrowed files while referenced. |
| Source/output limits | Keep them separate. The recorded 8 MiB index-text policy must not become a blanket source limit rejecting large PDFs/media. |

`text_path` is a reference to a file containing the required returned `text`; `grep_path` can be a different view. Outputs in `context.work_dir` are persisted as managed copies. Borrowed application outputs outside it remain host-owned and must not be overwritten/deleted while in use. SourceMap offsets refer to processed UTF-8 text; renderer character positions require conversion.

Adapters keep per-file mutable state in locals or the per-attempt context/work directory. Processor `sniff` is fast/stateless and can overlap `process`. Processor/Chunker concurrency is opt-in on the concrete class. Embedder can receive concurrent background and query calls, independently of Processor/Chunker admission; implement model thread safety, native-thread/memory limits, network deadlines, and provider throttling explicitly.

Cancellation checks, progress, and checkpoints are optional capabilities. Page/audio-unit processors can checkpoint their own files and resume from `resume_state`/`resume_files`; monolithic extraction may restart from scratch. Do not turn cancellation into a successful result. Map unavailable dependencies to capability errors, transient failures to `RetryableError`, and corrupt content to processing failure.

## Search and result mapping

| Application intent | MFS method |
| --- | --- |
| Keyword, literal/regex, path/name matching | `grep(namespace, filters=...)` |
| Open a known document's available text | `read(DocumentId(...))` |
| Recorded semantic/hybrid intent | `search(namespace, text, mode="hybrid")` |
| Lexical ranking | `search(namespace, text, mode="bm25")` |
| Dense-only ranking | `search(namespace, text, mode="vector")` |

The old public keyword operation meant disk/derived-text grep, not BM25. The old semantic operation commonly combined dense and BM25. Different chunk windows/candidate limits mean a common Milvus backend does not establish ranking equivalence. Preserve application match/snippet budgets and test byte-to-renderer positioning, case/whole-word behavior, extension filters, and original-source navigation.

Every MFS grep/search selects one namespace. Full-Library/MCP retrieval requires bounded host fan-out, one request deadline, partial-error aggregation, overlapping-source deduplication, and a defined ranking policy. Raw BM25/RRF scores from independent collections are not directly comparable as a global relevance score. Validate any merge/reranking strategy on the application corpus.

Both grep and ranked search default to eventual with five-second total caller budgets. Strong grep waits for current namespace text; strong ranked search waits for its current index configuration. Narrow filters do not narrow strong readiness to a directory. `wait(namespace, path=...)` is a separate current-work wait, not snapshot isolation.

Show `grep.failures` and `truncated` as partial results. Preserve structured `WaitTimeout`, `IndexUnavailable`, `CapabilityUnavailable`, `OperationFailed`, and error retryability through RPC. Do not convert failures into complete empty results. Exact search has a separate execution pool, but backend-independent startup is not implemented: a Milvus open failure can prevent MFS from opening. A host fallback for managed PDF/OCR text therefore needs its own validated retention strategy; ordinary source-text fallback can read disk under host policy.

## Daemon dispatch and resource admission

Retain request-ID pairing, bounded execution, and serialized stdout writes. Create a total request deadline when Node accepts the user request, before queueing, and pass only remaining time to MFS. Expired queued requests should not start. Long sync/wait/reindex calls must leave status, cancellation, and host callbacks runnable.

The latest recorded dispatcher already separated write/search/scan/status/probe capacity. Its pending `bind_folder` barrier could still block later status/scan behind a slow operation; the audit reproduced this with `_RequestDispatcher`, not a full UI journey. Suppress redundant binds using confirmed configuration and daemon generation, and permit safe status snapshots on an independent control path. Reads spanning real store changes still need correct generation/retirement ordering. A request timeout does not necessarily mean the daemon process is dead.

MFS defaults to four file workers, four ranked-query slots, four separate grep slots, and local `heavy=1`/`light=2` Processor/Chunker admission. It does not require Node grants. If the product requires playback and MFS preparation to share one global budget, use one actual capacity owner through optional `Admission`; two independent `heavy=1` counters are not a shared budget. Cross-process grants must be prefetched/asynchronous, not blocking RPC under Lifecycle's lock. Return a grant only when actual execution retires, including after disconnection or timeout.

Use `set_active_scopes` to prioritize the active Folder and checkpoints to yield at durable processing units. Playback handoff is temporary retirement/scheduling, not durable user cancellation. The adapter or host owns model-specific query priority; adding an unconditional shared model mutex can itself put queries behind long background batches.

## Source transactions and shared outputs

For host-controlled rename, move, or deletion:

1. Take host path locks and persist a replayable transaction with Folder identities, old/new ranges, disk steps, and compensation information.
2. Acquire one quiescence lease covering every affected namespace/range and retire host playback/viewer handles.
3. Perform the disk change and content-sync old/new ranges while holding the lease.
4. Check each report's `complete` and `failed`; durably record acceptance before releasing the lease.
5. Release the lease, then optionally wait for indexing and report failures independently.

Do not wait for indexing inside a lease that blocks it. Do not map a later embedding failure into the recorded `renameWithRollback` behavior that undoes an already accepted disk move. Pre-acceptance failure may require compensation/resync inside the lease; if the lease was released, reacquire all relevant scopes first.

A host journal must recover ambiguous disk outcomes by inspecting actual paths, not blindly repeating rename. The recorded `recovery-journal.ts` was an editing-draft journal, not a source move/delete journal. Quiescence is process-local and cannot replace durable host steps. On restart with unfinished steps, use `MFS.open(state, start_paused=True)`, recover under path locks and quiescence, bind adapters and complete observation, then release leases and call `resume_background`.

Removing a Folder removes membership, not the original source tree. `drop_namespace` must not cause shared derived files to disappear while a nested Folder or Viewer still references them. Prefer MFS-managed extraction per namespace, or immutable application paths with retention across all consumers. Actual disk deletion affects every overlapping namespace. If a whole External root disappears, sync is incomplete; explicitly decide whether to drop that registered Folder rather than treating it as an empty scan.

## Rules, pauses, and migration

Import old `.gitignore`/`.mfsignore` and application-specific sibling rules explicitly. The recorded old scanner and fallback paths did not necessarily use identical rule interpretation; translate semantics rather than copying simplified `fnmatch` expressions blindly. Use the same effective policy for MFS and host fallback. `sync(path)` is an observation scope, not a permanent whitelist.

MFS sync durably accepts and schedules work, so it is not a read-only replacement for the old `scan_diff`. If large-batch indexing needs a user decision, install the index pause **before** observation, allow preparation as intended, and then apply the decision. Keep transient budget decisions distinct from persistent user pause so resuming one cannot undo the other.

Initial state migration requires a persistent host log. Create namespaces with `processing_paused=True`, restore rules/settings, sync sources, read current revisions, and use `restore_document_state` to import old cancellation/failure (`TaskError` required for failure). Only after every intended state is restored should `configure_processing(..., paused=False)` admit preparation. Reopen preserves this pause and identical import replay does not undo later explicit retry. Index pause alone still runs processors. `migrate_namespace` only migrates legacy MFS catalogs, not the StashBase database.

Use document `revision`, `text_revision`, `indexed_revision`, `stage/state`, `executing`, `blocking_reason`, and errors to render status. A namespace's global ready flag does not say whether one PDF's text is usable, and a grep hit does not prove its vectors are ready. `wait(sync_report)` follows the current observed scope and rebuilds even when that sync changed no bytes.

## Model changes and index repair

| Host action | MFS mapping |
| --- | --- |
| Manual Sync / recorded MCP reindex / rescan | `sync(namespace, verify="content")`; explicitly retry/reprocess failed targets when requested |
| First enable dense retrieval for accepted files | `configure_namespace(namespace, embedder=..., indexing="hybrid")` |
| Change embedding space/dimension | `configure_namespace(namespace, embedder=...)`, retaining or explicitly choosing hybrid mode |
| Rotate credentials within the same space | Rebind compatible adapters; do not force OCR/transcription/vector recomputation |
| Force repair of the existing index | `reindex(namespace, timeout=...)`; this does not discover unsynced files |

The recorded StashBase `syncFolderNow` path did not mean clearing all vectors. Do not map an application operation name mechanically to MFS `reindex`. Historical provider choices also did not establish an arbitrary model-selection UI.

Configuration reports acknowledge acceptance and can be polled/waited afterward. G0 serves valid sources while G1 builds; successful members can publish as a set while failure/cancellation remains diagnosable. If a nonempty G1 has zero successes and G0 has published results, keep G0 except when disabling indexing. Partial publication means successful documents can use the new model, while whole-namespace wait/strong still reports unhealthy members. A failed wait is not proof of rollback.

On restart, read both manifests/revisions and bind each generation explicitly. New requests replace obsolete candidates; no prior `cancel` is needed. Do not query G0 using G1's Embedder or interpret continued G0 service as authorization to use revoked credentials. Preserve host credential/provider policy separately from index compatibility.

## External rename and computation reuse

A rename observed across old/new scopes deletes the old document identity and adds the new one. Syncing only the destination does not discover removal outside that scope. No separate MFS rename method is required.

The vector cache is independent of old search rows and keyed by namespace incarnation, index epoch, dense configuration, and chunk hash. Complete validated vectors can survive rename and reopen under the 32 MiB per-instance logical LRU bound. Reindex advances the epoch; drop clears its incarnation. There is no cross-namespace reuse or unlimited zero-embedding rename guarantee. A Processor must declare `cache_scope="content"` for compatible cross-path processing reuse; borrowed references must remain valid.

If a transition instead leaves preparation in Node and has MFS borrow completed outputs, define durable incarnation/revision-qualified notification and immutable output retention. Handle notifications arriving before blocked state, stale completions, and user cancellation. Unconditional retry is unsafe because it clears cancellation. This notification interface remains unselected/unimplemented; putting actual preparation inside Processor avoids the second queue's completion protocol.

## Shutdown, packaging, and acceptance

Call `close(timeout=...)` before daemon exit. Success means actual MFS retirement completed; `WaitTimeout` leaves cleanup and ownership active. The host may wait again or escalate process termination within its shutdown budget. On POSIX the supervisor can retain `PROCESS_LOCK` until old native descendants retire; retry `InstanceLocked` within a finite startup budget. Managed commands must not daemonize or escape their session.

Use Python 3.13, replace `mfs-cli` and its private imports/provider assumptions, include `mfs._process_supervisor`, and call `mfs.run_process_supervisor()` before frozen application initialization. Native Windows Job Object, handle-sharing, and hidden-console behavior need real packaging tests; macOS tests do not establish them.

Application acceptance must cover actual TXT/Markdown/JSON/HTML/PDF processors first, then OCR/transcription/playback; overlapping Folders and shared outputs; old-state migration replay; partial configuration failure and retry; source-operation crashes at each journal step; native-helper host death/reopen; timeouts followed by rebuild/drop/close; real provider/local-model limits; and full-Library retrieval evaluation on representative data. These remain host work, not evidence implied by passing library tests.
