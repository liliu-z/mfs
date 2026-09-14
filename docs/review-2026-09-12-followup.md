# Workspace follow-up and host integration: 2026-09-12

**Historical pre-fix review.** The three implementation findings below were subsequently addressed by concurrent lifecycle work. The [current design](design.md), [backlog](backlog.md), and [integration guide](stashbase-integration.md) describe present library behavior and remaining host responsibilities.

MFS HEAD was `a872bde037c55041d9adb20ebed2ea9952c96f00`, including all working-tree fixes/new source; StashBase HEAD was `7c146e1c1aa1e017f45695ab49dd6f34136d9ee9`, reviewed with its working tree. This review added only its report and did not modify either implementation or pre-existing changes.

## Validation and limits

The full baseline suite reported **146 passed, 3 skipped, 7 warnings in 324.48 seconds**. `ruff check .`, strict Pyright, and `git diff --check` passed. Skips required native Windows handles; warnings were PDF/SWIG deprecations.

Temporary probes separately reproduced the new interleavings; the first two used real SQLite/Milvus with controlled events/faults. Host findings came from source/contract comparison, not a migrated application, real models, or frozen sidecar. Previous fixes from [the initial review](review-2026-09-12.md) were not relisted as open defects.

## I1 / P1: cleanup restored processing after user cancellation

Baseline locations: `Lifecycle.claim` lines 621-628, `cancel` lines 874-879, and `Cleaned` commit lines 773-775 in [_lifecycle.py](../src/mfs/_lifecycle.py).

R1 was indexed, then R2 accepted. The probe paused old-row `delete_document`, cancelled R2, and released cleanup. The final state became `succeeded` and R2 was readable/searchable even though the SQLite cancellation gate remained true.

The first cleanup claim had saved `cleanup_restore_state="pending"`. Cancellation changed state/token but not the saved restore state; subsequent cleanup restored pending and restarted preparation/indexing. This differed from the earlier cancel/reindex stale-plan bug.

The correction needed cleanup to continue independently while restoring execution eligibility from **current** cancellation intent, including reclaim, completion, and reopen. Required regressions included cancellation during cleanup and close/reopen afterward. Evidence: `/tmp/mfs_review_cancel_cleanup_current.py` (historical temporary file).

## I2 / P2: reindex hid a fatal storage fault behind timeout

Baseline locations: `_core.py` lines 835-856 and `Lifecycle.finish_execution` lines 721-726. Repeated rebuild-completion transaction failures correctly exhausted the lifecycle budget and stopped the worker with `storage_error`; reindex's private loop checked neither that fault nor shared readiness.

The probe's `reindex(timeout=5)` waited five seconds and raised `WaitTimeout` after the worker had stopped. Ordinary `wait("n")` immediately raised `StorageFailed`; the default unbounded reindex wait could continue until external intervention.

The correction reused lifecycle fault/completion evaluation with the proper namespace-control ordering. Host dispatch would otherwise hide the actionable failure and potentially block other requests. Evidence: `/tmp/mfs_review_reindex_storage_current.py`.

## I3 / P2: unchanged stat sync still did quadratic path work

Baseline `_sync.py` lines 399-401 compared every existing document with all protected paths to detect file-for-directory replacement. Earlier repeated parent enumeration was fixed, but this independent O(N squared) loop remained. Sync also held the instance mutation lock, delaying other scans/rule/configuration writes; cancellation did not use that lock, so this was not a claim that all methods blocked.

A processing-paused External fixture with unchanged files produced:

| Files | `under()` calls | Profiled sync time | Changed |
| --- | ---: | ---: | ---: |
| 200 | 40,000 | 0.119 seconds | 0 |
| 400 | 160,000 | 0.452 seconds | 0 |
| 800 | 640,000 | 1.771 seconds | 0 |

Profiler overhead prevents treating these as production latency forecasts; call counts directly establish the complexity. The correction used canonical sets and bounded ancestor checks for protected/nonmember paths while retaining case and directory-replacement correctness. Tests needed to assert work scaling with file count and depth, not only `scandir` counts. Evidence: `/tmp/mfs_audit_scan_scaling.py`.

## S1: shared heavy capacity and playback handoff

At the recorded StashBase baseline, `code-review/data-lifecycle.md` required one capacity owner, `server/conversion.ts` configured two light/one heavy lanes, and `server/audio-transcription.ts` temporarily interrupted same-source transcription before playback conversion in that heavy lane.

Moving transcription to MFS while leaving playback in Node would split ownership: playback would no longer find the MFS task to retire, and each scheduler could admit a heavy task. Quiescence can retire source users but is not an application-wide capacity counter.

A host requiring that global budget needed one real admission owner with explicit actual-exit, return, disconnection, and recovery semantics. Later MFS added optional Admission; host-issued grants are not mandatory for standalone MFS. Keeping an external preparation queue instead would require a durable completion protocol, not a Processor waiting implicitly on another queue.

## S2: removing Folder membership must not delete shared outputs

The recorded host represented recent Folders by paths, keyed derived files by absolute source path, and removed outputs under a physical prefix when removing a Folder. A mechanical replacement of old `deletePathPrefix` with parent-namespace drop would still let host cleanup delete borrowed text used by a child namespace.

The contract needed persistent Folder UUIDs, independent membership removal, and separate actual-source deletion. Prefer per-namespace managed extraction, or immutable shared output paths with retention across every Folder/Viewer consumer. MFS's collection isolation and GC cannot preserve files the host deletes itself.

Historical source references: `server/app-config.ts`, `server/derived-store.ts`, and `server/routes/library.ts` in the external StashBase checkout. They are not required relative links in this repository.

## S3: disk transaction acknowledgement is not sync plus wait

The recorded file-transaction contract allowed acknowledging old-identity invalidation while reporting new-index lag; `renameWithRollback` and Folder rename still supported undoing disk operations when the indexing step failed.

A `sync + wait` inside quiescence waits for work that the lease forbids. Waiting after release and then blindly rolling back modifies sources after execution has resumed. The recommended commit point was disk success plus complete durable observation/invalidation of old/new identities. Subsequent indexing failures should be reported separately. If rollback is required after release, reacquire all scopes, compensate, and sync again.

The host also needed replayable file-operation/migration logs before background resume. ScopeLease is not a cross-process journal. Request-ID-based bounded dispatch must keep status/cancellation responsive; at this historical checkpoint the dispatcher was serial, a condition later changed in [the liveness work](review-2026-09-13-liveness.md).

## Recommended structure and acceptance

Reuse one Python daemon and one External namespace per stable Folder identity. MFS owns targets/preparation/checkpoint/chunk/index/recovery; the host owns membership, disk transactions, policy, Viewer/playback, and Library-wide results. Share capacity only where required by host policy.

The recommended sequence was: fix I1-I3; settle S1-S3; validate one Folder with TXT/Markdown/JSON/PDF; then add OCR/audio/video. Acceptance needed cleanup cancellation/restart, fatal rebuild writes, unchanged scan scaling, playback handoff, parent-Folder removal with child reads, lease-held daemon failure, observation-failure compensation, native-helper SIGKILL/reopen, query timeout followed by rebuild/close, and replay of old cancelled/failed state.

All three library defects were subsequently fixed; passing the baseline suite alone had not covered their original windows. Host integration and real application acceptance remain separate work.
