# MFS / StashBase boundary audit: 2026-09-13

**Historical pre-fix audit.** Confirmed discussion items 1, 2, 3, 4, 6, and 7 were later implemented; see [the September 14 fix record](review-2026-09-14-boundary-fixes.md). This report preserves original evidence and scope decisions, not an assertion that the same defects remain.

Baselines were clean MFS `9856ef9` and StashBase `7a6ac737`. The audit read current source, ran tests, and added probes without changing production implementation or StashBase files. Evidence is retained in [the probe script](review-2026-09-13-boundary-audit-repro.py) and [original JSONL](review-2026-09-13-boundary-audit-results.jsonl). It used temporary state/local models and records observations; successful probe execution alone is not correctness acceptance.

Discussion mapping: 1=R1 cleanup, 2=R2 bootstrap, 3=R3 maintenance concurrency, 4=R4 alias waits, 5=S1 host barrier, 6=D1 partial publication, 7=D2 blocking diagnostics, 8=D3 startup fallback, 9=D4 actual-retirement limits. Item 10's cross-process admission was optional. Host items 11-19 were outside library implementation scope; item 13 only applied to an alternative host-preparation protocol. BACKEND-005 remained open.

## R1 / P1: successful collection deletion left exhausted cleanup debt

Baseline locations: [_cleanup.py](../src/mfs/_cleanup.py) lines 25-26, [_indexing.py](../src/mfs/_indexing.py) lines 76-93, and [_lifecycle.py](../src/mfs/_lifecycle.py) lines 302-317.

The probe injected five old-snapshot cleanup `OSError`s, exhausted retries, removed the fault, and dropped the namespace. The drop target succeeded and Milvus listed no collections, yet `wait(drop_report)` kept raising the old cleanup `OperationFailed`, including after reopen. Recreating the same namespace/file produced successful BM25 results, but waiting on the new upsert still raised the old incarnation's debt error.

Exhausted debt was skipped before checking whether its collection still existed, and successful collection drop did not settle it. Wait selected debt by public namespace/path without incarnation separation. Status could show `pending_count=0, failed_count=0, ready=False`. Explicit retry on the new successful file could incidentally reset old debt, without a natural diagnostic path.

The correction needed exact incarnation/generation debt settlement after successful drop **and actual old-call retirement**, replayable on reopen, plus separation of new-file waits from old deletion responsibility. It could not clear debt before late writes were impossible.

## R2 / P1: interrupted first initialization could never reopen

Baseline `_core.py` lines 188-200 created locks/directories before catalog; a failure left a nonempty directory with no recognized catalog. The next open declared corruption.

An injected `OSError` immediately before Catalog construction caused `CorruptState: non-empty mfs_path has no recognizable catalog` after removing the fault. A real child SIGKILL in the same window exited `-9` and produced the same reopen failure. The probes did not fabricate state files or lose already accepted user documents.

The correction needed recognizable, replayable bootstrap ownership while refusing unrelated nonempty directories. Catalog-created/schema-uncommitted windows also required tests. A persistent pre-initialization ownership marker was subsequently implemented.

## R3 / concurrency discussion: one maintenance call delayed every namespace

Baseline configuration/cleanup shared one maintenance thread, and the serialized backend client held one instance-wide call mutex. `stage_timeout` supervised file workers but not collection create/load/drop or independent cleanup; namespace execution counts tracked occupancy without deadlines.

With `stage_timeout=0.15`, the probe blocked namespace a's candidate creation and then configured b. After 0.4 seconds both remained pending without maintenance errors/counts and b's wait timed out. Both completed after a was released. This event-gated control-flow probe did not demonstrate that real Milvus spontaneously hung; status remained responsive.

The audit revised its classification: serial DDL on the **same** collection is reasonable and is not itself a P1 defect. Two independent serialization layers existed, however: one maintenance thread and one backend mutex for all collections, including search/index calls. The probe measured maintenance queueing, not all potential search contention.

The proposed correction was bounded independent maintenance, per-collection serialization after inspecting the pinned backend, and actual-use leases preventing premature drop or late create publication. Simply adding threads cannot overcome a backend requiring global serialization. Later implementation verified the backend and added deadlines while retaining actual handler ownership; the original suggestion to pass RPC deadlines was refined because Lite handlers ignore cancellation. This library-internal change required no Node grants.

## R4 / P2: alias sync waited for the spelling instead of its target

With `alias.txt -> real.txt` and processing paused, `sync(n, "alias.txt")` returned complete with `changed=[real.txt]` but `path=alias.txt`. `wait(report, 0.1)` succeeded immediately while waiting directly for `DocumentId(n, "real.txt")` timed out and the target remained pending.

The report needed canonical wait scope plus additional identities/ranges observed outside that spelling, including unchanged targets. Root redirection that widened observation to `.` needed the same correction; the original run directly exercised the file alias case. No historical operation table was necessary. Baseline `_sync.py` locations were lines 137/161-171 and `link`; `_core.py` wait parsing was lines 407-410.

## S1 / host P1: pending bind barriers blocked later status and scan

The reviewed StashBase already had separate write/search/scan/status/probe capacity and propagated search errors. Earlier descriptions of serial handlers and swallowed search errors were obsolete by this baseline.

Its dispatcher nevertheless stopped scanning the pending queue at a `bind_folder` barrier. A slow upsert followed by a bind prevented later status/scan from using idle slots. Node sent repeated binds without eliminating unchanged confirmed configuration per daemon generation.

The actual `_RequestDispatcher` with controlled handlers let status before the barrier finish, but status/scan after it did not execute within 0.3 seconds. Releasing upsert allowed completion. This was a dispatch-layer probe, not UI or MFS-backend failure. Recorded locations: `python/stashbase_daemon.py` lines 1935-1952; `server/mfs-daemon.ts` lines 188-204/361-382; `server/state.ts` lines 274-300.

Recommendations were generation-aware redundant-bind suppression, safe health/task snapshots independent of store barriers, generation/retirement ordering for reads that genuinely span store changes, and request deadlines starting at Node admission. A timeout should not automatically mark the daemon dead. This remained host work outside the library fixes.

## D1-D4: design and product contracts

**D1, partial publication.** At the baseline only cancelled members with no valid text in either generation could be absent. A successful BM25 file plus a Processor-failed file, upgraded to hybrid, left the good candidate succeeded and bad candidate failed while active stayed BM25 and vectors remained unavailable. This matched the then-current strict design, but one bad OCR blocked otherwise usable new capability. The subsequent policy publishes successful members, retains failure/cancellation/retry state, never mixes spaces, and protects prior publication against nonempty zero-success replacement.

**D2, blocking diagnostics.** Reopen without binding an accepted file's Processor left `pending, attempts=0, executing=False, error=None`; a 0.2-second wait timed out. Binding then completed work in about 0.014 seconds. Since execution had not started, the stage timer could not help. The correction added explicit binding/pause/resource/retirement/configuration reasons and actionable missing-binding failures through existing status/wait interfaces.

**D3, startup fallback.** Grep and ranked queries have separate runtime slots, but `MFS.open` constructs Milvus and validates/loads collections. If the backend cannot open, managed PDF/OCR text is not independently available through MFS. A text-only open or host-retained hash/config-validated fallback would be required. This was a code-identified contract gap, not a simulated corrupt-backend incident, and remained deferred.

**D4, timeout versus actual retirement.** Search/stage/close timeout cannot force-stop arbitrary Python/native calls. Four stuck ranked calls can occupy their pool; Embedder may receive four worker plus four query calls under defaults. Hosts need provider/local-model thread safety, deadlines, native-thread/memory policy, and truthful error presentation. Read/sync do not expose search-style total deadlines. A timed-out caller does not authorize overwriting sources, unloading models, or starting a second daemon before actual ownership retires.

## Host integration context

Reuse one existing Node/Python daemon with stable External namespaces per Folder. Source authorization, disk operations, Viewer/playback, and product policy stay with StashBase; actual preparation runs inside Processor, followed by MFS chunking/indexing/recovery.

Remaining host acceptance included replacing old `mfs-cli` private modules, relative source identity across overlapping Folders, real format processors and SourceMaps, consistent input/fallback rules, separate source/output limits, pause-before-sync for large-batch decisions, old failure/cancellation import, complete source-operation journals with quiescence, compatible model/credential changes, evaluated full-Library fan-out and deduplication, structured errors through Node, responsive bind/status dispatch, and actual shutdown/reopen/Windows/frozen packaging.

Shared Node/Python capacity was optional unless the host required a global playback/preparation budget. The recorded Windows subprocess path did not explicitly set `CREATE_NO_WINDOW`; hidden-console behavior required native host acceptance. A Node-prepared-output transition additionally required incarnation/revision-qualified replayable completion notification; unconditional retry would erase user cancellation. None of these host requirements was presented as already completed by library tests.

## Recorded verification and final scope

The audit itself initially selected R1/R2/R4 plus D1/D2 for fixes, treated R3 as a design discussion, deferred D3, and excluded S1/host integration. R3 was subsequently authorized and implemented with the other boundary changes.

- MFS full suite: **218 passed, 3 skipped, 1 xfailed in 560.50 seconds**; seven PDF/SWIG warnings. Windows skips required native handles; xfail was BM25 flush-dependent ranking.
- Historical StashBase daemon unittest suite: **32 tests OK in 8.890 seconds**.
- Eight probes completed with fourteen observations, including real bootstrap SIGKILL, cleanup failure/drop/reopen/recreation/retry, alias waits, binding recovery, maintenance blockage, failed-member publication, and host bind barriers.
- Probe lint/format passed. Baseline tests passing did not cover the newly found windows and were not a claim that fixes were already implemented.
- No full Electron user journey, real cloud request, actual power failure, or Windows packaging was tested. The audit did not establish that all host requirements were satisfied.
