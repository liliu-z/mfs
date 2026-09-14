# Concurrency, fault recovery, and host review: 2026-09-13

**Historical review with completed library fixes.** Baselines were clean MFS `6706f850ca941c5e9de62868379cb122c9b363c6` and StashBase `7c146e1c1aa1e017f45695ab49dd6f34136d9ee9`. The review examined whole related modules, not just the latest diff or earlier fix claims. R1-R4 and D1 below preserve pre-fix evidence; line numbers refer to that baseline.

The four P1 defects were subsequently fixed. D1 first received a limited no-text cancellation exception; the broader partial-publication policy in [the September 14 fixes](review-2026-09-14-boundary-fixes.md) superseded it. The current contract is [design.md](design.md).

## Resulting changes at this checkpoint

| Item | Implemented result |
| --- | --- |
| R1 | Indexed per-file cleanup existence checks; no repeated full-queue reads per status item |
| R2 | Shared three-failure claim budget with monotonic 0.25/0.5-second backoff; exhausted failures stop scheduling and report `StorageFailed`; recovery through reopen after storage repair |
| R3 | Final Internal acceptance rereads persisted rules; reopen normalizes older excluded targets into deletion work |
| R4 | Memory refresh/admission hints perform no hidden database mutations; SQLite differences drive candidate reconciliation; persisted targets reject stale cached overwrites |
| D1 | Cancelled inputs without valid text in either generation could be absent from promotion while retaining cancellation; later superseded by CONFIG-PARTIAL-008 |
| Host dispatch | Separate bounded write/search/scan/status/probe execution with serial writes and exclusive rules/bind/close barriers; preserve actual retirement and propagate search faults |

Thirteen regressions in [test_liveness_recovery.py](../tests/test_liveness_recovery.py) covered these and follow-up windows: obsolete candidates hiding immediate cancellation, scheduling hints writing retirement state, and cached targets remaining after SQLite deletion. The Standards and Spec follow-up reviews recorded no unresolved finding at that checkpoint.

Retained evidence: [probe script](review-2026-09-13-liveness-repro.py), [original observations](review-2026-09-13-liveness-results.jsonl), and [post-fix observations](review-2026-09-13-liveness-fixed-results.jsonl). The same synthetic status fixture went from 250 full cleanup-queue reads to zero; page status and concurrent cancellation took about 7 ms after the fix. This is a local comparison, not a universal latency guarantee.

The historical host work improved its existing `mfs-cli` dispatch, not migration to this library. Later [boundary review](review-2026-09-13-boundary-audit.md) still found a pending-bind barrier issue.

## R1 / P1: status polling multiplied cleanup decoding under the global lock

Baseline `_core.py` lines 863/874 and `_catalog.py` line 458 called `cleanup_rows(namespace)` for each document status. A page of P files with D cleanup debts fetched/decoded about P times D rows under `Lifecycle.condition`; `any(...)` did not avoid building the full list.

With 250 targets and 3,000 exact snapshot debts, a public 250-item status page made 250 full-queue queries and took **8.27 seconds**. Concurrent cancel had not returned after 200 ms. A startup gate held the fixture stable; no artificial slow database/sleep amplified the operation.

A separate rerun without the full suite in parallel still took **8.18 seconds**. The 250 SQL fetches totaled 0.443 seconds; `cleanup_rows` including recursive JSON validation/copying totaled 8.042 seconds and decoded **750,000 records**. This was status processing, not search; cancel was waiting for the lifecycle lock, not for Processor or a fixed delay.

Ordinary update/rebuild/slow-query/failed-cleanup states naturally create debt, so repeated polling could stall other Folder status, cancellation, acceptance, and task commits exactly when diagnostics were needed. The fix used the existing cleanup scope index for per-file existence checks. Acceptance checked both query work and concurrent responsiveness, not only returned fields.

## R2 / P1: claim write failures caused a retry storm without a failure state

Baseline `_lifecycle.py` line 896 released resources after failed claim persistence, waited on a condition for 0.25 seconds, then retried. Resource release woke all four workers, so notifications defeated the intended delay. There was no shared fault budget or retry deadline.

Injecting persistent `StorageFailed` only into `Catalog.put_active(non-None)` after accepted input produced **7,940 failed claims in two seconds**. State remained pending with no public/internal storage error, and wait only timed out. `stage_timeout=0.2` did not help because no actual stage had started. Removing the fault allowed same-instance success in this pre-fix probe.

The fix added a shared finite claim-failure budget and monotonic backoff that wakeups cannot shorten. Lost commit acknowledgements still reconcile/adopt committed claims. If failure state itself cannot be persisted, the in-memory instance fault stops publication and makes waits actionable rather than spinning.

## R3 / P1: rule changes during Internal copying left false running work

Baseline `upsert` checked exclusion before copying but not inside final acceptance. If rules excluded the input during copy, rules could not revoke a target that did not yet exist; acceptance then returned added. A worker claimed it, but eligibility rejected its result and retirement left a same-revision running target without an actual owner.

The probe used Internal bytes, paused after the managed original was written but before target acceptance, committed exclusion in another thread, then resumed. It observed `MutationReport(outcome='added')`, later `state='running', executing=False`; wait timed out and retry said `task is still executing`. Close/reopen/rebind reproduced the same state, and a 0.2-second stage deadline could not end an invocation that no longer existed. Grep returned empty; no excluded-body leak was observed. External originals were not copied in this scenario.

Final acceptance now checks current persisted rules under the same lock as rule updates: if the rule commits first, reject with `SourceExcluded`; if acceptance commits first, the rule revokes it and schedules deletion. Reopen normalizes old invalid targets. Source I/O stays outside that lock. Baseline locations: `_core.py` lines 903/925 and `_lifecycle.py` lines 1385/609/940.

## R4 / P1: a post-rule candidate-write failure restored stale SQLite intent

After rule update committed, baseline `remember` updated memory and synchronously invoked candidate synchronization in a second transaction outside the original reconciliation scope. A failure there could stop memory refresh midway.

The probe accepted paused `a.txt`/`b.txt`, requested a new Chunker, waited for candidate members, then excluded `*.txt`. It injected one failure in the first subsequent candidate-member deletion. SQLite held delete targets for both files, but memory held delete for a and upsert for b. After removing the fault and resuming, wait still timed out, b remained pending/not executing, and stale memory wrote b back to upsert in SQLite. This particular original probe did not test convergence after reopen.

The fix made SQLite authoritative, removed writes from memory refresh/scheduling hints, recovered candidate differences independently, and revalidated durable targets before old cached tasks could update them. The rules/candidate path was shared by both ownership kinds; this actual reproduction used Internal. Baseline locations: `_lifecycle.py` lines 1370/563 and `_configuration.py` lines 151/236.

## Design questions recorded at the time

**D1: cancellation versus enabling new capability.** A BM25 namespace had one pre-processing cancelled file and one successful file. Adding hybrid built the healthy candidate successfully but left the entire namespace on BM25, with vector query unavailable. Wait reported cancellation, so this was an explicit design tradeoff, not a silent deadlock. The initial fix allowed absent cancelled members without valid text; later CONFIG-PARTIAL-008 generalized successful-member publication and retained cancellation/diagnostics without mixing models.

**D2: bounded caller response versus actual occupancy.** Query/grep/close deadlines do not kill arbitrary Python/native calls. Four stuck queries can occupy a pool. Current Embedder bypasses Processor/Chunker admission and may receive four worker plus four query calls. Local ONNX adapters need actual thread/native-memory/priority policy; cloud adapters need request limits and deadlines. A shared serial mutex can itself starve queries. Sync/read, reindex's final full scan, and all-member promotion also needed measured scale/deadline expectations; they were not additional reproduced deadlocks.

**D3: backend retrieval quality.** The strict BM25 flush-grouping xfail still reproduced. Hybrid RRF includes BM25, so backend commonality cannot establish migration-equivalent ranking. Representative host corpus evaluation remained required.

## Host integration findings and recommendations

Reuse Node's existing Python daemon, replace old `mfs.store/config/ingest/embedder` calls, and keep one stable External namespace per Folder. MFS owns the actual Processor-to-index lifecycle; the host owns source authorization, disk operations, Viewer/playback, and Library result presentation.

The reviewed host initially executed handlers serially with a ten-minute timeout and converted many search exceptions to empty hits. The subsequent work in this historical checkpoint fixed bounded dispatch and error propagation; those pre-fix descriptions must not be reused as present facts.

Further host requirements were stable Folder/source identity across nested roots; actual format/SourceMap adapters; a pause installed before sync when large-batch indexing needs a decision; explicit distinctions between user pause and temporary budget decisions; optional global admission sharing with playback; quiescence of all source consumers before disk changes; complete old/new observation inside leases; durable host file/intent migration logs; bounded Library fan-out and evaluated score merging; compatible credential rebinding versus model changes; and Python 3.13/frozen/native Windows packaging.

The recorded editing-draft `server/recovery-journal.ts` was not a rename/delete journal. An alternative Node-prepared-output path would need replayable incarnation/revision-qualified completion, including early/late notification and cancellation, rather than unconditional retry. Current full Processor execution avoids that dual-queue contract.

## Recorded validation

Baseline environment: Python 3.13.12 on macOS, real SQLite/Milvus Lite. Baseline full suite: **205 passed, 3 skipped, 1 xfailed in 525.32 seconds**, with seven PDF/SWIG warnings. A targeted source/write/binding/rules subset had **10 passed in 24.06 seconds** but did not cover newly found R3/R4 windows.

The script exercised R1-R4 and D1, including R3 reopen, R2 fault removal, and R4 post-fault stale overwrite. R1 was synthetic metadata, R2 injected claim failure rather than filling a real disk, and event gates controlled concurrency. It recorded defect observations, not maintained success assertions. The initial review did not run real cloud calls, full application journeys, power-failure tests, or Windows packaging.

Post-fix MFS validation: thirteen new tests passed; a related lifecycle selection passed 82 tests; final full suite **218 passed, 3 skipped, 1 xfailed in 563.39 seconds**. Ruff lint/format and Pyright passed.

Historical StashBase validation recorded typecheck, 192 conversion-scheduler tests, 22 retrieval tests, 42 Python tests, documentation checks, Electron tests, and a built Electron smoke test. An initial Node/SQLite native ABI mismatch was corrected by rebuilding `better-sqlite3` before relevant reruns passed. With a real child process/Milvus and local HTTP fixture blocking one index and two search embeddings, status/scan returned in about 5 ms; subsequent delete/close/rebind did not resurrect old rows. These were historical host changes and local fixtures, not cloud-service or complete business-flow acceptance, and were not repeated during documentation cleanup.
