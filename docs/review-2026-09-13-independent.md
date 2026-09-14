# Independent MFS and StashBase review: 2026-09-13

**Historical completed review.** At **2026-09-13 12:23 UTC**, four independently reproduced defects, two P1 and two P2, had all passed repeat verification with their original trigger mechanisms after fixes. No additional unresolved P1/P2 was confirmed by this reviewer. See [design.md](design.md) for the current contract.

The independent review read the related modules in both repositories rather than relying on diff or earlier conclusions. MFS started at `a872bde037c55041d9adb20ebed2ea9952c96f00` with working changes. It consulted StashBase's local instructions and lifecycle/file-transaction/search contracts. The reviewing agent changed no implementation/tests or StashBase files; its only repository write was this report. Temporary script paths below were local probe provenance and are not retained repository assets.

## 1. P1: a second source open during sniff could block the entire instance

The baseline `_sync.py` path called `_admit` after safe canonical opening; `_core.py` then reopened the source for sniff while holding `Lifecycle.condition`. An unknown-suffix file could be replaced with a FIFO between validation and that second open. Even built-in PDF sniff could trigger it without parsing/model work.

`/tmp/mfs_independent_fifo_probe.py` used real `os.mkfifo` and recorded:

```text
status_blocked_after_200ms True
healthy_grep WaitTimeout 0.103
close_blocked_after_200ms True
```

Only writing to the FIFO released the call; sync then reported complete. The fix reused the verified descriptor's head/selected route, kept sniff outside the state lock, and safely opened Internal inputs too.

Repeat verification using `/tmp/mfs_independent_fifo_verify.py` recorded `status_blocked_after_200ms False`, `healthy_grep_ok 0.001`, and `close_blocked_after_200ms False`. Cleanup allowed ENXIO when no FIFO reader remained. Source inspection confirmed the same correction. External acceptance still does not guarantee the path remains unchanged after final validation.

Historical fix locations: `_sync.py` lines 260/280 and `_core.py` lines 914/1384. **Closed.**

## 2. P1: reconstructing transient text bypassed Processor admission

A Processor returning `grep_path` plus in-memory index text could release transient index text after publication. A subsequent Chunker change or reopen then caused `Preparation.read_text` to call Processor directly while running under a chunk/embed lease, bypassing the actual Processor's serial/heavy grants.

`/tmp/mfs_independent_transient_admission_probe.py` prepared two files, then changed only Chunker. It reported:

```text
two_processor_calls_at_once True maximum 2
execution_stages ['chunk', 'chunk']
processor_admission_used 0
```

The fix returned `NeedsPreparation`, retired the chunk/embed invocation, and reentered process with the correct object/resource admission instead of nesting resource waits.

The same two-file/single-concurrency heavy-Processor mechanism in `/tmp/mfs_independent_transient_admission_verify.py` reported `two_processor_calls_at_once False maximum 1`, `execution_stages ['process']`, and `processor_admission_used 1`. Configuration finished after release and maximum concurrency remained one.

Historical locations: `_preparation.py` line 267, `_runtime.py` line 76; corrections in `_indexing.py`, `_lifecycle.py`, and `_preparation.py`. **Closed.**

## 3. P2: promotion failures never exhausted the five-failure budget

Maintenance cleared `building.error/failures/next_run` before attempting promotion. Repeated promotion writes could therefore fail forever while every recorded failure looked like the first.

`/tmp/mfs_independent_promote_probe.py` injected `StorageFailed` only at promotion's namespace write, while reconciliation reads worked. It observed `promotion_attempts 7 pending_failures 1` and `wait_result WaitTimeout`.

The correction made the budget cover the full pass including promotion, clearing errors only after success. `/tmp/mfs_independent_promote_verify.py` then stopped at `promotion_attempts 5 pending_failures 5`, produced `OperationFailed`, and made no sixth attempt during another second. Removing the fault and resubmitting the same manifest preserved revision and promoted in the same process: `same_process_retry_promoted True`.

Historical location: `_configuration.py` maintenance around lines 252/289. **Closed.**

## 4. P2: dropped namespaces retained old model objects

Promotion left entries in `runtime.build_bindings`, and drop only removed active bindings; retirement iterated only still-registered namespaces. Idle workers also retained their previous permit.

`/tmp/mfs_independent_drop_binding_probe.py` created/configured/dropped the same namespace three times, removed application references, and ran Python GC. It observed `namespaces ()`, `retained_model_instances 3`, and `retained_build_bindings 3` despite completed deletion.

The correction removed obsolete generation/namespace bindings and idle permit references. Actual queries/executions retained their own object references until exit, without closing host-owned shared adapters early. The unchanged three-cycle probe then reported zero retained models and build bindings.

Historical locations: `_core.py` lines 754/761, `_configuration.py` lines 393/444, `_runtime.py` line 96, `_worker.py` line 42. **Closed.**

## Other independently checked contracts

Permits captured revision, attempt, incarnation, and configuration; publication/query validation retained source input identity and generation leases. Binding installation rechecked generation. No additional stale-query/model overwrite defect was confirmed.

Candidate members and prepared references were persisted together and recovered after lost acknowledgements. Failure to read back persistent outcomes stopped publication and required reopen; it was not a reason for blind retries. Grep/ranked search had separate bounded pools and shared total-deadline semantics. File-level read/link/quiescence failures produced partial grep results; storage failures remained explicit.

Safe source reads registered leases; managed files had references/pins. Quiescence covered actual source users and startup gating supported host recovery. These mechanisms did not replace host source transaction locks or constrain external editors. Close retained actual execution ownership, and precise snapshot cleanup remained independent of current source state. File GC was correctly distinguished from Python adapter-object retention.

The independent selected suite recorded **23 passed in 64.82 seconds**: `test_review_boundaries.py`, `test_search_timeout.py`, and the named cases `test_failed_candidate_keeps_active_search_and_retry_can_promote`, `test_boot_gate_allows_path_recovery_before_any_execution`, `test_quiescence_waits_for_source_reads_and_blocks_new_reads`, and `test_managed_grep_text_survives_gc_and_reopen`. That suite ran before the final four corrections; afterward the reviewer reran each original probe mechanism successfully. It did not rerun or claim the primary implementer's full suite.

## Host mapping and unverified work

At this review's snapshot StashBase still depended on `mfs-cli[onnx]` and private old modules, so changing the package version alone was insufficient. Its then-serial dispatcher/ten-minute timeout required adaptation; later host dispatch improvements are documented in [the liveness review](review-2026-09-13-liveness.md).

The host needed stable Folder IDs and canonical relative source identity, actual format processors/SourceMaps, full-Library bounded fan-out and evaluated ranking, complete structured error propagation, source-operation/quiescence RPC and durable journals, old-state migration, and optional shared resource grants. Node retained authorization, Viewer/playback, source transactions, and credential/provider policy. Continuing to serve G0 did not authorize use of revoked credentials. A product/backend mismatch for hosted search was also host work, not a library defect.

Not verified here: native Windows Job Object/handle sharing; real frozen desktop startup and native retirement; large real directories, blocked NAS/FUSE I/O, or million-member promotion latency; actual StashBase database migration and cross-process admission; user data/cloud credentials/services; or every possible scheduling interleaving. Probes used temporary synthetic inputs. No inference of full application readiness follows from these four closed defects.
