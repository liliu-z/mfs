# MFS interface reference

This reference covers the current public interface. Import public classes from `mfs`; underscore-prefixed modules are implementation details. See [design.md](design.md) for invariants and recovery semantics and [types.py](../src/mfs/types.py) for complete result fields and protocols.

## Instance and namespace lifetime

| Method | Contract |
| --- | --- |
| `MFS.open(path, *, gc_policy=None, execution=None, admission=None, start_paused=False)` | Open/create an exclusively owned state directory. Returns an `MFS`; always close it in `finally`. Does not bind application models. |
| `close(timeout=30.0)` | Stop new calls and wait for actual retirement. `WaitTimeout` leaves cleanup running and ownership locked; another close can wait again. `None` waits indefinitely. |
| `create_namespace(namespace, kind, root=None, *, processors=(), chunker=None, embedder=None, indexing=None, sync_policy=None, ignore_rules=(), processing_paused=False)` | Create a namespace and its initial collection; returns `NamespaceInfo`. `kind` is `internal` or `external`; only External requires/allows `root`. Existing names raise `NamespaceConflict`. |
| `open_namespace(namespace, *, processors=(), chunker=None, embedder=None, configuration_revision=None)` | Bind compatible runtime objects to active or named pending configuration; returns `NamespaceInfo`. It does not create a namespace. |
| `get_namespace(namespace)` / `list_namespaces()` | Return `NamespaceInfo` / a tuple of registered namespaces. |
| `namespace_configuration(namespace)` | Detached persisted settings, manifests, active/pending revisions, pauses, limits, and maintenance diagnostics. No credentials or runtime objects. |
| `index_configuration(namespace)` | Detached active index manifest. |
| `drop_namespace(namespace)` | Logically remove a namespace and schedule physical cleanup; returns `DropReport`. External originals survive. |
| `resume_background()` | Release the process-local startup recovery gate. |

Processor registration is explicit: `processors=()` does not install built-in converters. Creation uses `DefaultChunker` when omitted and defaults to BM25 without an Embedder or hybrid with one. All deliberate adapter changes use configuration methods; reopening does not silently rebuild incompatible declarations.

`SyncPolicy(exclude_globs=(), max_file_bytes=None)` sets initial exclusions and a nonnegative source-file size limit. Initial exclusions become rules named `exclude-0`, `exclude-1`, etc.; choose distinct IDs for explicit rules. Source-size limits do not bound extracted text, `read`, or model output.

## Sources and current work

| Method | Contract |
| --- | --- |
| `upsert(namespace, doc_id, data, media_type=None, *, idempotency_key=None)` | Internal only; `data` is `bytes` or a `Path`. Copies/persists the original, accepts work, and returns `MutationReport`. |
| `remove(namespace, doc_id)` | Internal only; revoke eligibility and accept deletion. Returns `MutationReport`. |
| `sync(namespace, path=".", *, verify="stat")` | External only; observe a canonical relative path/scope. `verify="content"` forces content hashing. Returns `SyncReport`. No watcher is installed. |
| `wait(target, timeout=None, *, path=".")` | `target` is a `DocumentId`, namespace string, or Mutation/Sync/Drop/ConfigurationReport. Waits for current work and relevant cleanup/control work. A string is a namespace, never an operation ID. |
| `wait_ready(timeout=None)` | Wait for instance-wide index readiness. |
| `cancel(document_id)` | Persist user cancellation; does not wait for active calls to retire. |
| `retry(document_id, stage=None)` | Explicitly resume current file work/associated cleanup and clear user cancellation. |
| `reprocess(document_id)` | Explicitly accept a new processing target for the existing source; returns `MutationReport`. |

`MutationReport` contains `id`, `outcome`, `index_ready`, and `revision`. Outcomes are `added`, `updated`, `unchanged`, `removed`, or `not_found`. `index_ready` is an observation at return, not a durable future guarantee.

`SyncReport` contains `namespace`, canonical `path`, `complete`, `changed`, `removed`, `failed`, `skipped`, `index_ready`, and additional alias `wait_paths`. `changed` is not the complete set that `wait(report)` waits for. `SyncSkipped.reason` distinguishes `excluded`, `too_large`, `symlink`, `special_file`, and `unsupported_media_type`. A complete scan can still skip files by policy. Incomplete reports raise `OperationFailed(state="incomplete")` when waited on.

An idempotency key replays its first accepted result; reuse for a different request raises `IdempotencyConflict`. It does not overwrite newer file state. Subsequent `wait(report)` follows the current file, not the revision originally accepted under the key.

## Retrieval

```text
mfs.grep(namespace, filters=(), select="doc_id", limit=100,
         *, budget=None, consistency="eventual", timeout=5.0)
mfs.search(namespace, text, filters=(), mode="hybrid", select="chunk", limit=10,
           *, consistency="eventual", timeout=5.0)
mfs.read(document_id)
```

The signatures above are reference notation, not standalone runnable Python. Both query methods require one existing namespace. A five-second default budget includes queueing and all execution phases. `timeout=None` disables the deadline, `0` immediately times out, and other values must be finite and nonnegative. An expired caller can return while an already running invocation still holds resources.

| Setting | Grep | Ranked search |
| --- | --- | --- |
| Selection | `doc_id`, `doc`, or `chunk` | `chunk` or `doc_id` |
| Limit | `1..100000`, or `None` subject to budgets | `1..1000` |
| Text | `TextMatch` filters, each pattern `1..16384` UTF-8 bytes | Query text `1..65536` UTF-8 bytes |
| Mode | Literal/regex filters | `bm25`, `vector`, or `hybrid` |
| Strong consistency | Wait for the selected namespace's text, including candidate preparation | Wait for the selected namespace's current index/configuration |
| Result | `GrepResult(items, truncated, failures)` | `SearchResult(items, truncated)` |

`read` returns `Document | None` without a readiness wait. `None` means no currently eligible prepared document is available. Read errors raise directly. The returned Document includes `id`, `snapshot_id`, `media_type`, `text`, `source_map`, and `original` (`bytes` for Internal, `None` for External). `grep(select="doc")` uses budgeted text and always omits original bytes. A configured `grep_path` is also the read text view.

`GrepItem` carries `value` and byte-range `matches`; `SearchItem` adds `score` and currently has empty `matches`. A Chunk includes document/snapshot identity, ordinal, text, byte range, and source locations. Ranked document selection deduplicates documents based on their ranked chunks. Raw scores are not a cross-namespace relevance scale.

### Filters

Top-level filters combine with AND. Alternatives within a multi-value identity/type filter combine with OR; repeated filters intersect.

| Filter | Semantics |
| --- | --- |
| `TextMatch(pattern, regex=False, case_sensitive=False, smart_case=False, whole_word=False)` | Grep only. Literal by default; `regex=True` uses RE2. Smart case enables sensitivity if the pattern contains uppercase characters. Whole-word boundaries treat Unicode alphanumerics and `_` as word characters. Empty-matching regexes are rejected. |
| `ByDocumentId(id_or_sequence)` | Nonempty list of document identities within the selected namespace. |
| `UnderPath(namespace, path=".")` | External query filter covering that path and its descendants; unavailable for Internal queries. This restriction is about query filtering, not the `UnderPath` type used by scope leases. |
| `PathPrefix(value)` / `PathSuffix(value)` | Literal, case-sensitive affix of the source path/Internal ID. |
| `NamePrefix(value)` / `NameSuffix(value)` | Literal, case-sensitive affix of the final path/ID name. |
| `ByExtension(value_or_sequence)` | Source extension, normalized to lowercase with a leading dot. |
| `ByMediaType(value_or_sequence)` | Normalized source media type. |
| `AnyOf(filters)` | Nonempty OR of structured metadata filters, including nested alternatives. Cannot contain `TextMatch`. |

Filters never widen the selected namespace. `UnderPath`/`ByDocumentId` inside `AnyOf` must also use its identity. Ranked search rejects text filters; source metadata filters are applied before backend top-k. There is no `ByNamespace`, `query`, or public Query type.

### Grep budgets and partial results

`GrepBudget` defaults to `max_documents=10000`, `max_bytes=64 * 1024 * 1024`, `max_file_bytes=8 * 1024 * 1024`, and `max_matches=10000`. Values must be positive integers. `limit=None` removes the explicit item cap, not these budgets. A budget/limit can set `truncated=True` without a file error.

Unreadable or temporarily unavailable files yield `GrepFailure(id, TaskError)` and set `truncated=True`; other hits remain available. Consumers should inspect both fields rather than interpreting partial empty results as a complete no-match answer. Invalid queries and persistent storage corruption still fail the whole call.

## Rules and configuration

```python
from mfs import IgnoreRule

current = mfs.rules("files")
mfs.update_rules(
    "files",
    expected_revision=current.revision,
    add=[
        IgnoreRule("generated", "generated/"),
        IgnoreRule("keep-readme", "generated/README.md", action="include"),
    ],
)
```

This snippet assumes an open External namespace named `files`. `update_rules` also accepts `remove` (rule IDs), `replace` (rules with existing IDs), and `order` (the complete resulting ID order). It returns a new `RuleSet`. See [rule semantics](design.md#rules-and-eligibility).

| Method | Contract |
| --- | --- |
| `configure_namespace(namespace, *, processors=None, chunker=None, embedder=None, indexing=None)` | Accept the latest desired configuration; returns `ConfigurationReport(namespace, revision, changed)`. Unspecified values preserve the latest bound candidate/active adapters. `None` does not explicitly remove an Embedder. |
| `configure_index(namespace, *, indexing=None, paused=None)` | Update index mode and/or index pause; returns `None`. Use `wait(namespace)` to wait afterward. |
| `configure_processing(namespace, *, paused)` | Set the persistent preparation/indexing admission pause; returns `None`. Use quiescence if actual source-user retirement is required. |
| `reprocess_namespace(namespace, *, processors)` | Force namespace preparation using a supplied Processor set; returns `ConfigurationReport`. This explicit request can clear user cancellation. |
| `reindex(namespace, timeout=None, *, chunker=None, embedder=None, processors=None, indexing=None)` | Force and wait for an index rebuild; returns `ReindexReport(documents, chunks, dense_enabled)`. Processor declarations must remain compatible. Does not sync External sources. |
| `migrate_namespace(namespace, *, processors, indexing, chunker=None, embedder=None, ignore_rules=())` | Explicitly migrate a legacy MFS namespace from available originals; returns `NamespaceInfo`. Does not import host databases. |

Indexing modes are `off`, `bm25`, and `hybrid`; `vector` is a query mode only. Partial configuration publication is supported. A failed configuration wait does not imply rollback; consult active/pending revisions and member diagnostics. See [promotion conditions](design.md#configuration-and-indexing-control).

## Adapter implementation

[types.py](../src/mfs/types.py) defines structural protocols; inheriting from a base class is unnecessary.

- **Processor:** attributes `id`, `version`, JSON-compatible `options`, `media_types`, and `suffix_media_types`; methods `sniff(head: bytes)` and either `process(path, media_type)` or `process(path, media_type, context)`. Return `ProcessedDocument`. Keep per-file state out of shared object fields. `sniff` is fast/stateless and may run concurrently with `process` outside its grants.
- **Chunker:** attributes `id`, `version`, JSON-compatible `options`; method `chunk(text, source_map)` returning `Sequence[ChunkRange]`. Ranges use UTF-8 bytes, fully cover nonempty text without gaps, may overlap, and cannot split code points or exceed 65,535 bytes. Return no ranges for empty text.
- **Embedder:** attributes `embedding_space` and positive integer `dimension`; methods `embed_documents(texts)` and `embed_query(text)`. Return finite vectors with matching count/dimension. The object must support concurrent worker and query calls; provider throttling and local model constraints belong to the implementation.

`ProcessedDocument` takes `text`, `source_map`, optional `artifacts: Mapping[str, Path]`, `text_path`, and `grep_path`. `text_path` must decode to `text`; it does not replace the required text field. SourceMap version 1 uses ordered, non-overlapping half-open UTF-8 byte ranges with JSON source descriptors. See [text semantics](design.md#adapter-interfaces-and-text).

`ProcessingContext` exposes `document_id`, `revision`, `content_hash`, per-attempt `work_dir`, `cancellation`, `resume_state`, and immutable `resume_files`. Its operations are `report_progress(completed, total=None, unit=None)`, `checkpoint(state, *, files=None)`, and `run_process(argv, *, timeout=None, env=None)`. Copy checkpoint files into `work_dir` before modifying them. Checkpoints may yield using internal exceptions; do not catch `BaseException` around them. `run_process` runs without a shell, returns a `CompletedProcess[bytes]`, checks the exit status, and retires descendants. Adapters own argument/output-size policy.

Optional Processor/Chunker declarations are `concurrency`, `workload="heavy" | "light"`, or a `resources` mapping. Defaults are one concurrent invocation and one heavy resource. Only declarations on the concrete class opt into concurrency; subclasses must redeclare. `resources={}` requests no compute resource but retains the object concurrency gate. Processor `cache_scope="content"` opts into compatible cross-path processing reuse. Embedder ignores these admission declarations.

## Status, leases, and maintenance

| Method | Contract |
| --- | --- |
| `status()` | Instance counts, index state, dense availability, readiness, and failure/pending counts. |
| `document_status(id)` | Current `DocumentStatus` or `None`. Includes blocking reasons, errors, progress, actual execution, and cleanup. |
| `list_document_statuses(namespace=None, *, path=".", limit=100, offset=0)` | Paginated status; limit `1..1000`, offset nonnegative. |
| `scope_status(namespace=None, path=".")` | Counts grouped by task state and stage. |
| `set_active_scopes(scopes)` | Set scheduling hints, not source membership or authorization. |
| `quiesce(scopes, timeout=None)` | Acquire a `ScopeLease` for a nonempty sequence of `UnderPath` scopes. Context-manage it or call `close`; release before waiting on indexing. |
| `restore_document_state(id, *, expected_revision, state, error=None)` | Import `failed`/`cancelled` state for a matching unexecuted upsert under processing pause; failure requires `TaskError`. Replay does not undo later explicit user actions. |
| `open_artifact(id, name)` | Return a context-managed `ArtifactHandle` protecting an immutable managed file. |
| `collect_garbage()` / `garbage_collection_status()` | Run bounded collection / read the last `GCReport`, including `busy` and `error`. |

`GCPolicy` defaults: enabled, interval 3,600 seconds, idle delay 30 seconds, grace 3,600 seconds, batches of 32 files/0.05 seconds, and cycles of 256 files/1 second. Its logical work budgets do not guarantee interruption of one blocked OS operation.

## Errors

Public errors derive from `MFSError`, which provides `code`, `message`, and optional `document_id`/`path`. See [errors.py](../src/mfs/errors.py) for the full set.

| Error | Caller action/meaning |
| --- | --- |
| `NamespaceConflict` / `NamespaceNotFound` | Correct create/open/name handling. |
| `NamespaceCompatibilityError` | Supply matching declarations, explicitly configure a change, or reread a generation changed during binding. |
| `WrongNamespaceKind`, `InvalidPath`, `InvalidDocumentId`, `InvalidNamespace`, `InvalidFilter`, `InvalidPattern`, `InvalidQuery`, `InvalidConfiguration` | Correct the request. |
| `RootOverlap` | Choose a state directory separate from source roots. |
| `RuleConflict` | Reread rules and resolve the optimistic update conflict. |
| `SourceExcluded` / `UnsupportedMediaType` | Correct source rules/routing or intentionally skip the input. |
| `SourceUnavailable` / `SourceChanged` | Inspect source lifetime and sync/reprocess the intended current input. |
| `CapabilityUnavailable` | Bind/provide the required capability or release a temporary restriction. |
| `IndexUnavailable` / `MigrationRequired` | Explicit index repair / namespace migration is required. |
| `OperationFailed` | Current work is failed, blocked, cancelled, or an observation was incomplete. Inspect `state`, `revision`, `error_code`, and `retryable`, plus document/configuration status. |
| `RetryableError` | Adapter-requested bounded automatic retry; use for transient failures. |
| `ExecutionTimeout` | Background stage exceeded its execution budget; explicit retry waits for actual prior exit. |
| `WaitTimeout` | Caller wait expired; accepted/running work may continue. |
| `StorageFailed`, `CorruptState`, `SchemaVersionUnsupported` | Storage cannot safely proceed; diagnose/recover it rather than treating the result as empty. |
| `InstanceLocked` / `Closed` | State still has an owner / the instance is closing or closed. |

Strong readiness, partial-result handling, and actual retirement are specified together in the [design](design.md#search-reads-and-waits); error names alone are not enough to infer rollback or resource release.
