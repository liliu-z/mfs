# MFS

MFS is an embedded Python library for document and chunk search. It observes or stores source files, prepares searchable text, and runs BM25, vector, or hybrid search with durable background processing.

One instance manages multiple namespaces. Each namespace has its own processors, chunker, embedding configuration, rules, and Milvus collection; the instance shares a SQLite catalog and execution pools. Every search explicitly selects one namespace.

## Install

MFS currently requires **Python 3.13** (`>=3.13,<3.14`). From this checkout:

```sh
uv sync --locked --dev
```

Run examples with `uv run python`. To install the library into another Python 3.13 environment, run `python -m pip install /path/to/mfs`. The project pins its dependencies in [pyproject.toml](pyproject.toml) and [uv.lock](uv.lock); no separate Milvus server is required. CI is configured for Linux, macOS, and Windows.

## Store and search a document

An Internal namespace owns the originals supplied through `upsert` and deletes them through `remove` and garbage collection.

```python
from pathlib import Path

from mfs import MFS, DocumentId, TextMatch, Utf8TextProcessor

mfs = MFS.open(Path(".mfs-state"))
try:
    if "notes" in {item.namespace for item in mfs.list_namespaces()}:
        mfs.open_namespace("notes", processors=[Utf8TextProcessor()])
    else:
        mfs.create_namespace("notes", "internal", processors=[Utf8TextProcessor()])

    accepted = mfs.upsert("notes", "hello.md", b"Hello, world!\n")
    mfs.wait(accepted, timeout=30)

    print(mfs.grep("notes", [TextMatch("Hello")]).items)
    print(mfs.search("notes", "hello", mode="bm25").items)
    document = mfs.read(DocumentId("notes", "hello.md"))
    assert document is not None
    print(document.text)
finally:
    mfs.close()
```

Writes return after durable acceptance; processing and indexing finish in the background. `wait(report)` follows the file's **current** work, including newer changes accepted while waiting. Reports are not historical completion receipts.

## Search an existing directory

An External namespace borrows source paths. MFS never copies or deletes those originals. Change the files yourself, then call `sync`; External namespaces do not accept `upsert` or `remove`.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from mfs import MFS, TextMatch, UnderPath, Utf8TextProcessor

with TemporaryDirectory() as directory:
    base = Path(directory)
    root = base / "files"
    (root / "notes").mkdir(parents=True)
    (root / "notes/hello.md").write_text("Hello from disk!\n", encoding="utf-8")

    mfs = MFS.open(base / "state")
    try:
        mfs.create_namespace("files", "external", root, processors=[Utf8TextProcessor()])
        observed = mfs.sync("files", verify="content")
        if not observed.complete:
            raise RuntimeError(f"Incomplete scan: {observed.failed}")
        mfs.wait(observed, timeout=30)
        hits = mfs.grep("files", [UnderPath("files", "notes"), TextMatch("Hello")])
        print(hits.items)
    finally:
        mfs.close()
```

The state directory and External root must not overlap. External roots belonging to different namespaces may be identical or nested; each namespace must be synced independently. `sync` defaults to `verify="stat"`; choose `"content"` to hash files even when their observed stat is unchanged. Always inspect incomplete scans and skipped files.

## Choose a read operation

| Operation | Purpose | Default result |
| --- | --- | --- |
| `grep(namespace, filters=...)` | Literal/regex text matching and structured path/name filtering | `GrepItem.value` is a `DocumentId` |
| `read(DocumentId(...))` | Read an available document's text and source map | `Document` or `None`; includes Internal original bytes |
| `search(namespace, text, mode=...)` | Ranked BM25, vector, or hybrid retrieval | `SearchItem.value` is a `Chunk` |

`grep` also supports `select="doc"` and `select="chunk"`; `search` supports `select="chunk"` and `select="doc_id"`. Ranked search does not return full documents. `UnderPath` is an External namespace query filter; Internal IDs can be filtered with `ByDocumentId` or path/name affixes.

Both search methods default to `consistency="eventual"` and a **five-second total timeout**, including queueing. Strong grep waits for text; strong ranked search waits for the selected namespace's current index configuration. Neither provides snapshot isolation. `GrepBudget` bounds scanning, and `truncated`/`failures` indicate partial results. See the [interface reference](docs/reference.md) for filters, defaults, and errors.

## Configure processing and indexing

Register processors explicitly. The library includes `Utf8TextProcessor` for `.txt`/`.md`, `PdfProcessor`, and basic `DocxProcessor`. Applications can supply OCR, HTML, transcription, or other processors, along with source maps and optional artifacts. `DefaultChunker` is used when no chunker is supplied. MFS does not include an embedding model or provider client.

A new namespace defaults to `bm25` without an Embedder and `hybrid` with one. The **search call** independently defaults to `mode="hybrid"`, so use `mode="bm25"` for a BM25 namespace. `indexing="off"` retains processing and grep while removing ranked results. `configure_index(..., paused=True)` pauses new indexing; `configure_processing(..., paused=True)` pauses both preparation and new indexing.

Reopening the instance does not reconstruct adapters. Call `open_namespace` with compatible implementations to resume work that needs them; saved configuration and available text can be read without loading a model. Compatibility includes processor/chunker IDs, versions and options, routing, and embedding space and dimension. Use `configure_namespace` to intentionally change configuration.

Configuration changes build a candidate generation while the active generation serves valid results. Once all current members reach terminal states and execution retires, successful members can publish together; failed, blocked, or cancelled members remain diagnosable and retryable. If a nonempty candidate has no successful members and the active generation has published results, MFS retains the active generation, except when turning indexing off. Partial publication does not make the entire namespace ready: `wait` and strong ranked search still report terminal failures.

Ordered include/exclude rules belong to each namespace; MFS does not implicitly read `.gitignore`. Source replacement, deletion, and exclusion revoke old results immediately. A failed replacement never restores stale results. Between syncs, grep can read newer borrowed text than the index contains.

## Operate and recover

Use `document_status`, `list_document_statuses`, `scope_status`, and `namespace_configuration` to inspect progress, errors, blocking reasons, and pending configuration. `cancel` preserves user intent across sync and restart; explicit `retry` or `reprocess` resumes the file.

The default execution policy has four file workers, four ranked-search slots, four separate grep slots, and Processor/Chunker capacity of one heavy and two light tasks. Embedder implementations must handle concurrent calls from workers and queries. Background stages and asynchronous index maintenance have a default 300-second execution deadline.

Search timeout ends the caller's wait. A running adapter or Milvus handler retains its slot and leases until it actually returns. `close(timeout=30)` stops admission and waits for retirement; on `WaitTimeout`, cleanup continues with the instance locked. Call `close` again to wait longer. The host owns any final process termination policy.

Before renaming, moving, or deleting borrowed sources, use `quiesce` to retire the affected source users, perform the file operation and a complete sync inside the lease, then release it before waiting for indexing. The host must serialize its file operations and syncs and include every namespace sharing the source. Startup recovery and migration procedures are documented in the [design](docs/design.md#host-file-operations-and-recovery).

## Documentation and development

- [Documentation guide](docs/README.md): current contracts, open work, and historical evidence.
- [Design](docs/design.md): ownership, execution, visibility, configuration, and recovery guarantees.
- [Interface reference](docs/reference.md): public methods, adapter contracts, defaults, and error handling.
- [Domain language](CONTEXT.md) and [decision log](docs/decision-log.md).
- [StashBase integration](docs/stashbase-integration.md): host responsibilities and migration acceptance.
- [Backlog](docs/backlog.md): remaining limitations and recorded validation.

```sh
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyright
uv run pytest -q
```

Tests exercise real Milvus Lite and crash recovery. Native Windows handle tests skip on other platforms. A strict expected failure tracks the pinned backend's BM25 segment-dependent ranking; backend upgrades must recheck it. A metadata/text-only open when Milvus cannot start is still deferred.
