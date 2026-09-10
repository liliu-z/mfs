# MFS

MFS is an embedded Python 3.13 document search library. It owns accepted inputs,
processing and indexing tasks, with SQLite as the source of truth and one Milvus
collection for BM25 and optional dense vectors.

Writes return after durable acceptance. Background threads commit searchable text
first, then publish complete BM25/dense rows. Grep reads committed SQLite text;
ranked search supports `strong` (wait for indexing) and `eventual` (search now).

```python
from pathlib import Path

from mfs import ByExtension, ByNamespace, MFS, TextMatch, Utf8TextProcessor

mfs = MFS.open(Path(".mfs"), processors=[Utf8TextProcessor()])
try:
    mfs.create_namespace("notes", "internal")
    receipt = mfs.upsert("notes", "hello.md", b"Hello, world!", idempotency_key="hello-v1")
    result = mfs.search(
        "hello",
        filters=[ByNamespace("notes"), ByExtension("md")],
        mode="bm25",
        consistency="strong",
        timeout=30,
    )
    print(result.items)
    print(mfs.query([TextMatch("Hello", smart_case=True)]).items)
    print(mfs.document_status(receipt.id))
finally:
    mfs.close()
```

External files use `create_namespace("files", "external", root)` and
`sync("files", verify="content")`. Sync reports accepted changes; task status
reports processing/indexing failures. `retry`, `reprocess`, `cancel` and
`wait_ready` manage the lifecycle. Callbacks remain ordinary injected Python
functions; concurrent query/document embedding calls must be supported by the adapter.

- [Current API and semantics](docs/design.md)
- [Lifecycle and consistency](docs/indexing-lifecycle.md)
- [StashBase integration mapping](docs/stashbase-integration.md)
- [Verification and remaining application work](docs/backlog.md)

Development: `uv sync --locked --dev`, then `uv run pytest -q`,
`uv run ruff check src tests`, `uv run ruff format --check src tests`, and `uv run pyright`.
