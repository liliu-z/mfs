# MFS

MFS is an embedded Python 3.12 library for processing, storing, synchronizing,
querying, and searching documents. The normative V1 contract is
[`docs/design.md`](docs/design.md).

```python
from pathlib import Path

from mfs import ByNamespace, MFS, Utf8TextProcessor

mfs = MFS.open(Path(".mfs"), processors=[Utf8TextProcessor()])
mfs.create_namespace("notes", "internal")
mfs.upsert("notes", "hello.md", b"Hello, world!")
result = mfs.search("hello", filters=[ByNamespace("notes")], mode="bm25")
mfs.close()
```
