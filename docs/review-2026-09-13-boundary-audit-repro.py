"""Review evidence, not production tests. All MFS data uses temporary directories.

Run with the repository Python 3.13 environment. Optional positional arguments
select probe function names; without arguments every probe runs. Recorded
observations include intentional failures and do not certify fixed behavior.
"""

import importlib.util
import io
import json
import subprocess
import sys
import threading
import time
from contextlib import closing, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mfs import MFS, DefaultChunker, DocumentId, ExecutionPolicy, Utf8TextProcessor


def outcome(fn):
    start = time.monotonic()
    try:
        result = fn()
        return {"result": repr(result), "elapsed": round(time.monotonic() - start, 4)}
    except Exception as e:
        return {
            "error": type(e).__name__,
            "message": str(e),
            "elapsed": round(time.monotonic() - start, 4),
        }


def report(name, **values):
    print(json.dumps({"case": name, **values}, ensure_ascii=False), flush=True)


def open_and_close(path):
    with closing(MFS.open(path)):
        return "opened"


def alias_wait():
    with TemporaryDirectory() as d:
        root = Path(d) / "root"
        root.mkdir()
        (root / "real.txt").write_text("needle")
        (root / "alias.txt").symlink_to("real.txt")
        with closing(MFS.open(Path(d) / "state")) as m:
            m.create_namespace(
                "n",
                "external",
                root,
                processors=[Utf8TextProcessor()],
                indexing="off",
                processing_paused=True,
            )
            r = m.sync("n", "alias.txt")
            a = outcome(lambda: m.wait(r, 0.1))
            b = outcome(lambda: m.wait(DocumentId("n", "real.txt"), 0.1))
            report(
                "alias_wait",
                scope=r.path,
                changed=[i.doc_id for i in r.changed],
                report_wait=a,
                real_wait=b,
                status=repr(m.document_status(DocumentId("n", "real.txt"))),
            )


def unbound_wait():
    with TemporaryDirectory() as d:
        p = Path(d) / "state"
        with closing(MFS.open(p, start_paused=True)) as m:
            m.create_namespace("n", "internal", processors=[Utf8TextProcessor()], indexing="off")
            m.upsert("n", "a.txt", b"needle")
        with closing(MFS.open(p)) as m:
            report(
                "unbound_wait",
                wait=outcome(lambda: m.wait("n", 0.2)),
                status=repr(m.document_status(DocumentId("n", "a.txt"))),
                config=repr(m.namespace_configuration("n")),
            )
            m.open_namespace("n", processors=[Utf8TextProcessor()])
            report("unbound_after_binding", wait=outcome(lambda: m.wait("n", 5)))


def initial_failure():
    with TemporaryDirectory() as d:
        p = Path(d) / "state"
        with patch("mfs._core.Catalog", side_effect=OSError("temporary initialization failure")):
            first = outcome(lambda: open_and_close(p))
        second = outcome(lambda: open_and_close(p))
        report(
            "initial_failure", first=first, reopen=second, files=sorted(x.name for x in p.iterdir())
        )


def initial_crash():
    with TemporaryDirectory() as d:
        p = Path(d) / "state"
        code = """
import os, signal, sys
from pathlib import Path
from unittest.mock import patch
from mfs import MFS
def crash(*args, **kwargs):
    os.kill(os.getpid(), signal.SIGKILL)
with patch('mfs._core.Catalog', side_effect=crash):
    MFS.open(Path(sys.argv[1]))
"""
        child = subprocess.run(
            [sys.executable, "-c", code, str(p)], capture_output=True, timeout=15
        )
        assert child.returncode == -9, child.stderr.decode()
        report(
            "initial_sigkill", exitcode=child.returncode, reopen=outcome(lambda: open_and_close(p))
        )


def maintenance_timeout():
    from mfs._index import ChunkIndex

    entered = threading.Event()
    release = threading.Event()

    class Chunker(DefaultChunker):
        def __init__(self):
            super().__init__()
            self.version = "audit-2"

    original = ChunkIndex.recreate

    def slow(self, *args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(self, *args, **kwargs)

    with (
        TemporaryDirectory() as d,
        closing(MFS.open(Path(d) / "state", execution=ExecutionPolicy(stage_timeout=0.15))) as m,
    ):
        for n in ("a", "b"):
            m.create_namespace(n, "internal", processors=[Utf8TextProcessor()], indexing="off")
        with patch.object(ChunkIndex, "recreate", slow):
            try:
                r = m.configure_namespace("a", chunker=Chunker())
                assert entered.wait(3)
                s = m.configure_namespace("b", chunker=Chunker())
                w = outcome(lambda: m.wait(s, 0.4))
                report(
                    "maintenance_timeout",
                    other_wait=w,
                    first_configuration=repr(m.namespace_configuration("a")),
                    second_configuration=repr(m.namespace_configuration("b")),
                    status=repr(m.status()),
                    stage_timeout=0.15,
                )
            finally:
                release.set()
        report(
            "maintenance_after_release",
            a=outcome(lambda: m.wait(r, 5)),
            b=outcome(lambda: m.wait(s, 5)),
        )


def cleanup_drop():
    from mfs._index import ChunkIndex

    with TemporaryDirectory() as d:
        path = Path(d) / "state"
        with closing(MFS.open(path)) as m:
            m.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
            m.wait(m.upsert("n", "a.txt", b"old needle"), 10)
            with patch.object(
                ChunkIndex, "delete_snapshot", side_effect=OSError("temporary backend outage")
            ):
                m.upsert("n", "a.txt", b"new needle")
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    with m._condition:
                        rows = m._catalog.cleanup_rows("n")
                        if rows and rows[0][1]["failures"] >= 5:
                            break
                    time.sleep(0.05)
                assert rows and rows[0][1]["failures"] == 5, rows
            dropped = m.drop_namespace("n")
            with m._condition:
                assert m._condition.wait_for(
                    lambda: m._tasks.targets[DocumentId("n", "")]["state"] == "succeeded", 5
                )
            report(
                "drop_after_cleanup_failure",
                wait=outcome(lambda: m.wait(dropped, 0.2)),
                collections=m._runtime.legacy_index.client.list_collections(),
                debts=m._catalog.cleanup_rows("n"),
                status=repr(m.status()),
            )
        with closing(MFS.open(path)) as m:
            report("drop_failure_reopen", wait=outcome(lambda: m.wait(dropped, 0.2)))
            m.create_namespace("n", "internal", processors=[Utf8TextProcessor()])
            accepted = m.upsert("n", "a.txt", b"fresh namespace needle")
            report("recreated_namespace_wait", wait=outcome(lambda: m.wait(accepted, 0.2)))
            with m._condition:
                assert m._condition.wait_for(
                    lambda: m._tasks.targets[accepted.id]["state"] == "succeeded", 10
                )
            report(
                "recreated_namespace_indexed",
                status=repr(m.document_status(accepted.id)),
                wait=outcome(lambda: m.wait(accepted, 0.2)),
                hits=len(m.search("n", "needle", mode="bm25").items),
            )
            m.retry(accepted.id)
            report("explicit_retry_clears_old_debt", wait=outcome(lambda: m.wait(accepted, 5)))


def failed_member_activation():
    class Processor(Utf8TextProcessor):
        def process(self, path, media_type):
            if path.read_bytes() == b"bad":
                raise ValueError("unreadable source")
            return super().process(path, media_type)

    class Model:
        dimension = 2
        embedding_space = "audit-space"

        def embed_documents(self, texts):
            return [[1.0, 0.0] for _ in texts]

        def embed_query(self, text):
            return [1.0, 0.0]

    with TemporaryDirectory() as d, closing(MFS.open(Path(d) / "state")) as m:
        m.create_namespace("n", "internal", processors=[Processor()])
        good = m.upsert("n", "good.txt", b"needle")
        m.wait(good, 10)
        bad = m.upsert("n", "bad.txt", b"bad")
        assert outcome(lambda: m.wait(bad, 10)).get("error") == "OperationFailed"
        r = m.configure_namespace("n", embedder=Model(), indexing="hybrid")
        with m._condition:
            assert m._condition.wait_for(
                lambda: (
                    m.document_status(good.id).configuration_revision == r.revision
                    and m.document_status(good.id).state == "succeeded"
                    and m.document_status(bad.id).configuration_revision == r.revision
                    and m.document_status(bad.id).state == "failed"
                ),
                10,
            )
            # Publication is asynchronous; observe the new active generation when
            # supported, while the original all-or-nothing implementation times out.
            m._condition.wait_for(
                lambda: m.namespace_configuration("n").active_revision == r.revision, 1
            )
        report(
            "failed_member_activation",
            good_state=m.document_status(good.id).state,
            bad_state=m.document_status(bad.id).state,
            mode=m.namespace_configuration("n").indexing,
            wait=outcome(lambda: m.wait(r, 0.2)),
            vector=outcome(lambda: m.search("n", "needle", mode="vector")),
            bm25_hits=len(m.search("n", "needle", mode="bm25").items),
        )


def stashbase_barrier():
    path = Path(__file__).resolve().parents[2] / "stashbase/python/stashbase_daemon.py"
    spec = importlib.util.spec_from_file_location("stashbase_audit", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    with redirect_stdout(io.StringIO()):
        spec.loader.exec_module(mod)
    release = threading.Event()
    entered = threading.Event()
    condition = threading.Condition()
    replies = {}

    def slow(*_):
        entered.set()
        assert release.wait(10)
        return {}

    def emit(reply):
        with condition:
            replies[reply["id"]] = reply
            condition.notify_all()

    dispatcher = mod._RequestDispatcher(
        None,
        emit=emit,
        handlers={
            "upsert": slow,
            "bind_folder": lambda *_: {},
            "status": lambda *_: {},
            "scan_diff": lambda *_: {},
        },
    )
    try:
        dispatcher.submit({"id": "write", "op": "upsert"})
        assert entered.wait(2)
        dispatcher.submit({"id": "before", "op": "status"})
        with condition:
            assert condition.wait_for(lambda: "before" in replies, 1)
        for op in ("bind_folder", "status", "scan_diff"):
            dispatcher.submit({"id": op, "op": op})
        with condition:
            returned = condition.wait_for(lambda: "status" in replies, 0.3)
        report(
            "stashbase_pending_bind_barrier",
            status_before_bind=True,
            status_after_bind=returned,
            replies_before_release=sorted(replies),
            pending=[r[1] for r in dispatcher._pending],
        )
    finally:
        release.set()
        dispatcher.close()


if __name__ == "__main__":
    probes = (
        alias_wait,
        unbound_wait,
        initial_failure,
        initial_crash,
        maintenance_timeout,
        cleanup_drop,
        failed_member_activation,
        stashbase_barrier,
    )
    selected = set(sys.argv[1:])
    errors = []
    for fn in probes:
        if selected and fn.__name__ not in selected:
            continue
        try:
            fn()
        except Exception as error:
            errors.append(fn.__name__)
            report(fn.__name__, harness_error=repr(error))
    if errors:
        raise SystemExit(f"probe setup failed: {errors}")
