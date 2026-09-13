# pyright: reportPrivateUsage=false
from __future__ import annotations

import json
import threading
from collections.abc import Sequence
from pathlib import Path

import pytest

from mfs import (
    MFS,
    DocumentId,
    GCPolicy,
    GrepBudget,
    NamespaceCompatibilityError,
    RootOverlap,
    TextMatch,
    Utf8TextProcessor,
    WrongNamespaceKind,
)


class Model:
    def __init__(self, dimension: int, space: str) -> None:
        self.dimension = dimension
        self.embedding_space = space
        self.calls = 0

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls += len(texts)
        return [self.embed_query(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return [float(len(text) + i + 1) for i in range(self.dimension)]


def test_external_references_live_text_without_copy(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    original = source / "a.md"
    original.write_text("unique original searchable content")
    state = tmp_path / "state"
    mfs = MFS.open(state, gc_policy=GCPolicy(enabled=False))
    try:
        mfs.create_namespace("files", "external", source, processors=[Utf8TextProcessor()])
        receipt = mfs.sync("files")
        mfs.wait(receipt, 10)
        assert mfs.search("files", "searchable", mode="bm25").items
        assert mfs.grep("files", [TextMatch("original")]).items
        for directory in ("objects", "staging"):
            assert list((state / directory).iterdir()) == []
        for path in (state / "namespaces").rglob("*"):
            if path.is_file():
                assert b"unique original searchable content" not in path.read_bytes()
        # The only permanent plain text copy used by indexing is the Milvus chunk.
        for table in ("documents", "targets"):
            values = mfs._catalog.connection.execute(f"SELECT value FROM {table}").fetchall()
            assert all("unique original searchable content" not in value for (value,) in values)
        original.write_text("changed live text")
        assert mfs.grep("files", [TextMatch("changed")]).items
        assert not mfs.grep("files", [TextMatch("original")]).items
        document = mfs.read(DocumentId("files", "a.md"))
        assert document is not None and document.text == "changed live text"
        with pytest.raises(WrongNamespaceKind):
            mfs.upsert("files", "a.md", b"no")
        with pytest.raises(WrongNamespaceKind):
            mfs.remove("files", "a.md")
        original.unlink()
        missing = mfs.grep("files", [TextMatch("changed")])
        assert not missing.items and missing.truncated
        assert missing.failures[0].error.code == "SourceUnavailable"
        mfs.wait(mfs.sync("files"), 10)
        assert not mfs.search("files", "searchable", mode="bm25").items
    finally:
        mfs.close()


def test_namespaces_have_independent_models_and_reopen_binding(tmp_path: Path) -> None:
    state = tmp_path / "state"
    mfs = MFS.open(state)
    try:
        for namespace, dim in (("a", 3), ("b", 7)):
            mfs.create_namespace(
                namespace,
                "internal",
                processors=[Utf8TextProcessor()],
                embedder=Model(dim, namespace),
            )
            mfs.wait(mfs.upsert(namespace, "same.txt", b"hello namespace"), 10)
        for namespace in ("a", "b"):
            assert {
                i.value.document_id.namespace for i in mfs.search(namespace, "hello").items
            } == {namespace}
        collections = mfs._runtime.legacy_index.client.list_collections()
        assert len(collections) == 2
        dimensions: set[int] = set()
        for name in ("a", "b"):
            configuration = mfs.index_configuration(name)
            assert isinstance(configuration, dict)
            dense = configuration["dense"]
            assert isinstance(dense, dict) and isinstance(dense["dimension"], int)
            dimensions.add(dense["dimension"])
        assert dimensions == {3, 7}
    finally:
        mfs.close()
    mfs = MFS.open(state)
    try:
        with pytest.raises(NamespaceCompatibilityError, match="dimension"):
            mfs.open_namespace("a", processors=[Utf8TextProcessor()], embedder=Model(7, "a"))
        with pytest.raises(NamespaceCompatibilityError, match="embedding_space"):
            mfs.open_namespace("a", processors=[Utf8TextProcessor()], embedder=Model(3, "wrong"))
        mfs.open_namespace("b", processors=[Utf8TextProcessor()], embedder=Model(7, "b"))
        assert mfs.search("b", "hello").items
        mfs.open_namespace("a", processors=[Utf8TextProcessor()], embedder=Model(3, "a"))
        assert len(mfs.search("a", "hello").items) == 1
    finally:
        mfs.close()


def test_replacement_hides_old_results_while_processor_is_blocked(tmp_path: Path) -> None:
    started, release = threading.Event(), threading.Event()

    class Blocking(Utf8TextProcessor):
        def process(self, staged_path: Path, media_type: str):
            if staged_path.read_text() == "replacement":
                started.set()
                release.wait(10)
                raise ValueError("conversion failed")
            return super().process(staged_path, media_type)

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Blocking()])
        mfs.wait(mfs.upsert("n", "a.txt", b"old searchable"), 10)
        assert mfs.search("n", "searchable", mode="bm25").items
        replacement = mfs.upsert("n", "a.txt", b"replacement")
        assert started.wait(5)
        assert not mfs.grep("n", [TextMatch("old")]).items
        assert not mfs.search("n", "searchable", mode="bm25", consistency="eventual").items
        assert mfs.read(replacement.id) is None
        release.set()
        from mfs import OperationFailed

        with pytest.raises(OperationFailed):
            mfs.wait(replacement, 10)
        assert not mfs.search("n", "searchable", mode="bm25", consistency="eventual").items
        assert len([t for t in mfs._workers if t.name.startswith("mfs-worker-")]) == 4
    finally:
        release.set()
        mfs.close()


def test_state_root_overlap_rejected_and_grep_budget_is_visible(tmp_path: Path) -> None:
    root = tmp_path / "source"
    (root / "nested").mkdir(parents=True)
    (root / "a.txt").write_text("token " * 100)
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("a", "external", root, processors=[Utf8TextProcessor()])
        with pytest.raises(RootOverlap):
            mfs.create_namespace("b", "external", tmp_path, processors=[Utf8TextProcessor()])
        mfs.wait(mfs.sync("a"), 10)
        result = mfs.grep("a", [TextMatch("token")], budget=GrepBudget(max_matches=3))
        assert result.truncated and len(result.items[0].matches) == 3
        assert not hasattr(mfs, "query")
        assert "text" not in json.loads(
            mfs._catalog.connection.execute("SELECT value FROM documents").fetchone()[0]
        )
    finally:
        mfs.close()


def test_namespace_rules_include_children_and_update_atomically(tmp_path: Path) -> None:
    from mfs import IgnoreRule, RuleConflict

    root = tmp_path / "source"
    (root / "private").mkdir(parents=True)
    for name in ("public.md", "private/keep.md", "private/secret.md"):
        (root / name).write_text("needle " + name)
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace(
            "n",
            "external",
            root,
            processors=[Utf8TextProcessor()],
            ignore_rules=[
                IgnoreRule("private", "private/"),
                IgnoreRule("keep", "/private/keep.md", "include"),
            ],
        )
        mfs.wait(mfs.sync("n"), 10)
        assert {i.value.doc_id for i in mfs.grep("n").items} == {"public.md", "private/keep.md"}
        old = mfs.rules("n")
        new = mfs.update_rules(
            "n", expected_revision=old.revision, add=[IgnoreRule("public", "/public.md")]
        )
        assert {i.value.doc_id for i in mfs.grep("n").items} == {"private/keep.md"}
        assert {
            i.value.document_id.doc_id
            for i in mfs.search("n", "needle", mode="bm25", consistency="eventual").items
        } == {"private/keep.md"}
        with pytest.raises(RuleConflict):
            mfs.update_rules("n", expected_revision=old.revision, remove=["keep"])
        assert mfs.rules("n") == new
        mfs.update_rules("n", expected_revision=new.revision, order=["keep", "private", "public"])
        assert not mfs.grep("n").items
        latest = mfs.rules("n")
        mfs.update_rules("n", expected_revision=latest.revision, remove=["private", "public"])
        mfs.wait(mfs.sync("n"), 10)
        assert len(mfs.grep("n").items) == 3
    finally:
        mfs.close()
    mfs = MFS.open(tmp_path / "state")
    try:
        assert [r.rule_id for r in mfs.rules("n").rules] == ["keep"]
    finally:
        mfs.close()


def test_nested_and_identical_external_roots_keep_independent_namespace_lifecycles(
    tmp_path: Path,
) -> None:
    from mfs import IgnoreRule

    root = tmp_path / "source"
    nested = root / "nested"
    nested.mkdir(parents=True)
    original = nested / "a.txt"
    original.write_text("needle original")
    (root / "top.txt").write_text("needle top")
    mfs = MFS.open(tmp_path / "state")
    try:
        for name, path in (("child", nested), ("parent", root), ("same", nested)):
            mfs.create_namespace(name, "external", path, processors=[Utf8TextProcessor()])
            mfs.wait(mfs.sync(name), 10)
        assert {i.value.document_id for i in mfs.search("parent", "needle", mode="bm25").items} == {
            DocumentId("parent", "nested/a.txt"),
            DocumentId("parent", "top.txt"),
        }
        for namespace in ("child", "same"):
            assert {
                i.value.document_id for i in mfs.search(namespace, "needle", mode="bm25").items
            } == {DocumentId(namespace, "a.txt")}
        mfs.configure_index("child", paused=True)
        original.write_text("needle changed")
        child_sync = mfs.sync("child", verify="content")
        mfs.wait(mfs.sync("parent", verify="content"), 10)
        assert not mfs.search("child", "needle", mode="bm25").items
        assert mfs.search("parent", "changed", mode="bm25").items
        assert mfs.search("same", "original", mode="bm25").items
        rules = mfs.rules("parent")
        mfs.update_rules(
            "parent", expected_revision=rules.revision, add=[IgnoreRule("nested", "nested/")]
        )
        assert not mfs.search("parent", "changed", mode="bm25").items
        assert mfs.search("same", "original", mode="bm25").items
        mfs.configure_index("child", paused=False)
        mfs.wait(child_sync, 10)
        mfs.wait(mfs.drop_namespace("parent"), 10)
        assert original.read_text() == "needle changed"
        assert mfs.search("child", "changed", mode="bm25").items
        assert mfs.search("same", "original", mode="bm25").items
    finally:
        mfs.close()


def test_external_root_can_retarget_into_another_namespace_root(tmp_path: Path) -> None:
    previous, current = tmp_path / "previous", tmp_path / "current"
    previous.mkdir()
    current.mkdir()
    (previous / "a.txt").write_text("old needle")
    (current / "a.txt").write_text("new needle")
    alias = tmp_path / "alias"
    alias.symlink_to(previous, target_is_directory=True)
    mfs = MFS.open(tmp_path / "state")
    try:
        for name, root in (("alias", alias), ("current", current)):
            mfs.create_namespace(name, "external", root, processors=[Utf8TextProcessor()])
            mfs.wait(mfs.sync(name), 10)
        alias.unlink()
        alias.symlink_to(current, target_is_directory=True)
        mfs.wait(mfs.sync("alias", verify="content"), 10)
        for namespace in ("alias", "current"):
            assert not mfs.search(namespace, "old", mode="bm25").items
            assert {
                i.value.document_id.namespace
                for i in mfs.search(namespace, "new", mode="bm25").items
            } == {namespace}
    finally:
        mfs.close()


def test_pause_and_off_keep_processing_and_grep(tmp_path: Path) -> None:
    model = Model(3, "space")
    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.create_namespace("n", "internal", processors=[Utf8TextProcessor()], embedder=model)
        mfs.configure_index("n", paused=True)
        receipt = mfs.upsert("n", "a.txt", b"needle first")
        with mfs._condition:
            assert mfs._condition.wait_for(
                lambda: (
                    (status := mfs.document_status(receipt.id)) is not None
                    and status.text_revision is not None
                ),
                10,
            )
        assert model.calls == 0
        assert mfs.grep("n", [TextMatch("needle")]).items
        assert not mfs.search("n", "needle", consistency="eventual").items
        mfs.configure_index("n", paused=False)
        mfs.wait(receipt, 10)
        assert model.calls == 1
        mfs.configure_index("n", indexing="off")
        assert not mfs.search("n", "needle", consistency="eventual").items
        mfs.wait_ready(10)
        mfs.wait(mfs.upsert("n", "a.txt", b"needle replacement"), 10)
        assert mfs.grep("n", [TextMatch("replacement")]).items
        assert model.calls == 1
        mfs.configure_index("n", indexing="bm25")
        mfs.wait_ready(10)
        assert mfs.search("n", "needle", mode="bm25").items
        assert model.calls == 1
    finally:
        mfs.close()

    mfs = MFS.open(tmp_path / "state")
    try:
        mfs.open_namespace("n", processors=[Utf8TextProcessor()])
        mfs.wait(mfs.upsert("n", "b.txt", b"without loading the model"), 10)
        assert mfs.search("n", "model", mode="bm25").items
        mfs.configure_index("n", indexing="off")
        mfs.wait_ready(10)
        mfs.wait(mfs.upsert("n", "b.txt", b"grep without a model"), 10)
        assert mfs.grep("n", [TextMatch("without")]).items
    finally:
        mfs.close()
