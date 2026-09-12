# pyright: reportPrivateUsage=false
from __future__ import annotations

import uuid
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._namespace import NamespaceBinding
from ._rules import excluded, validate_rules
from .errors import InvalidConfiguration, RootOverlap, SourceUnavailable
from .types import DocumentId, IgnoreRule, IndexingMode, NamespaceInfo

if TYPE_CHECKING:
    from ._core import MFS


def migrate(
    mfs: MFS,
    namespace: str,
    binding: NamespaceBinding,
    indexing: IndexingMode,
    rules: tuple[IgnoreRule, ...],
) -> NamespaceInfo:
    previous = mfs._tasks.namespaces[namespace]
    if "manifest" in previous:
        raise InvalidConfiguration("namespace already uses the current storage format")
    if previous["kind"] == "external":
        root = Path(previous["root"]).resolve()
        for name, other in mfs._tasks.namespaces.items():
            if name == namespace or other["kind"] != "external":
                continue
            candidate = Path(other["root"]).resolve()
            if candidate == root or candidate in root.parents or root in candidate.parents:
                raise RootOverlap(
                    f"drop overlapping namespace {name!r} before migrating; sources are preserved"
                )
    incarnation = uuid.uuid4().hex
    record = dict(
        previous,
        version=4,
        incarnation=incarnation,
        binding=uuid.uuid4().hex,
        manifest=binding.manifest,
        pending_manifest=binding.manifest,
        indexing=indexing,
        paused=False,
        rules=[asdict(r) for r in validate_rules(rules)],
        rules_revision=uuid.uuid4().hex,
        max_file_bytes=None,
        legacy_collection=True,
    )
    old_targets = {doc: value for _, doc, value in mfs._catalog.list_targets(namespace) if doc}
    documents = dict(mfs._catalog.list_namespace_documents(namespace))
    updates: list[tuple[DocumentId, dict[str, Any]]] = []
    for doc_id in sorted(old_targets.keys() | documents.keys()):
        old = old_targets.get(doc_id) or documents[doc_id]
        if old.get("kind", "upsert") != "upsert" or excluded(rules, doc_id):
            continue
        identity = DocumentId(namespace, doc_id)
        external = previous["kind"] == "external"
        if external:
            input_name = str(Path(previous["root"]) / doc_id)
        else:
            input_name = old.get("input") or old.get("source", {}).get("object")
            if not input_name or not mfs._artifacts.path(input_name).is_file():
                raise SourceUnavailable(f"{doc_id}: migration requires the owned original")
        processor = next(
            (p for p in binding.processors if binding.descriptions[id(p)] == old.get("processor")),
            None,
        )
        # Conversion is explicit: changed adapters may select a new supported route.
        if processor is None:
            suffix = Path(doc_id).suffix.lower()
            processor = next(
                (p for p in binding.processors if suffix in binding.suffixes[id(p)]), None
            )
        if processor is None:
            raise InvalidConfiguration(f"{doc_id}: supply a Processor for legacy input or drop it")
        source = dict(
            old.get("source", {}),
            object=None if external else input_name,
            path=input_name if external else None,
        )
        job = dict(
            revision=uuid.uuid4().hex,
            identity=asdict(identity),
            kind="upsert",
            stage="process",
            state="pending",
            attempts=0,
            failures=0,
            next_run=0,
            error=None,
            input=input_name,
            borrowed_input=external,
            source=source,
            incarnation=incarnation,
            content_hash=old["content_hash"],
            media_type=old["media_type"],
            processor=binding.descriptions[id(processor)],
            binding=record["binding"],
            indexed_revision=None,
            force=True,
        )
        updates.append((identity, job))
    control = dict(
        mfs._delete_job("rebuild"),
        incarnation=incarnation,
        identity=asdict(DocumentId(namespace, "")),
        manifest=binding.manifest,
        legacy_cleanup=True,
    )
    with mfs._condition:
        with mfs._catalog.transaction():
            mfs._catalog.put_namespace(namespace, record)
            for doc_id in documents:
                mfs._catalog.delete_document(namespace, doc_id)
            for old in old_targets.values():
                if old.get("revision"):
                    mfs._catalog.clear_prepared(old["revision"])
            mfs._catalog.delete_targets(namespace)
            for identity, job in updates:
                mfs._catalog.put_target(namespace, identity.doc_id, job)
            mfs._catalog.put_target(namespace, "", control)
        mfs._tasks.namespaces[namespace] = record
        mfs._bindings[namespace] = binding
        mfs._tasks.targets = {
            i: j for i, j in mfs._tasks.targets.items() if i.namespace != namespace
        }
        mfs._tasks.visible = {
            i: v for i, v in mfs._tasks.visible.items() if i.namespace != namespace
        }
        mfs._tasks.refresh_pending()
        for identity, job in updates:
            mfs._tasks.remember(identity, job)
        mfs._tasks.remember(DocumentId(namespace, ""), control)
    return mfs._namespace_info(namespace, record)
