# pyright: reportPrivateUsage=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import blake3

from ._index import IndexRow
from ._validation import validate_chunk_ranges
from .errors import CorruptState, SourceChanged
from .types import DocumentId

if TYPE_CHECKING:
    from ._core import MFS


def execute(mfs: MFS, identity: DocumentId, job: dict[str, Any]) -> None:
    with mfs._condition:
        if not mfs._tasks.current(identity, job):
            return
    kind, stage = job["kind"], job["stage"]
    if kind == "rebuild":
        mfs._rebuild_job(identity, job)
        return
    index = (
        mfs._namespace_index(identity.namespace, job.get("incarnation")) if kind != "drop" else None
    )
    if stage == "drop":
        if job.get("legacy_cleanup") and mfs._index.client.has_collection(
            mfs._index.collection_name
        ):
            mfs._index.delete_namespace(identity.namespace)
        for incarnation in job["incarnations"]:
            old_index = mfs._namespace_index(identity.namespace, incarnation)
            if old_index.client.has_collection(old_index.collection_name):
                old_index.drop()
    elif stage == "delete":
        assert index is not None
        if index.client.has_collection(index.collection_name):
            index.delete_document(identity, incarnation=job.get("incarnation"))
            index.flush()
    elif mfs._tasks.namespaces[identity.namespace]["indexing"] == "off":
        complete(mfs, identity, job, indexed=False)
        return
    elif stage == "chunk":
        record = snapshot(mfs, identity, job)
        text = mfs._read_text(record)
        with mfs._chunker_lock:
            ranges = validate_chunk_ranges(
                text, mfs._binding(identity.namespace).chunker.chunk(text, mfs._source_map(record))
            )
        encoded = text.encode()
        plan = [
            dict(
                ordinal=i,
                text_start=r.text_start,
                text_end=r.text_end,
                text_hash=blake3.blake3(encoded[r.text_start : r.text_end]).hexdigest(),
            )
            for i, r in enumerate(ranges)
        ]
        mfs._tasks.advance(
            identity,
            job,
            stage="embed" if plan else "publish",
            plan=plan,
            batches=(len(plan) + 127) // 128,
            completed_batches=0,
        )
        return
    elif stage == "embed":
        assert index is not None
        record = snapshot(mfs, identity, job)
        encoded = mfs._read_text(record).encode()
        batch = int(job.get("completed_batches", 0))
        plan = job["plan"][batch * 128 : (batch + 1) * 128]
        texts: dict[str, str] = {}
        for chunk in plan:
            raw = encoded[chunk["text_start"] : chunk["text_end"]]
            if blake3.blake3(raw).hexdigest() != chunk["text_hash"]:
                raise SourceChanged("text reference changed after chunk planning; sync again")
            texts[chunk["text_hash"]] = raw.decode()
        vectors: dict[str, list[float]] = {}
        dense = mfs._dense_config(identity.namespace)
        if dense:
            candidates = index.vector_candidates(tuple(texts))
            with mfs._condition:
                for row in candidates:
                    owner = DocumentId(row["namespace"], row["doc_id"])
                    if mfs._tasks.visible.get(owner) == row["snapshot_id"] or (
                        owner == identity and row["snapshot_id"] == job["snapshot_id"]
                    ):
                        vectors[row["text_hash"]] = mfs._validate_vectors(
                            [row["dense_vector"]], 1, int(dense["dimension"])
                        )[0]
            missing = [h for h in texts if h not in vectors]
            if missing:
                computed = mfs._embed_documents(identity.namespace, [texts[h] for h in missing])
                vectors.update(zip(missing, computed, strict=True))
        source_map = mfs._source_map(record)
        rows: list[IndexRow] = []
        for chunk in plan:
            location = mfs._source_location(source_map, chunk["text_start"], chunk["text_end"])
            rows.append(
                IndexRow(
                    namespace=identity.namespace,
                    doc_id=identity.doc_id,
                    ordinal=chunk["ordinal"],
                    text=texts[chunk["text_hash"]],
                    text_hash=chunk["text_hash"],
                    text_start=chunk["text_start"],
                    text_end=chunk["text_end"],
                    dense_vector=vectors.get(chunk["text_hash"], []),
                    snapshot_id=job["snapshot_id"],
                    source_location=dict(version=1, sources=list(location.sources)),
                    media_type=job["media_type"],
                    incarnation=job["incarnation"],
                )
            )
        with mfs._condition:
            if not mfs._tasks.current(identity, job):
                return
        # Complete vectors are durable in Milvus. These rows stay invisible until publish.
        index.insert(rows)
        index.flush()
        mfs._tasks.advance(
            identity,
            job,
            completed_batches=batch + 1,
            stage="publish" if batch + 1 == job["batches"] else "embed",
        )
        return
    elif stage == "publish":
        assert index is not None
        index.publish(identity, job["snapshot_id"], job["incarnation"], len(job["plan"]))
    else:
        raise CorruptState(f"unknown task stage {stage!r}")
    complete(mfs, identity, job, indexed=kind == "upsert")


def snapshot(mfs: MFS, identity: DocumentId, job: dict[str, Any]) -> dict[str, Any]:
    record = mfs._catalog.get_document(identity.namespace, identity.doc_id)
    if record is None or record.get("revision") != job["revision"]:
        raise SourceChanged("processed text no longer belongs to the current source")
    return record


def complete(mfs: MFS, identity: DocumentId, job: dict[str, Any], *, indexed: bool) -> None:
    with mfs._condition:
        if not mfs._tasks.current(identity, job):
            return
        job.update(
            state="succeeded",
            indexed_revision=job["revision"] if indexed else None,
            published_artifacts=job.get("artifacts", {}),
            error=None,
            failures=0,
        )
        for name in ("plan", "snapshot", "chunks", "vectors"):
            job.pop(name, None)
        mfs._tasks.persist(identity, job)
        if indexed:
            mfs._tasks.visible[identity] = job["snapshot_id"]
        if mfs._transient_text is not None and mfs._transient_text[0] == job["revision"]:
            mfs._transient_text = None
