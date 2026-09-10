# pyright: reportPrivateUsage=false
from __future__ import annotations

import inspect
import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from ._json import JSONValue
from ._validation import validate_processed
from .processing import ProcessingContext, _ProcessingStopped
from .types import ContextProcessor, DocumentId, LegacyProcessor, Processor

if TYPE_CHECKING:
    from ._core import MFS


def cache_key(mfs: MFS, identity: DocumentId, job: dict[str, Any], processor: Processor) -> str:
    # Opt-in on the concrete adapter: subclasses that change process must explicitly
    # redeclare path independence, rather than accidentally inheriting a cache promise.
    content_only = type(processor).__dict__.get("cache_scope") == "content"
    return mfs._artifacts.key(
        "process",
        dict(
            hash=job["content_hash"],
            media=job["media_type"],
            processor=job["processor"],
            identity=None
            if content_only
            else [identity.namespace, identity.doc_id, job["incarnation"], job["binding"]],
        ),
    )


def prepare(
    mfs: MFS, identity: DocumentId, job: dict[str, Any], processor: Processor
) -> dict[str, Any]:
    key = cache_key(mfs, identity, job, processor)
    cached = None if job.get("force") else mfs._artifacts.cached(key)
    if cached is not None and all(
        (mfs._path / p).is_file() for p in cached.get("artifacts", {}).values()
    ):
        return cast(dict[str, Any], cached)
    token = str(job["attempt_token"])
    cancellation = mfs._cancellations[(identity, token)]
    work_dir = mfs._path / "work" / uuid.uuid4().hex
    with mfs._catalog.transaction():
        mfs._catalog.register_artifact(work_dir.relative_to(mfs._path).as_posix())
    work_dir.mkdir()
    resume = job.get("checkpoint", {})
    last_progress = 0.0

    def progress(completed: float, total: float | None, unit: str | None) -> None:
        nonlocal last_progress
        with mfs._condition:
            if not mfs._current(identity, job) or mfs._stopping:
                raise _ProcessingStopped()
            job["progress"] = dict(completed=completed, total=total, unit=unit)
            mfs._progress[identity] = job["progress"]
            if time.monotonic() - last_progress >= 1.0:
                mfs._store_job(identity, job)
                last_progress = time.monotonic()

    def checkpoint(state: JSONValue, files: Any) -> bool:
        cancellation.check()
        saved = mfs._artifacts.copy_files(work_dir, files)
        with mfs._condition:
            if not mfs._current(identity, job) or mfs._stopping:
                raise _ProcessingStopped()
            job["checkpoint"] = dict(state=state, files=saved)
            mfs._store_job(identity, job)
            priority = mfs._priority(identity, job)
            workload, _ = mfs._processor_resources[id(processor)]
            for other, item in mfs._targets.items():
                if (
                    other == identity
                    or item["stage"] != "process"
                    or item["state"] != "pending"
                    or mfs._priority(other, item) >= priority
                    or any(i == other for i, _ in mfs._executing)
                ):
                    continue
                candidate = mfs._processor_for(item)
                candidate_workload, capacity = mfs._processor_resources.get(
                    id(candidate), ("light", 1)
                )
                occupied = sum(p == id(candidate) for p in mfs._preparing.values()) - int(
                    candidate is processor
                )
                if candidate_workload == workload and occupied < capacity:
                    return True
            return False

    context = ProcessingContext(
        identity,
        str(job["revision"]),
        str(job["content_hash"]),
        work_dir,
        cancellation,
        resume.get("state"),
        {name: mfs._path / p for name, p in resume.get("files", {}).items()},
        checkpoint,
        progress,
    )
    cancellation.check()
    if len(inspect.signature(processor.process).parameters) >= 3:
        processed = cast(ContextProcessor, processor).process(
            mfs._path / job["input"], job["media_type"], context
        )
    else:
        processed = cast(LegacyProcessor, processor).process(
            mfs._path / job["input"], job["media_type"]
        )
    cancellation.check()
    processed = validate_processed(processed)
    files = mfs._artifacts.copy_files(work_dir, processed.artifacts)
    cancellation.check()
    value = dict(
        text=processed.text, source_map=mfs._source_map_json(processed.source_map), artifacts=files
    )
    with mfs._condition:
        if not mfs._current(identity, job) or mfs._stopping:
            raise _ProcessingStopped()
    artifact = mfs._write_artifact(uuid.uuid4().hex + "-processed", value)
    mfs._artifacts.cache(key, artifact)
    return value
