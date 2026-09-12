# pyright: reportPrivateUsage=false
from __future__ import annotations

import inspect
import os
import time
import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

import blake3

from ._json import JSONValue
from ._validation import validate_processed
from .errors import SourceChanged
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
            namespace=identity.namespace,
            incarnation=job["incarnation"],
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
    input_path = mfs._path / job["input"]
    if job.get("borrowed_input"):
        with input_path.open("rb") as stream:
            digest = blake3.blake3()
            while block := stream.read(1024 * 1024):
                digest.update(block)
        if digest.hexdigest() != job["content_hash"]:
            raise SourceChanged("external input changed after sync; sync again")
    cached = None if job.get("force") else mfs._artifacts.cached(key)
    if (
        cached is not None
        and (
            cached.get("uses_input")
            or cast(dict[str, Any], cached.get("text_ref") or {}).get("owned")
        )
        and all((mfs._path / p).is_file() for p in cached.get("artifacts", {}).values())
        and (cached.get("uses_input") or (mfs._path / cached["text_ref"]["path"]).is_file())
    ):
        if cached.get("uses_input"):
            cached["text_ref"] = dict(
                path=job["input"], owned=not job.get("borrowed_input"), encoding="utf-8-sig"
            )
        return cast(dict[str, Any], cached)
    token = str(job["attempt_token"])
    cancellation = mfs._tasks.cancellations[(identity, token)]
    work_dir = mfs._artifacts.directory(job["incarnation"], "work") / uuid.uuid4().hex
    with mfs._catalog.transaction():
        mfs._catalog.register_artifact(work_dir.relative_to(mfs._path).as_posix())
    work_dir.mkdir()
    resume = job.get("checkpoint", {})
    last_progress = 0.0

    def progress(completed: float, total: float | None, unit: str | None) -> None:
        nonlocal last_progress
        with mfs._condition:
            if not mfs._tasks.current(identity, job) or mfs._stopping:
                raise _ProcessingStopped()
            job["progress"] = dict(completed=completed, total=total, unit=unit)
            mfs._tasks.progress[identity] = job["progress"]
            if time.monotonic() - last_progress >= 1.0:
                mfs._tasks.persist(identity, job)
                last_progress = time.monotonic()

    def checkpoint(state: JSONValue, files: Any) -> bool:
        cancellation.check()
        saved = mfs._artifacts.copy_files(work_dir, files)
        with mfs._condition:
            if not mfs._tasks.current(identity, job) or mfs._stopping:
                raise _ProcessingStopped()
            job["checkpoint"] = dict(state=state, files=saved)
            mfs._tasks.persist(identity, job)
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
    reference: dict[str, Any] | None
    uses_input = False
    text_hash = blake3.blake3(processed.text.encode()).hexdigest()
    if (
        processed.text_path is None
        and processed.grep_path is None
        and text_hash == job["content_hash"]
    ):
        processed = replace(processed, text_path=input_path)
    if processed.text_path is not None:
        path = processed.text_path.resolve(strict=True)
        source_path = (mfs._path / job["input"]).resolve()
        uses_input = path == source_path
        if path == source_path and not job.get("borrowed_input"):
            reference = dict(path=job["input"], owned=True, encoding="utf-8-sig")
        elif path.is_relative_to(work_dir):
            reference = dict(
                path=mfs._artifacts.copy_files(work_dir, {"text": path})["text"],
                owned=True,
                encoding="utf-8-sig",
            )
        else:
            reference = dict(path=str(path), owned=False, encoding="utf-8-sig")
    elif processed.grep_path is not None:
        # HTML-style adapters can grep the source and index extracted text in memory.
        mfs._transient_text = (str(job["revision"]), processed.text)
        reference = None
    else:
        directory = mfs._artifacts.directory(job["incarnation"], "derived")
        relative = (directory / (uuid.uuid4().hex + ".md")).relative_to(mfs._path).as_posix()
        with mfs._catalog.transaction():
            mfs._catalog.register_artifact(relative)
        with (mfs._path / relative).open("xb") as stream:
            stream.write(processed.text.encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
        mfs._fsync_directory(directory)
        reference = dict(path=relative, owned=True, encoding="utf-8")
    value = dict(
        text_ref=reference,
        text_hash=text_hash,
        uses_input=uses_input,
        transient=reference is None,
        source_map=mfs._source_map_json(processed.source_map),
        artifacts=files,
    )
    if processed.grep_path is not None:
        value["grep_ref"] = dict(
            path=str(processed.grep_path.resolve(strict=True)), owned=False, encoding="utf-8-sig"
        )
    with mfs._condition:
        if not mfs._tasks.current(identity, job) or mfs._stopping:
            raise _ProcessingStopped()
    artifact = mfs._write_artifact(uuid.uuid4().hex + "-processed", value, job["incarnation"])
    mfs._artifacts.cache(key, artifact)
    return value
