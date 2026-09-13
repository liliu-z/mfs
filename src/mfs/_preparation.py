# pyright: reportPrivateUsage=false
from __future__ import annotations

import inspect
import os
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import blake3

from ._artifacts import ArtifactStore
from ._catalog import Catalog
from ._documents import source_map_json
from ._json import JSONValue, compact_json
from ._lifecycle import Lifecycle
from ._namespace import NamespaceBinding
from ._platform import fsync_directory
from ._runtime import NamespaceRuntime
from ._source import SourceGuard
from ._validation import validate_processed
from ._work import ExecutionPermit, Prepared
from .errors import CapabilityUnavailable, CorruptState, SourceChanged
from .processing import ProcessingContext
from .types import ContextProcessor, DocumentId, LegacyProcessor, Processor


class Preparation:
    """Invoke processors and own their text snapshots; lifecycle commits own task state."""

    def __init__(
        self,
        root: Path,
        catalog: Catalog,
        artifacts: ArtifactStore,
        lifecycle: Lifecycle,
        runtime: NamespaceRuntime,
        process_owner: int | None = None,
    ) -> None:
        self.root, self.catalog, self.artifacts = root, catalog, artifacts
        self.lifecycle, self.runtime = lifecycle, runtime
        self.process_owner = process_owner
        self.transient_text: dict[str, str] = {}

    def execute(self, permit: ExecutionPermit) -> Prepared:
        _, record = self.prepare_snapshot(permit.payload, permit.binding)
        return Prepared(record)

    def release_text(self, revision: str) -> None:
        self.transient_text.pop(revision, None)

    def cache_key(self, identity: DocumentId, job: dict[str, Any], processor: Processor) -> str:
        # Opt-in on the concrete adapter: subclasses that change process must explicitly
        # redeclare path independence, rather than accidentally inheriting a cache promise.
        content_only = type(processor).__dict__.get("cache_scope") == "content"
        return self.artifacts.key(
            "process",
            dict(
                format=2,
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
        self, identity: DocumentId, job: dict[str, Any], processor: Processor
    ) -> dict[str, Any]:
        key = self.cache_key(identity, job, processor)
        input_path = self.root / job["input"]
        guard = (
            SourceGuard(input_path, job["content_hash"], job["source"])
            if job.get("borrowed_input")
            else None
        )

        def check_source() -> None:
            self.lifecycle.check_execution(identity, job)
            if guard is not None:
                guard.check(self.lifecycle.target_record(identity)["source"])

        cached = None if job.get("force") else self.artifacts.cached(key)
        if cached is not None and cached.get("uses_input"):
            cached["text_ref"] = dict(
                path=job["input"], owned=not job.get("borrowed_input"), encoding="utf-8-sig"
            )
        if (
            cached is not None
            and (
                cached.get("uses_input")
                or cast(dict[str, Any], cached.get("text_ref") or {}).get("owned")
            )
            and all((self.root / p).is_file() for p in self.catalog.references(cached))
            and (cached.get("uses_input") or (self.root / cached["text_ref"]["path"]).is_file())
        ):
            check_source()
            return cast(dict[str, Any], cached)
        cancellation = self.lifecycle.cancellation_for(identity, job)
        work_dir = self.artifacts.directory(job["incarnation"], "work") / uuid.uuid4().hex
        self.artifacts.protect(work_dir.relative_to(self.root).as_posix())
        with self.catalog.transaction():
            self.catalog.register_artifact(work_dir.relative_to(self.root).as_posix())
        work_dir.mkdir()
        resume = job.get("checkpoint", {})
        last_progress = 0.0

        def progress(completed: float, total: float | None, unit: str | None) -> None:
            nonlocal last_progress
            now = time.monotonic()
            persist = now - last_progress >= 1.0
            self.lifecycle.report_progress(
                identity, job, dict(completed=completed, total=total, unit=unit), persist=persist
            )
            if persist:
                last_progress = now

        def checkpoint(state: JSONValue, files: Any) -> bool:
            cancellation.check()
            saved = self.artifacts.copy_files(work_dir, files)
            check_source()
            return self.lifecycle.checkpoint(identity, job, state, saved)

        context = ProcessingContext(
            identity,
            str(job["revision"]),
            str(job["content_hash"]),
            work_dir,
            cancellation,
            resume.get("state"),
            {name: self.root / p for name, p in resume.get("files", {}).items()},
            checkpoint,
            progress,
            process_owner=self.process_owner,
        )
        cancellation.check()
        if len(inspect.signature(processor.process).parameters) >= 3:
            processed = cast(ContextProcessor, processor).process(
                self.root / job["input"], job["media_type"], context
            )
        else:
            processed = cast(LegacyProcessor, processor).process(
                self.root / job["input"], job["media_type"]
            )
        cancellation.check()
        check_source()
        processed = validate_processed(processed)
        files = self.artifacts.copy_files(work_dir, processed.artifacts)
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
            source_path = (self.root / job["input"]).resolve()
            uses_input = path == source_path
            if path == source_path and not job.get("borrowed_input"):
                reference = dict(path=job["input"], owned=True, encoding="utf-8-sig")
            elif path.is_relative_to(work_dir):
                reference = dict(
                    path=self.artifacts.copy_files(work_dir, {"text": path})["text"],
                    owned=True,
                    encoding="utf-8-sig",
                )
            else:
                reference = dict(path=str(path), owned=False, encoding="utf-8-sig")
        elif processed.grep_path is not None:
            # HTML-style adapters can grep the source and index extracted text in memory.
            self.transient_text[str(job["revision"])] = processed.text
            reference = None
        else:
            directory = self.artifacts.directory(job["incarnation"], "derived")
            relative = (directory / (uuid.uuid4().hex + ".md")).relative_to(self.root).as_posix()
            self.artifacts.protect(relative)
            with self.catalog.transaction():
                self.catalog.register_artifact(relative)
            with (self.root / relative).open("xb") as stream:
                stream.write(processed.text.encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            fsync_directory(directory)
            reference = dict(path=relative, owned=True, encoding="utf-8")
        value = dict(
            text_ref=reference,
            text_hash=text_hash,
            uses_input=uses_input,
            transient=reference is None,
            source_map=source_map_json(processed.source_map),
            artifacts=files,
        )
        if processed.grep_path is not None:
            path = processed.grep_path.resolve(strict=True)
            if path.is_relative_to(work_dir):
                value["grep_ref"] = dict(
                    path=self.artifacts.copy_files(work_dir, {"grep": path})["grep"],
                    owned=True,
                    encoding="utf-8-sig",
                )
            elif path == input_path.resolve() and not job.get("borrowed_input"):
                value["grep_ref"] = dict(path=job["input"], owned=True, encoding="utf-8-sig")
            else:
                value["grep_ref"] = dict(path=str(path), owned=False, encoding="utf-8-sig")
        check_source()
        artifact = self.artifacts.write(uuid.uuid4().hex + "-processed", value, job["incarnation"])
        self.artifacts.cache(key, artifact)
        return value

    def prepare_snapshot(
        self, job: dict[str, Any], binding: NamespaceBinding | None
    ) -> tuple[str, dict[str, Any]]:
        saved = self.catalog.connection.execute(
            "SELECT path FROM prepared WHERE revision=?", (job["revision"],)
        ).fetchone()
        artifact = str(saved[0]) if saved else "artifacts/" + job["revision"] + "-snapshot.json"
        if (self.root / artifact).exists():
            cached: object = self.artifacts.read(artifact)
            if not isinstance(cached, dict):
                raise CorruptState("processed artifact is not an object")
            snapshot = cast(dict[str, Any], cached)
            if snapshot.get("revision") != job["revision"]:
                raise CorruptState("processed artifact does not match target revision")
            if snapshot.get("transient") and job["revision"] not in self.transient_text:
                processor = self.runtime.processor_for(job, binding)
                if processor is None:
                    raise CapabilityUnavailable("transient index text requires its Processor")
                self.prepare(DocumentId(**job["identity"]), job, processor)
                if (
                    blake3.blake3(self.transient_text[job["revision"]].encode()).hexdigest()
                    != snapshot["text_hash"]
                ):
                    raise SourceChanged("transient text changed since preparation; reprocess again")
            return artifact, snapshot
        processor = self.runtime.processor_for(job, binding)
        if processor is None:
            raise CapabilityUnavailable("registered Processor does not match accepted input")
        prepared = self.prepare(DocumentId(**job["identity"]), job, processor)
        record: dict[str, Any] = dict(
            version=2,
            revision=job["revision"],
            media_type=job["media_type"],
            content_hash=job["content_hash"],
            processor=job["processor"],
            **prepared,
            source=job["source"],
            binding=job["binding"],
            incarnation=job["incarnation"],
        )
        record["identity"] = job["identity"]
        record["preparation_attempt"] = job.get("attempt_token")
        record["snapshot_id"] = blake3.blake3(compact_json(record).encode()).hexdigest()
        artifact = self.artifacts.write(job["revision"] + "-snapshot", record, job["incarnation"])
        self.lifecycle.save_prepared(DocumentId(**job["identity"]), job, artifact, record)
        return artifact, record

    def read_text(
        self, record: dict[str, Any], permit: ExecutionPermit, *, grep: bool = False
    ) -> str:
        if record.get("transient") and not grep:
            if record["revision"] not in self.transient_text:
                raise CorruptState("transient text was not restored by the preparation stage")
            text = self.transient_text[record["revision"]]
        else:
            text = self.artifacts.read_text(record, grep=grep)
        if not grep and blake3.blake3(text.encode()).hexdigest() != record.get("text_hash"):
            raise SourceChanged("text reference changed since preparation; sync/reprocess again")
        return text
