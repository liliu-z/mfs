from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from ._json import compact_json, load_json
from .errors import CorruptState, SchemaVersionUnsupported, StorageFailed


class Catalog:
    """Thread-owned SQLite connections; callers coordinate related state transitions."""

    def __init__(self, path: Path, *, initialize: bool) -> None:
        self.path = path
        self._local = threading.local()
        self._connections: list[sqlite3.Connection] = []
        self._connections_lock = threading.Lock()
        self.migrated = False
        version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
        if version > 6:
            raise SchemaVersionUnsupported(f"catalog schema version {version} is unsupported")
        if not initialize and version not in (1, 2, 3, 4, 5, 6):
            raise CorruptState("catalog schema is missing or unrecognized")
        if initialize or version < 6:
            self._initialize()
            self.migrated = version == 1
        expected = {
            "namespaces",
            "documents",
            "targets",
            "operations",
            "artifacts",
            "artifact_refs",
            "cache",
            "cancel_gates",
            "prepared",
        }
        actual = {
            r[0]
            for r in self.connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if actual != expected:
            raise CorruptState("catalog schema does not match version 6")

    @property
    def connection(self) -> sqlite3.Connection:
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(
                self.path, check_same_thread=False, isolation_level=None, timeout=30
            )
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.create_function("mfs_name", 1, _name, deterministic=True)
            connection.create_function("mfs_suffix", 1, _suffix, deterministic=True)
            self._local.connection = connection
            with self._connections_lock:
                self._connections.append(connection)
        return connection

    def _initialize(self) -> None:
        self.connection.executescript("""
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS namespaces (
                namespace TEXT PRIMARY KEY COLLATE BINARY,
                value TEXT NOT NULL CHECK(json_valid(value))
            );
            CREATE TABLE IF NOT EXISTS documents (
                namespace TEXT NOT NULL, doc_id TEXT NOT NULL COLLATE BINARY,
                value TEXT NOT NULL CHECK(json_valid(value)),
                PRIMARY KEY(namespace,doc_id),
                FOREIGN KEY(namespace) REFERENCES namespaces(namespace) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS targets (
                namespace TEXT NOT NULL, doc_id TEXT NOT NULL COLLATE BINARY,
                revision TEXT NOT NULL, value TEXT NOT NULL CHECK(json_valid(value)),
                PRIMARY KEY(namespace,doc_id)
            );
            CREATE INDEX IF NOT EXISTS targets_state ON targets(json_extract(value,'$.state'));
            CREATE TABLE IF NOT EXISTS operations (
                key TEXT PRIMARY KEY, request_hash TEXT NOT NULL,
                value TEXT NOT NULL CHECK(json_valid(value))
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                path TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'live',
                unreferenced_at REAL, size INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS artifacts_gc ON artifacts(state,unreferenced_at);
            CREATE TABLE IF NOT EXISTS artifact_refs (
                owner TEXT NOT NULL, namespace TEXT NOT NULL, doc_id TEXT NOT NULL,
                path TEXT NOT NULL, PRIMARY KEY(owner,namespace,doc_id,path)
            );
            CREATE INDEX IF NOT EXISTS refs_path ON artifact_refs(path);
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY, path TEXT NOT NULL, digest TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cancel_gates (
                namespace TEXT NOT NULL, doc_id TEXT NOT NULL, PRIMARY KEY(namespace,doc_id)
            );
            CREATE TABLE IF NOT EXISTS prepared (revision TEXT PRIMARY KEY, path TEXT NOT NULL);
            DROP TABLE IF EXISTS run_dependencies;
            DROP TABLE IF EXISTS wait_operations;
            DROP TABLE IF EXISTS wait_target_sets;
            DROP TABLE IF EXISTS runs;
            UPDATE operations SET value=json_remove(value,'$.operation_id');
            PRAGMA user_version = 6;
            COMMIT;
        """)

    @contextmanager
    def transaction(self) -> Generator[None]:
        connection = self.connection
        nested = connection.in_transaction
        try:
            if not nested:
                connection.execute("BEGIN IMMEDIATE")
            yield
            if not nested:
                connection.execute("COMMIT")
        except Exception as error:
            if not nested and connection.in_transaction:
                connection.execute("ROLLBACK")
            if isinstance(error, sqlite3.Error):
                raise StorageFailed(f"catalog transaction failed: {error}") from error
            raise

    def close(self) -> None:
        with self._connections_lock:
            for connection in self._connections:
                connection.close()
            self._connections.clear()

    def put_namespace(self, namespace: str, value: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO namespaces VALUES(?,?) ON CONFLICT(namespace) "
            "DO UPDATE SET value=excluded.value",
            (namespace, compact_json(value)),
        )

    def get_namespace(self, namespace: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT value FROM namespaces WHERE namespace=?", (namespace,)
        ).fetchone()
        return self.decode(row[0]) if row else None

    def list_namespaces(self) -> list[tuple[str, dict[str, Any]]]:
        return [
            (n, self.decode(v))
            for n, v in self.connection.execute(
                "SELECT namespace,value FROM namespaces ORDER BY namespace"
            )
        ]

    def delete_namespace(self, namespace: str) -> None:
        for doc, _ in self.list_namespace_documents(namespace):
            self.set_references("document", namespace, doc, set())
        self.connection.execute("DELETE FROM cancel_gates WHERE namespace=?", (namespace,))
        self.connection.execute("DELETE FROM namespaces WHERE namespace=?", (namespace,))

    def put_document(self, namespace: str, doc_id: str, value: dict[str, Any]) -> None:
        self.set_references("document", namespace, doc_id, self.references(value))
        self.connection.execute(
            "INSERT INTO documents VALUES(?,?,?) ON CONFLICT(namespace,doc_id) "
            "DO UPDATE SET value=excluded.value",
            (namespace, doc_id, compact_json(value)),
        )

    def delete_document(self, namespace: str, doc_id: str) -> None:
        self.set_references("document", namespace, doc_id, set())
        self.connection.execute(
            "DELETE FROM documents WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        )

    def get_document(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT value FROM documents WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        ).fetchone()
        return self.decode(row[0]) if row else None

    def get_document_revision(self, namespace: str, doc_id: str) -> str | None:
        row = self.connection.execute(
            "SELECT json_extract(value,'$.revision') FROM documents WHERE namespace=? AND doc_id=?",
            (namespace, doc_id),
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else None

    def select_documents(
        self, where: str = "1", params: Sequence[Any] = ()
    ) -> list[tuple[str, str, dict[str, Any]]]:
        return [
            (n, d, self.decode(v))
            for n, d, v in self.connection.execute(
                "SELECT namespace,doc_id,value FROM documents WHERE "
                + where
                + " ORDER BY namespace,doc_id",
                params,
            )
        ]

    def list_documents(self) -> list[tuple[str, str, dict[str, Any]]]:
        return self.select_documents()

    def iter_documents(
        self, where: str = "1", params: Sequence[Any] = ()
    ) -> Generator[tuple[str, str, dict[str, Any]]]:
        cursor = self.connection.execute(
            "SELECT namespace,doc_id,value FROM documents WHERE "
            + where
            + " ORDER BY namespace,doc_id",
            params,
        )
        try:
            for namespace, doc_id, value in cursor:
                yield namespace, doc_id, self.decode(value)
        finally:
            cursor.close()

    def list_namespace_documents(self, namespace: str) -> list[tuple[str, dict[str, Any]]]:
        return [(d, v) for _, d, v in self.select_documents("namespace=?", (namespace,))]

    def document_count(self, namespace: str | None = None) -> int:
        sql = "SELECT count(*) FROM documents"
        return int(
            self.connection.execute(
                sql + (" WHERE namespace=?" if namespace is not None else ""),
                (namespace,) if namespace is not None else (),
            ).fetchone()[0]
        )

    def namespace_count(self) -> int:
        return int(self.connection.execute("SELECT count(*) FROM namespaces").fetchone()[0])

    def put_target(self, namespace: str, doc_id: str, value: dict[str, Any]) -> None:
        previous = self.get_target(namespace, doc_id)
        self.set_references("target", namespace, doc_id, self.references(value))
        if previous and previous["revision"] != value["revision"]:
            self.clear_prepared(str(previous["revision"]))
        if value["stage"] != "process":
            self.clear_prepared(str(value["revision"]))
        self.connection.execute(
            "INSERT INTO targets VALUES(?,?,?,?) ON CONFLICT(namespace,doc_id) "
            "DO UPDATE SET revision=excluded.revision,value=excluded.value",
            (namespace, doc_id, value["revision"], compact_json(value)),
        )

    def get_target(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT value FROM targets WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        ).fetchone()
        return self.decode_target(row[0], namespace, doc_id) if row else None

    def list_targets(self, namespace: str | None = None) -> list[tuple[str, str, dict[str, Any]]]:
        sql = "SELECT namespace,doc_id,value FROM targets"
        rows = self.connection.execute(
            sql
            + (" WHERE namespace=?" if namespace is not None else "")
            + " ORDER BY namespace,doc_id",
            (namespace,) if namespace is not None else (),
        )
        return [(n, d, self.decode_target(v, n, d)) for n, d, v in rows]

    def delete_targets(self, namespace: str) -> None:
        for _, doc, job in self.list_targets(namespace):
            self.set_references("target", namespace, doc, set())
            self.clear_prepared(str(job["revision"]))
        self.connection.execute("DELETE FROM targets WHERE namespace=?", (namespace,))

    def get_operation(self, key: str) -> tuple[str, dict[str, Any]] | None:
        row = self.connection.execute(
            "SELECT request_hash,value FROM operations WHERE key=?", (key,)
        ).fetchone()
        return (str(row[0]), self.decode(row[1])) if row else None

    def put_operation(self, key: str, request_hash: str, value: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO operations VALUES(?,?,?)", (key, request_hash, compact_json(value))
        )

    def clear_prepared(self, revision: str) -> None:
        self.set_references("prepared", "", revision, set())
        self.connection.execute("DELETE FROM prepared WHERE revision=?", (revision,))

    @staticmethod
    def _scope(namespace: str | None, path: str) -> tuple[str, list[str]]:
        clauses = ["namespace != ''"]
        parameters: list[str] = []
        if namespace is not None:
            clauses.append("namespace=?")
            parameters.append(namespace)
        if path != ".":
            clauses.append("(doc_id=? OR substr(doc_id,1,length(?)+1)=?||'/')")
            parameters.extend([path, path, path])
        return " AND ".join(clauses), parameters

    def status_ids(
        self, namespace: str | None, path: str, limit: int, offset: int
    ) -> list[tuple[str, str]]:
        where, parameters = self._scope(namespace, path)
        return self.connection.execute(
            f"SELECT namespace,doc_id FROM targets WHERE {where} "
            "ORDER BY namespace,doc_id LIMIT ? OFFSET ?",
            [*parameters, limit, offset],
        ).fetchall()

    def status_counts(
        self, namespace: str | None, path: str
    ) -> tuple[dict[str, int], dict[str, int]]:
        where, parameters = self._scope(namespace, path)
        states: dict[str, int] = {}
        stages: dict[str, int] = {}
        for state, stage, count in self.connection.execute(
            "SELECT json_extract(value,'$.state'),json_extract(value,'$.stage'),count(*) "
            f"FROM targets WHERE {where} GROUP BY 1,2",
            parameters,
        ):
            states[state] = states.get(state, 0) + count
            stages[stage] = stages.get(stage, 0) + count
        return states, stages

    def set_cancelled(self, namespace: str, doc_id: str, cancelled: bool) -> None:
        if cancelled:
            self.connection.execute(
                "INSERT OR IGNORE INTO cancel_gates VALUES(?,?)", (namespace, doc_id)
            )
        else:
            self.connection.execute(
                "DELETE FROM cancel_gates WHERE namespace=? AND doc_id=?", (namespace, doc_id)
            )

    def cancelled(self, namespace: str, doc_id: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM cancel_gates WHERE namespace=? AND doc_id=?", (namespace, doc_id)
            ).fetchone()
            is not None
        )

    @staticmethod
    def references(value: dict[str, Any]) -> set[str]:
        paths = {value.get(k) for k in ("snapshot", "chunks")}
        if not value.get("borrowed_input"):
            paths.add(value.get("input"))
        for key in ("text_ref", "grep_ref"):
            reference = value.get(key)
            if reference and reference.get("owned"):
                paths.add(reference["path"])
        paths.update(value.get("vectors", []))
        paths.add(value.get("source", {}).get("object"))
        paths.update(value.get("artifacts", {}).values())
        paths.update(value.get("published_artifacts", {}).values())
        paths.update(value.get("checkpoint", {}).get("files", {}).values())
        return {p for p in paths if isinstance(p, str)}

    def register_artifact(self, path: str, size: int = 0) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO artifacts(path,unreferenced_at,size) VALUES(?,?,?)",
            (path, time.time(), size),
        )

    def set_references(self, owner: str, namespace: str, doc_id: str, paths: set[str]) -> None:
        params = (owner, namespace, doc_id)
        previous = {
            str(r[0])
            for r in self.connection.execute(
                "SELECT path FROM artifact_refs WHERE owner=? AND namespace=? AND doc_id=?", params
            )
        }
        for path in paths - previous:
            self.register_artifact(path)
            row = self.connection.execute(
                "SELECT state FROM artifacts WHERE path=?", (path,)
            ).fetchone()
            if row[0] != "live":
                raise CorruptState("cannot reference an artifact claimed for deletion")
            self.connection.execute("INSERT INTO artifact_refs VALUES(?,?,?,?)", (*params, path))
            self.connection.execute(
                "UPDATE artifacts SET unreferenced_at=NULL WHERE path=?", (path,)
            )
        for path in previous - paths:
            self.connection.execute(
                "DELETE FROM artifact_refs WHERE owner=? AND namespace=? AND doc_id=? AND path=?",
                (*params, path),
            )
            self.connection.execute(
                "UPDATE artifacts SET unreferenced_at=? WHERE path=? "
                "AND NOT EXISTS(SELECT 1 FROM artifact_refs WHERE path=?)",
                (time.time(), path, path),
            )

    @classmethod
    def decode_target(cls, encoded: str, namespace: str, doc_id: str) -> dict[str, Any]:
        value = cls.decode(encoded)
        kind, stage, state = value.get("kind"), value.get("stage"), value.get("state")
        stages = {
            "upsert": ("process", "chunk", "embed", "publish"),
            "delete": ("delete",),
            "drop": ("drop",),
            "rebuild": ("rebuild",),
        }
        if (
            not isinstance(kind, str)
            or kind not in stages
            or stage not in stages[kind]
            or state
            not in (
                "pending",
                "running",
                "retry_wait",
                "failed",
                "blocked",
                "cancelled",
                "succeeded",
            )
            or not isinstance(value.get("revision"), str)
            or not value["revision"]
            or (bool(doc_id) != (kind in ("upsert", "delete")) and bool(namespace))
        ):
            raise CorruptState(f"invalid durable task for {namespace!r}/{doc_id!r}")
        return value

    @staticmethod
    def decode(encoded: str) -> dict[str, Any]:
        try:
            value = load_json(encoded)
            if not isinstance(value, dict):
                raise ValueError("record is not an object")
            return value
        except (TypeError, ValueError) as error:
            raise CorruptState(f"invalid catalog record: {error}") from error


def _name(value: str) -> str:
    return value.rsplit("/", 1)[-1]


def _suffix(value: str) -> str:
    return PurePosixPath(value).suffix.lower()
