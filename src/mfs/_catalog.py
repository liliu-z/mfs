from __future__ import annotations

import sqlite3
import threading
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
        if version > 2:
            raise SchemaVersionUnsupported(f"catalog schema version {version} is unsupported")
        if not initialize and version not in (1, 2):
            raise CorruptState("catalog schema is missing or unrecognized")
        if initialize or version == 1:
            self._initialize()
            self.migrated = version == 1
        expected = {"namespaces", "documents", "targets", "operations"}
        actual = {
            r[0]
            for r in self.connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if actual != expected:
            raise CorruptState("catalog schema does not match version 2")

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
            PRAGMA user_version = 2;
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
        self.connection.execute("DELETE FROM namespaces WHERE namespace=?", (namespace,))

    def put_document(self, namespace: str, doc_id: str, value: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO documents VALUES(?,?,?) ON CONFLICT(namespace,doc_id) "
            "DO UPDATE SET value=excluded.value",
            (namespace, doc_id, compact_json(value)),
        )

    def delete_document(self, namespace: str, doc_id: str) -> None:
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
        self.connection.execute(
            "INSERT INTO targets VALUES(?,?,?,?) ON CONFLICT(namespace,doc_id) "
            "DO UPDATE SET revision=excluded.revision,value=excluded.value",
            (namespace, doc_id, value["revision"], compact_json(value)),
        )

    def get_target(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT value FROM targets WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        ).fetchone()
        return self.decode(row[0]) if row else None

    def list_targets(self, namespace: str | None = None) -> list[tuple[str, str, dict[str, Any]]]:
        sql = "SELECT namespace,doc_id,value FROM targets"
        rows = self.connection.execute(
            sql
            + (" WHERE namespace=?" if namespace is not None else "")
            + " ORDER BY namespace,doc_id",
            (namespace,) if namespace is not None else (),
        )
        return [(n, d, self.decode(v)) for n, d, v in rows]

    def delete_targets(self, namespace: str) -> None:
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
