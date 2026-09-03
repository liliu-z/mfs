from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from ._json import JSONValue, compact_json, load_json
from .errors import CorruptState, SchemaVersionUnsupported, StorageFailed


class Catalog:
    def __init__(self, path: Path, *, initialize: bool) -> None:
        try:
            self.connection = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
            self.connection.execute("PRAGMA foreign_keys = ON")
            version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
            if version > 1:
                raise SchemaVersionUnsupported(f"catalog schema version {version} is unsupported")
            if initialize:
                self._initialize()
            elif version != 1:
                raise CorruptState("catalog schema is missing or unrecognized")
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = FULL")
            self._validate_schema()
        except (CorruptState, SchemaVersionUnsupported):
            raise
        except sqlite3.Error as error:
            raise StorageFailed(f"failed to open catalog: {error}") from error

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            BEGIN IMMEDIATE;
            CREATE TABLE namespaces (
                namespace TEXT PRIMARY KEY COLLATE BINARY,
                value TEXT NOT NULL CHECK (json_valid(value))
            );
            CREATE TABLE documents (
                namespace TEXT NOT NULL,
                doc_id TEXT NOT NULL COLLATE BINARY,
                value TEXT NOT NULL CHECK (json_valid(value)),
                PRIMARY KEY (namespace, doc_id),
                FOREIGN KEY (namespace) REFERENCES namespaces(namespace) ON DELETE CASCADE
            );
            PRAGMA user_version = 1;
            COMMIT;
            """
        )

    def _validate_schema(self) -> None:
        tables = {
            row[0]
            for row in self.connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if tables != {"namespaces", "documents"}:
            raise CorruptState("catalog must contain exactly namespaces and documents tables")

    def close(self) -> None:
        self.connection.close()

    def put_namespace(self, namespace: str, value: dict[str, JSONValue]) -> None:
        try:
            self.connection.execute(
                "INSERT INTO namespaces(namespace,value) VALUES(?,?)",
                (namespace, compact_json(value)),
            )
        except sqlite3.Error as error:
            raise StorageFailed(f"failed to write namespace: {error}") from error

    def get_namespace(self, namespace: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT value FROM namespaces WHERE namespace = ?", (namespace,)
        ).fetchone()
        if row is None:
            return None
        value = load_json(row[0])
        if not isinstance(value, dict):
            raise CorruptState(f"namespace {namespace!r} is not a JSON object")
        return value

    def list_namespaces(self) -> list[tuple[str, dict[str, Any]]]:
        result: list[tuple[str, dict[str, Any]]] = []
        for namespace, encoded in self.connection.execute(
            "SELECT namespace,value FROM namespaces ORDER BY namespace"
        ):
            value = load_json(encoded)
            if not isinstance(value, dict):
                raise CorruptState(f"namespace {namespace!r} is not a JSON object")
            result.append((namespace, value))
        return result

    def put_document(self, namespace: str, doc_id: str, value: dict[str, JSONValue]) -> None:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute(
                "INSERT OR REPLACE INTO documents(namespace,doc_id,value) VALUES(?,?,?)",
                (namespace, doc_id, compact_json(value)),
            )
            self.connection.execute("COMMIT")
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise StorageFailed(f"failed to commit document: {error}") from error

    def delete_document(self, namespace: str, doc_id: str) -> None:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute(
                "DELETE FROM documents WHERE namespace = ? AND doc_id = ?", (namespace, doc_id)
            )
            self.connection.execute("COMMIT")
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise StorageFailed(f"failed to delete document: {error}") from error

    def delete_namespace(self, namespace: str) -> None:
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute("DELETE FROM namespaces WHERE namespace = ?", (namespace,))
            self.connection.execute("COMMIT")
        except sqlite3.Error as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise StorageFailed(f"failed to delete namespace: {error}") from error

    def get_document(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT value FROM documents WHERE namespace = ? AND doc_id = ?", (namespace, doc_id)
        ).fetchone()
        if row is None:
            return None
        return self._decode_document(namespace, doc_id, row[0])

    def list_documents(self) -> list[tuple[str, str, dict[str, Any]]]:
        return [
            (namespace, doc_id, self._decode_document(namespace, doc_id, encoded))
            for namespace, doc_id, encoded in self.connection.execute(
                "SELECT namespace,doc_id,value FROM documents ORDER BY namespace,doc_id"
            )
        ]

    def list_namespace_documents(self, namespace: str) -> list[tuple[str, dict[str, Any]]]:
        return [
            (doc_id, self._decode_document(namespace, doc_id, encoded))
            for doc_id, encoded in self.connection.execute(
                "SELECT doc_id,value FROM documents WHERE namespace = ? ORDER BY doc_id",
                (namespace,),
            )
        ]

    def document_count(self, namespace: str | None = None) -> int:
        if namespace is None:
            row = self.connection.execute("SELECT count(*) FROM documents").fetchone()
        else:
            row = self.connection.execute(
                "SELECT count(*) FROM documents WHERE namespace = ?", (namespace,)
            ).fetchone()
        return int(row[0])

    def namespace_count(self) -> int:
        return int(self.connection.execute("SELECT count(*) FROM namespaces").fetchone()[0])

    @staticmethod
    def _decode_document(namespace: str, doc_id: str, encoded: str) -> dict[str, Any]:
        try:
            value = load_json(encoded)
        except (TypeError, ValueError) as error:
            raise CorruptState(f"document {namespace}/{doc_id} contains invalid JSON") from error
        if not isinstance(value, dict):
            raise CorruptState(f"document {namespace}/{doc_id} is not a JSON object")
        return value
