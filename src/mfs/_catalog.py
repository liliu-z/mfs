from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

from ._json import compact_json, load_json
from .errors import CorruptState, SchemaVersionUnsupported, StorageFailed


class Catalog:
    """Short database leases; no connection follows the lifetime of a caller thread.

    Transactions pin a lease for nested catalog operations. Do not run adapters,
    filesystem work or acquire Lifecycle.condition inside a catalog transaction.
    """

    def __init__(self, path: Path, *, initialize: bool) -> None:
        self.path = path
        self._local = threading.local()
        self._connections: list[sqlite3.Connection] = []
        self._idle: list[sqlite3.Connection] = []
        self._connections_lock = threading.Condition()
        self._closed = False
        self._trace: Callable[[str], None] | None = None
        self.migrated = False
        try:
            self._load(initialize)
        except BaseException:
            self.close()
            raise

    def _load(self, initialize: bool) -> None:
        version = int(self.query("PRAGMA user_version")[0][0])
        if version > 8:
            raise SchemaVersionUnsupported(f"catalog schema version {version} is unsupported")
        if version not in (1, 2, 3, 4, 5, 6, 7, 8) and not (initialize and version == 0):
            raise CorruptState("catalog schema is missing or unrecognized")
        if (
            initialize
            and version == 0
            and self.one("SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1")
        ):
            raise CorruptState("bootstrap catalog contains an unrecognized schema")
        if version < 8:
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
            "vector_cache",
            "active_runs",
            "build_targets",
            "build_documents",
            "index_cleanup",
        }
        actual = {
            r[0]
            for r in self.query(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if actual != expected:
            raise CorruptState("catalog schema does not match version 8")

    @contextmanager
    def _connection(self) -> Generator[sqlite3.Connection]:
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            yield connection
            return
        with self._connections_lock:
            if not self._connections_lock.wait_for(
                lambda: self._closed or self._idle or len(self._connections) < 8, timeout=30
            ):
                raise StorageFailed("catalog connection acquisition timed out")
            if self._closed:
                raise StorageFailed("catalog is closed")
            if self._idle:
                connection = self._idle.pop()
            else:
                connection = sqlite3.connect(
                    self.path, check_same_thread=False, isolation_level=None, timeout=30
                )
                try:
                    connection.execute("PRAGMA foreign_keys = ON")
                    connection.execute("PRAGMA journal_mode = WAL")
                    connection.execute("PRAGMA synchronous = FULL")
                    connection.create_function("mfs_name", 1, _name, deterministic=True)
                    connection.create_function("mfs_suffix", 1, _suffix, deterministic=True)
                except BaseException:
                    connection.close()
                    raise
                self._connections.append(connection)
        self._local.connection = connection
        try:
            connection.set_trace_callback(self._trace)
            yield connection
        finally:
            self._local.connection = None
            reusable = False
            try:
                if connection.in_transaction:
                    connection.rollback()
                reusable = True
            finally:
                with self._connections_lock:
                    if reusable:
                        self._idle.append(connection)
                    else:
                        connection.close()
                        self._connections.remove(connection)
                    self._connections_lock.notify()

    def set_trace_callback(self, callback: Callable[[str], None] | None) -> None:
        self._trace = callback

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        with self._connection() as connection:
            connection.execute(sql, params).close()

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        with self._connection() as connection:
            cursor = connection.execute(sql, params)
            try:
                return cursor.fetchall()
            finally:
                cursor.close()

    def one(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None:
        with self._connection() as connection:
            cursor = connection.execute(sql, params)
            try:
                return cursor.fetchone()
            finally:
                cursor.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript("""
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
            CREATE INDEX IF NOT EXISTS targets_namespace_kind
                ON targets(namespace,json_extract(value,'$.kind'),doc_id);
            CREATE TABLE IF NOT EXISTS active_runs (
                namespace TEXT NOT NULL, doc_id TEXT NOT NULL,
                value TEXT NOT NULL CHECK(json_valid(value)), PRIMARY KEY(namespace,doc_id)
            );
            CREATE TABLE IF NOT EXISTS build_targets (
                namespace TEXT NOT NULL, doc_id TEXT NOT NULL,
                value TEXT NOT NULL CHECK(json_valid(value)), PRIMARY KEY(namespace,doc_id)
            );
            CREATE TABLE IF NOT EXISTS build_documents (
                namespace TEXT NOT NULL, doc_id TEXT NOT NULL,
                value TEXT NOT NULL CHECK(json_valid(value)), PRIMARY KEY(namespace,doc_id)
            );
            CREATE TABLE IF NOT EXISTS index_cleanup (
                key TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value))
            );
            CREATE INDEX IF NOT EXISTS cleanup_scope ON index_cleanup(
                json_extract(value,'$.namespace'),json_extract(value,'$.doc_id')
            );
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
            CREATE TABLE IF NOT EXISTS vector_cache (
                key TEXT PRIMARY KEY, incarnation TEXT NOT NULL, vector BLOB NOT NULL,
                digest TEXT NOT NULL, touched INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS vector_cache_lru ON vector_cache(touched);
            DROP TABLE IF EXISTS run_dependencies;
            DROP TABLE IF EXISTS wait_operations;
            DROP TABLE IF EXISTS wait_target_sets;
            DROP TABLE IF EXISTS runs;
            UPDATE operations SET value=json_remove(value,'$.operation_id');
            PRAGMA user_version = 8;
            COMMIT;
        """)

    @contextmanager
    def transaction(self, *, blocking: bool = True) -> Generator[None]:
        with self._connection() as connection:
            nested = connection.in_transaction
            try:
                if not blocking:
                    connection.execute("PRAGMA busy_timeout=0")
                if not nested:
                    connection.execute("BEGIN IMMEDIATE")
                yield
                if not nested:
                    connection.execute("COMMIT")
            except BaseException as error:
                if not nested and connection.in_transaction:
                    connection.execute("ROLLBACK")
                if isinstance(error, sqlite3.Error):
                    raise StorageFailed(f"catalog transaction failed: {error}") from error
                raise
            finally:
                if not blocking:
                    connection.execute("PRAGMA busy_timeout=30000")

    def close(self) -> None:
        with self._connections_lock:
            self._closed = True
            for connection in self._connections:
                connection.close()
            self._connections.clear()
            self._idle.clear()
            self._connections_lock.notify_all()

    def put_namespace(self, namespace: str, value: dict[str, Any]) -> None:
        self.execute(
            "INSERT INTO namespaces VALUES(?,?) ON CONFLICT(namespace) "
            "DO UPDATE SET value=excluded.value",
            (namespace, compact_json(value)),
        )

    def get_namespace(self, namespace: str) -> dict[str, Any] | None:
        row = self.one("SELECT value FROM namespaces WHERE namespace=?", (namespace,))
        return self.decode(row[0]) if row else None

    def list_namespaces(self) -> list[tuple[str, dict[str, Any]]]:
        return [
            (n, self.decode(v))
            for n, v in self.query("SELECT namespace,value FROM namespaces ORDER BY namespace")
        ]

    def delete_namespace(self, namespace: str) -> None:
        record = self.get_namespace(namespace)
        if record is not None:
            self.clear_vector_cache(record.get("incarnation", ""))
        for doc, _ in self.list_namespace_documents(namespace):
            self.set_references("document", namespace, doc, set())
        self.execute("DELETE FROM cancel_gates WHERE namespace=?", (namespace,))
        self.execute("DELETE FROM namespaces WHERE namespace=?", (namespace,))

    def clear_vector_cache(self, incarnation: str) -> None:
        self.execute("DELETE FROM vector_cache WHERE incarnation=?", (incarnation,))

    def put_document(self, namespace: str, doc_id: str, value: dict[str, Any]) -> None:
        self.set_references("document", namespace, doc_id, self.references(value))
        self.execute(
            "INSERT INTO documents VALUES(?,?,?) ON CONFLICT(namespace,doc_id) "
            "DO UPDATE SET value=excluded.value",
            (namespace, doc_id, compact_json(value)),
        )

    def delete_document(self, namespace: str, doc_id: str) -> None:
        self.set_references("document", namespace, doc_id, set())
        self.execute("DELETE FROM documents WHERE namespace=? AND doc_id=?", (namespace, doc_id))

    def get_document(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        row = self.one(
            "SELECT value FROM documents WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        )
        return self.decode(row[0]) if row else None

    def get_document_revision(
        self, namespace: str, doc_id: str, *, candidate: bool = False
    ) -> str | None:
        table = "build_documents" if candidate else "documents"
        row = self.one(
            f"SELECT json_extract(value,'$.revision') FROM {table} WHERE namespace=? AND doc_id=?",
            (namespace, doc_id),
        )
        return str(row[0]) if row and row[0] is not None else None

    def select_documents(
        self, where: str = "1", params: Sequence[Any] = ()
    ) -> list[tuple[str, str, dict[str, Any]]]:
        return [
            (n, d, self.decode(v))
            for n, d, v in self.query(
                "SELECT namespace,doc_id,value FROM documents WHERE "
                + where
                + " ORDER BY namespace,doc_id",
                params,
            )
        ]

    def list_documents(self) -> list[tuple[str, str, dict[str, Any]]]:
        return self.select_documents()

    def iter_documents(
        self, where: str = "1", params: Sequence[Any] = (), *, candidate: bool = False
    ) -> Generator[tuple[str, str, dict[str, Any]]]:
        source = (
            "(SELECT namespace,doc_id,value FROM build_documents UNION ALL "
            "SELECT d.namespace,d.doc_id,d.value FROM documents d WHERE NOT EXISTS "
            "(SELECT 1 FROM build_targets b WHERE b.namespace=d.namespace AND b.doc_id=d.doc_id))"
            if candidate
            else "documents"
        )
        # Never retain a database lease across a caller yield (grep may run adapters).
        # Membership may change between pages; ReadView validates each selected revision.
        after: tuple[str, str] | None = None
        while True:
            rows = self.query(
                f"SELECT namespace,doc_id,value FROM {source} WHERE ("
                + where
                + ")"
                + (" AND (namespace,doc_id) > (?,?)" if after is not None else "")
                + " ORDER BY namespace,doc_id LIMIT 128",
                (*params, *after) if after is not None else params,
            )
            if not rows:
                return
            for namespace, doc_id, value in rows:
                yield namespace, doc_id, self.decode(value)
            after = rows[-1][0], rows[-1][1]

    def list_namespace_documents(self, namespace: str) -> list[tuple[str, dict[str, Any]]]:
        return [(d, v) for _, d, v in self.select_documents("namespace=?", (namespace,))]

    def document_count(self, namespace: str | None = None) -> int:
        sql = "SELECT count(*) FROM documents"
        return int(
            self.query(
                sql + (" WHERE namespace=?" if namespace is not None else ""),
                (namespace,) if namespace is not None else (),
            )[0][0]
        )

    def namespace_count(self) -> int:
        return int(self.query("SELECT count(*) FROM namespaces")[0][0])

    def put_target(self, namespace: str, doc_id: str, value: dict[str, Any]) -> None:
        previous = self.get_target(namespace, doc_id)
        if previous and (
            previous.get("snapshot_id") != value.get("snapshot_id")
            or previous.get("collection_generation") != value.get("collection_generation")
        ):
            self.enqueue_cleanup(namespace, doc_id, previous)
        self.set_references("target", namespace, doc_id, self.references(value))
        if previous and previous["revision"] != value["revision"]:
            self.clear_prepared(str(previous["revision"]))
        if value["stage"] != "process":
            self.clear_prepared(str(value["revision"]))
        self.execute(
            "INSERT INTO targets VALUES(?,?,?,?) ON CONFLICT(namespace,doc_id) "
            "DO UPDATE SET revision=excluded.revision,value=excluded.value",
            (namespace, doc_id, value["revision"], compact_json(value)),
        )

    def get_target(self, namespace: str, doc_id: str) -> dict[str, Any] | None:
        row = self.one(
            "SELECT value FROM targets WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        )
        return self.decode_target(row[0], namespace, doc_id) if row else None

    def put_active(self, namespace: str, doc_id: str, value: dict[str, Any] | None) -> None:
        self.set_references("active", namespace, doc_id, self.references(value) if value else set())
        if value is None:
            self.execute(
                "DELETE FROM active_runs WHERE namespace=? AND doc_id=?", (namespace, doc_id)
            )
        else:
            self.execute(
                "INSERT INTO active_runs VALUES(?,?,?) ON CONFLICT(namespace,doc_id) "
                "DO UPDATE SET value=excluded.value",
                (namespace, doc_id, compact_json(value)),
            )

    def put_build(
        self,
        namespace: str,
        doc_id: str,
        value: dict[str, Any] | None,
        *,
        document: bool = False,
        retire: bool = True,
    ) -> None:
        table = "build_documents" if document else "build_targets"
        if not document and retire:
            previous = self.get_build(namespace, doc_id)
            if previous and (
                value is None
                or any(
                    previous.get(k) != value.get(k)
                    for k in ("snapshot_id", "collection_generation")
                )
            ):
                self.enqueue_cleanup(namespace, doc_id, previous)
        self.set_references(table, namespace, doc_id, self.references(value) if value else set())
        if value is None:
            self.execute(f"DELETE FROM {table} WHERE namespace=? AND doc_id=?", (namespace, doc_id))
        else:
            self.execute(
                f"INSERT INTO {table} VALUES(?,?,?) ON CONFLICT(namespace,doc_id) "
                "DO UPDATE SET value=excluded.value",
                (namespace, doc_id, compact_json(value)),
            )

    def get_build(
        self, namespace: str, doc_id: str, *, document: bool = False
    ) -> dict[str, Any] | None:
        table = "build_documents" if document else "build_targets"
        row = self.one(
            f"SELECT value FROM {table} WHERE namespace=? AND doc_id=?", (namespace, doc_id)
        )
        return self.decode(row[0]) if row else None

    def enqueue_cleanup(self, namespace: str, doc_id: str, job: dict[str, Any]) -> None:
        snapshot = job.get("snapshot_id")
        if not snapshot:
            return
        value = dict(
            namespace=namespace,
            doc_id=doc_id,
            incarnation=job["incarnation"],
            generation=job.get("collection_generation"),
            snapshot=snapshot,
            failures=0,
            next_run=0,
            error=None,
        )
        key = compact_json([namespace, doc_id, value["incarnation"], value["generation"], snapshot])
        self.execute("INSERT OR IGNORE INTO index_cleanup VALUES(?,?)", (key, compact_json(value)))

    def cleanup_pending(self, namespace: str, doc_id: str, incarnation: str | None = None) -> bool:
        return (
            self.one(
                "SELECT 1 FROM index_cleanup WHERE json_extract(value,'$.namespace')=? "
                "AND json_extract(value,'$.doc_id')=? "
                + ("AND json_extract(value,'$.incarnation')=? " if incarnation else "")
                + "LIMIT 1",
                (namespace, doc_id, incarnation) if incarnation else (namespace, doc_id),
            )
            is not None
        )

    def settle_collection(self, incarnation: str, generation: str | None) -> None:
        """Call in the retirement transaction, after this exact collection is gone."""
        self.execute(
            "DELETE FROM index_cleanup WHERE json_extract(value,'$.incarnation')=? "
            "AND json_extract(value,'$.generation') IS ?",
            (incarnation, generation),
        )

    def cleanup_rows(self, namespace: str | None = None) -> list[tuple[str, dict[str, Any]]]:
        rows = self.query(
            "SELECT key,value FROM index_cleanup"
            + (" WHERE json_extract(value,'$.namespace')=?" if namespace is not None else ""),
            (namespace,) if namespace is not None else (),
        )
        return [(key, self.decode(value)) for key, value in rows]

    def list_targets(self, namespace: str | None = None) -> list[tuple[str, str, dict[str, Any]]]:
        sql = "SELECT namespace,doc_id,value FROM targets"
        rows = self.query(
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
        self.execute("DELETE FROM targets WHERE namespace=?", (namespace,))

    def get_operation(self, key: str) -> tuple[str, dict[str, Any]] | None:
        row = self.one("SELECT request_hash,value FROM operations WHERE key=?", (key,))
        return (str(row[0]), self.decode(row[1])) if row else None

    def put_operation(self, key: str, request_hash: str, value: dict[str, Any]) -> None:
        self.execute(
            "INSERT INTO operations VALUES(?,?,?)", (key, request_hash, compact_json(value))
        )

    def clear_prepared(self, revision: str) -> None:
        self.set_references("prepared", "", revision, set())
        self.execute("DELETE FROM prepared WHERE revision=?", (revision,))

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
        return self.query(
            f"SELECT namespace,doc_id FROM targets WHERE {where} "
            "ORDER BY namespace,doc_id LIMIT ? OFFSET ?",
            [*parameters, limit, offset],
        )

    def status_counts(
        self, namespace: str | None, path: str
    ) -> tuple[dict[str, int], dict[str, int]]:
        where, parameters = self._scope(namespace, path)
        states: dict[str, int] = {}
        stages: dict[str, int] = {}
        for state, stage, count in self.query(
            "SELECT json_extract(value,'$.state'),json_extract(value,'$.stage'),count(*) "
            "FROM (SELECT t.namespace,t.doc_id,coalesce(b.value,t.value) AS value "
            "FROM targets t LEFT JOIN build_targets b USING(namespace,doc_id)) "
            f"WHERE {where} GROUP BY 1,2",
            parameters,
        ):
            states[state] = states.get(state, 0) + count
            stages[stage] = stages.get(stage, 0) + count
        return states, stages

    def set_cancelled(self, namespace: str, doc_id: str, cancelled: bool) -> None:
        if cancelled:
            self.execute("INSERT OR IGNORE INTO cancel_gates VALUES(?,?)", (namespace, doc_id))
        else:
            self.execute(
                "DELETE FROM cancel_gates WHERE namespace=? AND doc_id=?", (namespace, doc_id)
            )

    def cancelled(self, namespace: str, doc_id: str) -> bool:
        return (
            self.one(
                "SELECT 1 FROM cancel_gates WHERE namespace=? AND doc_id=?", (namespace, doc_id)
            )
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
        self.execute(
            "INSERT OR IGNORE INTO artifacts(path,unreferenced_at,size) VALUES(?,?,?)",
            (path, time.time(), size),
        )

    def set_references(self, owner: str, namespace: str, doc_id: str, paths: set[str]) -> None:
        params = (owner, namespace, doc_id)
        previous = {
            str(r[0])
            for r in self.query(
                "SELECT path FROM artifact_refs WHERE owner=? AND namespace=? AND doc_id=?", params
            )
        }
        for path in paths - previous:
            self.register_artifact(path)
            row = self.one("SELECT state FROM artifacts WHERE path=?", (path,))
            if row is None or row[0] != "live":
                raise CorruptState("cannot reference an artifact claimed for deletion")
            self.execute("INSERT INTO artifact_refs VALUES(?,?,?,?)", (*params, path))
            self.execute("UPDATE artifacts SET unreferenced_at=NULL WHERE path=?", (path,))
        for path in previous - paths:
            self.execute(
                "DELETE FROM artifact_refs WHERE owner=? AND namespace=? AND doc_id=? AND path=?",
                (*params, path),
            )
            self.execute(
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
