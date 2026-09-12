from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ._validation import (
    normalized_media_type,
    validate_external_path,
    validate_internal_id,
    validate_namespace,
)
from .errors import InvalidFilter, NamespaceNotFound
from .types import (
    AnyOf,
    ByDocumentId,
    ByExtension,
    ByMediaType,
    ByNamespace,
    DocumentId,
    Filter,
    NamePrefix,
    NamespaceKind,
    NameSuffix,
    PathPrefix,
    PathSuffix,
    TextMatch,
    UnderPath,
)


def literal(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def prefix_expression(field: str, prefix: str) -> str:
    # Lexical intervals avoid LIKE wildcard/escaping differences between Milvus versions.
    chars = list(prefix)
    while chars and ord(chars[-1]) == 0x10FFFF:
        chars.pop()
    if not chars:
        return f"{field} >= {literal(prefix)}"
    next_code = ord(chars[-1]) + 1
    if next_code == 0xD800:
        next_code = 0xE000
    upper = "".join(chars[:-1]) + chr(next_code)
    return f"({field} >= {literal(prefix)} and {field} < {literal(upper)})"


@dataclass(frozen=True)
class CompiledFilters:
    sql: str
    params: tuple[Any, ...]
    expressions: tuple[str, ...]
    text: tuple[TextMatch, ...]


def compile_filters(
    filters: Sequence[Filter],
    namespaces: Mapping[str, NamespaceKind],
    *,
    search: bool,
) -> CompiledFilters:
    sql: list[str] = []
    params: list[Any] = []
    clauses: list[str] = []
    wanted_ids: set[DocumentId] | None = None
    text: list[TextMatch] = []

    def namespace_exists(name: str) -> None:
        validate_namespace(name)
        if name not in namespaces:
            raise NamespaceNotFound(f"namespace {name!r} does not exist")

    for item in filters:
        if isinstance(item, AnyOf):
            if not item.filters:
                raise InvalidFilter("AnyOf must not be empty")
            branches = [
                compile_filters([child], namespaces, search=search) for child in item.filters
            ]
            if any(branch.text for branch in branches):
                raise InvalidFilter("AnyOf accepts only structured metadata filters")
            sql.append("(" + " OR ".join("(" + b.sql + ")" for b in branches) + ")")
            params.extend(value for branch in branches for value in branch.params)
            clauses.append(
                "("
                + " or ".join(
                    "(" + expression + ")"
                    for branch in branches
                    for expression in branch.expressions
                )
                + ")"
            )
        elif isinstance(item, ByNamespace):
            if not item.namespaces:
                raise InvalidFilter("ByNamespace must not be empty")
            for name in item.namespaces:
                namespace_exists(name)
            sql.append("namespace IN (" + ",".join("?" for _ in item.namespaces) + ")")
            params.extend(item.namespaces)
            clauses.append("namespace in " + literal_list(item.namespaces))
        elif isinstance(item, ByDocumentId):
            if not item.ids:
                raise InvalidFilter("ByDocumentId must not be empty")
            ids = tuple(dict.fromkeys(item.ids))
            for identity in ids:
                namespace_exists(identity.namespace)
                validate_internal_id(identity.doc_id)
                if namespaces[identity.namespace] == "external":
                    validate_external_path(identity.doc_id, allow_root=False)
            wanted_ids = set(ids) if wanted_ids is None else wanted_ids.intersection(ids)
        elif isinstance(item, UnderPath):
            namespace_exists(item.namespace)
            if namespaces[item.namespace] != "external":
                raise InvalidFilter("UnderPath requires an external namespace")
            path = validate_external_path(item.path, allow_root=True)
            sql.append("namespace = ?")
            params.append(item.namespace)
            clauses.append(f"namespace == {literal(item.namespace)}")
            if path != ".":
                prefix = path + "/"
                sql.append("(doc_id = ? OR substr(doc_id,1,?) = ?)")
                params.extend((path, len(prefix), prefix))
                clauses.append(
                    f"(source_path == {literal(path)} or "
                    f"{prefix_expression('source_path', prefix)})"
                )
        elif isinstance(item, (PathPrefix, PathSuffix, NamePrefix, NameSuffix)):
            value = item.value
            if not isinstance(_runtime(value), str) or not value or "\0" in value:
                raise InvalidFilter("path/name affix must be a non-empty string without NUL")
            value.encode("utf-8", errors="strict")
            field = "source_name" if isinstance(item, (NamePrefix, NameSuffix)) else "source_path"
            sql_field = "mfs_name(doc_id)" if field == "source_name" else "doc_id"
            if isinstance(item, (PathSuffix, NameSuffix)):
                sql.append(f"substr({sql_field},-?) = ?")
                params.extend((len(value), value))
                clauses.append(prefix_expression(field + "_rev", value[::-1]))
            else:
                sql.append(f"substr({sql_field},1,?) = ?")
                params.extend((len(value), value))
                clauses.append(prefix_expression(field, value))
        elif isinstance(item, (ByExtension, ByMediaType)):
            values = item.extensions if isinstance(item, ByExtension) else item.media_types
            if not values or any(
                not isinstance(_runtime(x), str) or not x or "\0" in x for x in values
            ):
                raise InvalidFilter("extension/media type values must be non-empty strings")
            if isinstance(item, ByExtension):
                values = tuple(x.lower() if x.startswith(".") else "." + x.lower() for x in values)
                field, sql_field = "source_ext", "mfs_suffix(doc_id)"
            else:
                values = tuple(normalized_media_type(x) for x in values)
                field, sql_field = "media_type", "json_extract(value,'$.media_type')"
            sql.append(f"{sql_field} IN (" + ",".join("?" for _ in values) + ")")
            params.extend(values)
            clauses.append(field + " in " + literal_list(values))
        elif isinstance(item, TextMatch):
            if search:
                raise InvalidFilter("TextMatch belongs to grep; search accepts structural filters")
            text.append(item)
        else:
            raise InvalidFilter(f"unsupported Filter type: {type(item).__name__}")
    id_groups: list[str] = [""]
    if wanted_ids is not None:
        ordered = sorted(wanted_ids)
        # Row-value IN lets SQLite use the composite primary key for point reads.
        sql.append(
            "(namespace,doc_id) IN (SELECT json_extract(value,'$[0]'),"
            "json_extract(value,'$[1]') FROM json_each(?))"
        )
        params.append(json.dumps([(x.namespace, x.doc_id) for x in ordered]))
        id_groups = [
            "("
            + " or ".join(
                f"(namespace == {literal(x.namespace)} and doc_id == {literal(x.doc_id)})"
                for x in ordered[start : start + 200]
            )
            + ")"
            for start in range(0, len(ordered), 200)
        ] or ['id == ""']
    expressions = tuple(" and ".join([*clauses, part] if part else clauses) for part in id_groups)
    return CompiledFilters(" AND ".join(sql) or "1", tuple(params), expressions, tuple(text))


def literal_list(values: Sequence[str]) -> str:
    return "[" + ",".join(literal(x) for x in values) + "]"


def _runtime(value: object) -> object:
    return value
