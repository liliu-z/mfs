from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

from ._index import dense_config, index_config
from ._validation import (
    chunker_description,
    processor_description,
    validate_chunker,
    validate_embedder,
    validate_processors,
)
from .adapters import DefaultChunker
from .errors import InvalidConfiguration, NamespaceCompatibilityError
from .types import Chunker, Embedder, IndexingMode, Processor


@dataclass(frozen=True)
class NamespaceBinding:
    processors: tuple[Processor, ...]
    chunker: Chunker
    embedder: Embedder | None
    descriptions: dict[int, dict[str, Any]]
    media_types: dict[int, tuple[str, ...]]
    suffixes: dict[int, dict[str, str]]
    manifest: dict[str, Any]

    @classmethod
    def build(
        cls,
        processors: Sequence[Processor],
        chunker: Chunker | None,
        embedder: Embedder | None,
        indexing: IndexingMode,
    ) -> NamespaceBinding:
        selected = validate_processors(processors)
        splitter = validate_chunker(chunker or DefaultChunker())
        model = validate_embedder(embedder)
        if indexing not in ("off", "bm25", "hybrid"):
            raise InvalidConfiguration("indexing must be off, bm25 or hybrid")
        if indexing == "hybrid" and model is None:
            raise InvalidConfiguration("hybrid indexing requires an Embedder")
        descriptions = {id(p): processor_description(p) for p in selected}
        media_types = {id(p): tuple(p.media_types) for p in selected}
        suffixes = {id(p): dict(p.suffix_media_types) for p in selected}
        manifest = {
            "embedder": dense_config(model.embedding_space, model.dimension) if model else None,
            "processors": sorted(
                [
                    dict(
                        descriptions[id(p)],
                        media_types=sorted(media_types[id(p)]),
                        suffix_media_types=suffixes[id(p)],
                    )
                    for p in selected
                ],
                key=lambda d: (d["id"], d["version"]),
            ),
            "index": index_config(
                cast(dict[str, object], chunker_description(splitter)),
                dense_config(model.embedding_space, model.dimension)
                if model is not None and indexing == "hybrid"
                else None,
            ),
        }
        return cls(selected, splitter, model, descriptions, media_types, suffixes, manifest)

    def verify(self, namespace: str, expected: dict[str, Any]) -> None:
        differences: list[str] = []

        def compare(path: str, before: Any, after: Any) -> None:
            if isinstance(before, dict) and isinstance(after, dict):
                before = cast(dict[str, Any], before)
                after = cast(dict[str, Any], after)
                for key in sorted(before.keys() | after.keys()):
                    compare(f"{path}.{key}", before.get(key), after.get(key))
            elif before != after:
                differences.append(f"{path}: expected {before!r}, got {after!r}")

        supplied = self.manifest
        if self.embedder is None and expected["index"]["dense"] is None:
            # An inactive model need not be constructed just to process text or build BM25.
            supplied = dict(supplied, embedder=expected.get("embedder"))
        compare("manifest", expected, supplied)
        if differences:
            raise NamespaceCompatibilityError(f"{namespace}: " + "; ".join(differences))
