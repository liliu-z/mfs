# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import cast

from ._json import JSONValue
from .types import ChunkRange, ProcessedDocument, SourceMap, SourceSpan


class Utf8TextProcessor:
    cache_scope = "content"
    workload = "light"
    concurrency = 4

    def __init__(self) -> None:
        self.id: str = "utf8-text"
        self.version: str = "1"
        self.options: JSONValue = {}
        self.media_types: tuple[str, ...] = ("text/plain", "text/markdown")
        self.suffix_media_types: Mapping[str, str] = MappingProxyType(
            {".txt": "text/plain", ".md": "text/markdown"}
        )

    def sniff(self, head: bytes) -> str | None:
        del head
        return None

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        del media_type
        raw = staged_path.read_bytes()
        if raw.startswith(b"\xef\xbb\xbf"):
            raw = raw[3:]
        text = raw.decode("utf-8", errors="strict")
        spans: list[SourceSpan] = []
        offset = 0
        for line_no, line in enumerate(text.splitlines(keepends=True), start=1):
            end = offset + len(line.encode("utf-8"))
            spans.append(
                SourceSpan(
                    text_start=offset,
                    text_end=end,
                    source={"kind": "lines", "start": line_no, "end": line_no},
                )
            )
            offset = end
        return ProcessedDocument(
            text=text, source_map=SourceMap(version=1, spans=tuple(spans)), text_path=staged_path
        )


class PdfProcessor:
    cache_scope = "content"
    workload = "heavy"

    def __init__(self) -> None:
        self.id: str = "pdf"
        self.version: str = "1+pymupdf4llm-1.28.2"
        self.options: JSONValue = {}
        self.media_types: tuple[str, ...] = ("application/pdf",)
        self.suffix_media_types: Mapping[str, str] = MappingProxyType({".pdf": "application/pdf"})

    def sniff(self, head: bytes) -> str | None:
        return "application/pdf" if head.startswith(b"%PDF-") else None

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        del media_type
        import pymupdf4llm

        pages_raw = pymupdf4llm.to_markdown(str(staged_path), page_chunks=True)
        if isinstance(pages_raw, str):
            raise ValueError("pymupdf4llm did not return page chunks")
        pages = cast(list[dict[str, object]], pages_raw)
        texts: list[str] = []
        spans: list[SourceSpan] = []
        offset = 0
        for page_no, page in enumerate(pages, start=1):
            page_text = str(page.get("text", ""))
            if texts:
                texts.append("\n")
                offset += 1
            texts.append(page_text)
            if page_text:
                end = offset + len(page_text.encode("utf-8"))
                spans.append(
                    SourceSpan(
                        text_start=offset,
                        text_end=end,
                        source={"kind": "pages", "start": page_no, "end": page_no},
                    )
                )
                offset = end
        return ProcessedDocument(text="".join(texts), source_map=SourceMap(1, tuple(spans)))


class DefaultChunker:
    workload = "light"
    concurrency = 4

    def __init__(self) -> None:
        self.id: str = "utf8-window"
        self.version: str = "1"
        self.options: JSONValue = {"max_bytes": 4096, "overlap_bytes": 512}

    def chunk(self, text: str, source_map: SourceMap) -> tuple[ChunkRange, ...]:
        del source_map
        data = text.encode("utf-8", errors="strict")
        if not data:
            return ()
        boundaries = _boundaries(text)
        ranges: list[ChunkRange] = []
        start = 0
        total = len(data)
        while start < total:
            hard_end = min(total, start + 4096)
            end = _boundary_at_or_before(boundaries, hard_end)
            if end < total:
                search_start = max(start, end - 1024)
                selected = _separator_end(text, boundaries, search_start, end)
                if selected is not None:
                    end = selected
            if end <= start:
                end = _boundary_after(boundaries, start)
            ranges.append(ChunkRange(start, end))
            if end == total:
                break
            next_target = max(start + 1, end - 512)
            start = _boundary_at_or_after(boundaries, next_target)
        return tuple(ranges)


class DocxProcessor:
    """Basic DOCX paragraph/table text; applications can supply richer extraction."""

    cache_scope = "content"
    workload = "light"

    def __init__(self) -> None:
        self.id: str = "docx"
        self.version: str = "1"
        self.options: JSONValue = {}
        self.media_types: tuple[str, ...] = (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        self.suffix_media_types: Mapping[str, str] = MappingProxyType(
            {".docx": self.media_types[0]}
        )

    def sniff(self, head: bytes) -> str | None:
        del head
        return None

    def process(self, staged_path: Path, media_type: str) -> ProcessedDocument:
        import xml.etree.ElementTree as ET
        import zipfile

        del media_type
        with zipfile.ZipFile(staged_path) as archive:
            root = ET.fromstring(archive.read("word/document.xml"))
        word = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
        texts: list[str] = []
        spans: list[SourceSpan] = []
        offset = 0
        for number, paragraph in enumerate(root.iter(word + "p"), 1):
            text = (
                "".join(
                    node.text or ""
                    if node.tag == word + "t"
                    else "\t"
                    if node.tag == word + "tab"
                    else "\n"
                    if node.tag in (word + "br", word + "cr")
                    else ""
                    for node in paragraph.iter()
                )
                + "\n"
            )
            end = offset + len(text.encode())
            texts.append(text)
            spans.append(
                SourceSpan(offset, end, {"kind": "paragraphs", "start": number, "end": number})
            )
            offset = end
        return ProcessedDocument("".join(texts), SourceMap(1, tuple(spans)))


def _boundaries(text: str) -> tuple[int, ...]:
    result = [0]
    position = 0
    for character in text:
        position += len(character.encode("utf-8"))
        result.append(position)
    return tuple(result)


def _boundary_at_or_before(boundaries: tuple[int, ...], value: int) -> int:
    import bisect

    return boundaries[bisect.bisect_right(boundaries, value) - 1]


def _boundary_at_or_after(boundaries: tuple[int, ...], value: int) -> int:
    import bisect

    index = bisect.bisect_left(boundaries, value)
    return boundaries[min(index, len(boundaries) - 1)]


def _boundary_after(boundaries: tuple[int, ...], value: int) -> int:
    import bisect

    return boundaries[bisect.bisect_right(boundaries, value)]


def _separator_end(
    text: str, boundaries: tuple[int, ...], search_start: int, search_end: int
) -> int | None:
    import bisect

    first_char = bisect.bisect_left(boundaries, search_start)
    last_char = bisect.bisect_right(boundaries, search_end) - 1
    fragment = text[first_char:last_char]
    for separator in ("\n\n", "\n"):
        index = fragment.rfind(separator)
        if index >= 0:
            return boundaries[first_char + index + len(separator)]
    for index in range(len(fragment) - 1, -1, -1):
        if fragment[index].isspace():
            return boundaries[first_char + index + 1]
    return None
