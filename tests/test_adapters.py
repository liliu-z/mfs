# pyright: reportUnknownMemberType=false
from itertools import pairwise
from pathlib import Path

import pytest

from mfs import DefaultChunker, PdfProcessor, SourceMap, Utf8TextProcessor


def test_utf8_text_processor_preserves_text_and_maps_lines(tmp_path: Path) -> None:
    source = tmp_path / "text.md"
    source.write_bytes(b"\xef\xbb\xbffirst\r\n\xe4\xba\x8c")

    processed = Utf8TextProcessor().process(source, "text/markdown")

    assert processed.text == "first\r\n二"
    assert [(span.text_start, span.text_end) for span in processed.source_map.spans] == [
        (0, 7),
        (7, 10),
    ]
    assert processed.source_map.spans[1].source == {"kind": "lines", "start": 2, "end": 2}


@pytest.mark.parametrize(
    "text",
    [
        "",
        "a" * 4096,
        "a" * 5000,
        "界" * 2000,
        ("paragraph words\n\n" * 400),
    ],
)
def test_default_chunker_covers_utf8_without_oversized_chunks(text: str) -> None:
    source_map = SourceMap(1, ())
    ranges = DefaultChunker().chunk(text, source_map)
    encoded = text.encode()

    if not encoded:
        assert ranges == ()
        return
    assert ranges[0].text_start == 0
    assert ranges[-1].text_end == len(encoded)
    assert all(0 < item.text_end - item.text_start <= 4096 for item in ranges)
    assert all(
        encoded[item.text_start : item.text_end].decode("utf-8") is not None for item in ranges
    )
    assert all(current.text_start <= previous.text_end for previous, current in pairwise(ranges))


def test_pdf_processor_emits_page_spans(tmp_path: Path) -> None:
    import pymupdf

    source = tmp_path / "two-pages.pdf"
    document = pymupdf.open()
    document.new_page().insert_text((72, 72), "Page one")
    document.new_page().insert_text((72, 72), "Page two")
    document.save(source)
    document.close()

    processed = PdfProcessor().process(source, "application/pdf")

    assert "Page one" in processed.text and "Page two" in processed.text
    assert [span.source for span in processed.source_map.spans] == [
        {"kind": "pages", "start": 1, "end": 1},
        {"kind": "pages", "start": 2, "end": 2},
    ]
    first, second = processed.source_map.spans
    assert processed.text.encode()[first.text_end : second.text_start] == b"\n"
