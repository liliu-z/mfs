# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
from __future__ import annotations

from itertools import islice

import re2


def regex_ranges(
    text: str,
    pattern: str,
    *,
    regex: bool,
    case_sensitive: bool,
    limit: int | None = None,
    whole_word: bool = False,
) -> list[tuple[int, int]]:
    options = re2.Options()
    options.case_sensitive = case_sensitive
    expression = pattern if regex else re2.escape(pattern)
    compiled = re2.compile(expression, options=options)
    if compiled.search("") is not None:
        raise ValueError("pattern must not match an empty string")

    def word(character: str) -> bool:
        return character == "_" or character.isalnum()

    matches = (
        match
        for match in compiled.finditer(text)
        if not whole_word
        or (
            (match.start() == 0 or not word(text[match.start() - 1]))
            and (match.end() == len(text) or not word(text[match.end()]))
        )
    )
    return [(match.start(), match.end()) for match in islice(matches, limit)]
