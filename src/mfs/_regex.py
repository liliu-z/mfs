# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
from __future__ import annotations

import re2


def regex_ranges(
    text: str, pattern: str, *, regex: bool, case_sensitive: bool
) -> list[tuple[int, int]]:
    options = re2.Options()
    options.case_sensitive = case_sensitive
    expression = pattern if regex else re2.escape(pattern)
    compiled = re2.compile(expression, options=options)
    if compiled.search("") is not None:
        raise ValueError("pattern must not match an empty string")
    return [(match.start(), match.end()) for match in compiled.finditer(text)]
