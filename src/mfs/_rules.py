from __future__ import annotations

import fnmatch
import re
from collections.abc import Sequence
from functools import lru_cache
from typing import cast

from .errors import InvalidConfiguration
from .types import IgnoreRule


def validate_rules(rules: Sequence[IgnoreRule]) -> tuple[IgnoreRule, ...]:
    result = tuple(rules)
    ids: set[str] = set()
    for rule in result:
        if (
            not isinstance(cast(object, rule.rule_id), str)
            or not rule.rule_id
            or rule.rule_id in ids
        ):
            raise InvalidConfiguration("rule_id must be non-empty and unique")
        ids.add(rule.rule_id)
        if rule.action not in ("include", "exclude"):
            raise InvalidConfiguration("rule action must be include or exclude")
        pattern = rule.pattern
        if (
            not isinstance(cast(object, pattern), str)
            or not pattern.strip("/")
            or "\0" in pattern
            or "\\" in pattern
            or any(p in (".", "..", "") for p in pattern.strip("/").split("/"))
        ):
            raise InvalidConfiguration("rule patterns must be non-empty relative POSIX globs")
        _pattern(pattern)
    return result


@lru_cache(maxsize=1024)
def _pattern(pattern: str) -> re.Pattern[str]:
    anchored = pattern.startswith("/") or "/" in pattern.strip("/")
    pieces = pattern.strip("/").split("/")
    expression = "" if anchored else "(?:.*/)?"
    for index, part in enumerate(pieces):
        last = index == len(pieces) - 1
        if part == "**":
            expression += ".*" if last else "(?:[^/]+/)*"
        else:
            # fnmatch translates a single segment, so * never traverses a slash.
            expression += _segment(part)
            if not last:
                expression += "/"
    return re.compile("^(?:" + expression + ")$")


def _segment(part: str) -> str:
    expression = ""
    index = 0
    while index < len(part):
        character = part[index]
        if character == "*":
            expression += "[^/]*"
        elif character == "?":
            expression += "[^/]"
        elif character == "[":
            end = part.find("]", index + 2)
            if end < 0:
                expression += r"\["
            else:
                translated = fnmatch.translate(part[index : end + 1])
                expression += (
                    translated.removeprefix("(?s:").removesuffix(")\\z").removesuffix(")\\Z")
                )
                index = end
        else:
            expression += re.escape(character)
        index += 1
    return expression


def excluded(rules: Sequence[IgnoreRule], relative: str, *, directory: bool = False) -> bool:
    parts = relative.split("/")
    result = False
    for rule in rules:
        matcher = _pattern(rule.pattern)
        # Every ancestor is a directory. A trailing slash doesn't match a plain file.
        count = len(parts) if directory or not rule.pattern.endswith("/") else len(parts) - 1
        if any(matcher.fullmatch("/".join(parts[:end])) for end in range(1, count + 1)):
            result = rule.action == "exclude"
    return result
