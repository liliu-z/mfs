from __future__ import annotations

import copy
import json
import math
from collections.abc import Mapping, Sequence
from typing import cast

import rfc8785

type JSONScalar = bool | int | float | str | None
type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]


def copy_json(value: object) -> JSONValue:
    _validate_json(value)
    return copy.deepcopy(value)  # type: ignore[return-value]


def canonical_json(value: object) -> bytes:
    validated = copy_json(value)
    return rfc8785.dumps(validated)


def compact_json(value: object) -> str:
    validated = copy_json(value)
    return json.dumps(validated, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def load_json(value: str) -> JSONValue:
    decoded: object = json.loads(value)
    return copy_json(decoded)


def _validate_json(value: object) -> None:
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, int) and not isinstance(value, bool):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return
    if isinstance(value, Mapping):
        for key, item in cast(Mapping[object, object], value).items():
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            _validate_json(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        for item in cast(Sequence[object], value):
            _validate_json(item)
        return
    raise ValueError(f"not a JSON value: {type(value).__name__}")
