from pathlib import Path


def read_text(path: Path, encoding: str = "utf-8-sig") -> str:
    """Decode text without universal-newline translation; offsets count UTF-8 bytes."""
    return path.read_bytes().decode(encoding)
