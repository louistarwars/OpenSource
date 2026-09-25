"""A tiny on-disk database of failing schedules.

Like Hypothesis' example database: when a test fails, its minimal token is
saved under ``.detangle/`` and replayed first on the next run, so a bug that
was found once keeps failing until it is actually fixed -- even if the random
exploration would not stumble on it again.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

__all__ = ["ExampleDatabase"]


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class ExampleDatabase:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)

    def _dir(self, key: str) -> Path:
        return self.root / "examples" / _digest(key)

    def fetch(self, key: str) -> list[str]:
        directory = self._dir(key)
        if not directory.is_dir():
            return []
        tokens = []
        for path in sorted(directory.iterdir()):
            try:
                text = path.read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if text:
                tokens.append(text)
        return tokens

    def save(self, key: str, token: str) -> None:
        directory = self._dir(key)
        try:
            directory.mkdir(parents=True, exist_ok=True)
            gitignore = self.root / ".gitignore"
            if not gitignore.exists():
                gitignore.write_text("*\n", encoding="utf-8")
            (directory / _digest(token)).write_text(token + "\n", encoding="utf-8")
        except OSError:
            pass

    def delete(self, key: str, token: str) -> None:
        path = self._dir(key) / _digest(token)
        try:
            path.unlink()
        except OSError:
            pass
