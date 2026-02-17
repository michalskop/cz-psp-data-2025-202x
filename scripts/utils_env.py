from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(repo_root: Path) -> None:
    """Load environment variables from a .env file at repo root.

    Minimal implementation to avoid extra dependencies.
    - Ignores blank lines and comments (#...)
    - Parses KEY=VALUE (no shell expansion)
    - Does not override already-set environment variables
    """

    env_path = repo_root / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        os.environ.setdefault(k, v)
