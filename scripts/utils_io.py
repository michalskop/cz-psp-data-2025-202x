import csv
import logging
from pathlib import Path
from typing import Any, Iterable


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_unl(path: Path, expected_ncols: int | None = None, encoding: str = "cp1250") -> list[list[str]]:
    """Read PSP UNL file: pipe-separated, windows-1250, empty string = NULL."""
    raw = path.read_bytes().decode(encoding, errors="replace")
    rows = [line.split("|") for line in raw.splitlines() if line != ""]

    if expected_ncols is not None:
        bad = [i for i, r in enumerate(rows) if len(r) != expected_ncols]
        if bad:
            raise ValueError(f"{path} has {len(bad)} rows with unexpected column count (expected {expected_ncols}).")

    return rows


def read_unl_iter(path: Path, expected_ncols: int | None = None, encoding: str = "cp1250") -> Iterable[list[str]]:
    """Stream PSP UNL rows.

    This avoids loading large vote files into memory.
    """
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        for i, line in enumerate(f):
            line = line.rstrip("\n")
            if line == "":
                continue
            cols = line.split("|")
            if expected_ncols is not None and len(cols) != expected_ncols:
                raise ValueError(
                    f"{path} has unexpected column count on row {i} (expected {expected_ncols}, got {len(cols)})."
                )
            yield cols


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    ensure_dir(path.parent)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    logging.info("Wrote %s", path)
