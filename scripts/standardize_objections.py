"""
Standardize CZ PSP voting objections into vote-event-objection.dt format.

Data sources in hl-2025ps.zip:
  zmatecne.unl  — void (zmatecné) vote IDs; cumulative across ALL parliamentary
                  terms; use --min-id to filter to the current term only.
  hl2025s.unl   — valid (non-void) votes of the current term; used to look up
                  dates. Void votes are NOT in this file, so date will be absent
                  for most records.

Limitations of the current PSP open-data zip:
  - raised_by_id: NOT available (hl_zposlanec table not published in the zip)
  - decision_vote_event_id: NOT available (hl_check table not published)
  - repeated_vote_event_id: NOT available (hl_check table not published)
  - date: absent for void votes (they are excluded from hl2025s.unl)

All PSP void votes are type=vote_correction with outcome=invalidated:
  an MP stated they voted differently from their intention, the body agreed
  to repeat the vote (zmatecné hlasování).

Usage:
  python scripts/standardize_objections.py [--min-id 85000]
"""

import json
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.utils_io import read_unl, ensure_dir

RAW_DIR = _REPO_ROOT / "work" / "raw" / "hl-2025ps"
OUT_PATH = _REPO_ROOT / "work" / "standard" / "vote_event_objections.json"

SOURCE_URL = "https://www.psp.cz/eknih/cdrom/opendata/hl-2025ps.zip"

# Lower bound for IDs considered part of the current parliamentary term.
# zmatecne.unl is cumulative; the 2025ps term starts around ID 85000.
DEFAULT_MIN_ID = 85000


def _to_start_date(date_raw: str | None, time_raw: str | None) -> str | None:
    d = (date_raw or "").strip()
    t = (time_raw or "").strip()
    if not d:
        return None
    if "." in d:
        dd, mm, yyyy = d.split(".")
        if t and ":" in t:
            return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}T{t}:00"
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return None


def standardize_objections(
    raw_dir: Path = RAW_DIR,
    out_path: Path = OUT_PATH,
    min_id: int = DEFAULT_MIN_ID,
) -> list[dict]:
    # Load all void vote IDs, filter to current term
    zmatecne_path = raw_dir / "zmatecne.unl"
    all_void_ids: list[int] = []
    if zmatecne_path.exists() and zmatecne_path.stat().st_size > 0:
        for r in read_unl(zmatecne_path):
            if r and r[0] and r[0].strip().isdigit():
                all_void_ids.append(int(r[0]))

    current_void_ids = sorted(i for i in all_void_ids if i >= min_id)
    logging.info(
        "zmatecne.unl: %d total void IDs, %d with id >= %d (current term)",
        len(all_void_ids), len(current_void_ids), min_id,
    )

    # Load valid vote dates from hl2025s.unl (void votes are absent here)
    hlasovani_path = raw_dir / "hl2025s.unl"
    hlas_by_id: dict[str, list[str]] = {}
    if hlasovani_path.exists():
        for r in read_unl(hlasovani_path, expected_ncols=18):
            hlas_by_id[str(r[0])] = r

    # Build objection records
    objections: list[dict] = []
    for hid in current_void_ids:
        hid_str = str(hid)
        row = hlas_by_id.get(hid_str)  # will be None for void votes

        obj: dict = {
            "id": f"psp:objection:{hid_str}",
            "vote_event_id": f"psp:vote-event:{hid_str}",
            "type": "vote_correction",
            "outcome": "invalidated",
            # raised_by_id not available: hl_zposlanec not in PSP open-data zip
            # decision_vote_event_id not available: hl_check not in PSP open-data zip
            # repeated_vote_event_id not available: hl_check not in PSP open-data zip
            "sources": [
                {
                    "url": SOURCE_URL,
                    "note": f"zmatecne.unl id_hlasovani={hid_str}",
                }
            ],
        }

        if row:
            date_str = _to_start_date(row[5], row[6])
            if date_str:
                obj["date"] = date_str
        # date absent for void votes: they are not in hl2025s.unl

        objections.append(obj)

    ensure_dir(out_path.parent)
    out_path.write_text(
        json.dumps(objections, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    logging.info(
        "Wrote %d objection records to %s (dates unavailable; raised_by_id unavailable)",
        len(objections), out_path,
    )
    return objections


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-id", type=int, default=DEFAULT_MIN_ID,
        help=f"Minimum vote ID to include (default: {DEFAULT_MIN_ID})",
    )
    parser.add_argument("--raw-dir", default=str(RAW_DIR))
    parser.add_argument("--output",  default=str(OUT_PATH))
    args = parser.parse_args()

    standardize_objections(
        raw_dir=Path(args.raw_dir),
        out_path=Path(args.output),
        min_id=args.min_id,
    )
