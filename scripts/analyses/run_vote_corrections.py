"""
Run the vote-corrections analysis for cz-psp-data-2025-202x.

Steps:
  1. Ensure vote_event_objections.json exists (runs standardize_objections if not)
  2. Ensure votes.csv and vote_events.json exist (downloads from B2 if needed)
  3. Run vote_corrections.py (from legislature-data-analyses)
  4. Run output_flourish_table.py to produce the Flourish CSV

Outputs:
  analyses/vote-corrections/outputs/vote_corrections.json
  analyses/vote-corrections/outputs/vote_corrections_flourish_table.csv

Usage:
  python scripts/analyses/run_vote_corrections.py \\
      --script /path/to/legislature-data-analyses/vote-corrections/vote_corrections.py \\
      --flourish-script /path/to/legislature-data-analyses/vote-corrections/outputs/output_flourish_table.py \\
      [--use-current-members]

The --script and --flourish-script arguments are required because the
vote-corrections analysis lives in a separate repository (legislature-data-analyses)
and its location relative to this repo is not assumed.
"""

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.download_b2 import download_latest_from_pointer


# ── Default paths (all within this repo) ──────────────────────────────────────

_OBJECTIONS  = _REPO_ROOT / "work" / "standard" / "vote_event_objections.json"
_VOTES       = _REPO_ROOT / "work" / "standard" / "votes.csv"
_VOTE_EVENTS = _REPO_ROOT / "work" / "standard" / "vote_events.json"
_PERSONS     = _REPO_ROOT / "analyses" / "all-members" / "outputs" / "all_members.csv"
_CURRENT_MEM = _REPO_ROOT / "analyses" / "current-members" / "outputs" / "current_members.csv"
_OUTPUT_JSON = _REPO_ROOT / "analyses" / "vote-corrections" / "outputs" / "vote_corrections.json"
_OUTPUT_CSV  = _REPO_ROOT / "analyses" / "vote-corrections" / "outputs" / "vote_corrections_flourish_table.csv"
_WORK_DIR    = _REPO_ROOT / "work" / "b2-cache"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ensure_objections(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    logging.info("vote_event_objections.json missing — running standardize_objections.py")
    subprocess.run(
        [sys.executable, str(_REPO_ROOT / "scripts" / "standardize_objections.py")],
        check=True,
    )


def _ensure_votes_csv(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    import pyarrow.parquet as pq
    parquet = _WORK_DIR / "votes.latest.parquet"
    download_latest_from_pointer(
        pointer_path=_REPO_ROOT / "data" / "votes" / "latest.json",
        out_path=parquet,
    )
    table = pq.read_table(parquet)
    path.parent.mkdir(parents=True, exist_ok=True)
    table.to_pandas().to_csv(path, index=False)


def _ensure_vote_events_json(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    import pyarrow.parquet as pq
    parquet = _WORK_DIR / "vote_events.latest.parquet"
    download_latest_from_pointer(
        pointer_path=_REPO_ROOT / "data" / "vote-events" / "latest.json",
        out_path=parquet,
    )
    table = pq.read_table(parquet)
    records = table.to_pylist()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _filter_persons_to_current(*, all_members_csv: Path, current_members_csv: Path, out_csv: Path) -> None:
    import csv
    current_ids: set[str] = set()
    with open(current_members_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = (row.get("id") or "").strip()
            if pid:
                current_ids.add(pid)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(all_members_csv, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        with open(out_csv, "w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
            writer.writeheader()
            kept = 0
            for row in reader:
                if (row.get("id") or "").strip() in current_ids:
                    writer.writerow(row)
                    kept += 1
    logging.info("Filtered persons to %d current members -> %s", kept, out_csv)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    # External analysis scripts — no default; caller must supply
    parser.add_argument(
        "--script", required=True,
        help="Path to vote_corrections.py from the legislature-data-analyses repository.",
    )
    parser.add_argument(
        "--flourish-script", required=True, dest="flourish_script",
        help="Path to vote-corrections/outputs/output_flourish_table.py from legislature-data-analyses.",
    )

    # Data paths — all within this repo, with sensible defaults
    parser.add_argument("--objections",  default=str(_OBJECTIONS))
    parser.add_argument("--votes",       default=str(_VOTES))
    parser.add_argument("--vote-events", default=str(_VOTE_EVENTS), dest="vote_events")
    parser.add_argument("--persons",     default=str(_PERSONS))
    parser.add_argument("--current-members", default=str(_CURRENT_MEM), dest="current_members")
    parser.add_argument("--output",      default=str(_OUTPUT_JSON))
    parser.add_argument("--flourish-output", default=str(_OUTPUT_CSV), dest="flourish_output")
    parser.add_argument(
        "--use-current-members", action="store_true",
        help="Filter the all-members file to current members before running the analysis.",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    script         = Path(args.script).resolve()
    flourish_script = Path(args.flourish_script).resolve()
    objections     = Path(args.objections)
    votes          = Path(args.votes)
    vote_events    = Path(args.vote_events)
    persons        = Path(args.persons)
    output         = Path(args.output)
    flourish_out   = Path(args.flourish_output)

    if not script.exists():
        raise FileNotFoundError(f"Analysis script not found: {script}")
    if not flourish_script.exists():
        raise FileNotFoundError(f"Flourish script not found: {flourish_script}")

    # Ensure input data
    _ensure_objections(objections)
    _ensure_votes_csv(votes)
    _ensure_vote_events_json(vote_events)

    # Optionally filter persons to current members
    if args.use_current_members:
        filtered = _WORK_DIR / "persons.current.csv"
        _filter_persons_to_current(
            all_members_csv=persons,
            current_members_csv=Path(args.current_members),
            out_csv=filtered,
        )
        persons = filtered

    output.parent.mkdir(parents=True, exist_ok=True)

    # Run analysis
    cmd = [
        sys.executable, str(script),
        "--objections",  str(objections),
        "--votes",       str(votes),
        "--vote_events", str(vote_events),
        "--persons",     str(persons),
        "--output",      str(output),
    ]
    logging.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)

    # Produce Flourish CSV
    cmd2 = [
        sys.executable, str(flourish_script),
        "--input",  str(output),
        "--output", str(flourish_out),
    ]
    logging.info("Running: %s", " ".join(cmd2))
    subprocess.run(cmd2, check=True)

    logging.info("Done. Outputs:")
    logging.info("  JSON: %s", output)
    logging.info("  CSV:  %s", flourish_out)


if __name__ == "__main__":
    main()
