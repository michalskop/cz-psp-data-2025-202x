"""
Run the wpca analysis for cz-psp-data-2025-202x.

Steps:
  1. Ensure votes.csv and vote_events.json exist (downloads from B2 if needed)
  2. Run wpca.py (from legislature-data-analyses)
  3. Run output_flourish_table.py to produce the Flourish CSVs

Outputs:
  analyses/wpca/outputs/wpca.json
  analyses/wpca/outputs/wpca_flourish.csv
  analyses/wpca/outputs/wpca_time.json
  analyses/wpca/outputs/wpca_time_flourish.csv

Usage:
  python scripts/analyses/run_wpca.py \\
      --script /path/to/legislature-data-analyses/wpca/wpca.py \\
      --flourish-script /path/to/legislature-data-analyses/wpca/outputs/output_flourish_table.py

The --script and --flourish-script arguments are required because the
wpca analysis lives in a separate repository (legislature-data-analyses)
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

_DEFINITION = _REPO_ROOT / "analyses" / "wpca" / "wpca_definition.json"
_VOTES = _REPO_ROOT / "work" / "standard" / "votes.csv"
_VOTE_EVENTS = _REPO_ROOT / "work" / "standard" / "vote_events.json"
_PERSONS = _REPO_ROOT / "analyses" / "all-members" / "outputs" / "all_members.json"
_OUTPUT_JSON = _REPO_ROOT / "analyses" / "wpca" / "outputs" / "wpca.json"
_OUTPUT_TIME_JSON = _REPO_ROOT / "analyses" / "wpca" / "outputs" / "wpca_time.json"
_OUTPUT_CSV = _REPO_ROOT / "analyses" / "wpca" / "outputs" / "wpca_flourish.csv"
_OUTPUT_TIME_CSV = _REPO_ROOT / "analyses" / "wpca" / "outputs" / "wpca_time_flourish.csv"
_WORK_DIR = _REPO_ROOT / "work" / "b2-cache"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_dt_schema_file(*, filename: str, url: str, schema_dir: str) -> None:
    import requests

    schema_dir_path = Path(schema_dir)
    out_path = schema_dir_path / filename
    if out_path.exists() and out_path.stat().st_size > 0:
        return

    schema_dir_path.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_text(r.text, encoding="utf-8")


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


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        "--script",
        required=True,
        help="Path to wpca.py from the legislature-data-analyses repository.",
    )
    parser.add_argument(
        "--flourish-script",
        required=True,
        dest="flourish_script",
        help="Path to wpca/outputs/output_flourish_table.py from legislature-data-analyses.",
    )

    parser.add_argument("--definition", default=str(_DEFINITION))
    parser.add_argument("--votes", default=str(_VOTES))
    parser.add_argument("--vote-events", default=str(_VOTE_EVENTS), dest="vote_events")
    parser.add_argument("--persons", default=str(_PERSONS))
    parser.add_argument("--output", default=str(_OUTPUT_JSON))
    parser.add_argument("--output-time", default=str(_OUTPUT_TIME_JSON), dest="output_time")
    parser.add_argument("--flourish-output", default=str(_OUTPUT_CSV), dest="flourish_output")
    parser.add_argument("--flourish-output-time", default=str(_OUTPUT_TIME_CSV), dest="flourish_output_time")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    script = Path(args.script).resolve()
    flourish_script = Path(args.flourish_script).resolve()
    definition = Path(args.definition)
    votes = Path(args.votes)
    vote_events = Path(args.vote_events)
    persons = Path(args.persons)
    output = Path(args.output)
    output_time = Path(args.output_time) if args.output_time else None
    flourish_out = Path(args.flourish_output)
    flourish_out_time = Path(args.flourish_output_time)

    if not script.exists():
        raise FileNotFoundError(f"Analysis script not found: {script}")
    if not flourish_script.exists():
        raise FileNotFoundError(f"Flourish script not found: {flourish_script}")

    _ensure_votes_csv(votes)
    _ensure_vote_events_json(vote_events)

    _ensure_dt_schema_file(
        filename="vote-events.dt.json",
        url="https://michalskop.github.io/legislature-data-standard/dt/0.1.0/schemas/vote-events.dt.json",
        schema_dir="/tmp/legislature-data-standard/dist/dt/latest/schemas",
    )
    _ensure_dt_schema_file(
        filename="votes-table.dt.json",
        url="https://michalskop.github.io/legislature-data-standard/dt/0.1.0/schemas/votes-table.dt.json",
        schema_dir="/tmp/legislature-data-standard/dist/dt/latest/schemas",
    )
    _ensure_dt_schema_file(
        filename="all-members.dt.analyses.json",
        url="https://michalskop.github.io/legislature-data-standard/dt.analyses/all-members/latest/schemas/all-members.dt.analyses.json",
        schema_dir="/tmp/legislature-data-standard/dist/dt.analyses/all-members/latest/schemas",
    )
    _ensure_dt_schema_file(
        filename="wpca-definition.dt.analyses.json",
        url="https://michalskop.github.io/legislature-data-standard/dt.analyses/wpca-definition/latest/schemas/wpca-definition.dt.analyses.json",
        schema_dir="/tmp/legislature-data-standard/dist/dt.analyses/wpca-definition/latest/schemas",
    )
    _ensure_dt_schema_file(
        filename="wpca.dt.analyses.json",
        url="https://michalskop.github.io/legislature-data-standard/dt.analyses/wpca/latest/schemas/wpca.dt.analyses.json",
        schema_dir="/tmp/legislature-data-standard/dist/dt.analyses/wpca/latest/schemas",
    )
    _ensure_dt_schema_file(
        filename="wpca-time.dt.analyses.json",
        url="https://michalskop.github.io/legislature-data-standard/dt.analyses/wpca-time/latest/schemas/wpca-time.dt.analyses.json",
        schema_dir="/tmp/legislature-data-standard/dist/dt.analyses/wpca-time/latest/schemas",
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(script),
        "--definition",
        str(definition),
        "--votes",
        str(votes),
        "--vote-events",
        str(vote_events),
        "--persons",
        str(persons),
        "--output",
        str(output),
    ]
    if output_time is not None:
        cmd += ["--output-time", str(output_time)]

    logging.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)

    cmd2 = [
        sys.executable,
        str(flourish_script),
        "--input",
        str(output),
        "--output",
        str(flourish_out),
    ]
    logging.info("Running: %s", " ".join(cmd2))
    subprocess.run(cmd2, check=True)

    if output_time is not None:
        cmd3 = [
            sys.executable,
            str(flourish_script),
            "--input",
            str(output_time),
            "--output",
            str(flourish_out_time),
            "--time",
        ]
        logging.info("Running: %s", " ".join(cmd3))
        subprocess.run(cmd3, check=True)


if __name__ == "__main__":
    main()
