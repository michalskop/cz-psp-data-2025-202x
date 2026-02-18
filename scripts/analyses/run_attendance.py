import argparse
import json
import logging
import subprocess
import sys
import csv
from pathlib import Path

import pyarrow.parquet as pq

_REPO_ROOT = Path(__file__).resolve().parents[2]

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.download_b2 import download_latest_from_pointer


def _ensure_votes_csv(votes_csv: Path, *, pointer_path: Path, work_dir: Path) -> None:
    if votes_csv.exists() and votes_csv.stat().st_size > 0:
        return

    parquet_path = work_dir / "votes.latest.parquet"
    download_latest_from_pointer(pointer_path=pointer_path, out_path=parquet_path)

    table = pq.read_table(parquet_path)
    votes_csv.parent.mkdir(parents=True, exist_ok=True)
    table.to_pandas().to_csv(votes_csv, index=False)


def _ensure_vote_events_json(vote_events_json: Path, *, pointer_path: Path, work_dir: Path) -> None:
    if vote_events_json.exists() and vote_events_json.stat().st_size > 0:
        return

    parquet_path = work_dir / "vote_events.latest.parquet"
    download_latest_from_pointer(pointer_path=pointer_path, out_path=parquet_path)

    table = pq.read_table(parquet_path)
    records = table.to_pylist()
    vote_events_json.parent.mkdir(parents=True, exist_ok=True)
    vote_events_json.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _person_ids_from_all_members_csv(path: Path) -> set[str]:
    ids: set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            pid = (row.get("id") or "").strip()
            if pid:
                ids.add(pid)
    if not ids:
        raise ValueError(f"No person ids found in {path}")
    return ids


def _filter_votes_for_persons(*, votes_in: Path, persons_csv: Path, votes_out: Path) -> None:
    """Create a reduced votes CSV containing only rows for person ids in the persons file.

    This keeps the external attendance.py runtime reasonable, since it validates each vote row.
    """
    person_ids = _person_ids_from_all_members_csv(persons_csv)

    votes_out.parent.mkdir(parents=True, exist_ok=True)
    with open(votes_in, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            raise ValueError(f"{votes_in}: missing header")

        with open(votes_out, "w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
            writer.writeheader()
            kept = 0
            for row in reader:
                voter_id = (row.get("voter_id") or "").strip()
                if voter_id in person_ids:
                    writer.writerow(row)
                    kept += 1

    logging.info("Wrote %s (kept %d rows from %s)", votes_out, kept, votes_in)


def _person_ids_from_current_members_csv(path: Path) -> set[str]:
    # current_members.csv uses the same primary id column.
    ids: set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            pid = (row.get("id") or "").strip()
            if pid:
                ids.add(pid)
    if not ids:
        raise ValueError(f"No person ids found in {path}")
    return ids


def _filter_all_members_to_current(*, all_members_csv: Path, current_members_csv: Path, out_csv: Path) -> None:
    current_ids = _person_ids_from_current_members_csv(current_members_csv)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(all_members_csv, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            raise ValueError(f"{all_members_csv}: missing header")

        with open(out_csv, "w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
            writer.writeheader()
            kept = 0
            for row in reader:
                pid = (row.get("id") or "").strip()
                if pid in current_ids:
                    writer.writerow(row)
                    kept += 1

    logging.info(
        "Wrote %s (kept %d rows from %s based on %s)",
        out_csv,
        kept,
        all_members_csv,
        current_members_csv,
    )


def run_attendance(*, definition: Path, votes: Path, vote_events: Path, persons: Path, output: Path) -> None:
    script_path = (_REPO_ROOT / ".." / ".." / "legislature-data-analyses" / "attendance" / "attendance.py").resolve()
    if not script_path.exists():
        raise FileNotFoundError(f"Attendance script not found: {script_path}")

    cmd = [
        sys.executable,
        str(script_path),
        "--definition",
        str(definition),
        "--votes",
        str(votes),
        "--vote_events",
        str(vote_events),
        "--persons",
        str(persons),
        "--output",
        str(output),
    ]

    logging.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def rewrite_group_names(*, attendance_json: Path) -> None:
    mapping = {
        "ANO 2011": "ANO",
        "Starostové a nezávislí": "STAN",
        "Motoristé sobě": "Motoristé",
        "Svoboda a přímá demokracie": "SPD",
    }

    data = json.loads(attendance_json.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{attendance_json} must contain a JSON array")

    changed = 0
    for row in data:
        orgs = row.get("organizations") or []
        if not isinstance(orgs, list):
            continue
        for org in orgs:
            if not isinstance(org, dict):
                continue
            if org.get("classification") != "group":
                continue
            name = org.get("name")
            if name in mapping:
                org["name"] = mapping[name]
                changed += 1

    if changed:
        attendance_json.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    logging.info("Rewrote %d group names in %s", changed, attendance_json)


def run_flourish_table(*, attendance_json: Path, output_csv: Path) -> None:
    script_path = (
        _REPO_ROOT
        / ".."
        / ".."
        / "legislature-data-analyses"
        / "attendance"
        / "outputs"
        / "output_flourish_table.py"
    ).resolve()
    if not script_path.exists():
        raise FileNotFoundError(f"Flourish table script not found: {script_path}")

    cmd = [
        sys.executable,
        str(script_path),
        "--input",
        str(attendance_json),
        "--output",
        str(output_csv),
    ]

    logging.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--definition",
        default=str(_REPO_ROOT / "analyses/attendance/attendance_definition.json"),
    )
    parser.add_argument(
        "--votes",
        default=str(_REPO_ROOT / "work/standard/votes.csv"),
    )
    parser.add_argument(
        "--vote-events",
        dest="vote_events",
        default=str(_REPO_ROOT / "work/standard/vote_events.json"),
    )
    parser.add_argument(
        "--persons",
        default=str(_REPO_ROOT / "analyses/all-members/outputs/all_members.csv"),
        help="all-members.dt.analyses CSV/JSON (default: all_members.csv)",
    )
    parser.add_argument(
        "--current-members",
        default=str(_REPO_ROOT / "analyses/current-members/outputs/current_members.csv"),
        help="current-members.dt.analyses CSV used to filter all-members for analysis (default: current_members.csv)",
    )
    parser.add_argument(
        "--use-current-members",
        action="store_true",
        help="If set, filter the all-members persons file down to current members before computing attendance.",
    )
    parser.add_argument(
        "--output",
        default=str(_REPO_ROOT / "analyses/attendance/outputs/attendance.json"),
    )
    parser.add_argument(
        "--flourish-output",
        default=str(_REPO_ROOT / "analyses/attendance/outputs/attendance_flourish_table.csv"),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    definition = Path(args.definition)
    votes = Path(args.votes)
    vote_events = Path(args.vote_events)
    persons = Path(args.persons)
    current_members = Path(args.current_members)
    output = Path(args.output)
    flourish_output = Path(args.flourish_output)

    work_dir = _REPO_ROOT / "work" / "b2-cache"

    _ensure_votes_csv(votes, pointer_path=_REPO_ROOT / "data/votes/latest.json", work_dir=work_dir)
    _ensure_vote_events_json(
        vote_events,
        pointer_path=_REPO_ROOT / "data/vote-events/latest.json",
        work_dir=work_dir,
    )

    persons_for_run = persons
    if args.use_current_members:
        if persons.suffix.lower() != ".csv":
            raise ValueError("--use-current-members currently requires --persons to be a CSV")
        if current_members.suffix.lower() != ".csv":
            raise ValueError("--use-current-members requires --current-members to be a CSV")
        filtered_persons = work_dir / "persons.current.csv"
        _filter_all_members_to_current(
            all_members_csv=persons,
            current_members_csv=current_members,
            out_csv=filtered_persons,
        )
        persons_for_run = filtered_persons

    # attendance.py validates every vote row; use a reduced votes file for the requested persons.
    # Note: attendance.py expects all-members.dt.analyses schema for persons (CSV/JSON).
    filtered_votes = work_dir / "votes.filtered.csv"
    if persons_for_run.suffix.lower() == ".csv":
        _filter_votes_for_persons(votes_in=votes, persons_csv=persons_for_run, votes_out=filtered_votes)
        votes_for_run = filtered_votes
    else:
        votes_for_run = votes

    run_attendance(
        definition=definition,
        votes=votes_for_run,
        vote_events=vote_events,
        persons=persons_for_run,
        output=output,
    )

    rewrite_group_names(attendance_json=output)

    run_flourish_table(attendance_json=output, output_csv=flourish_output)


if __name__ == "__main__":
    main()
