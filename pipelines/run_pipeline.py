import logging
import sys
from pathlib import Path
import json
from datetime import datetime, timezone
import os

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.download_poslanci import download_file, unpack_zip, DEFAULT_URL
from scripts.standardize_poslanci import standardize
from scripts.validate_tables import validate_from_config
from scripts.upload_b2 import prune_snapshots, upload_file
from scripts.utils_env import load_dotenv
from scripts.analyses.run_all import run_all
from scripts.validate_analysis_current_members import validate_outputs
from scripts.validate_analysis_current_groups import validate_current_groups_json
from scripts.validate_analysis_current_groups_table import validate_current_groups_csv
from scripts.validate_analysis_current_term import validate_current_term_json
from scripts.validate_analysis_all_groups import validate_all_groups
from scripts.validate_analysis_all_members import validate_all_members


def _ensure_work_dirs() -> None:
    for p in [
        Path("work/raw"),
        Path("work/raw/poslanci"),
        Path("work/standard"),
        Path("work/db"),
        Path("data"),
    ]:
        p.mkdir(parents=True, exist_ok=True)


def _read_psp_term_identifier(current_term_json: Path) -> str:
    term = json.loads(current_term_json.read_text(encoding="utf-8"))
    for ident in term.get("identifiers", []) or []:
        if ident.get("scheme") == "psp" and ident.get("identifier"):
            return str(ident["identifier"])
    raise ValueError(f"No PSP term identifier found in {current_term_json}")


def _write_latest_pointer(
    *,
    out_path: Path,
    locations: list[dict],
    term_identifier: str,
    term_org_id: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "locations": locations,
        "term_identifier": term_identifier,
        "term_org_id": term_org_id,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run() -> None:
    _ensure_work_dirs()

    load_dotenv(_REPO_ROOT)

    remote_prefix = "legislatures/cz-psp-data-2025-202x"

    # 1) download + unpack
    zip_path = Path("work/raw/poslanci.zip")
    raw_dir = Path("work/raw/poslanci")
    download_file(DEFAULT_URL, zip_path)
    unpack_zip(zip_path, raw_dir)

    # 2) standardize
    standard_dir = Path("work/standard")
    standardize(raw_dir=raw_dir, out_dir=standard_dir)

    # 3) validate
    validate_from_config(Path("config/schemas.yml"), standard_dir)

    # 4) analyses (small committed outputs)
    run_all(standard_dir)

    # 4b) validate analysis outputs
    validate_outputs(
        Path("analyses/current-members/outputs/current_members.csv"),
        Path("analyses/current-members/outputs/current_members.json"),
    )

    validate_current_groups_json(Path("analyses/current-groups/outputs/current_groups.json"))
    validate_current_groups_csv(Path("analyses/current-groups/outputs/current_groups.csv"))

    validate_current_term_json(Path("analyses/current-term/outputs/current_term.json"))

    current_term_json = Path("analyses/current-term/outputs/current_term.json")
    term_identifier = _read_psp_term_identifier(current_term_json)
    term_org_id = json.loads(current_term_json.read_text(encoding="utf-8")).get("id")
    if not term_org_id:
        raise ValueError(f"Missing current term org id in {current_term_json}")

    validate_all_groups(
        Path("analyses/all-groups/outputs/all_groups.csv"),
        Path("analyses/all-groups/outputs/all_groups.json"),
    )

    validate_all_members(
        Path("analyses/all-members/outputs/all_members.csv"),
        Path("analyses/all-members/outputs/all_members.json"),
    )

    # 5) publish canonical tables as B2-hosted CSV snapshots.
    # Commit only provider-agnostic pointers under data/<dataset>/latest.json.
    bucket_name = os.getenv("B2_BUCKET")
    snapshot_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for dataset, src_csv in [
        ("persons", standard_dir / "persons.csv"),
        ("organizations", standard_dir / "organizations.csv"),
        ("memberships", standard_dir / "memberships.csv"),
    ]:
        out_dir = Path("data") / dataset
        out_dir.mkdir(parents=True, exist_ok=True)

        pointer_path = out_dir / "latest.json"
        locations: list[dict] = []

        if bucket_name:
            snapshot_key = f"{remote_prefix}/{dataset}/snapshots/{dataset}.snapshot-{snapshot_ts}.csv"
            upload_file(src_csv, snapshot_key)
            prune_snapshots(f"{remote_prefix}/{dataset}/snapshots/", keep=5)
            locations.append(
                {
                    "provider": "b2",
                    "bucket": bucket_name,
                    "key": snapshot_key,
                    "uri": f"b2://{bucket_name}/{snapshot_key}",
                }
            )

        _write_latest_pointer(
            out_path=pointer_path,
            locations=locations,
            term_identifier=term_identifier,
            term_org_id=term_org_id,
        )

        if bucket_name:
            upload_file(pointer_path, f"{remote_prefix}/{dataset}/latest.json")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
