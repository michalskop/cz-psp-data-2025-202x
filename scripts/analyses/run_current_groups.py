import json
import logging
from pathlib import Path

import pandas as pd

from scripts.utils_io import write_csv


def run_current_groups(standard_dir: Path) -> None:
    orgs = pd.read_csv(standard_dir / "organizations.csv")

    # Identify current parliamentary term organization (dissolution_date is null)
    cur_terms = orgs[
        orgs["name"].fillna("").str.contains("Poslanecká sněmovna", regex=False)
        & orgs["dissolution_date"].isna()
    ].copy()
    if len(cur_terms) != 1:
        raise ValueError(f"Expected exactly 1 current term org, found {len(cur_terms)}")
    current_term_id = cur_terms.iloc[0]["id"]

    # Parliamentary clubs are organizations under the current term PSP org with name containing "Poslanecký klub".
    # We intentionally derive this from org hierarchy, not from poslanec.unl, to avoid candidate list orgs (e.g., SPOLU).
    groups = orgs[(orgs["parent_id"] == current_term_id) & (orgs["name"].fillna("").str.contains("Poslanecký klub", regex=False))].copy()

    # Normalize group name by stripping the prefix.
    groups["name"] = groups["name"].astype(str).str.replace(r"^Poslanecký klub\s+", "", regex=True).str.strip()

    # In PSP data, clubs are organizations; set classification to 'group'
    if "classification" in groups.columns:
        groups["classification"] = "group"

    # Parse identifiers/sources which are stored as JSON strings in standardized orgs
    array_fields = {"other_names", "identifiers", "contact_details", "links", "sources"}
    records = groups.to_dict(orient="records")
    for r in records:
        for k in list(array_fields):
            if k not in r:
                continue
            v = r.get(k)
            if v is None or v == "":
                r[k] = []
                continue
            if isinstance(v, str) and v.strip().startswith("["):
                r[k] = json.loads(v)

    # Keep deterministic order by name, then id
    records = sorted(records, key=lambda r: (str(r.get("name") or ""), str(r.get("id") or "")))

    out_path = Path("analyses/current-groups/outputs/current_groups.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info("Wrote %s (%d rows)", out_path, len(records))

    csv_cols = ["id", "name", "classification", "parent_id", "founding_date", "dissolution_date"]
    rows = []
    for r in records:
        rows.append({c: r.get(c) for c in csv_cols})
    csv_path = Path("analyses/current-groups/outputs/current_groups.csv")
    write_csv(csv_path, rows, csv_cols)
    logging.info("Wrote %s (%d rows)", csv_path, len(rows))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_current_groups(Path("work/standard"))
