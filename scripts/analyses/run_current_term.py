import json
import logging
from pathlib import Path
from datetime import date

import pandas as pd


def _term_number_from_raw_organy(raw_organy_path: Path, current_org_numeric_id: str) -> str:
    raw = raw_organy_path.read_bytes().decode("cp1250", errors="replace")
    for line in raw.splitlines():
        if not line:
            continue
        cols = line.split("|")
        if len(cols) < 4:
            continue
        id_organ = cols[0]
        abbr = cols[3]
        if id_organ != current_org_numeric_id:
            continue
        if abbr.startswith("PSP") and abbr[3:].isdigit():
            return abbr[3:]
        raise ValueError(f"Unexpected abbreviation for current term org {id_organ}: {abbr}")

    raise ValueError(f"Current term org {current_org_numeric_id} not found in {raw_organy_path}")


def _add_years_iso(d: str, years: int) -> str:
    yyyy, mm, dd = (int(x) for x in d.split("-"))
    try:
        return date(yyyy + years, mm, dd).isoformat()
    except ValueError:
        # Handle Feb 29 -> Feb 28 on non-leap years
        if mm == 2 and dd == 29:
            return date(yyyy + years, 2, 28).isoformat()
        raise


def run_current_term(standard_dir: Path) -> None:
    orgs = pd.read_csv(standard_dir / "organizations.csv")

    cur_terms = orgs[
        orgs["name"].fillna("").str.contains("Poslanecká sněmovna", regex=False)
        & orgs["dissolution_date"].isna()
    ].copy()
    if len(cur_terms) != 1:
        raise ValueError(f"Expected exactly 1 current term org, found {len(cur_terms)}")

    r = cur_terms.iloc[0].to_dict()

    term_id = str(r["id"])
    base_name = str(r["name"])
    since = r.get("founding_date")
    until = r.get("dissolution_date")

    if not since:
        raise ValueError("Current term missing founding_date (since)")

    start_year = since.split("-")[0]
    term_name = f"{base_name} {start_year} -"

    # term_id is psp:org:<id_organ>
    current_org_numeric_id = term_id.split(":")[-1]
    term_no = _term_number_from_raw_organy(Path("work/raw/poslanci/organy.unl"), current_org_numeric_id)

    obj = {
        "id": term_id,
        "name": term_name,
        "since": since,
        "until": until if until else None,
        "until_latest": _add_years_iso(since, 4),
        "identifiers": [
            {"scheme": "psp", "identifier": term_no},
        ],
    }

    out_path = Path("analyses/current-term/outputs/current_term.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info("Wrote %s", out_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_current_term(Path("work/standard"))
