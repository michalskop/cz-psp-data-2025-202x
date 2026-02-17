import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import requests

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


SCHEMA_URL = "https://michalskop.github.io/legislature-data-standard/dt/0.1.0/schemas/votes-table.dt.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def validate_votes_table(csv_path: Path, schema_url: str = SCHEMA_URL) -> None:
    schema = _load_schema(schema_url)
    props = schema.get("definitions", {}).get("DtTableVotesRow", {}).get("properties", {})
    required = set(schema.get("definitions", {}).get("DtTableVotesRow", {}).get("required", []))
    allowed = set(props.keys())

    # voter_type is optional in schema; allow missing (we don't emit it for PSP MP votes)
    allowed.add("voter_type")

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    cols = set(df.columns)

    missing = sorted(required - cols)
    unexpected = sorted(cols - allowed)
    if missing:
        raise ValueError(f"{csv_path}: missing required columns: {missing}")
    if unexpected:
        raise ValueError(f"{csv_path}: unexpected columns: {unexpected}")

    if len(df) == 0:
        raise ValueError(f"{csv_path}: no rows")

    for i, r in df.head(50).iterrows():
        for k in required:
            if str(r.get(k, "")).strip() == "":
                raise ValueError(f"{csv_path}: row {i} empty required field {k}")

    logging.info("Validated %s against %s", csv_path, schema_url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="work/standard/votes_sample.csv")
    parser.add_argument("--schema-url", default=SCHEMA_URL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_votes_table(Path(args.csv), args.schema_url)


if __name__ == "__main__":
    main()
