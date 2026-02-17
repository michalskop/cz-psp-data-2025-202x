import argparse
import logging
from pathlib import Path

import pandas as pd
import requests


TABLE_SCHEMA_URL = "https://michalskop.github.io/legislature-data-standard/dt.analyses/current-groups/latest/schemas/current-groups.dt.analyses.table.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def validate_current_groups_csv(csv_path: Path, table_schema_url: str = TABLE_SCHEMA_URL) -> None:
    schema = _load_schema(table_schema_url)
    fields = schema.get("fields", [])
    required = {f["name"] for f in fields if f.get("constraints", {}).get("required") is True}
    allowed = {f["name"] for f in fields}

    df = pd.read_csv(csv_path, dtype=str)
    cols = set(df.columns)

    missing = sorted(required - cols)
    unexpected = sorted(cols - allowed)

    if missing:
        raise ValueError(f"CSV {csv_path}: missing required columns: {missing}")
    if unexpected:
        raise ValueError(f"CSV {csv_path}: unexpected columns: {unexpected}")

    logging.info("Validated %s against %s", csv_path, table_schema_url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="analyses/current-groups/outputs/current_groups.csv")
    parser.add_argument("--schema-url", default=TABLE_SCHEMA_URL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_current_groups_csv(Path(args.csv), args.schema_url)


if __name__ == "__main__":
    main()
