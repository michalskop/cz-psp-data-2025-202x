import argparse
import logging
from pathlib import Path

import pandas as pd
import requests
import yaml


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def _validate_table(df: pd.DataFrame, schema: dict, table_name: str) -> None:
    fields = schema.get("fields", [])
    required = {f["name"] for f in fields if f.get("constraints", {}).get("required") is True}
    allowed = {f["name"] for f in fields}

    cols = set(df.columns)
    missing = sorted(required - cols)
    unexpected = sorted(cols - allowed)

    if missing:
        raise ValueError(f"{table_name}: missing required columns: {missing}")
    if unexpected:
        raise ValueError(f"{table_name}: unexpected columns (schema drift): {unexpected}")

    # basic type sanity: required strings should not be all-null
    for f in fields:
        name = f["name"]
        if name not in df.columns:
            continue
        t = f.get("type")
        if t == "string" and f.get("constraints", {}).get("required") is True:
            if df[name].isna().all():
                raise ValueError(f"{table_name}.{name}: required string column is entirely null")


def validate_from_config(config_path: Path, standard_dir: Path) -> None:
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    mapping = {
        "persons": "persons.csv",
        "organizations": "organizations.csv",
        "memberships": "memberships.csv",
    }

    for key, fn in mapping.items():
        url = cfg.get(key, {}).get("url")
        if not url:
            raise ValueError(f"schemas.yml missing url for {key}")
        table_path = standard_dir / fn
        if not table_path.exists():
            logging.info("Skipping validation for %s (missing %s)", key, table_path)
            continue

        schema = _load_schema(url)
        df = pd.read_csv(table_path)
        _validate_table(df, schema, key)
        logging.info("Validated %s against %s", table_path, url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schemas", default="config/schemas.yml")
    parser.add_argument("--standard-dir", default="work/standard")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    validate_from_config(Path(args.schemas), Path(args.standard_dir))


if __name__ == "__main__":
    main()
