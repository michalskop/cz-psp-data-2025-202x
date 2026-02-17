import argparse
import json
import logging
from pathlib import Path

import requests


SCHEMA_URL = "https://michalskop.github.io/legislature-data-standard/dt/0.1.0/schemas/vote-event.dt.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def validate_vote_events(json_path: Path, schema_url: str = SCHEMA_URL) -> None:
    schema = _load_schema(schema_url)
    props = schema.get("definitions", {}).get("DtVoteEvent", {}).get("properties", {})
    required = set(schema.get("definitions", {}).get("DtVoteEvent", {}).get("required", []))
    allowed = set(props.keys())

    records = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(records, list) or not records:
        raise ValueError(f"{json_path}: expected non-empty list")

    for i, r in enumerate(records[:50]):
        if not isinstance(r, dict):
            raise ValueError(f"{json_path}: record {i} expected object")
        miss = sorted([k for k in required if k not in r or r.get(k) in (None, "")])
        if miss:
            raise ValueError(f"{json_path}: record {i} missing required keys: {miss}")

        unexp = sorted([k for k in r.keys() if k not in allowed])
        if unexp:
            raise ValueError(f"{json_path}: record {i} unexpected keys: {unexp}")

        if "extras" in r and r["extras"] is not None and not isinstance(r["extras"], dict):
            raise ValueError(f"{json_path}: record {i} extras must be object or null")

    logging.info("Validated %s against %s", json_path, schema_url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="work/standard/vote_events_sample.json")
    parser.add_argument("--schema-url", default=SCHEMA_URL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_vote_events(Path(args.json), args.schema_url)


if __name__ == "__main__":
    main()
