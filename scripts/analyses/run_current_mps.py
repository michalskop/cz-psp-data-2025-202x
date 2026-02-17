import json
import logging
from pathlib import Path

import pandas as pd

from scripts.utils_io import write_csv


def run_current_mps(standard_dir: Path) -> None:
    persons = pd.read_csv(standard_dir / "persons.csv")
    orgs = pd.read_csv(standard_dir / "organizations.csv")

    # Identify current parliamentary term organization (dissolution_date is null)
    cur_terms = orgs[
        orgs["name"].fillna("").str.contains("Poslanecká sněmovna", regex=False)
        & orgs["dissolution_date"].isna()
    ].copy()
    if len(cur_terms) != 1:
        raise ValueError(f"Expected exactly 1 current term org, found {len(cur_terms)}")
    current_term_id = cur_terms.iloc[0]["id"]

    # PSP current MPs list is in poslanec.unl (ephemeral raw workspace)
    raw_poslanec_path = Path("work/raw/poslanci/poslanec.unl")
    raw = raw_poslanec_path.read_bytes().decode("cp1250", errors="replace")
    pos_rows = [line.split("|") for line in raw.splitlines() if line]
    df_pos = pd.DataFrame(pos_rows)
    if df_pos.shape[1] != 16:
        raise ValueError(f"Unexpected poslanec.unl column count: {df_pos.shape[1]} (expected 16)")

    df_pos = df_pos.rename(
        columns={
            0: "id_poslanec",
            1: "id_osoba",
            3: "id_klub",
            4: "id_obdobi",
            14: "is_current",
        }
    )
    df_pos = df_pos[["id_poslanec", "id_osoba", "id_klub", "id_obdobi", "is_current"]].copy()
    df_pos = df_pos[df_pos["is_current"] == "1"].copy()

    persons_small = persons[["id", "name"]].rename(columns={"id": "person_id", "name": "person_name"})
    orgs_small = orgs[["id", "name"]].rename(columns={"id": "org_id", "name": "org_name"})

    df_pos["person_id"] = df_pos["id_osoba"].map(lambda x: f"psp:person:{x}")
    df_pos["party_org_id"] = df_pos["id_klub"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)
    df_pos["term_org_id"] = df_pos["id_obdobi"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)

    df_pos = df_pos[df_pos["term_org_id"] == current_term_id].copy()

    out = df_pos.merge(persons_small, on="person_id", how="left")
    out = out.merge(orgs_small, left_on="party_org_id", right_on="org_id", how="left")
    out = out.rename(columns={"org_name": "party_name"}).drop(columns=["org_id"]) 

    out = out.sort_values(["person_name", "person_id"], ascending=[True, True])

    rows = []
    for _, r in out.iterrows():
        rows.append(
            {
                "person_id": r["person_id"],
                "person_name": r["person_name"],
                "party_id": r.get("party_org_id"),
                "party_name": r.get("party_name"),
                "term_id": r.get("term_org_id"),
            }
        )

    out_path = Path("analyses/example/outputs/current_mps.csv")
    write_csv(out_path, rows, ["person_id", "person_name", "party_id", "party_name", "term_id"])
    logging.info("Wrote %s (%d rows)", out_path, len(rows))
