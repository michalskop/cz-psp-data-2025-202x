import logging
from pathlib import Path

import pandas as pd
import json
import math

from scripts.utils_io import write_csv


def _none_if_nan(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def _sanitize_json(v):
    if isinstance(v, dict):
        return {k: _sanitize_json(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_sanitize_json(x) for x in v]
    return _none_if_nan(v)


def run_current_members(standard_dir: Path) -> None:
    persons = pd.read_csv(standard_dir / "persons.csv")
    orgs = pd.read_csv(standard_dir / "organizations.csv")
    memberships = pd.read_csv(standard_dir / "memberships.csv")

    # Identify current parliamentary term organization (dissolution_date is null)
    cur_terms = orgs[
        orgs["name"].fillna("").str.contains("Poslanecká sněmovna", regex=False)
        & orgs["dissolution_date"].isna()
    ].copy()
    if len(cur_terms) != 1:
        raise ValueError(f"Expected exactly 1 current term org, found {len(cur_terms)}")
    current_term_id = cur_terms.iloc[0]["id"]
    current_term_since = cur_terms.iloc[0]["founding_date"]
    current_term_name = cur_terms.iloc[0]["name"]
    if not current_term_since:
        raise ValueError("Current term missing founding_date")
    current_term_year = str(current_term_since).split("-")[0]

    # PSP current MPs list is in poslanec.unl (ephemeral raw workspace)
    raw_poslanec_path = Path("work/raw/poslanci/poslanec.unl")
    raw = raw_poslanec_path.read_bytes().decode("cp1250", errors="replace")
    pos_rows = [line.split("|") for line in raw.splitlines() if line]
    df_pos = pd.DataFrame(pos_rows)
    if df_pos.shape[1] != 16:
        raise ValueError(f"Unexpected poslanec.unl column count: {df_pos.shape[1]} (expected 16)")

    df_pos = df_pos.rename(
        columns={
            1: "id_osoba",
            2: "id_kraj",
            3: "id_candidate_list",
            4: "id_obdobi",
            14: "is_current",
        }
    )
    df_pos = df_pos[["id_osoba", "id_kraj", "id_candidate_list", "id_obdobi", "is_current"]].copy()
    df_pos = df_pos[df_pos["is_current"] == "1"].copy()

    df_pos["person_id"] = df_pos["id_osoba"].map(lambda x: f"psp:person:{x}")
    df_pos["term_org_id"] = df_pos["id_obdobi"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)
    df_pos["constituency_org_id"] = df_pos["id_kraj"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)
    df_pos["candidate_list_org_id"] = df_pos["id_candidate_list"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)
    df_pos = df_pos[df_pos["term_org_id"] == current_term_id].copy()

    df_pos["image"] = df_pos["id_osoba"].map(
        lambda x: f"https://www.psp.cz/eknih/cdrom/{current_term_year}ps/eknih/{current_term_year}ps/poslanci/i{int(x)}.jpg"
    )

    current_person_ids = set(df_pos["person_id"].tolist())

    out = persons[persons["id"].isin(current_person_ids)].copy()

    out = out.merge(df_pos[["person_id", "image"]], left_on="id", right_on="person_id", how="left").drop(columns=["person_id"])

    org_id_to_name = dict(zip(orgs["id"].tolist(), orgs["name"].tolist(), strict=False))

    # Clubs in this term (all groups)
    clubs = orgs[(orgs["parent_id"] == current_term_id) & (orgs["name"].fillna("").str.contains("Poslanecký klub", regex=False))].copy()
    clubs["club_name"] = clubs["name"].astype(str).str.replace(r"^Poslanecký klub\s+", "", regex=True).str.strip()
    club_id_to_name = dict(zip(clubs["id"].tolist(), clubs["club_name"].tolist(), strict=False))

    # parliament memberships (fallback to term dates for independents / missing zarazeni rows)
    term_m = memberships[(memberships["organization_id"] == current_term_id) & (memberships["person_id"].isin(current_person_ids))].copy()
    groups_m = memberships[(memberships["organization_id"].isin(set(club_id_to_name.keys()))) & (memberships["person_id"].isin(current_person_ids))].copy()

    memberships_by_person: dict[str, dict] = {}

    for pid in current_person_ids:
        memberships_by_person[pid] = {"parliament": [], "groups": [], "candidate_list": [], "constituency": []}

    for _, r in term_m.iterrows():
        pid = r["person_id"]
        memberships_by_person[pid]["parliament"].append(
            {
                "id": current_term_id,
                "name": current_term_name,
                "start_date": _none_if_nan(r.get("start_date")),
                "end_date": _none_if_nan(r.get("end_date")),
            }
        )

    for pid, m in memberships_by_person.items():
        if not m["parliament"]:
            m["parliament"].append(
                {
                    "id": current_term_id,
                    "name": current_term_name,
                    "start_date": current_term_since,
                    "end_date": None,
                }
            )

    candidate_by_person = dict(zip(df_pos["person_id"].tolist(), df_pos["candidate_list_org_id"].tolist(), strict=False))
    constituency_by_person = dict(zip(df_pos["person_id"].tolist(), df_pos["constituency_org_id"].tolist(), strict=False))

    for _, r in groups_m.iterrows():
        pid = r["person_id"]
        oid = r["organization_id"]
        memberships_by_person[pid]["groups"].append(
            {
                "id": oid,
                "name": club_id_to_name.get(oid) or oid,
                "start_date": _none_if_nan(r.get("start_date")),
                "end_date": _none_if_nan(r.get("end_date")),
            }
        )

    def _mem_for(pid: str) -> dict:
        m = memberships_by_person.get(pid) or {"parliament": [], "groups": [], "candidate_list": [], "constituency": []}
        m["parliament"] = sorted(m.get("parliament") or [], key=lambda x: (x.get("start_date") or "", x.get("id") or ""))
        m["groups"] = sorted(m.get("groups") or [], key=lambda x: (x.get("start_date") or "", x.get("id") or ""))

        p0 = (m.get("parliament") or [{}])[0]
        start_date = p0.get("start_date")
        end_date = p0.get("end_date")

        cand_id = candidate_by_person.get(pid)
        if cand_id:
            m["candidate_list"] = [
                {
                    "id": cand_id,
                    "name": org_id_to_name.get(cand_id) or cand_id,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            ]
        else:
            m["candidate_list"] = []

        cons_id = constituency_by_person.get(pid)
        if cons_id:
            m["constituency"] = [
                {
                    "id": cons_id,
                    "name": org_id_to_name.get(cons_id) or cons_id,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            ]
        else:
            m["constituency"] = []

        return m

    out["memberships"] = out["id"].map(_mem_for)

    # Schema requires at least id + name; we emit a stable subset of available fields
    cols = [
        "id",
        "name",
        "memberships",
        "identifiers",
        "sources",
        "given_name",
        "family_name",
        "birth_date",
        "death_date",
        "gender",
        "image",
    ]
    cols = [c for c in cols if c in out.columns]
    out = out[cols].sort_values(["name", "id"], ascending=[True, True])

    out_path = Path("analyses/current-members/outputs/current_members.csv")
    csv_rows = []
    for r in out.to_dict(orient="records"):
        rr = _sanitize_json(dict(r))
        if "memberships" in rr:
            rr["memberships"] = json.dumps(rr.get("memberships") or {}, ensure_ascii=False, sort_keys=True)
        csv_rows.append(rr)
    write_csv(out_path, csv_rows, cols)
    logging.info("Wrote %s (%d rows)", out_path, len(out))

    json_path = Path("analyses/current-members/outputs/current_members.json")
    json_path.parent.mkdir(parents=True, exist_ok=True)

    records = [_sanitize_json(r) for r in out.to_dict(orient="records")]
    array_fields = {"other_names", "identifiers", "contact_details", "links", "sources"}
    for r in records:
        for k in array_fields:
            if k not in r:
                continue
            v = r.get(k)
            if v is None or v == "":
                r[k] = []
                continue
            if isinstance(v, str) and v.strip().startswith("["):
                r[k] = json.loads(v)

        if "memberships" in r and isinstance(r.get("memberships"), dict):
            r["memberships"] = _sanitize_json(r["memberships"])

    json_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    logging.info("Wrote %s (%d rows)", json_path, len(records))
