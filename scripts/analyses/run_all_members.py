import json
import logging
from pathlib import Path

import pandas as pd
import math

from scripts.utils_io import write_csv


def _ensure_list(v):
    return v if isinstance(v, list) else []


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


def _membership_item(org_id: str, org_name: str | None, start_date: str | None, end_date: str | None) -> dict:
    return {
        "id": org_id,
        "name": org_name or org_id,
        "start_date": _none_if_nan(start_date),
        "end_date": _none_if_nan(end_date),
    }


def run_all_members(standard_dir: Path) -> None:
    persons = pd.read_csv(standard_dir / "persons.csv")
    orgs = pd.read_csv(standard_dir / "organizations.csv")
    memberships = pd.read_csv(standard_dir / "memberships.csv")

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
        }
    )
    df_pos = df_pos[["id_osoba", "id_kraj", "id_candidate_list", "id_obdobi"]].copy()
    df_pos["person_id"] = df_pos["id_osoba"].map(lambda x: f"psp:person:{x}")
    df_pos["term_org_id"] = df_pos["id_obdobi"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)
    df_pos["constituency_org_id"] = df_pos["id_kraj"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)
    df_pos["candidate_list_org_id"] = df_pos["id_candidate_list"].replace({"": None}).map(lambda x: f"psp:org:{x}" if x else None)

    # Identify current parliamentary term organization (dissolution_date is null)
    cur_terms = orgs[
        orgs["name"].fillna("").str.contains("Poslanecká sněmovna", regex=False)
        & orgs["dissolution_date"].isna()
    ].copy()
    if len(cur_terms) != 1:
        raise ValueError(f"Expected exactly 1 current term org, found {len(cur_terms)}")
    current_term_id = cur_terms.iloc[0]["id"]
    current_term_name = cur_terms.iloc[0]["name"]

    df_pos = df_pos[df_pos["term_org_id"] == current_term_id].copy()
    candidate_by_person = dict(zip(df_pos["person_id"].tolist(), df_pos["candidate_list_org_id"].tolist(), strict=False))
    constituency_by_person = dict(zip(df_pos["person_id"].tolist(), df_pos["constituency_org_id"].tolist(), strict=False))

    org_id_to_name = dict(zip(orgs["id"].tolist(), orgs["name"].tolist(), strict=False))

    # All clubs within the term
    clubs = orgs[(orgs["parent_id"] == current_term_id) & (orgs["name"].fillna("").str.contains("Poslanecký klub", regex=False))].copy()
    clubs["club_name"] = clubs["name"].astype(str).str.replace(r"^Poslanecký klub\s+", "", regex=True).str.strip()
    club_id_to_name = dict(zip(clubs["id"].tolist(), clubs["club_name"].tolist(), strict=False))

    allowed_org_ids = {current_term_id} | set(club_id_to_name.keys())
    memberships = memberships[memberships["organization_id"].isin(allowed_org_ids)].copy()

    # Only keep memberships that overlap with the current term.
    # Note: PSP may include historical rows for the same organization_id; we drop rows
    # that clearly ended before the term started.
    term_since = cur_terms.iloc[0].get("founding_date")
    if not term_since:
        raise ValueError("Current term missing founding_date")
    memberships["end_date"] = memberships["end_date"].fillna("")
    memberships = memberships[(memberships["end_date"] == "") | (memberships["end_date"] >= str(term_since))].copy()

    # All members in the term: any person with a membership in the term org
    term_m = memberships[memberships["organization_id"] == current_term_id].copy()
    person_ids = sorted(set(term_m["person_id"].tolist()))

    out = persons[persons["id"].isin(person_ids)].copy()

    # Photo URL based on current term year (from founding_date)
    term_year = str(term_since).split("-")[0]
    out["image"] = out["id"].map(lambda pid: f"https://www.psp.cz/eknih/cdrom/{term_year}ps/eknih/{term_year}ps/poslanci/i{int(pid.split(':')[-1])}.jpg")

    # Build memberships structure
    groups_m = memberships[memberships["organization_id"].isin(set(club_id_to_name.keys()))].copy()

    memberships_by_person: dict[str, dict] = {}

    # parliament memberships
    for _, r in term_m.iterrows():
        pid = r["person_id"]
        memberships_by_person.setdefault(pid, {"parliament": [], "groups": [], "candidate_list": [], "constituency": []})
        memberships_by_person[pid]["parliament"].append(
            _membership_item(current_term_id, current_term_name, r.get("start_date"), r.get("end_date"))
        )

    # group memberships
    for _, r in groups_m.iterrows():
        pid = r["person_id"]
        oid = r["organization_id"]
        memberships_by_person.setdefault(pid, {"parliament": [], "groups": [], "candidate_list": [], "constituency": []})
        memberships_by_person[pid]["groups"].append(
            _membership_item(oid, club_id_to_name.get(oid), r.get("start_date"), r.get("end_date"))
        )

    # Attach memberships
    def _mem_for(pid: str) -> dict:
        m = memberships_by_person.get(pid) or {"parliament": [], "groups": [], "candidate_list": [], "constituency": []}
        # deterministic ordering
        m["parliament"] = sorted(_ensure_list(m.get("parliament")), key=lambda x: (x.get("start_date") or "", x.get("id") or ""))
        m["groups"] = sorted(_ensure_list(m.get("groups")), key=lambda x: (x.get("start_date") or "", x.get("id") or ""))

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

        m["candidate_list"] = sorted(_ensure_list(m.get("candidate_list")), key=lambda x: (x.get("start_date") or "", x.get("id") or ""))
        m["constituency"] = sorted(_ensure_list(m.get("constituency")), key=lambda x: (x.get("start_date") or "", x.get("id") or ""))
        return m

    out["memberships"] = out["id"].map(_mem_for)

    # Write JSON (parse array fields that are JSON strings)
    cols = [
        "id",
        "name",
        "identifiers",
        "sources",
        "given_name",
        "family_name",
        "birth_date",
        "death_date",
        "gender",
        "image",
        "memberships",
    ]
    cols = [c for c in cols if c in out.columns]

    out = out[cols].sort_values(["name", "id"], ascending=[True, True])

    records = [_sanitize_json(r) for r in out.to_dict(orient="records")]
    array_fields = {"other_names", "identifiers", "contact_details", "links", "sources"}
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

        if "memberships" in r and isinstance(r.get("memberships"), dict):
            r["memberships"] = _sanitize_json(r["memberships"])

    json_path = Path("analyses/all-members/outputs/all_members.json")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    logging.info("Wrote %s (%d rows)", json_path, len(records))

    # CSV (memberships as JSON string)
    csv_cols = [c for c in cols if c != "memberships"] + ["memberships"]
    rows = []
    for r in records:
        rr = _sanitize_json({k: r.get(k) for k in csv_cols})
        rr["memberships"] = json.dumps(r.get("memberships") or {}, ensure_ascii=False, sort_keys=True)
        rows.append(rr)

    csv_path = Path("analyses/all-members/outputs/all_members.csv")
    write_csv(csv_path, rows, csv_cols)
    logging.info("Wrote %s (%d rows)", csv_path, len(rows))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_all_members(Path("work/standard"))
