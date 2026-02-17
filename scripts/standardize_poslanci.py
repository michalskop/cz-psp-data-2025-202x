import argparse
import json
import logging
from pathlib import Path

import pandas as pd

from scripts.utils_io import ensure_dir, read_unl


def _psp_person_id(id_osoba: str) -> str:
    return f"psp:person:{id_osoba}"


def _psp_org_id(id_organ: str) -> str:
    return f"psp:org:{id_organ}"


def _psp_membership_id(id_osoba: str, id_organ: str, od: str, do_: str) -> str:
    return f"psp:membership:{id_osoba}:{id_organ}:{od or ''}:{do_ or ''}"


def _parse_psp_date(d: str) -> str | None:
    if not d:
        return None
    # input examples: "09.07.1994" or "2009-11-04 00"
    if "." in d and len(d.split(".")) == 3:
        dd, mm, yyyy = d.split(".")
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    if "-" in d:
        return d.split(" ")[0]
    return None


def _parse_psp_gender(g: str) -> str | None:
    if not g:
        return None
    gg = str(g).strip().upper()
    if gg == "M":
        return "male"
    if gg in {"Z", "Ž"}:
        return "female"
    return None


def standardize(raw_dir: Path, out_dir: Path) -> dict[str, Path]:
    ensure_dir(out_dir)

    osoby = read_unl(raw_dir / "osoby.unl", expected_ncols=10)
    # columns inferred from docs + sample:
    # 0 id_osoba | 1 titul_pred | 2 prijmeni | 3 jmeno | 4 titul_za | 5 narozeni | 6 pohlavi | 7 umrti | 8 ? | 9 ?
    df_osoby = pd.DataFrame(
        osoby,
        columns=[
            "id_osoba",
            "title_before",
            "family_name",
            "given_name",
            "title_after",
            "birth_date_raw",
            "gender_raw",
            "death_date_raw",
            "col8",
            "col9",
        ],
    )

    df_persons = pd.DataFrame(
        {
            "id": df_osoby["id_osoba"].map(_psp_person_id),
            "name": (
                (df_osoby["given_name"].fillna("").astype(str).str.strip() + " " + df_osoby["family_name"].fillna("").astype(str).str.strip())
                .str.strip()
            ),
            "given_name": df_osoby["given_name"].astype(str).str.strip(),
            "family_name": df_osoby["family_name"].astype(str).str.strip(),
            "birth_date": df_osoby["birth_date_raw"].map(_parse_psp_date),
            "death_date": df_osoby["death_date_raw"].map(_parse_psp_date),
            "gender": df_osoby["gender_raw"].map(_parse_psp_gender),
            "identifiers": df_osoby["id_osoba"].map(lambda x: json.dumps([{"scheme": "psp", "identifier": str(x)}], ensure_ascii=False)),
            "sources": df_osoby["id_osoba"].map(
                lambda x: json.dumps([{"url": "https://www.psp.cz/sqw/hp.sqw?k=1301", "note": f"id_osoba={x}"}], ensure_ascii=False)
            ),
        }
    )

    # organizations
    organy = read_unl(raw_dir / "organy.unl", expected_ncols=11)
    # 0 id_organ | 1 id_organ_nadr | 2 id_typ_org | 3 zkratka | 4 nazev | 5 nazev_en | 6 od | 7 do | 8 priorita? | 9 ? | 10 ?
    df_org = pd.DataFrame(
        organy,
        columns=[
            "id_organ",
            "parent_id_organ",
            "id_typ_org",
            "abbr",
            "name_cs",
            "name_en",
            "from_raw",
            "to_raw",
            "col8",
            "col9",
            "col10",
        ],
    )
    df_orgs = pd.DataFrame(
        {
            "id": df_org["id_organ"].map(_psp_org_id),
            "name": df_org["name_cs"].astype(str).str.strip(),
            "classification": "organization",
            "parent_id": df_org["parent_id_organ"].replace({"": None}).map(lambda x: _psp_org_id(x) if x else None),
            "founding_date": df_org["from_raw"].map(_parse_psp_date),
            "dissolution_date": df_org["to_raw"].map(_parse_psp_date),
            "identifiers": df_org["id_organ"].map(lambda x: json.dumps([{"scheme": "psp", "identifier": str(x)}], ensure_ascii=False)),
            "sources": df_org["id_organ"].map(
                lambda x: json.dumps([{"url": "https://www.psp.cz/sqw/hp.sqw?k=1301", "note": f"id_organ={x}"}], ensure_ascii=False)
            ),
        }
    )

    # memberships (zarazeni)
    zar = read_unl(raw_dir / "zarazeni.unl", expected_ncols=8)
    # 0 id_osoba | 1 id_of_something (org?) | 2 typ? | 3 od_o | 4 do_o | 5 od_f | 6 do_f | 7 ?
    df_z = pd.DataFrame(
        zar,
        columns=[
            "id_osoba",
            "id_organ",
            "membership_kind",
            "start_raw",
            "end_raw",
            "start_f_raw",
            "end_f_raw",
            "col7",
        ],
    )
    df_memberships = pd.DataFrame(
        {
            "id": [
                _psp_membership_id(o, g, s, e)
                for o, g, s, e in zip(df_z["id_osoba"], df_z["id_organ"], df_z["start_raw"], df_z["end_raw"], strict=False)
            ],
            "person_id": df_z["id_osoba"].map(_psp_person_id),
            "organization_id": df_z["id_organ"].map(_psp_org_id),
            "start_date": df_z["start_raw"].map(_parse_psp_date),
            "end_date": df_z["end_raw"].map(_parse_psp_date),
            "sources": df_z.apply(
                lambda r: json.dumps(
                    [
                        {
                            "url": "https://www.psp.cz/sqw/hp.sqw?k=1301",
                            "note": f"id_osoba={r['id_osoba']} id_organ={r['id_organ']}",
                        }
                    ],
                    ensure_ascii=False,
                ),
                axis=1,
            ),
        }
    )

    # write CSV
    paths: dict[str, Path] = {}
    persons_path = out_dir / "persons.csv"
    orgs_path = out_dir / "organizations.csv"
    mem_path = out_dir / "memberships.csv"

    df_persons.to_csv(persons_path, index=False, encoding="utf-8")
    df_orgs.to_csv(orgs_path, index=False, encoding="utf-8")
    df_memberships.to_csv(mem_path, index=False, encoding="utf-8")

    logging.info("Wrote %s (%d rows)", persons_path, len(df_persons))
    logging.info("Wrote %s (%d rows)", orgs_path, len(df_orgs))
    logging.info("Wrote %s (%d rows)", mem_path, len(df_memberships))

    paths["persons"] = persons_path
    paths["organizations"] = orgs_path
    paths["memberships"] = mem_path
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="work/raw/poslanci")
    parser.add_argument("--out-dir", default="work/standard")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    standardize(Path(args.raw_dir), Path(args.out_dir))


if __name__ == "__main__":
    main()
