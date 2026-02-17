import argparse
import logging
import sys
from pathlib import Path
import json

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.utils_io import read_unl, write_csv


def _map_option(code: str) -> str:
    c = (code or "").strip().upper()
    if c == "A":
        return "yes"
    if c in {"B", "N"}:
        return "no"
    if c in {"C", "K"}:
        return "abstain"
    if c == "F":
        return "not voting"
    if c == "@":
        return "absent"
    if c == "M":
        return "excused"
    if c == "W":
        return "not member"
    return "unknown"


def standardize_votes_sample(raw_dir: Path, out_csv: Path, n_vote_events: int = 5) -> None:
    # vote events (hl_hlasovani)
    hlasovani_path = raw_dir / "hl2025s.unl"
    hlas_rows = read_unl(hlasovani_path, expected_ncols=18)
    df_h = pd.DataFrame(hlas_rows)
    df_h = df_h.rename(
        columns={
            0: "id_hlasovani",
            1: "id_organ",
            2: "sitting_number",
            3: "vote_number",
            4: "agenda_item_number",
            5: "date_raw",
            6: "time_raw",
            15: "title_raw",
            14: "result_code",
        }
    )

    # exclude mistaken votes (zmatecne)
    zmatecne_ids: set[str] = set()
    zmatecne_path = raw_dir / "zmatecne.unl"
    if zmatecne_path.exists() and zmatecne_path.stat().st_size > 0:
        z_rows = read_unl(zmatecne_path)
        for r in z_rows:
            if r and r[0]:
                zmatecne_ids.add(r[0])

    df_h = df_h[~df_h["id_hlasovani"].isin(zmatecne_ids)].copy()

    # deterministic sample: first N vote-event ids (numeric sort)
    df_h["id_hlasovani_int"] = pd.to_numeric(df_h["id_hlasovani"], errors="coerce")
    df_h = df_h.sort_values(["id_hlasovani_int", "id_hlasovani"], ascending=[True, True])
    sample_vote_ids = df_h.head(n_vote_events)["id_hlasovani"].tolist()
    if not sample_vote_ids:
        raise ValueError("No vote events found")

    hlas_src_url = "https://www.psp.cz/eknih/cdrom/opendata/hl-2025ps.zip"

    # Vote-events sample JSON
    df_hs = df_h[df_h["id_hlasovani"].isin(set(sample_vote_ids))].copy()
    df_hs = df_hs.sort_values(["id_hlasovani_int", "id_hlasovani"], ascending=[True, True])

    vote_events = []
    for _, r in df_hs.iterrows():
        date_raw = (r.get("date_raw") or "").strip()
        time_raw = (r.get("time_raw") or "").strip()
        start_date = None
        if date_raw and time_raw and "." in date_raw and ":" in time_raw:
            dd, mm, yyyy = date_raw.split(".")
            start_date = f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}T{time_raw}:00"
        elif date_raw and "." in date_raw:
            dd, mm, yyyy = date_raw.split(".")
            start_date = f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"

        sitting = (r.get("sitting_number") or "").strip() or None
        vote_no = (r.get("vote_number") or "").strip() or None
        agenda_item_no = (r.get("agenda_item_number") or "").strip() or None

        result_code = (r.get("result_code") or "").strip().upper()
        result = None
        if result_code == "A":
            result = "pass"
        elif result_code == "R":
            result = "fail"

        hid = str(r["id_hlasovani"])
        vote_events.append(
            {
                "id": f"psp:vote-event:{hid}",
                "identifier": hid,
                "motion_id": f"psp:motion:{hid}",
                "organization_id": f"psp:org:{r['id_organ']}",
                "extras": {
                    "sitting_number": sitting,
                    "voting_number": vote_no,
                    "agenda_item_number": agenda_item_no,
                },
                "start_date": start_date,
                "result": result,
                "sources": [
                    {
                        "url": hlas_src_url,
                        "note": f"hl2025s.unl id_hlasovani={hid}",
                    }
                ],
            }
        )

    vote_events_path = out_csv.parent / "vote_events_sample.json"
    vote_events_path.write_text(json.dumps(vote_events, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info("Wrote %s (%d rows)", vote_events_path, len(vote_events))

    # Motions sample JSON (1 motion per vote-event; motion_id == vote_event_id)
    motions = []
    for _, r in df_hs.iterrows():
        date_raw = (r.get("date_raw") or "").strip()
        date_iso = None
        if date_raw and "." in date_raw:
            dd, mm, yyyy = date_raw.split(".")
            date_iso = f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"

        text = (r.get("title_raw") or "").strip() or None

        sitting = (r.get("sitting_number") or "").strip() or None
        vote_no = (r.get("vote_number") or "").strip() or None
        agenda_item_no = (r.get("agenda_item_number") or "").strip() or None

        result_code = (r.get("result_code") or "").strip().upper()
        result = None
        if result_code == "A":
            result = "passed"
        elif result_code == "R":
            result = "failed"

        hid = str(r["id_hlasovani"])

        motions.append(
            {
                "id": f"psp:motion:{hid}",
                "identifier": hid,
                "organization_id": f"psp:org:{r['id_organ']}",
                "extras": {
                    "sitting_number": sitting,
                    "voting_number": vote_no,
                    "agenda_item_number": agenda_item_no,
                },
                "date": date_iso,
                "text": text,
                "result": result,
                "sources": [
                    {
                        "url": hlas_src_url,
                        "note": f"hl2025s.unl id_hlasovani={hid}",
                    }
                ],
            }
        )

    motions_path = out_csv.parent / "motions_sample.json"
    motions_path.write_text(json.dumps(motions, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info("Wrote %s (%d rows)", motions_path, len(motions))

    # mapping id_poslanec -> id_osoba from poslanci.zip (needed to build psp:person:* ids)
    poslanec_rows = read_unl(Path("work/raw/poslanci/poslanec.unl"), expected_ncols=16)
    df_p = pd.DataFrame(poslanec_rows).rename(columns={0: "id_poslanec", 1: "id_osoba"})
    df_p = df_p[["id_poslanec", "id_osoba"]].copy()
    poslanec_to_osoba = dict(zip(df_p["id_poslanec"].tolist(), df_p["id_osoba"].tolist(), strict=False))

    # votes (hl_poslanec): (id_poslanec, id_hlasovani, vysledek, ?)
    votes_path = raw_dir / "hl2025h1.unl"
    vote_rows = read_unl(votes_path, expected_ncols=4)
    df_v = pd.DataFrame(vote_rows).rename(columns={0: "id_poslanec", 1: "id_hlasovani", 2: "code"})

    df_v = df_v[df_v["id_hlasovani"].isin(set(sample_vote_ids))].copy()

    rows_out = []
    for _, r in df_v.iterrows():
        osoba = poslanec_to_osoba.get(r["id_poslanec"])
        if not osoba:
            continue
        rows_out.append(
            {
                "vote_event_id": f"psp:vote-event:{r['id_hlasovani']}",
                "voter_id": f"psp:person:{osoba}",
                "option": _map_option(r["code"]),
            }
        )

    # stable order
    rows_out = sorted(rows_out, key=lambda x: (int(x["vote_event_id"].split(":")[-1]), x["voter_id"], x["option"]))

    write_csv(out_csv, rows_out, ["vote_event_id", "voter_id", "option"])
    logging.info("Wrote %s (%d rows) for vote-event ids: %s", out_csv, len(rows_out), ",".join(sample_vote_ids))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="work/raw/hl-2025ps")
    parser.add_argument("--out", default="work/standard/votes_sample.csv")
    parser.add_argument("--n", type=int, default=5)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    standardize_votes_sample(Path(args.raw_dir), Path(args.out), n_vote_events=args.n)
