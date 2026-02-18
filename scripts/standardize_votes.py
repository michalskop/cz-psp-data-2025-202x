import json
import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.utils_io import ensure_dir, read_unl, read_unl_iter


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


def _to_start_date(date_raw: str | None, time_raw: str | None) -> str | None:
    d = (date_raw or "").strip()
    t = (time_raw or "").strip()
    if not d:
        return None
    if "." in d:
        dd, mm, yyyy = d.split(".")
        if t and ":" in t:
            return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}T{t}:00"
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return None


def _to_date(date_raw: str | None) -> str | None:
    d = (date_raw or "").strip()
    if not d:
        return None
    if "." in d:
        dd, mm, yyyy = d.split(".")
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return None


def standardize_hl_votes(
    *,
    raw_dir: Path,
    out_votes_csv: Path,
    out_vote_events_json: Path,
    out_motions_json: Path,
    out_votes_parquet: Path,
    out_vote_events_parquet: Path,
    out_motions_parquet: Path,
) -> None:
    ensure_dir(out_votes_csv.parent)
    ensure_dir(out_vote_events_json.parent)
    ensure_dir(out_motions_json.parent)
    ensure_dir(out_votes_parquet.parent)
    ensure_dir(out_vote_events_parquet.parent)
    ensure_dir(out_motions_parquet.parent)

    hlasovani_path = raw_dir / "hl2025s.unl"
    hlas_rows = read_unl(hlasovani_path, expected_ncols=18)

    zmatecne_ids: set[str] = set()
    zmatecne_path = raw_dir / "zmatecne.unl"
    if zmatecne_path.exists() and zmatecne_path.stat().st_size > 0:
        for r in read_unl(zmatecne_path):
            if r and r[0]:
                zmatecne_ids.add(r[0])

    hlas_src_url = "https://www.psp.cz/eknih/cdrom/opendata/hl-2025ps.zip"

    vote_events: list[dict] = []
    motions: list[dict] = []
    valid_vote_ids: set[str] = set()

    for r in hlas_rows:
        hid = str(r[0])
        if hid in zmatecne_ids:
            continue

        org = str(r[1])
        sitting = (r[2] or "").strip() or None
        vote_no = (r[3] or "").strip() or None
        agenda_item_no = (r[4] or "").strip() or None
        date_raw = r[5]
        time_raw = r[6]
        title_raw = r[15]
        result_code = (r[14] or "").strip().upper()

        ve_result = None
        if result_code == "A":
            ve_result = "pass"
        elif result_code == "R":
            ve_result = "fail"

        m_result = None
        if result_code == "A":
            m_result = "passed"
        elif result_code == "R":
            m_result = "failed"

        vote_events.append(
            {
                "id": f"psp:vote-event:{hid}",
                "identifier": hid,
                "motion_id": f"psp:motion:{hid}",
                "organization_id": f"psp:org:{org}",
                "extras": {
                    "sitting_number": sitting,
                    "voting_number": vote_no,
                    "agenda_item_number": agenda_item_no,
                },
                "start_date": _to_start_date(date_raw, time_raw),
                "result": ve_result,
                "sources": [
                    {
                        "url": hlas_src_url,
                        "note": f"hl2025s.unl id_hlasovani={hid}",
                    }
                ],
            }
        )

        motions.append(
            {
                "id": f"psp:motion:{hid}",
                "identifier": hid,
                "organization_id": f"psp:org:{org}",
                "extras": {
                    "sitting_number": sitting,
                    "voting_number": vote_no,
                    "agenda_item_number": agenda_item_no,
                },
                "date": _to_date(date_raw),
                "text": (title_raw or "").strip() or None,
                "result": m_result,
                "sources": [
                    {
                        "url": hlas_src_url,
                        "note": f"hl2025s.unl id_hlasovani={hid}",
                    }
                ],
            }
        )

        valid_vote_ids.add(hid)

    vote_events.sort(key=lambda x: int(x["identifier"]))
    motions.sort(key=lambda x: int(x["identifier"]))

    out_vote_events_json.write_text(
        json.dumps(vote_events, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    logging.info("Wrote %s (%d rows)", out_vote_events_json, len(vote_events))

    out_motions_json.write_text(
        json.dumps(motions, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    logging.info("Wrote %s (%d rows)", out_motions_json, len(motions))

    pq.write_table(pa.Table.from_pylist(vote_events), out_vote_events_parquet)
    logging.info("Wrote %s (%d rows)", out_vote_events_parquet, len(vote_events))

    pq.write_table(pa.Table.from_pylist(motions), out_motions_parquet)
    logging.info("Wrote %s (%d rows)", out_motions_parquet, len(motions))

    poslanec_rows = read_unl(Path("work/raw/poslanci/poslanec.unl"), expected_ncols=16)
    poslanec_to_osoba = {str(r[0]): str(r[1]) for r in poslanec_rows if len(r) >= 2 and r[0] and r[1]}

    votes_schema = pa.schema(
        [
            pa.field("vote_event_id", pa.string()),
            pa.field("voter_id", pa.string()),
            pa.field("option", pa.string()),
        ]
    )

    ensure_dir(out_votes_parquet.parent)
    writer = pq.ParquetWriter(out_votes_parquet, votes_schema)

    import csv

    ensure_dir(out_votes_csv.parent)
    with open(out_votes_csv, "w", newline="", encoding="utf-8") as f_csv:
        w = csv.DictWriter(f_csv, fieldnames=["vote_event_id", "voter_id", "option"])
        w.writeheader()

        batch: list[dict] = []
        batch_size = 50000

        for votes_file in sorted(raw_dir.glob("hl2025h*.unl")):
            for row in read_unl_iter(votes_file, expected_ncols=4):
                id_poslanec = row[0]
                hid = row[1]
                code = row[2]

                if hid not in valid_vote_ids:
                    continue
                osoba = poslanec_to_osoba.get(id_poslanec)
                if not osoba:
                    continue

                rec = {
                    "vote_event_id": f"psp:vote-event:{hid}",
                    "voter_id": f"psp:person:{osoba}",
                    "option": _map_option(code),
                }

                w.writerow(rec)
                batch.append(rec)
                if len(batch) >= batch_size:
                    writer.write_table(pa.Table.from_pylist(batch, schema=votes_schema))
                    batch = []

        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=votes_schema))

    writer.close()
    logging.info("Wrote %s", out_votes_csv)
    logging.info("Wrote %s", out_votes_parquet)

    logging.info("HL vote standardization finished at %s", datetime.utcnow().isoformat())
