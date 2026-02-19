# Data Handling for Czech Lower Chamber

Data pipeline for the **Czech Chamber of Deputies (PSP)** for the 2025–202x term.

- Scrapes / downloads official PSP data.
- Standardizes tables using [Legislature Data Standard](https://github.com/michalskop/legislature-data-standard/dt/) (dt.* schemas).
- Runs small analyses.
- Uses a **stateless pattern**: big data is stored outside Git, only small outputs/pointers are committed.

## Structure

- `config/` – source URLs, schema links.
- `scripts/` – download + standardization + analyses.
- `pipelines/` – orchestration entrypoint.
- `analyses/` – per-analysis configs and outputs (small CSV/JSON/PNG).
- `work/` – ephemeral workspace (raw downloads, DuckDB DB, standard tables), **not committed**.
- `.github/workflows/` – GitHub Actions (nightly pipeline).

## Outputs

### Canonical datasets

Canonical datasets are published as immutable **snapshots** to Backblaze B2 and tracked in-repo via pointer files.

- `persons`, `organizations`, `memberships`
  - **Local build output:** `work/standard/<dataset>.csv`
  - **Published snapshot:** B2 CSV under `legislatures/cz-psp-data-2025-202x/<dataset>/snapshots/`
  - **Pointer (committed):** `data/<dataset>/latest.json`

- `votes`, `vote-events`, `motions`
  - **Local build output:**
    - `work/standard/votes.csv`
    - `work/standard/vote_events.json` (pretty JSON array)
    - `work/standard/motions.json` (pretty JSON array)
  - **Published snapshot:** B2 Parquet under `legislatures/cz-psp-data-2025-202x/<dataset>/snapshots/`
  - **Pointer (committed):** `data/<dataset>/latest.json`

Snapshots are pruned in B2 to keep the newest 5 per dataset.

### Analyses

Analyses write small outputs under `analyses/<analysis>/outputs/`. Some outputs are committed, others are local-only.

## Usage (local dev)

```bash
python -m venv .venv
source .venv/bin/activate       # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# optional: enable B2 uploads + pointer locations
cp .env.example .env
# edit .env (B2_BUCKET, B2_KEY_ID, B2_APP_KEY, B2_BUCKET_ID)

python pipelines/run_pipeline.py
```

## Attendance analysis

Attendance is computed using a shared script from the sibling repo `legislature-data-analyses`.

Generate attendance for **current members** and a Flourish-friendly CSV table:

```bash
python scripts/analyses/run_attendance.py \
  --use-current-members \
  --definition analyses/attendance/attendance_definition.json \
  --votes work/standard/votes.csv \
  --vote-events work/standard/vote_events.json \
  --persons analyses/all-members/outputs/all_members.csv \
  --current-members analyses/current-members/outputs/current_members.csv \
  --output analyses/attendance/outputs/attendance.json \
  --flourish-output analyses/attendance/outputs/attendance_flourish_table.csv
```

The runner uses existing local `work/standard/*` files. If missing, it will download the latest snapshots from B2 using `data/<dataset>/latest.json`.

## Vote-corrections analysis

Counts per-member vote corrections (*zmatečná hlasování* — cases where an MP declared they voted differently from their intention, the chamber agreed to repeat the vote, and the original was invalidated).

**PSP data limitations:** `raised_by_id` (who raised the objection) and dates are not available in the PSP open-data zip (`hl_zposlanec` and `hl_check` tables are not published).

Step 1 — standardize objections (produces `work/standard/vote_event_objections.json`):

```bash
python scripts/standardize_objections.py
```

Step 2 — run the analysis and produce a Flourish-friendly CSV:

```bash
python scripts/analyses/run_vote_corrections.py \
  --script /path/to/legislature-data-analyses/vote-corrections/vote_corrections.py \
  --flourish-script /path/to/legislature-data-analyses/vote-corrections/outputs/output_flourish_table.py \
  --use-current-members
```

`--script` and `--flourish-script` are required because the analysis lives in the separate `legislature-data-analyses` repository.
Outputs (`analyses/vote-corrections/outputs/`) are local-only (not committed).

```
cz-psp-data-2025-202x/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ config/
│  ├─ sources.yml
│  └─ schemas.yml
├─ scripts/
│  ├─ standardize_objections.py
│  ├─ standardize_poslanci.py
│  ├─ standardize_votes.py
│  ├─ download_*.py
│  ├─ upload_b2.py
│  └─ analyses/
│     ├─ run_attendance.py
│     ├─ run_vote_corrections.py
│     └─ run_*.py
├─ pipelines/
│  └─ run_pipeline.py
├─ analyses/
│  ├─ attendance/
│  │  ├─ attendance_definition.json
│  │  └─ outputs/.gitkeep
│  ├─ vote-corrections/
│  │  └─ outputs/.gitkeep
│  └─ ...
├─ work/                   # ephemeral, not committed
│  ├─ raw/
│  ├─ standard/
│  └─ b2-cache/
└─ .github/
   └─ workflows/
      └─ nightly.yml
```
