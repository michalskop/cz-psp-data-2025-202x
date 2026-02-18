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


```
cz-psp-data-2025-202x/
├─ README.md
├─ LICENSE                 # optional
├─ requirements.txt
├─ .gitignore
├─ config/
│  ├─ sources.yml
│  └─ schemas.yml
├─ scripts/
│  ├─ download_votes.py
│  ├─ build_db.py
│  └─ analyses/
│     └─ run_all.py
├─ pipelines/
│  └─ run_pipeline.py
├─ analyses/
│  ├─ attendance/
│  │  ├─ attendance_definition.json
│  │  └─ outputs/.gitkeep
│  └─ example/
│     └─ outputs/.gitkeep
├─ work/
│  ├─ raw/.gitkeep
│  ├─ db/.gitkeep
│  └─ standard/.gitkeep
└─ .github/
   └─ workflows/
      └─ nightly.yml
```
