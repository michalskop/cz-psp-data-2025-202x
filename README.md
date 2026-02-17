# Data Handling for Czech Lower Chamber

Data pipeline for the **Czech Chamber of Deputies (PSP)** for the 2025–202x term.

- Scrapes / downloads official PSP data.
- Standardizes tables using [Legislature Data Standard](https://github.com/michalskop/legislature-data-standard/dt/) (dt.* schemas).
- Runs small analyses (attendance, etc.).
- Uses a **stateless pattern**: no big data is stored in Git, only small outputs.

## Structure

- `config/` – source URLs, schema links.
- `scripts/` – download + standardization + analyses.
- `pipelines/` – orchestration entrypoint.
- `analyses/` – per-analysis configs and outputs (small CSV/JSON/PNG).
- `work/` – ephemeral workspace (raw downloads, DuckDB DB, standard tables), **not committed**.
- `.github/workflows/` – GitHub Actions (nightly pipeline).

## Usage (local dev)

```bash
python -m venv .venv
source .venv/bin/activate       # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

python pipelines/run_pipeline.py
```


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
