# Analyses

Analyses are small derived outputs produced from canonical datasets.

- Input data comes from `work/standard/` (built locally) and/or from B2 snapshots referenced by `data/<dataset>/latest.json`.
- Outputs are written to `analyses/<analysis>/outputs/`.

## Attendance

Definition:

- `analyses/attendance/attendance_definition.json`

Run (current members):

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

Outputs (local-only by default):

- `analyses/attendance/outputs/attendance.json`
- `analyses/attendance/outputs/attendance_flourish_table.csv`

## Vote corrections

Counts per-member *zmatečná hlasování* — cases where an MP declared they voted differently from their intention, the chamber agreed to repeat the vote, and the original was invalidated.

Step 1 — standardize PSP objections data:

```bash
python scripts/standardize_objections.py
# → work/standard/vote_event_objections.json
```

Step 2 — run the analysis (requires paths to scripts in `legislature-data-analyses`):

```bash
python scripts/analyses/run_vote_corrections.py \
  --script /path/to/legislature-data-analyses/vote-corrections/vote_corrections.py \
  --flourish-script /path/to/legislature-data-analyses/vote-corrections/outputs/output_flourish_table.py \
  --use-current-members
```

Outputs (local-only by default):

- `analyses/vote-corrections/outputs/vote_corrections.json`
- `analyses/vote-corrections/outputs/vote_corrections_flourish_table.csv`

**PSP data limitations:** `raised_by_id` (who raised the objection) and vote-event dates are not available in the PSP open-data zip.
