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
