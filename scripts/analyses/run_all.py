import logging
from pathlib import Path

from scripts.analyses.run_current_members import run_current_members
from scripts.analyses.run_current_groups import run_current_groups
from scripts.analyses.run_current_term import run_current_term
from scripts.analyses.run_all_groups import run_all_groups
from scripts.analyses.run_all_members import run_all_members


def run_all(standard_dir: Path) -> None:
    run_current_members(standard_dir)
    run_current_groups(standard_dir)
    run_current_term(standard_dir)
    run_all_groups(standard_dir)
    run_all_members(standard_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_all(Path("work/standard"))
