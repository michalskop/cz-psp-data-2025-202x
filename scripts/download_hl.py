import logging
from pathlib import Path
from zipfile import ZipFile

import requests


DEFAULT_URL = "https://www.psp.cz/eknih/cdrom/opendata/hl-2025ps.zip"


def download_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Downloading %s -> %s", url, out_path)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    out_path.write_bytes(r.content)


def unpack_zip(zip_path: Path, out_dir: Path) -> None:
    logging.info("Unpacking %s -> %s", zip_path, out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
        logging.info("hl zip contains %d files", len(z.namelist()))
        for n in z.namelist():
            logging.info("- %s", n)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    zip_path = Path("work/raw/hl-2025ps.zip")
    out_dir = Path("work/raw/hl-2025ps")
    download_file(DEFAULT_URL, zip_path)
    unpack_zip(zip_path, out_dir)
