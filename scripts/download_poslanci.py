import argparse
import logging
import os
import zipfile
from pathlib import Path

import requests


DEFAULT_URL = "https://www.psp.cz/eknih/cdrom/opendata/poslanci.zip"


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def download_file(url: str, dest_path: Path) -> None:
    _ensure_dir(dest_path.parent)

    logging.info("Downloading %s -> %s", url, dest_path)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        os.replace(tmp_path, dest_path)


def unpack_zip(zip_path: Path, dest_dir: Path) -> list[str]:
    _ensure_dir(dest_dir)

    logging.info("Unpacking %s -> %s", zip_path, dest_dir)
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        zf.extractall(dest_dir)

    logging.info("poslanci.zip contains %d files:", len(names))
    for name in names:
        logging.info("- %s", name)

    return names


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--zip-path", default="work/raw/poslanci.zip")
    parser.add_argument("--out-dir", default="work/raw/poslanci")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    zip_path = Path(args.zip_path)
    out_dir = Path(args.out_dir)

    download_file(args.url, zip_path)
    unpack_zip(zip_path, out_dir)


if __name__ == "__main__":
    main()
