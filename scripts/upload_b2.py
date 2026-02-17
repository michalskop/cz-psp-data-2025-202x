import argparse
import hashlib
import logging
import os
from pathlib import Path

import requests

from scripts.utils_env import load_dotenv


def _b2_env() -> tuple[str, str, str] | None:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root)

    key_id = os.getenv("B2_KEY_ID")
    app_key = os.getenv("B2_APP_KEY")
    bucket = os.getenv("B2_BUCKET")
    if not key_id or not app_key or not bucket:
        return None
    return key_id, app_key, bucket


def _b2_bucket_id_env() -> str | None:
    # Optional: if set, we can avoid b2_list_buckets (needs listBuckets capability).
    return os.getenv("B2_BUCKET_ID")


def _b2_authorize(key_id: str, app_key: str) -> dict:
    r = requests.get(
        "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
        auth=(key_id, app_key),
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _b2_get_upload_url(api_url: str, auth_token: str, bucket_id: str) -> dict:
    r = requests.post(
        f"{api_url}/b2api/v2/b2_get_upload_url",
        headers={"Authorization": auth_token},
        json={"bucketId": bucket_id},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _b2_list_file_names(
    api_url: str,
    auth_token: str,
    bucket_id: str,
    *,
    prefix: str,
    start_file_name: str | None = None,
    max_file_count: int = 1000,
) -> dict:
    payload: dict = {"bucketId": bucket_id, "prefix": prefix, "maxFileCount": max_file_count}
    if start_file_name:
        payload["startFileName"] = start_file_name
    r = requests.post(
        f"{api_url}/b2api/v2/b2_list_file_names",
        headers={"Authorization": auth_token},
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _b2_delete_file_version(api_url: str, auth_token: str, *, file_name: str, file_id: str) -> dict:
    r = requests.post(
        f"{api_url}/b2api/v2/b2_delete_file_version",
        headers={"Authorization": auth_token},
        json={"fileName": file_name, "fileId": file_id},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _b2_list_buckets(api_url: str, auth_token: str, account_id: str) -> dict:
    r = requests.post(
        f"{api_url}/b2api/v2/b2_list_buckets",
        headers={"Authorization": auth_token},
        json={"accountId": account_id},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_file(local_path: Path, remote_name: str) -> None:
    env = _b2_env()
    if env is None:
        logging.info("B2 env vars not set; skipping upload")
        return

    key_id, app_key, bucket_name = env

    auth = _b2_authorize(key_id, app_key)
    api_url = auth["apiUrl"]
    auth_token = auth["authorizationToken"]
    account_id = auth["accountId"]

    bucket_id = _b2_bucket_id_env()
    if not bucket_id:
        try:
            buckets = _b2_list_buckets(api_url, auth_token, account_id)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                raise RuntimeError(
                    "B2 application key is not authorized to list buckets. "
                    "Either add the 'listBuckets' capability to the key, or set B2_BUCKET_ID in .env "
                    "(preferred for restricted keys)."
                ) from e
            raise

        for b in buckets.get("buckets", []):
            if b.get("bucketName") == bucket_name:
                bucket_id = b.get("bucketId")
                break
        if not bucket_id:
            raise ValueError(f"B2 bucket not found: {bucket_name}")

    up = _b2_get_upload_url(api_url, auth_token, bucket_id)
    upload_url = up["uploadUrl"]
    upload_auth = up["authorizationToken"]

    sha1 = _sha1(local_path)
    with open(local_path, "rb") as f:
        r = requests.post(
            upload_url,
            headers={
                "Authorization": upload_auth,
                "X-Bz-File-Name": remote_name,
                "Content-Type": "b2/x-auto",
                "X-Bz-Content-Sha1": sha1,
            },
            data=f,
            timeout=300,
        )
    r.raise_for_status()
    logging.info("Uploaded %s -> b2://%s/%s", local_path, bucket_name, remote_name)


def prune_snapshots(prefix: str, *, keep: int = 5) -> None:
    """Keep only the newest `keep` files under a given B2 prefix; delete older ones.

    This is intended for snapshot-style objects under e.g.
    `cz-psp/2025-202x/persons/snapshots/`.
    """
    env = _b2_env()
    if env is None:
        logging.info("B2 env vars not set; skipping prune")
        return

    if keep < 1:
        raise ValueError("keep must be >= 1")

    key_id, app_key, bucket_name = env
    auth = _b2_authorize(key_id, app_key)
    api_url = auth["apiUrl"]
    auth_token = auth["authorizationToken"]
    account_id = auth["accountId"]

    bucket_id = _b2_bucket_id_env()
    if not bucket_id:
        try:
            buckets = _b2_list_buckets(api_url, auth_token, account_id)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                raise RuntimeError(
                    "B2 application key is not authorized to list buckets. "
                    "Either add the 'listBuckets' capability to the key, or set B2_BUCKET_ID in .env "
                    "(preferred for restricted keys)."
                ) from e
            raise

        for b in buckets.get("buckets", []):
            if b.get("bucketName") == bucket_name:
                bucket_id = b.get("bucketId")
                break
        if not bucket_id:
            raise ValueError(f"B2 bucket not found: {bucket_name}")

    files: list[dict] = []
    start: str | None = None
    while True:
        page = _b2_list_file_names(api_url, auth_token, bucket_id, prefix=prefix, start_file_name=start)
        items = page.get("files", []) or []
        files.extend(items)
        start = page.get("nextFileName")
        if not start:
            break

    if len(files) <= keep:
        logging.info("Prune %s: %d files <= keep=%d; nothing to delete", prefix, len(files), keep)
        return

    # Sort by uploadTimestamp descending, then by fileName for determinism.
    files_sorted = sorted(
        files,
        key=lambda f: (-(int(f.get("uploadTimestamp") or 0)), str(f.get("fileName") or "")),
    )
    to_delete = files_sorted[keep:]
    for f in to_delete:
        fn = f.get("fileName")
        fid = f.get("fileId")
        if not fn or not fid:
            continue
        _b2_delete_file_version(api_url, auth_token, file_name=fn, file_id=fid)
        logging.info("Deleted old snapshot b2://%s/%s (fileId=%s)", bucket_name, fn, fid)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", required=True)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--prune-prefix", default=None)
    parser.add_argument("--keep", type=int, default=5)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    upload_file(Path(args.local), args.remote)
    if args.prune_prefix:
        prune_snapshots(args.prune_prefix, keep=args.keep)


if __name__ == "__main__":
    main()
