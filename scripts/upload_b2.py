import argparse
import hashlib
import logging
import os
from pathlib import Path

import requests


def _b2_env() -> tuple[str, str, str] | None:
    key_id = os.getenv("B2_KEY_ID")
    app_key = os.getenv("B2_APP_KEY")
    bucket = os.getenv("B2_BUCKET")
    if not key_id or not app_key or not bucket:
        return None
    return key_id, app_key, bucket


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

    buckets = _b2_list_buckets(api_url, auth_token, account_id)
    bucket_id = None
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", required=True)
    parser.add_argument("--remote", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    upload_file(Path(args.local), args.remote)


if __name__ == "__main__":
    main()
