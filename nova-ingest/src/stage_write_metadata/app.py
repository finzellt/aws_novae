# app.py
from __future__ import annotations

import json
import os
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import boto3
from botocore.client import Config

# ---- Config via env vars ----
STAGING_BUCKET   = os.getenv("STAGING_BUCKET", "nova-catalog")  # change if you prefer
STAGING_PREFIX   = os.getenv("STAGING_PREFIX", "staging/metadata")
WRITE_LATEST     = os.getenv("WRITE_LATEST", "true").lower() in {"1","true","yes"}
S3_SSE_MODE      = os.getenv("S3_SSE", "AES256")                # AES256 | aws:kms | none
KMS_KEY_ID       = os.getenv("KMS_KEY_ID")                      # used when S3_SSE=aws:kms
S3_ENDPOINT_URL  = os.getenv("S3_ENDPOINT_URL")                 # for LocalStack
AWS_REGION       = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=S3_ENDPOINT_URL or None,
    config=Config(s3={"addressing_style": "virtual"})
)

def _slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = "".join(ch if ch.isalnum() else "-" for ch in s.lower())
    s = "-".join(filter(None, s.split("-")))
    return s[:80] or "unknown"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _ymd(ts: float) -> Tuple[str, str, str]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

def _put_json(bucket: str, key: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    kwargs = {
        "Bucket": bucket,
        "Key": key,
        "Body": (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"),
        "ContentType": "application/json; charset=utf-8",
    }
    if S3_SSE_MODE and S3_SSE_MODE.lower() != "none":
        if S3_SSE_MODE.upper() == "AES256":
            kwargs["ServerSideEncryption"] = "AES256"
        elif S3_SSE_MODE == "aws:kms":
            kwargs["ServerSideEncryption"] = "aws:kms"
            if KMS_KEY_ID:
                kwargs["SSEKMSKeyId"] = KMS_KEY_ID
    return s3.put_object(**kwargs)

def handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    # Basic sanity
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Expected upstream status=OK", "input": event}

    # Derive identifiers
    preferred = (event.get("preferred_name") or event.get("candidate_name") or "").strip() or "Unknown"
    name_norm = event.get("name_norm") or _slugify(preferred)
    ts = time.time()
    y, m, d = _ymd(ts)
    ts_compact = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    stage_written_at = _now_iso()

    # Assemble staged document (you can keep this as the whole event)
    doc = dict(event)
    doc.setdefault("metadata_version", "1")
    doc.setdefault("stage_written_at", stage_written_at)
    doc.setdefault("name_norm", name_norm)

    # Immutable snapshot key + optional "latest" pointer
    snapshot_key = f"{STAGING_PREFIX}/{y}/{m}/{d}/{name_norm}-{ts_compact}.json"
    latest_key   = f"{STAGING_PREFIX}/latest/{name_norm}.json"

    put_res = _put_json(STAGING_BUCKET, snapshot_key, doc)
    latest_res = None
    if WRITE_LATEST:
        latest_res = _put_json(STAGING_BUCKET, latest_key, {"ref": snapshot_key, "updated_at": _now_iso()})

    # return includes the timestamp too
    out = dict(event)
    out.setdefault("name_norm", name_norm)
    out.setdefault("stage_written_at", stage_written_at)  # <-- add this
    out["staging"] = {
        "bucket": STAGING_BUCKET,
        "snapshot_key": snapshot_key,
        "latest_key": latest_key if WRITE_LATEST else None,
        "etag": put_res.get("ETag"),
        "latest_etag": latest_res.get("ETag") if latest_res else None,
    }
    return out
