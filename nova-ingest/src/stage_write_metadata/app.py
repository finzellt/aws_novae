"""
Revised write_metadata Lambda: consumes ADS records produced upstream (ads_query)
— no ADS HTTP calls — and writes immutable, date-partitioned snapshots to S3
for both metadata and ADS. This version:

- **Removes S3 encryption settings** entirely (no SSE headers used).
- **Writes metadata that includes all accumulated fields** from prior
  state-machine steps (e.g., validate_nova_and_coords, resolve_simbad_data,
  determine_host_galaxy, ads_query). It includes the ADS **bibcodes** but **not**
  the full ADS records (those are stored separately under ADS_PREFIX).
- Still supports an optional "latest" pointer for convenience.
- Keeps the payload trim by default (no heavy ADS inline), while preserving
  the full ADS in S3.
"""
from __future__ import annotations

import json
import os
import re
import time
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.config import Config

# -----------------------------------------------------------------------------
# Configuration (env)
# -----------------------------------------------------------------------------
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")  # e.g., LocalStack

STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "nova-catalog")
META_PREFIX = os.environ.get("META_PREFIX", "staging/metadata")
ADS_PREFIX = os.environ.get("ADS_PREFIX", "staging/ads")

WRITE_LATEST = os.environ.get("WRITE_LATEST", "true").lower() in {"1", "t", "true", "yes", "y"}
# Controls whether the **returned payload** includes heavy ADS fields. Metadata
# written to S3 will *always* omit heavy ADS (per requirements), independent of
# this flag.
DROP_ADS_FROM_PAYLOAD = os.environ.get("DROP_ADS_FROM_PAYLOAD", "true").lower() in {"1", "t", "true", "yes", "y"}

# -----------------------------------------------------------------------------
# AWS clients
# -----------------------------------------------------------------------------
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=S3_ENDPOINT_URL or None,
    config=Config(s3={"addressing_style": "virtual"}),
)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
_slug_re = re.compile(r"[^a-z0-9]+")


def _slugify(s: str) -> str:
    """Generate an S3-safe slug from an arbitrary string."""
    if not s:
        return "unknown"
    s_norm = (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        .strip()
    )
    s_norm = _slug_re.sub("-", s_norm).strip("-")
    s_norm = re.sub(r"-+", "-", s_norm)
    return s_norm[:80] or "unknown"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _ymd(ts: float) -> Tuple[str, str, str]:
    t = time.gmtime(ts)
    return f"{t.tm_year:04d}", f"{t.tm_mon:02d}", f"{t.tm_mday:02d}"


def _ts_compact(ts: float) -> str:
    t = time.gmtime(ts)
    return f"{t.tm_year:04d}{t.tm_mon:02d}{t.tm_mday:02d}T{t.tm_hour:02d}{t.tm_min:02d}{t.tm_sec:02d}Z"


def _put_json(bucket: str, key: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    body = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "").encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
    )
    return {"bucket": bucket, "key": key, "size": len(body)}


# -----------------------------------------------------------------------------
# ADS helpers (consume, not fetch)
# -----------------------------------------------------------------------------

def _extract_bibcodes(records: Iterable[Dict[str, Any]]) -> List[str]:
    bibs: List[str] = []
    for r in records or []:
        bc = r.get("bibcode")
        if isinstance(bc, str):
            bibs.append(bc)
    return bibs


def _slim_ads_for_payload(ads_in: Dict[str, Any], bibs: List[str], ads_snapshot_key: Optional[str]) -> Dict[str, Any]:
    """Build a lightweight ADS summary for *payload* and *metadata*.

    Heavy fields like `records` or arbitrary `raw` blobs are intentionally
    omitted. Only summary keys are included, plus bibcodes and the snapshot key.
    """
    summary: Dict[str, Any] = {}
    pass_through_keys = [
        "query",
        "has_fast_notice",
        "num_open_access",
        "earliest_bibcode",
        "discovery_date",
        "discovery_basis",
    ]
    for k in pass_through_keys:
        if k in ads_in:
            summary[k] = ads_in[k]
    if bibs:
        summary["bibcodes"] = bibs
    if ads_snapshot_key:
        summary["ads_snapshot_key"] = ads_snapshot_key
    return summary


# -----------------------------------------------------------------------------
# Lambda handler
# -----------------------------------------------------------------------------

def handler(event: Dict[str, Any], _context: Any = None) -> Dict[str, Any]:
    """Main entrypoint.

    This version **does not call ADS**. It expects any ADS content to be
    supplied by the upstream `ads_query` step, under `event["ads"]`.

    Accepted shapes for `event["ads"]`:
      - { "records": [ {"bibcode": "...", ...}, ... ], "query": "...", ... }
      - { "raw": <any JSON-serializable object>, "query": "...", ... }

    Requirements implemented:
      - Remove S3 encryption usage.
      - Metadata written to S3 includes *all accumulated fields* from previous
        steps, plus ADS **bibcodes**, but *never* the heavy ADS records.
    """

    # -------------------- validate upstream status --------------------
    if not isinstance(event, dict):
        return {"status": "BAD_REQUEST", "reason": "Expected JSON object event", "input": str(type(event))}

    upstream_status = event.get("status")
    if upstream_status and upstream_status != "OK":
        return {
            "status": "BAD_REQUEST",
            "reason": "Expected upstream status=OK",
            "input": {"status": upstream_status},
        }

    # -------------------- identity + time scaffolding -----------------
    preferred = (
        (event.get("preferred_name") or event.get("candidate_name") or "Unknown").strip()
    )
    name_norm = (event.get("name_norm") or _slugify(preferred))

    ts = time.time()
    y, m, d = _ymd(ts)
    ts_compact = _ts_compact(ts)

    meta_key = f"{META_PREFIX}/{y}/{m}/{d}/{name_norm}-{ts_compact}.json"
    latest_key = f"{META_PREFIX}/latest/{name_norm}.json"
    ads_key = f"{ADS_PREFIX}/{y}/{m}/{d}/{name_norm}-{ts_compact}.json"

    # -------------------- start with the full accumulated state --------
    # Shallow copy is enough as we only prune a couple of top-level fields.
    out: Dict[str, Any] = dict(event)
    out["status"] = "OK"
    out["preferred_name"] = preferred
    out["name_norm"] = name_norm
    out["_written_at"] = _now_iso()
    out["metadata_snapshot_key"] = meta_key

    # -------------------- consume ADS from upstream -------------------
    ads_in = event.get("ads") if isinstance(event.get("ads"), dict) else None

    ads_snapshot_written: Optional[Dict[str, Any]] = None
    bibs: List[str] = []

    if ads_in:
        # Choose payload to snapshot (heavy data goes to ADS snapshot file)
        if isinstance(ads_in.get("records"), list):
            records = ads_in["records"]
            ads_snapshot_written = _put_json(STAGING_BUCKET, ads_key, {"records": records})
            bibs = _extract_bibcodes(records)
        elif "raw" in ads_in:
            raw = ads_in["raw"]
            ads_snapshot_written = _put_json(STAGING_BUCKET, ads_key, {"raw": raw})
            bibs = _extract_bibcodes(raw if isinstance(raw, list) else [])
        else:
            # Nothing heavy to snapshot; still allow bibcodes if already computed upstream
            bibs = list(ads_in.get("bibcodes") or [])

        # Build slim summary for both payload and metadata
        ads_summary = _slim_ads_for_payload(ads_in, bibs, ads_key if ads_snapshot_written else None)

        # Attach slim summary to the outgoing state
        out["ads"] = ads_summary
    else:
        # If ADS not supplied, ensure we don't leave a stale heavy field around
        if "ads" in out:
            out.pop("ads", None)

    # -------------------- ALWAYS slim the metadata for S3 --------------
    # Per requirements, metadata should contain all accumulated fields but *not*
    # the heavy ADS structures; the slim summary in `out["ads"]` already meets
    # this. We just ensure no stray heavy subfields exist.
    def _strip_heavy_ads(obj: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(obj.get("ads"), dict):
            return obj
        slim_ads = dict(obj["ads"])  # make sure it's a copy
        slim_ads.pop("records", None)
        slim_ads.pop("raw", None)
        out2 = dict(obj)
        out2["ads"] = slim_ads
        return out2

    metadata_slim = _strip_heavy_ads(out)

    # -------------------- write metadata snapshot ---------------------
    _put_json(STAGING_BUCKET, meta_key, metadata_slim)

    # -------------------- optional latest pointer ---------------------
    if WRITE_LATEST:
        _put_json(
            STAGING_BUCKET,
            latest_key,
            {
                "preferred_name": preferred,
                "name_norm": name_norm,
                "metadata_snapshot_key": meta_key,
                # Point to ADS snapshot if we actually wrote one in this run
                "ads_snapshot_key": (ads_key if ads_snapshot_written else None),
                "updated_at": _now_iso(),
            },
        )
        out["latest_pointer_key"] = latest_key

    # -------------------- trim the RETURN payload (optional) ----------
    if DROP_ADS_FROM_PAYLOAD and isinstance(out.get("ads"), dict):
        out = _strip_heavy_ads(out)

    return out