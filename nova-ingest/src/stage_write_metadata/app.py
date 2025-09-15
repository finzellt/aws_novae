# app.py — Stage metadata & enqueue harvest candidates
# Input:  { "canonical": {...}, "harvest_candidates": [ {...}, ... ] }
# Output: see docstring above

import os, json, hashlib, math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal

import boto3
from pydantic import ValidationError
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from nova_schema.nova import Nova  # canonical validator
from nova_schema.biblio import BiblioSource  # candidate validator
from nova_schema.mapping.ads_mapping import merge_updates  # candidate updater

# ---- Config ------------------------------------------------------------------

# S3 buckets (you can keep these as constants or read from env)
S3_BUCKET = os.environ.get("NOVA_DATA_BUCKET", "nova-data-bucket-finzell")
# DynamoDB table name comes from the template param → env var
HARVEST_TABLE = os.environ["HARVEST_QUEUE_TABLE"]  # fail fast if missing
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

s3 = boto3.client("s3")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION) if HARVEST_TABLE else None
queue_table = dynamodb.Table(HARVEST_TABLE) if dynamodb and HARVEST_TABLE else None

# ---- Helpers -----------------------------------------------------------------

def _utc_now():
    return datetime.now(timezone.utc)

def _ymd_paths(dt: datetime) -> Tuple[str, str, str]:
    return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

def _to_decimal(x: Any):
    if isinstance(x, float):
        # Avoid float -> Binary FP issues in DynamoDB
        return Decimal(str(x))
    return x

def _to_dynamo(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _to_dynamo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_dynamo(v) for v in obj]
    return _to_decimal(obj)

def _put_s3_json(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

def _get_biblio_objs(candidates: List[Dict[str, Any]]) -> List[BiblioSource]:
    """
    Convert a list of candidate dictionaries into BiblioSource objects.
    """
    return [BiblioSource(**c) for c in candidates]

def _get_json_objs(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert a list of candidate dictionaries into JSON-serializable objects.
    """
    return [c.model_dump(mode="json") for c in candidates]

# ---- Upsert -----------------------------------------------------------------

def _upsert_candidates_ddb(table: str, candidates: List[Dict[str, Any]]) -> int:
    """
    Upsert candidates individually. If you want BatchWrite, you can group in 25s,
    but PutItem is simplest and idempotent with stable candidate_id.
    """
    count = 0
    new_candidates = []
    for c in candidates:
        new_candidate = merge_updates(c,{"status": "queued", "updated_at": _utc_now()})
        item = {**new_candidate.model_dump(mode="json")}
        try:
            queue_table.put_item(Item=_to_dynamo(item))
            new_candidates.append(new_candidate)
            count += 1
        except Exception as e:
            logger.error("Failed to upsert item %s: %s", item, e)
            new_candidates.append(c)

    return count,new_candidates



# ---- Handler -----------------------------------------------------------------

def handler(event, _ctx):
    """
    Input:
      {
        "canonical": {...},                 # required
        "harvest_candidates": [ {...}, ... ]  # optional list
      }
    """
    canonical_in: Dict[str, Any] = event.get("canonical") or {}
    candidates_in: List[Dict[str, Any]] = event.get("harvest_candidates") or []

    # 1) Validate canonical (don’t mutate)
    try:
        nova = Nova(**canonical_in)
    except ValidationError as e:
        # Let the state machine catch this as a failure
        raise

    # Validate candidates (filter out any invalid ones)
    candidates_bib = _get_biblio_objs(candidates_in)
    # 2) Compute date-based paths (UTC)
    now = _utc_now()
    yyyy, mm, dd = _ymd_paths(now)
    ts = now.strftime("%Y%m%dT%H%M%SZ")

    # 3) Write canonical to S3 staging (metadata)
    nova_json = nova.model_dump(mode="json")
    # choose a stable-ish filename
    nid = nova_json.get("nova_id") or "unknown"
    meta_key = f"staging/metadata/{yyyy}/{mm}/{dd}/nova_{nid}.json"
    _put_s3_json(S3_BUCKET, meta_key, nova_json)


    #4) Upsert all candidates into DynamoDB
    upserted,new_candidates = _upsert_candidates_ddb(HARVEST_TABLE, candidates_bib)

    # 5) Persist the expanded candidates bundle to S3 (harvest log)
    candidates_key = f"harvest/harvest-candidates/{yyyy}/{mm}/{dd}/candidates_{ts}_{nid}.json"
    _put_s3_json(S3_BUCKET, candidates_key, {"canonical_ref": {"nova_id": nid}, "candidates": _get_json_objs(new_candidates)})

    # 6) Return updated state (canonical unchanged; include paths & counts)
    return {
        "canonical": nova_json,
        "harvest_candidates": _get_json_objs(new_candidates),
        "written": {"metadata_s3_key": meta_key, "candidates_s3_key": candidates_key},
        "dynamodb": {"table": HARVEST_TABLE, "upserted": upserted},
    }
