# app.py — Stage metadata & enqueue harvest candidates
# Input:  { "canonical": {...}, "harvest_candidates": [ {...}, ... ] }
# Output: see docstring above

import os, json, hashlib, math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal

import boto3
# from psycopg import logger
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

    # 4) Expand candidates (per-data entries + overall entries) and assign priority
    # expanded = _expand_candidates(candidates_in)
    # expanded: List[Dict[str, Any]] = []
    # for c in cands or []:
    # 5) Upsert all candidates into DynamoDB
    # Attach a couple of helpful foreign keys into each candidate
    # for c in expanded:
    #     c.setdefault("nova_id", nid)
    #     c.setdefault("created_at", ts)  # ISO-ish stamp for queue auditing
    #     c.setdefault("ingest_source", c.get("ingest_source") or "ads")
    #     c.setdefault("status", "queued")  # optional initial status

    upserted,new_candidates = _upsert_candidates_ddb(HARVEST_TABLE, candidates_bib)

    # 6) Persist the expanded candidates bundle to S3 (harvest log)
    candidates_key = f"harvest/harvest-candidates/{yyyy}/{mm}/{dd}/candidates_{ts}_{nid}.json"
    _put_s3_json(S3_BUCKET, candidates_key, {"canonical_ref": {"nova_id": nid}, "candidates": _get_json_objs(new_candidates)})

    # 7) Return updated state (canonical unchanged; include paths & counts)
    return {
        "canonical": nova_json,
        "harvest_candidates": _get_json_objs(new_candidates),
        "written": {"metadata_s3_key": meta_key, "candidates_s3_key": candidates_key},
        "dynamodb": {"table": HARVEST_TABLE, "upserted": upserted},
    }













# from __future__ import annotations

# import json
# import os
# import time
# import hashlib
# import unicodedata
# from datetime import datetime, timezone
# from typing import Any, Dict, List, Optional, Tuple

# import boto3
# from botocore.client import Config
# from botocore.exceptions import ClientError

# # ---------- Environment / Config ----------
# STAGING_BUCKET   = os.getenv("STAGING_BUCKET", "nova-catalog")
# META_PREFIX      = os.getenv("STAGING_PREFIX", "staging/metadata")
# ADS_PREFIX       = os.getenv("ADS_STAGING_PREFIX", "staging/ads")
# WRITE_LATEST     = os.getenv("WRITE_LATEST", "true").lower() in {"1","true","yes"}
# DROP_ADS_FROM_PAYLOAD = os.getenv("DROP_ADS_FROM_PAYLOAD", "true").lower() in {"1","true","yes"}
# ADS_SNAPSHOT_MODE = os.getenv("ADS_SNAPSHOT_MODE", "slim").lower()  # slim | off

# # Eligibility / priority knobs
# TOP_DOCTYPES = {s.strip().lower() for s in (os.getenv("TOP_DOCTYPES") or "circular,catalog,dataset").split(",")}
# BIG_JOURNALS = {s.strip().lower() for s in (os.getenv("BIG_JOURNAL_BIBSTEMS") or "mnras,aj,apj,a&a,pasp").split(",")}
# EXCLUDED_DOCTYPES = {s.strip().lower() for s in (os.getenv("EXCLUDED_DOCTYPES") or
#                       "proposal,book,bookreview,editorial,inbook,obituary,inproceedings,phdthesis,talk,software").split(",")}

# PRIORITY_P0 = int(os.getenv("PRIORITY_P0", "10"))   # top: circular/catalog/dataset
# PRIORITY_P1 = int(os.getenv("PRIORITY_P1", "50"))   # big journals OA
# PRIORITY_P2 = int(os.getenv("PRIORITY_P2", "90"))   # all other eligible
# PRIORITY_RECENCY_MAX_BONUS = int(os.getenv("PRIORITY_RECENCY_MAX_BONUS", "5"))
# PRIORITY_RECENCY_WINDOW_DAYS = int(os.getenv("PRIORITY_RECENCY_WINDOW_DAYS", "365"))
# ELIGIBILITY_RULE_VERSION = os.getenv("ELIGIBILITY_RULE_VERSION", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# # DynamoDB priority queue
# ADS_QUEUE_TABLE = os.getenv("ADS_QUEUE_TABLE")  # required to enqueue
# AWS_REGION       = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
# S3_ENDPOINT_URL  = os.getenv("S3_ENDPOINT_URL")  # LocalStack/testing optional

# s3 = boto3.client(
#     "s3",
#     region_name=AWS_REGION,
#     endpoint_url=S3_ENDPOINT_URL or None,
#     config=Config(s3={"addressing_style": "virtual"}),
# )
# dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION) if ADS_QUEUE_TABLE else None
# queue_table = dynamodb.Table(ADS_QUEUE_TABLE) if dynamodb and ADS_QUEUE_TABLE else None

# # ---------- Helpers ----------
# def _slugify(s: str) -> str:
#     s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
#     s = "".join(ch if ch.isalnum() else "-" for ch in s.lower())
#     s = "-".join(filter(None, s.split("-")))
#     return s[:80] or "unknown"

# def _now_iso() -> str:
#     return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# def _ymd(ts: float) -> Tuple[str, str, str]:
#     dt = datetime.fromtimestamp(ts, tz=timezone.utc)
#     return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

# def _put_json(bucket: str, key: str, obj: Dict[str, Any]) -> Dict[str, Any]:
#     body = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
#     return s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8")

# def _s3_uri(bucket: str, key: str) -> str:
#     return f"s3://{bucket}/{key}"

# # allowed keys for slim ADS snapshot
# _ALLOWED_ADS_KEYS = {
#     "bibcode","bibstem","doctype","date","entry_date",
#     "authors_count","has_abstract","has_data_links", "data", "has_data"
#     "is_open_access","open_access_url","oa_reason","has_arxiv_id",
# }

# def _slim_ads_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
#     out: List[Dict[str, Any]] = []
#     for r in records or []:
#         bibstem = r.get("bibstem")
#         if isinstance(bibstem, list) and bibstem:
#             bibstem = bibstem[0]
#         slim = {"bibstem": bibstem}
#         for k in _ALLOWED_ADS_KEYS:
#             if k == "bibstem":
#                 continue
#             if k in r:
#                 slim[k] = r[k]
#         slim["bibcode"] = slim.get("bibcode") or r.get("bibcode")
#         out.append(slim)
#     return out

# def _parse_ymd(s: Optional[str]) -> Optional[datetime]:
#     if not s:
#         return None
#     try:
#         return datetime.fromisoformat(s.replace("Z", "+00:00"))
#     except Exception:
#         try:
#             return datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
#         except Exception:
#             return None

# def _recency_bonus(entry_date: Optional[str]) -> int:
#     '''
#     Add bonus points to priority for recent entries.

#     input:
#     - entry_date (str): The entry date of the record.

#     output:
#     - int: The bonus points to add to the priority.
#     '''

#     if not entry_date:
#         return 0
#     dt = _parse_ymd(entry_date)
#     if not dt:
#         return 0
#     age_days = max(0, int((datetime.now(timezone.utc) - dt).total_seconds() // 86400))
#     if age_days >= PRIORITY_RECENCY_WINDOW_DAYS:
#         return 0
#     # linear bonus up to max within window
#     bonus = int(round(PRIORITY_RECENCY_MAX_BONUS * (PRIORITY_RECENCY_WINDOW_DAYS - age_days) / PRIORITY_RECENCY_WINDOW_DAYS))
#     return max(0, min(PRIORITY_RECENCY_MAX_BONUS, bonus))

# def _is_big_journal(bibstem: Optional[str]) -> bool:
#     return (bibstem or "").strip().lower() in BIG_JOURNALS

# def _eligible_and_priority(rec: Dict[str, Any]) -> Tuple[bool, Optional[int], Optional[str]]:
#     """
#     Apply eligibility & priority rules.
#     Returns (eligible, priority, reason)
#     """
#     doctype = (rec.get("doctype") or "").lower()
#     bibstem = (rec.get("bibstem") or "").lower()
#     authors_ok = int(rec.get("authors_count") or 0) > 0
#     has_abs = bool(rec.get("has_abstract"))
#     is_oa = bool(rec.get("is_open_access"))
#     entry_date = rec.get("entry_date") or rec.get("date")

#     # Exclusions
#     if doctype in EXCLUDED_DOCTYPES:
#         return False, None, None

#     # P0: circular/catalog/dataset + authors + (abstract or OA)
#     if doctype in TOP_DOCTYPES and authors_ok and (has_abs or is_oa):
#         base = PRIORITY_P0
#         reason = f"P0 {doctype}"
#         prio = max(1, base - _recency_bonus(entry_date))
#         return True, prio, reason

#     # P1: big journal article + OA + authors
#     if doctype == "article" and _is_big_journal(bibstem) and is_oa and authors_ok:
#         base = PRIORITY_P1
#         reason = "P1 big-journal OA"
#         prio = max(1, base - _recency_bonus(entry_date))
#         return True, prio, reason

#     # P2: other types require OA + authors
#     if is_oa and authors_ok:
#         base = PRIORITY_P2
#         reason = "P2 other OA"
#         prio = max(1, base - _recency_bonus(entry_date))
#         return True, prio, reason

#     return False, None, None

# def _fingerprint(nova_id: str, bibcode: str) -> str:
#     '''
#     Generate a unique fingerprint for a given nova_id and bibcode. Used as part of the primary key
#     '''
#     key = f"{(bibcode or '').lower()}|{(nova_id or '').lower()}".encode("utf-8")
#     return hashlib.sha256(key).hexdigest()

# def _enqueue_snapshot(nova_id: str, bibcode: str, bibstem: str, doctype: str,
#                       entry_date: Optional[str], ads_snapshot_key: Optional[str],
#                       priority: int, reason: str,
#                       data: Optional[List[Any]] = None,
#                       open_access_url: Optional[str] = None,
#                       oa_reason: Optional[str] = None) -> Dict[str, Any]:
#     """
#     Upsert into ADS_QUEUE_TABLE priority queue.
#     Keys:
#       PK = SNAP#<fingerprint>
#       SK = NOVA#<nova_id>#BIB#<bibcode>
#     GSIs:
#       gsi1_pk = STATUS#<status>
#       gsi1_sk = {priority:03d}|{entry_date}|{PK}
#       gsi2_pk = NOVA#<nova_id>
#       gsi2_sk = {status}|{priority:03d}|{updated_at}
#     """
#     if not queue_table:
#         return {"enqueued": False, "reason": "ADS_QUEUE_TABLE not configured"}

#     fp = _fingerprint(nova_id or "unknown", bibcode or "unknown")
#     pk = f"SNAP#{fp}"
#     sk = f"NOVA#{nova_id or 'UNKNOWN'}#BIB#{bibcode or 'UNKNOWN'}"

#     status = "READY"
#     created_at = _now_iso()
#     updated_at = created_at
#     attempts = 0
#     lease_expires_at = 0
#     entry_day = (entry_date or "")[:10] or "0000-00-00"

#     item = {
#         "pk": pk,
#         "sk": sk,
#         "status": status,
#         "priority": int(priority),
#         "ads_snapshot_key": ads_snapshot_key,   # store just the key; bucket is known at runtime
#         "eligibility_rule_version": ELIGIBILITY_RULE_VERSION,
#         "reason": reason,
#         "bibcode": bibcode,
#         "nova_id": nova_id,
#         "bibstem": bibstem,
#         "doctype": doctype,
#         "entry_date": entry_date,
#         "open_access_url": open_access_url,     # <-- NEW
#         "oa_reason": oa_reason,                 # <-- NEW
#         "has_data": bool(data),
#         "data": data,
#         "attempts": attempts,
#         "lease_expires_at": lease_expires_at,
#         "created_at": created_at,
#         "updated_at": updated_at,
#         # optional query keys for a priority view (if you’re using them):
#         "gsi1_pk": f"STATUS#{status}",
#         "gsi1_sk": f"{priority:03d}|{(entry_date or '')[:10] or '0000-00-00'}|{pk}",
#         "gsi2_pk": f"NOVA#{nova_id or 'UNKNOWN'}",
#         "gsi2_sk": f"{status}|{priority:03d}|{updated_at}",
#     }
#     # strip None (DynamoDB can’t store None)
#     item = {k: v for k, v in item.items() if v is not None}

#     try:
#         queue_table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
#         return {"enqueued": True, "created": True, "pk": pk, "sk": sk, "priority": priority}
#     except ClientError as e:
#         if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
#             raise
#         # update existing to (a) lower priority if better, and (b) refresh OA fields / pointer
#         try:
#             queue_table.update_item(
#                 Key={"pk": pk, "sk": sk},
#                 UpdateExpression=(
#                     "SET #p = :new_prio, "
#                     "updated_at = :t, ads_snapshot_key = :k, reason = :r, gsi1_sk = :g1"
#                 ),
#                 ConditionExpression="attribute_not_exists(#p) OR #p > :new_prio",
#                 ExpressionAttributeNames={"#p": "priority"},
#                 ExpressionAttributeValues={
#                     ":new_prio": int(priority),
#                     ":t": _now_iso(),
#                     ":k": ads_snapshot_key,
#                     ":r": reason,
#                     ":g1": f"{priority:03d}|{(entry_date or '')[:10] or '0000-00-00'}|{pk}",
#                 },
#                 ReturnValues="UPDATED_NEW",
#             )
#         except ClientError as e:
#             if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
#                 raise
#         # 2) Always refresh OA fields & pointer (NOTE: do NOT touch priority here)
#         queue_table.update_item(
#             Key={"pk": pk, "sk": sk},
#             UpdateExpression="SET open_access_url=:o, oa_reason=:or, ads_snapshot_key=:k, updated_at=:t",
#             ExpressionAttributeValues={
#                 ":o": (open_access_url or ""),
#                 ":or": (oa_reason or ""),
#                 ":k": ads_snapshot_key,
#                 ":t": _now_iso(),
#             },
#         )
#         return {"enqueued": True, "created": False, "pk": pk, "sk": sk, "priority": priority, "updated_priority": True}
# # ---------- Handler ----------
# def handler(event: Dict[str, Any], _context) -> Dict[str, Any]:
#     """
#     - Writes immutable metadata snapshot (full event)
#     - Writes slim ADS snapshot (from event['ads'].records) when enabled
#     - Evaluates eligibility & priority for each ADS record and enqueues READY items into ADS_QUEUE_TABLE
#     - Returns original event with staging pointers and enqueue summary. Optionally trims ads payload.
#     """
#     if not isinstance(event, dict) or event.get("status") != "OK":
#         return {"status": "BAD_REQUEST", "reason": "3 Expected upstream status=OK", "input": event}

#     preferred = (event.get("preferred_name") or event.get("candidate_name") or "").strip() or "Unknown"
#     name_norm = event.get("name_norm") or _slugify(preferred)
#     nova_id = event.get("nova_id") or ""

#     ts = time.time()
#     y, m, d = _ymd(ts)
#     ts_compact = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")

#     stage_written_at = _now_iso()

#     # 1) Write immutable metadata snapshot
#     meta_doc = dict(event)
#     meta_doc.setdefault("metadata_version", "1")
#     meta_doc.setdefault("name_norm", name_norm)
#     meta_doc.setdefault("stage_written_at", stage_written_at)

#     meta_key = f"{META_PREFIX}/{y}/{m}/{d}/{name_norm}-{ts_compact}.json"
#     latest_key = f"{META_PREFIX}/latest/{name_norm}.json"

#     put_res = _put_json(STAGING_BUCKET, meta_key, meta_doc)
#     latest_res = None
#     if WRITE_LATEST:
#         latest_res = _put_json(STAGING_BUCKET, latest_key, {"ref": meta_key, "updated_at": _now_iso()})

#     # 2) Write slim ADS snapshot (no re-query)
#     ads_block = event.get("ads") or {}
#     slim_records = _slim_ads_records(ads_block.get("records") or [])
#     ads_snapshot_key = None
#     ads_snapshot_written = False

#     if ADS_SNAPSHOT_MODE == "slim" and slim_records:
#         ads_snap = {
#             "mode": "slim",
#             "query": ads_block.get("query"),
#             "bibcodes": ads_block.get("bibcodes") or [r.get("bibcode") for r in slim_records if r.get("bibcode")],
#             "records": slim_records,
#             "generated_at": _now_iso(),
#             "candidate": preferred,
#             "name_norm": name_norm,
#         }
#         ads_snapshot_key = f"{ADS_PREFIX}/{y}/{m}/{d}/{name_norm}-{ts_compact}.json"
#         _put_json(STAGING_BUCKET, ads_snapshot_key, ads_snap)
#         ads_snapshot_written = True

#     # 3) Enqueue eligible sources into ADS_QUEUE_TABLE (only if we wrote a snapshot)
#     enqueue_results: List[Dict[str, Any]] = []
#     eligible_count = 0
#     if slim_records and queue_table and ads_snapshot_written:
#         for rec in slim_records:
#             eligible, prio, reason = _eligible_and_priority(rec)
#             if not eligible:
#                 continue
#             eligible_count += 1
#             res = _enqueue_snapshot(
#                 nova_id=nova_id or name_norm,
#                 bibcode=rec.get("bibcode"),
#                 bibstem=(rec.get("bibstem") or ""),
#                 doctype=(rec.get("doctype") or ""),
#                 entry_date=(rec.get("entry_date") or rec.get("date")),
#                 ads_snapshot_key=ads_snapshot_key,
#                 priority=int(prio),
#                 reason=str(reason),
#                 data=rec.get("data"),
#                 open_access_url=rec.get("open_access_url"),
#                 oa_reason=rec.get("oa_reason"),
#             )
#             enqueue_results.append({**res, "bibcode": rec.get("bibcode")})

#     # 4) Prepare return payload (with pointers) and optionally trim ads
#     out = dict(event)
#     out.setdefault("name_norm", name_norm)
#     out.setdefault("stage_written_at", stage_written_at)
#     out["staging"] = {
#         "bucket": STAGING_BUCKET,
#         "snapshot_key": meta_key,
#         "latest_key": latest_key if WRITE_LATEST else None,
#         "ads_snapshot_key": ads_snapshot_key if ads_snapshot_written else None,
#         "ads_snapshot_status": "OK" if ads_snapshot_written else "SKIPPED",
#         "etag": put_res.get("ETag"),
#         "latest_etag": latest_res.get("ETag") if latest_res else None,
#     }
#     out["enqueue"] = {
#         "table": ADS_QUEUE_TABLE,
#         "eligible": eligible_count,
#         "enqueued": sum(1 for r in enqueue_results if r.get("enqueued")),
#         "created": sum(1 for r in enqueue_results if r.get("created")),
#         "updated_priority": sum(1 for r in enqueue_results if r.get("updated_priority")),
#         "items": enqueue_results[:50],  # keep small; at most list first 50
#     }

#     if DROP_ADS_FROM_PAYLOAD and isinstance(out.get("ads"), dict):
#         out["ads"] = {
#             "query": ads_block.get("query"),
#             "bibcodes": ads_block.get("bibcodes"),
#             "has_fast_notice": ads_block.get("has_fast_notice"),
#             "num_open_access": ads_block.get("num_open_access"),
#             "earliest_bibcode": ads_block.get("earliest_bibcode"),
#             "discovery_date": ads_block.get("discovery_date"),
#             "discovery_basis": ads_block.get("discovery_basis"),
#             "ads_snapshot_key": ads_snapshot_key if ads_snapshot_written else None,
#         }

#     return out
