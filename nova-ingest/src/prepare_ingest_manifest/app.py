# app.py
from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError

# ────────────────────────────── Config ──────────────────────────────
ADS_API_URL = os.getenv("ADS_API_URL", "https://api.adsabs.harvard.edu/v1/search/query")

# S3 outputs
MANIFEST_BUCKET = os.getenv("MANIFEST_BUCKET", "nova-catalog")
MANIFEST_PREFIX = os.getenv("MANIFEST_PREFIX", "harvest/manifests/pending")

# Optional: write a per-nova index file
WRITE_NOVA_INDEX = os.getenv("WRITE_NOVA_INDEX", "true").lower() in {"1", "true", "yes"}

# DynamoDB registry for idempotency (optional)
REGISTRY_TABLE = os.getenv("HARVEST_REG_TABLE", "")  # if set, we claim sources here

# Journal/bibstem lists
BIG_JOURNALS = {s.strip().lower() for s in (os.getenv("BIG_JOURNAL_BIBSTEMS") or "mnras,aj,apj,a&a,pasp").split(",")}
# doctypes to exclude entirely
EXCLUDED_DOCTYPES = {s.strip().lower() for s in (os.getenv("EXCLUDED_DOCTYPES")
                      or "proposal,book,bookreview,editorial,inbook,obituary,talk,software").split(",")}

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "ingest-1.0.0")

s3 = boto3.client("s3", region_name=REGION, endpoint_url=S3_ENDPOINT_URL)
dynamodb = boto3.resource("dynamodb", region_name=REGION) if REGISTRY_TABLE else None
reg_table = dynamodb.Table(REGISTRY_TABLE) if REGISTRY_TABLE else None

# ───────────────────── Helpers: token & time ────────────────────────
def _get_ads_token() -> str:
    tok = os.getenv("ADS_TOKEN") or os.getenv("ADS_DEV_KEY")
    if tok:
        return tok.strip()
    # optional: secrets manager fallback if ADS_SECRET_NAME is set
    secret_name = os.getenv("ADS_SECRET_NAME")
    if secret_name:
        sm = boto3.client("secretsmanager", region_name=REGION)
        resp = sm.get_secret_value(SecretId=secret_name)
        val = resp.get("SecretString") or ""
        try:
            obj = json.loads(val)
            return obj.get("token") or obj.get("ADS_TOKEN") or val
        except Exception:
            return val
    raise RuntimeError("ADS token not configured (set ADS_TOKEN/ADS_DEV_KEY or ADS_SECRET_NAME).")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# ─────────────────────── Helpers: ADS & OA ──────────────────────────
def _as_label_url(ld: Any) -> Tuple[str, str]:
    if isinstance(ld, dict):
        label = (ld.get("title") or ld.get("type") or ld.get("link_type") or "").strip().lower()
        url = (ld.get("url") or ld.get("link") or ld.get("value") or "").strip()
        return label, url
    if isinstance(ld, str):
        return "", ld.strip()
    return "", ""

def _collect_links(doc: Dict[str, Any]) -> List[Tuple[str, str]]:
    raw = doc.get("links_data") or doc.get("link")
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        return [_as_label_url(x) for x in raw]
    if isinstance(raw, dict) or isinstance(raw, str):
        return [_as_label_url(raw)]
    return []

def evaluate_open_access(doc: Dict[str, Any]) -> Tuple[bool, Optional[str], str]:
    """Return (is_open_access, best_free_url, reason)"""
    props = {str(p).lower() for p in (doc.get("property") or [])}
    links = _collect_links(doc)

    # 1) arXiv PDF
    for label, url in links:
        u = (url or "").lower()
        if ("arxiv" in (label or "") or "arxiv.org" in u) and "pdf" in u:
            return True, url, "arxiv"

    # 2) ADS PDF/scan
    for label, url in links:
        l = (label or "")
        if "ads pdf" in l or "ads scanned" in l or "ads full text" in l:
            return True, url, "ads"

    # 3) Publisher OA (only if OA flagged)
    is_oa_prop = ("openaccess" in props) or ("eprint_openaccess" in props)
    if is_oa_prop:
        for label, url in links:
            l = (label or "")
            u = (url or "").lower()
            if "publisher" in l and ("pdf" in u or "article" in l or "html" in l):
                return True, url, "publisher_oa"

    if is_oa_prop:
        return True, None, "property_only"

    return False, None, "none"

def fetch_ads_details(bibcodes: List[str], token: str) -> List[Dict[str, Any]]:
    """Batch-fetch doctype/author/abstract/links/property; normalize bibstem to string."""
    if not bibcodes:
        return []
    q = " OR ".join(f'bibcode:"{b}"' for b in bibcodes)
    params = {
        "q": f"({q})",
        "fl": ",".join([
            "bibcode","bibstem","doctype","author","abstract","property","links_data","link","identifier","date","entry_date"
        ]),
        "rows": max(1000, len(bibcodes)),
        "sort": "date asc",
    }
    resp = requests.get(
        ADS_API_URL,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params,
        timeout=25,
    )
    if resp.status_code == 401:
        raise PermissionError("ADS unauthorized; check token.")
    resp.raise_for_status()
    docs = resp.json().get("response", {}).get("docs", []) or []
    out: List[Dict[str, Any]] = []
    for d in docs:
        raw_bibstem = d.get("bibstem")
        bibstem = raw_bibstem[0] if isinstance(raw_bibstem, list) and raw_bibstem else raw_bibstem
        out.append({**d, "bibstem": bibstem})
    return out

# ───────────────────── Categorization & priority ─────────────────────
CIRCULAR_STEMS = {"atel","cbet","iauc"}
CATALOG_STEMS  = {"ycat"}  # extend if needed

def is_circular(doc: Dict[str, Any]) -> bool:
    dt = (doc.get("doctype") or "").lower()
    stem = (doc.get("bibstem") or "").lower()
    return dt == "circular" or stem in CIRCULAR_STEMS

def is_catalog(doc: Dict[str, Any]) -> bool:
    dt = (doc.get("doctype") or "").lower()
    stem = (doc.get("bibstem") or "").lower()
    return dt == "catalog" or stem in CATALOG_STEMS

def is_big_journal(doc: Dict[str, Any]) -> bool:
    stem = (doc.get("bibstem") or "").lower()
    return stem in BIG_JOURNALS

def has_authors(doc: Dict[str, Any]) -> bool:
    authors = doc.get("author") or []
    return isinstance(authors, list) and len(authors) > 0

def has_abstract(doc: Dict[str, Any]) -> bool:
    return bool((doc.get("abstract") or "").strip())

def excluded_doctype(doc: Dict[str, Any]) -> bool:
    return (doc.get("doctype") or "").lower() in EXCLUDED_DOCTYPES

def choose_worker(doc: Dict[str, Any]) -> str:
    if is_circular(doc): return "circular"
    if is_catalog(doc):  return "catalog"
    if (doc.get("doctype") or "").lower() == "article" or is_big_journal(doc): return "article"
    return "other"

def assign_priority(doc: Dict[str, Any]) -> int:
    # Lower number = higher priority
    if is_circular(doc) or is_catalog(doc): return 10   # top
    if is_big_journal(doc):                 return 20   # second
    return 30                                           # others that pass checks

# ─────────────────────── Id & registry helpers ───────────────────────
def manifest_id(nova_key: str, bibcode: str) -> str:
    h = hashlib.sha256(f"{nova_key}|{bibcode}|{PIPELINE_VERSION}".encode("utf-8")).hexdigest()[:24]
    return f"manf_{h}"

def claim_in_registry(bibcode: str) -> bool:
    """Return True if we claimed this source; False if it already exists."""
    if not reg_table:
        return True
    try:
        reg_table.put_item(
            Item={
                "pk": f"BIB#{bibcode}",
                "sk": PIPELINE_VERSION,
                "status": "PENDING",
                "claimed_at": _now_iso(),
            },
            ConditionExpression="attribute_not_exists(pk)"
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ConditionalCheckFailedException",):
            return False
        raise

def mark_done_in_registry(bibcode: str, manifest_key: str) -> None:
    if not reg_table:
        return
    reg_table.update_item(
        Key={"pk": f"BIB#{bibcode}", "sk": PIPELINE_VERSION},
        UpdateExpression="SET #s=:d, manifest_key=:m, done_at=:t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":d": "PENDING_MANIFEST", ":m": manifest_key, ":t": _now_iso()},
    )

# ─────────────────────────── S3 helpers ─────────────────────────────
def put_json(bucket: str, key: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    body = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    return s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8")

def append_nova_index(name_norm: str, manifest_rec: Dict[str, Any]) -> None:
    """Write/append a tiny index per nova to help dashboards. Simple overwrite merge."""
    if not WRITE_NOVA_INDEX:
        return
    idx_key = f"harvest/manifests/index/{name_norm}.json"
    try:
        cur = s3.get_object(Bucket=MANIFEST_BUCKET, Key=idx_key)["Body"].read().decode("utf-8")
        idx = json.loads(cur)
    except Exception:
        idx = {"manifests": []}
    idx.setdefault("manifests", []).append(manifest_rec)
    put_json(MANIFEST_BUCKET, idx_key, idx)

# ─────────────────────────── Main logic ─────────────────────────────
def handler(event: Dict[str, Any], _context) -> Dict[str, Any]:
    """
    Input: enriched event with event['ads']['bibcodes'] or event['ads']['records'] (bibcode at least)
    Output: event + { "manifests": { "created": int, "skipped": int, "keys": [..] } }
    """
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Upstream status!=OK"}

    # Identify the nova (nova_id if present; else name_norm or preferred)
    nova_id = event.get("nova_id")
    name_norm = event.get("name_norm") or (event.get("preferred_name") or event.get("candidate_name") or "unknown").lower().replace(" ", "-")
    nova_key = nova_id or name_norm

    # Collect bibcodes to evaluate
    bibs = event.get("ads", {}).get("bibcodes") or []
    if not bibs and event.get("ads", {}).get("records"):
        bibs = [r.get("bibcode") for r in event["ads"]["records"] if r.get("bibcode")]
    bibs = [b for b in bibs if b]

    if not bibs:
        out = dict(event)
        out["manifests"] = {"created": 0, "skipped": 0, "keys": []}
        return out

    token = _get_ads_token()
    docs = fetch_ads_details(bibs, token)

    created_keys: List[str] = []
    skipped = 0

    for doc in docs:
        # Basic normalization
        bibcode = doc.get("bibcode")
        if not bibcode:
            skipped += 1
            continue

        # Exclusions first
        if excluded_doctype(doc):
            skipped += 1
            continue

        # OA, authors, abstract checks
        is_oa, free_url, oa_reason = evaluate_open_access(doc)
        authors_ok = has_authors(doc)
        abstract_ok = has_abstract(doc)

        doc_doctype = (doc.get("doctype") or "").lower()
        stem = (doc.get("bibstem") or "")
        worker = choose_worker(doc)
        priority = assign_priority(doc)

        # Apply acceptance rules
        accept = False
        if worker in {"circular", "catalog"}:
            # pass if has authors AND (abstract or OA)
            accept = authors_ok and (abstract_ok or is_oa)
        elif worker == "article":
            if is_big_journal(doc):
                # big journals require OA and (prefer authors too)
                accept = is_oa and authors_ok
            else:
                # other articles: OA + authors
                accept = is_oa and authors_ok
        else:
            # other types: OA + authors
            accept = is_oa and authors_ok

        if not accept:
            skipped += 1
            continue

        # Optional idempotency guard via DynamoDB registry
        claimed = claim_in_registry(bibcode)
        if not claimed:
            skipped += 1
            continue

        # Build manifest payload
        man_id = manifest_id(nova_key, bibcode)
        man_key = f"{MANIFEST_PREFIX}/{man_id}.json"

        manifest = {
            "version": "1",
            "created_at": _now_iso(),
            "manifest_id": man_id,
            "pipeline_version": PIPELINE_VERSION,
            "nova": {
                "nova_id": nova_id,
                "name_norm": name_norm,
                "preferred_name": event.get("preferred_name") or event.get("candidate_name"),
                "aliases": event.get("aliases") or [],
                "coords": event.get("coords") or {},
                "galactic": event.get("galactic") or {},
                "constellation": event.get("constellation"),
                "host": {
                    "galaxy": event.get("host_galaxy"),
                    "confidence": event.get("external_galaxy_confidence"),
                    "nearest": event.get("nearest_galaxy"),
                },
            },
            "source": {
                "type": worker,               # circular | catalog | article | other
                "bibcode": bibcode,
                "bibstem": stem,
                "doctype": doc_doctype,
                "priority": priority,
                "authors_count": len(doc.get("author") or []),
                "has_abstract": abstract_ok,
                "is_open_access": is_oa,
                "open_access_url": free_url,
                "oa_reason": oa_reason,
                "ads": {
                    "query": event.get("ads", {}).get("query"),
                    "date": doc.get("date"),
                    "entry_date": doc.get("entry_date"),
                    "identifier": doc.get("identifier") or [],
                },
            },
            "upstream": {
                "staged_metadata_key": event.get("staging", {}).get("snapshot_key"),
            },
            "dedupe": {
                "fingerprint": f"sha256:{hashlib.sha256((bibcode).encode()).hexdigest()}",
                "pipeline_version": PIPELINE_VERSION,
            },
            "expected_output": {
                "table": "nova_data",
                "schema": ["nova_id", "mjd", "band", "mag", "mag_err", "source"],
                "partition": {
                    "constellation": event.get("constellation"),
                    "nova_id": event.get("nova_id") or "TBD",
                },
            },
        }

        # Write manifest to S3
        put_json(MANIFEST_BUCKET, man_key, manifest)
        created_keys.append(man_key)

        # Registry: mark PENDING_MANIFEST with the manifest key
        mark_done_in_registry(bibcode, man_key)

        # Optional: append per-nova index
        if WRITE_NOVA_INDEX:
            append_nova_index(name_norm, {
                "id": man_id,
                "bibcode": bibcode,
                "worker_type": worker,
                "priority": priority,
                "key": man_key,
                "status": "pending",
            })

    out = dict(event)
    out["manifests"] = {
        "created": len(created_keys),
        "skipped": skipped,
        "keys": created_keys,
        "bucket": MANIFEST_BUCKET,
        "prefix": MANIFEST_PREFIX,
    }
    return out
