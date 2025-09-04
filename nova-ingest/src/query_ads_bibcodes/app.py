# app.py
"""
Lambda: query_ads_bibcodes

Input (from previous step):
{
  "status": "OK",
  "candidate_name": "V1324 Sco",
  "preferred_name": "V1324 Sco",
  "aliases": ["...", ...],
  ...
}

Output (adds):
{
  "ads": {
    "query": "<string>",
    "records": [
      {"bibcode": "...", "date": "YYYY-MM-DD", "entry_date": "YYYY-MM-DDTHH:MM:SSZ",
       "data": [...], "bibstem": "ATel" | "CBET" | "IAUC" | "yCat" | "..."}
    ],
    "bibcodes": ["...", "..."],
    "priority_citations": [ ...subset of records with bibstem in IAUC/CBET/ATel/yCat... ],
    "has_fast_notice": true|false,   # ATel or CBET present
    "earliest_bibcode": "...",
    "discovery_date": "YYYY-MM-DDTHH:MM:SSZ" | "YYYY-MM-DD",
    "discovery_basis": "entry_date" | "date"
  }
}
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import boto3

# from cv2 import norm
import requests

ADS_API_URL = os.getenv("ADS_API_URL", "https://api.adsabs.harvard.edu/v1/search/query")
# Fields to retrieve from ADS
ADS_FIELDS = ["bibcode", "date", "entry_date", "data", "bibstem"]
# Priority bibstems we’ll set aside for early harvesting
PRIORITY_BIBSTEMS = {"IAUC", "CBET", "ATel", "yCat"}
FAST_NOTICE_BIBSTEMS = {"ATel"}

# Accept token from either env var
def _get_ads_token() -> str:
    # 1) env var (useful for local/dev or Option A)
    env = os.getenv("ADS_TOKEN") or os.getenv("ADS_DEV_KEY")
    if env:
        return env

    # 2) Secrets Manager (Option B)
    secret_name = os.getenv("ADS_SECRET_NAME", "ADSQueryToken")
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_name)
    val = resp.get("SecretString") or ""
    try:
        obj = json.loads(val)  # handle JSON secrets like {"token":"..."}
        return obj.get("token") or obj.get("ADS_TOKEN") or val
    except Exception:
        return val

def _quote(s: str) -> str:
    # Escape embedded quotes and wrap with quotes for ADS query
    s = (s or "").strip()
    if not s:
        return ""
    return '"' + s.replace('"', r'\"') + '"'

def _collect_names(event: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for k in ("candidate_name", "preferred_name"):
        v = event.get(k)
        if isinstance(v, str) and v.strip():
            names.append(v.strip())
    aliases = event.get("aliases")
    if isinstance(aliases, list):
        for a in aliases:
            if isinstance(a, str) and a.strip():
                names.append(a.strip())
    # de-dup while preserving order
    seen = set()
    out = []
    for n in names:
        key = n.lower()
        if key not in seen:
            out.append(n)
            seen.add(key)
    return out

def _build_ads_query(names: List[str]) -> str:
    # Single query: full:( "name1" OR "name2" OR ... )
    # Using the "full" field hits title/abstract/identifiers/objects.
    quoted = [q for q in (_quote(n) for n in names) if q]
    if not quoted:
        # This should not happen; fall back to a benign query that matches nothing
        return 'bibcode:"NO_MATCH"'
    joined = " OR ".join(quoted)
    return f"full:({joined}) AND collection:astronomy"

def _ads_request(query: str, token: str, rows: int = 2000) -> List[Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    params = {
        "q": query,
        "fl": ",".join(ADS_FIELDS),
        "rows": rows,
        "sort": "date asc",
    }
    resp = requests.get(ADS_API_URL, headers=headers, params=params, timeout=20)
    if resp.status_code == 401:
        raise PermissionError("ADS unauthorized (check ADS_TOKEN).")
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", {}).get("docs", []) or []

def _parse_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    # ADS "date" is usually YYYY-MM-DD; handle YYYY or YYYY-MM as well.
    m = re.match(r"^(\d{4})", date_str)
    return int(m.group(1)) if m else None

def _pick_discovery(records: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return (earliest_bibcode, discovery_date, discovery_basis)
    Basis rule: for each record, if year >= 2000 use entry_date; else use date. Then take earliest.
    """
    best_key: Optional[Tuple[int, str]] = None  # sort key: (timestamp, tie-break bibcode)
    best: Optional[Dict[str, Any]] = None
    basis_used: Optional[str] = None

    def to_ts(s: str) -> Optional[int]:
        # try ISO; fall back to YYYY-MM-DD
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            try:
                return int(datetime.strptime(s, "%Y-%m-%d").timestamp())
            except Exception:
                return None

    for rec in records:
        date = rec.get("date")
        entry = rec.get("entry_date")
        year = _parse_year(date)
        if year is None:
            continue
        if year >= 2000 and entry:
            ts = to_ts(entry)
            basis = "entry_date"
        else:
            if not date:
                continue
            ts = to_ts(date)
            basis = "date"
        if ts is None:
            continue
        k = (ts, rec.get("bibcode", ""))
        if best_key is None or k < best_key:
            best_key = k
            best = rec
            basis_used = basis

    if best is None:
        return None, None, None

    chosen_date = best.get(basis_used) if basis_used else None
    return best.get("bibcode"), chosen_date, basis_used

def _stem_lower(s: Optional[str]) -> str:
    return (s or "").lower()


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Upstream status!=OK"}

    token = _get_ads_token()
    if not token:
        # Bubble up as a function error so Step Functions can retry/stop properly
        raise RuntimeError("ADS token not configured (set ADS_TOKEN env var).")

    names = _collect_names(event)
    query = _build_ads_query(names)
    docs = _ads_request(query, token)

    # Normalize/trim fields and compute extras
    def norm(rec: Dict[str, Any]) -> Dict[str, Any]:
        raw_bibstem = rec.get("bibstem")
        if isinstance(raw_bibstem, list):
            bibstem = raw_bibstem[0] if raw_bibstem else None
        else:
            bibstem = raw_bibstem  # already a string or None

        return {
            "bibcode": rec.get("bibcode"),
            "date": rec.get("date"),
            "entry_date": rec.get("entry_date"),
            "data": rec.get("data") or [],
            "bibstem": bibstem,              # ← normalized to a single string
            "bibstem_raw": raw_bibstem,      # (optional) keep original for debugging
        }

    records = [norm(d) for d in docs]
    bibcodes = [r["bibcode"] for r in records if r.get("bibcode")]

    has_fast_notice = any(_stem_lower(r.get("bibstem")) in {"atel"} for r in records)

    priority_citations = [
        r for r in records
        if _stem_lower(r.get("bibstem")) in {"iauc", "cbet", "atel", "ycat"}
    ]

    earliest_bib, discovery_date, discovery_basis = _pick_discovery(records)

    out = dict(event)
    out["ads"] = {
        "query": query,
        "records": records,
        "bibcodes": bibcodes,
        "priority_citations": priority_citations,
        "has_fast_notice": has_fast_notice,
        "earliest_bibcode": earliest_bib,
        "discovery_date": discovery_date,
        "discovery_basis": discovery_basis,
    }
    return out
