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

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import boto3, json
import requests

ADS_API_URL = os.getenv("ADS_API_URL", "https://api.adsabs.harvard.edu/v1/search/query")

# We still request these (to derive OA & heuristics), but won't return the heavy ones in payload
ADS_FIELDS = [
    "bibcode","bibstem","doctype","property","links_data","link","identifier",
    "date","entry_date","data","author","abstract","title"
]

PRIORITY_BIBSTEMS = {"IAUC","CBET","ATel","yCat"}

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
    s = (s or "").strip()
    return '"' + s.replace('"', r'\"') + '"' if s else ""

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
    # de-dup, preserve order
    seen = set(); out = []
    for n in names:
        k = n.lower()
        if k not in seen:
            out.append(n); seen.add(k)
    return out

def _build_ads_query(names: List[str]) -> str:
    qs = [q for q in (_quote(n) for n in names) if q]
    if not qs:
        return 'bibcode:"NO_MATCH"'
    return f"full:({ ' OR '.join(qs) }) AND collection:astronomy"

def _ads_request(query: str, token: str, rows: int = 2000) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"q": query, "fl": ",".join(ADS_FIELDS), "rows": rows, "sort": "date asc"}
    resp = requests.get(ADS_API_URL, headers=headers, params=params, timeout=25)
    if resp.status_code == 401:
        raise PermissionError("ADS unauthorized (check ADS_TOKEN/ADS_DEV_KEY).")
    resp.raise_for_status()
    return resp.json().get("response", {}).get("docs", []) or []

# ---------- OA detection (robust to list/dict/str link entries) ----------
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
    if not raw: return []
    if isinstance(raw, (list, tuple)): return [_as_label_url(x) for x in raw]
    if isinstance(raw, dict) or isinstance(raw, str): return [_as_label_url(raw)]
    return []

def evaluate_open_access(doc: Dict[str, Any]) -> Tuple[bool, Optional[str], str]:
    """
    (is_open_access, best_free_url, reason):
    reasons: 'arxiv' | 'ads' | 'publisher_oa' | 'property_only' | 'none'
    """
    props = {str(p).lower() for p in (doc.get("property") or [])}
    links = _collect_links(doc)

    # 1) arXiv PDF
    for label, url in links:
        u = (url or "").lower()
        if ("arxiv" in (label or "") or "arxiv.org" in u) and "pdf" in u:
            return True, url, "arxiv"

    # 2) ADS PDFs/scans
    for label, url in links:
        l = (label or "")
        if "ads pdf" in l or "ads scanned" in l or "ads full text" in l:
            return True, url, "ads"

    # 3) Publisher OA (only if flagged OA)
    is_oa_prop = ("openaccess" in props) or ("eprint_openaccess" in props)
    if is_oa_prop:
        for label, url in links:
            l = (label or "")
            u = (url or "").lower()
            if "publisher" in l and ("pdf" in u or "article" in l or "html" in l):
                return True, url, "publisher_oa"

    # 4) OA by properties but no link we can extract
    if is_oa_prop:
        return True, None, "property_only"

    return False, None, "none"

# ---------- helpers for discovery date ----------
def _parse_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str: return None
    m = re.match(r"^(\d{4})", date_str)
    return int(m.group(1)) if m else None

def _pick_discovery(records: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (earliest_bibcode, discovery_date, discovery_basis) with >=2000→entry_date else date."""
    def to_ts(s: str) -> Optional[int]:
        try:
            return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
        except Exception:
            try:
                return int(datetime.strptime(s, "%Y-%m-%d").timestamp())
            except Exception:
                return None
    best_key = None; best = None; basis_used = None
    for rec in records:
        date = rec.get("date"); entry = rec.get("entry_date"); year = _parse_year(date)
        if year is None: continue
        if year >= 2000 and entry: ts = to_ts(entry); basis = "entry_date"
        else:
            if not date: continue
            ts = to_ts(date); basis = "date"
        if ts is None: continue
        k = (ts, rec.get("bibcode",""))
        if best_key is None or k < best_key:
            best_key = k; best = rec; basis_used = basis
    if not best: return None, None, None
    return best.get("bibcode"), best.get(basis_used) if basis_used else None, basis_used

# ---------- normalization to a SLIM record ----------
def _to_slim(doc: Dict[str, Any]) -> Dict[str, Any]:
    # bibstem: list -> first string
    raw_bibstem = doc.get("bibstem")
    bibstem = raw_bibstem[0] if isinstance(raw_bibstem, list) and raw_bibstem else raw_bibstem

    # basic deriveds
    authors = doc.get("author") or []
    authors_count = len(authors)
    has_abs = bool((doc.get("abstract") or "").strip())
    has_data_links = bool(doc.get("data") or [])

    is_oa, free_url, oa_reason = evaluate_open_access(doc)

    return {
        "bibcode": doc.get("bibcode"),
        "bibstem": bibstem,                     # str
        "doctype": doc.get("doctype"),
        "date": doc.get("date"),
        "entry_date": doc.get("entry_date"),
        "authors_count": authors_count,         # int (no author list)
        "has_abstract": has_abs,                # bool (no abstract text)
        "has_data_links": has_data_links,       # bool (no 'data' array)
        "is_open_access": is_oa,                # bool
        "open_access_url": free_url,            # str | None
        "oa_reason": oa_reason,                 # str
        # keep a tiny hint for arXiv without exposing full identifier list
        "has_arxiv_id": any(isinstance(x,str) and x.lower().startswith("arxiv:")
                            for x in (doc.get("identifier") or [])),
    }

def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Upstream status!=OK"}

    token = _get_ads_token()
    if not token:
        raise RuntimeError("ADS token not configured (set ADS_TOKEN or ADS_DEV_KEY).")

    names = _collect_names(event)
    query = _build_ads_query(names)
    docs_raw = _ads_request(query, token)

    # Build SLIM records for the payload
    records_slim = [_to_slim(d) for d in docs_raw]

    # Flat list of bibcodes
    bibcodes = [r["bibcode"] for r in records_slim if r.get("bibcode")]

    # Priority subset (same rule), now on slim records
    priority_citations = [
        r for r in records_slim if (r.get("bibstem") or "").lower() in {"iauc","cbet","atel","ycat"}
    ]
    has_fast_notice = any((r.get("bibstem") or "").lower() in {"atel","cbet"} for r in records_slim)

    # Discovery decision runs on the slim records (date/entry_date preserved)
    earliest_bib, discovery_date, discovery_basis = _pick_discovery(records_slim)

    out = dict(event)
    out["ads"] = {
        "query": query,
        "records": records_slim,          # SLIM ONLY to keep payload small
        "bibcodes": bibcodes,
        "priority_citations": priority_citations,
        "has_fast_notice": has_fast_notice,
        "num_open_access": sum(1 for r in records_slim if r["is_open_access"]),
        "earliest_bibcode": earliest_bib,
        "discovery_date": discovery_date,
        "discovery_basis": discovery_basis,
        # for visibility (what we intentionally dropped)
        "omitted_fields": ["author","abstract","property","links_data","link","data","identifier","title"]
    }
    return out
