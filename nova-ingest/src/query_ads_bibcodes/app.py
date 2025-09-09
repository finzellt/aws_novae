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
from urllib.parse import urlparse

ADS_API_URL = os.getenv("ADS_API_URL", "https://api.adsabs.harvard.edu/v1/search/query")

# We still request these (to derive OA & heuristics), but won't return the heavy ones in payload
ADS_FIELDS = [
    "bibcode","bibstem","doctype","property","identifier","data",
    "date","entry_date","data","author","abstract","title"
]

PRIORITY_BIBSTEMS = {"IAUC","CBET","ATel","yCat"}

RESOLVER_URL = os.getenv("ADS_RESOLVER_URL", "https://api.adsabs.harvard.edu/v1/resolver")
RESOLVER_MODE = os.getenv("RESOLVER_MODE", "smart").lower()  # smart | all
RESOLVER_TIMEOUT = float(os.getenv("RESOLVER_TIMEOUT", "6"))
RESOLVER_MAX_CALLS = int(os.getenv("RESOLVER_MAX_CALLS", "200"))

_ARXIV_RE = re.compile(r"arxiv:(?P<id>\d{4}\.\d{4,5}(v\d+)?)", re.I)

# Accept token from either env var
def _get_ads_token() -> str:
    '''
    Retrieves the ADS API token from environment variables or AWS Secrets Manager.

    outputs:
        str: The ADS API token.
    '''

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

def _first_arxiv_id(identifiers):
    for x in identifiers or []:
        m = _ARXIV_RE.match(str(x))
        if m: return m.group("id")
    return None

def _ads_abs_url(bib: str) -> str:
    # Stable abstract URL users can access
    return f"https://ui.adsabs.harvard.edu/abs/{bib}/abstract"

def _atel_link(code: str) -> str:
    # Find the number between "ATel" and "...."
    try:
        number = code.split("ATel.")[1].split("....")[0]
        return f"https://www.astronomerstelegram.org/?read={number}"
    except (IndexError, ValueError):
        raise ValueError("Input string is not in the expected format.")

def is_good_url(url: str) -> bool:
    return "validate" not in url.lower()

ERROR_SNIPPET = "The requested resource does not exist"

def is_real_resource(url: str, timeout: int = 8) -> bool:
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            return False
        text = r.text.lower()
        if ERROR_SNIPPET.lower() in text:
            return False
        return True
    except requests.RequestException:
        return False

def _resolve_open_access_links(url: str) -> str:
    """
    Takes a potential URL and determines if it is valid.
    
    - If the URL resolves (with redirects), returns the resolved URL.
    - If it doesn't redirect, returns the original URL.
    - If the URL isn't valid or can't be reached, returns an empty string.
    """
    try:
        # Ensure the URL has a scheme; if not, assume http
        parsed = urlparse(url)
        if not parsed.scheme:
            url = "http://" + url
            parsed = urlparse(url)

        # If still invalid (no netloc), return empty string
        if not parsed.netloc:
            return ""

        # Attempt to resolve the URL (follow redirects)
        response = requests.head(url, allow_redirects=True, timeout=5)
        
        # Some servers don’t respond well to HEAD; fallback to GET
        if response.status_code >= 400:
            response = requests.get(url, allow_redirects=True, timeout=5)
        
        # Return the final resolved URL
        return response.url

    except Exception:
        # Invalid URL or network error
        return ""


def _resolve_open_access_via_resolver(bib: str) -> tuple[Optional[str], Optional[str]]:
    '''
    Attempts to resolve open access links for a given bibcode via the ADS resolver.

    inputs:
        bib (str): The bibcode to resolve.
        token (str): The ADS API token.

    outputs:
        tuple[Optional[str], Optional[str]]: The resolved URL and the source type (if found).
    '''

    potential_suffixes = ["ADS_PDF","PUB_PDF","EPRINT_PDF"]
    # potential_suffixes = ["esource"]
    for suffix in potential_suffixes:
        url = f"https://ui.adsabs.harvard.edu/link_gateway/{bib}/{suffix}"
        resolved_url = _resolve_open_access_links(url)
        if resolved_url and (("pdf" in resolved_url and is_good_url(resolved_url)) or (is_good_url(resolved_url) and is_real_resource(resolved_url))):
            return resolved_url, suffix
    return None, None

def _quote(s: str) -> str:
    '''
    Ensure output is a string, regardless of input type. Quotes a string for inclusion in an ADS 
    query. Adds escape character for internal quotes. 

    inputs:
        s (str): The input string to quote.

    outputs:
        str: The quoted string.
    '''
    s = (s or "").strip()
    return '"' + s.replace('"', r'\"') + '"' if s else ""

def _collect_names(event: Dict[str, Any]) -> List[str]:
    '''
    Collects names from the event dictionary. Includes candidate, preferred names
    and aliases. Ensures that names are strings with no extra whitespace, and are unique.

    inputs:
        event (Dict[str, Any]): The event dictionary containing name information.

    outputs:
        List[str]: A list of collected names.
    '''
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
    '''
    Sends a request to the ADS API with the specified query and returns the response.

    inputs:
        query (str): The ADS query string.
        token (str): The ADS API token.
        rows (int): The number of rows to return (default: 2000).

    outputs:
        List[Dict[str, Any]]: The list of documents returned by the ADS API.
    '''
    
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
        print(f"LINK FOUND: {label} ({url})")
        return label, url
    if isinstance(ld, str):
        return "", ld.strip()
    return "", ""

def _collect_links(doc: Dict[str, Any]) -> List[Tuple[str, str]]:
    # raw = doc.get("links_data") or doc.get("link")
    # raw = doc.get("links_data")
    raw = doc.get("data")
    if not raw: return []
    if isinstance(raw, (list, tuple)): return [_as_label_url(x) for x in raw]
    if isinstance(raw, dict) or isinstance(raw, str): return [_as_label_url(raw)]
    return []

def evaluate_open_access(doc: Dict[str, Any]) -> Tuple[bool, Optional[str], str]:
    """
    Determines if a document is open access and returns relevant information.

    inputs:
        doc (Dict[str, Any]): The document dictionary to evaluate.

    outputs:
        Tuple[bool, Optional[str], str]: A tuple containing:
            - is_open_access (bool): Whether the document is open access.
            - best_free_url (Optional[str]): The best available free URL.
            - reason (str): The reason for the open access status.
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
def _to_slim_with_resolver(doc: Dict[str, Any], token: str, ctx: Dict[str,int]) -> Dict[str, Any]:
    '''
    Transforms the input document into a slimmed-down version with resolver information.
    Adds open access information, as well as an OA URL (if available). Adds other metadata 
    information used to determine if source is good for harvesting.

    inputs:
        doc (Dict[str, Any]): The document dictionary to evaluate.
        token (str): The ADS API token.
        ctx (Dict[str, int]): The context dictionary containing request metadata.

    outputs:
        Dict[str, Any]: The slimmed-down document with resolver information.
    '''
    
    
    raw_bibstem = doc.get("bibstem")
    bibstem = raw_bibstem[0] if isinstance(raw_bibstem, list) and raw_bibstem else raw_bibstem
    authors = doc.get("author") or []
    has_abs = bool((doc.get("abstract") or "").strip())
    props = {str(p).lower() for p in (doc.get("property") or [])}
    doctype = (doc.get("doctype") or "").lower()
    bib = doc.get("bibcode")
    data = doc.get("data") or []

    # Defaults
    is_oa = False
    oa_url = None
    oa_reason = "none"

    # 0) circulars are public HTML
    if doctype == "circular" and bib and bibstem in {"ATel"}:
        is_oa, oa_url, oa_reason = True, _atel_link(bib), "circular_html"

    elif doctype == "circular":
        is_oa, oa_url, oa_reason = True, _ads_abs_url(bib), "circular_html"

    # 1) arXiv present → synthesize PDF (no resolver call needed)
    if not is_oa:
        aid = _first_arxiv_id(doc.get("identifier"))
        if aid:
            is_oa, oa_url, oa_reason = True, f"https://arxiv.org/pdf/{aid}.pdf", "arxiv"

    # 2) property flags indicate OA → try resolver for publisher URL
    if not is_oa and ({"openaccess","pub_openaccess","eprint_openaccess","ads_openaccess"} & props):
        if ctx["resolver_calls"] < RESOLVER_MAX_CALLS or RESOLVER_MODE == "all":
            url, reason = _resolve_open_access_via_resolver(bib)
            # print(bib, url, reason)
            ctx["resolver_calls"] += 1
            if url:
                is_oa, oa_url, oa_reason = True, url, "publisher_oa" if reason=="publisher" else reason
            else:
                is_oa, oa_url, oa_reason = True, None, "property_only"

    # 3) fallback: if allowed, try resolver for ADS PDFs/scans even w/o flags
    if not is_oa and RESOLVER_MODE == "all" and ctx["resolver_calls"] < RESOLVER_MAX_CALLS:
        url, reason = _resolve_open_access_via_resolver(bib)
        ctx["resolver_calls"] += 1
        if url in (None, ""):
            pass
        elif reason in {"ads","arxiv"}:
            is_oa, oa_url, oa_reason = True, url, reason
        elif reason == "publisher":
            # don’t claim OA unless props hinted; keep as non-OA
            pass

    return {
        "bibcode": bib,
        "bibstem": bibstem,
        "doctype": doc.get("doctype"),
        "date": doc.get("date"),
        "entry_date": doc.get("entry_date"),
        "authors_count": len(authors),
        "has_abstract": has_abs,
        "has_data_links": bool(doc.get("data") or []),
        "is_open_access": is_oa,
        "open_access_url": oa_url,
        "oa_reason": oa_reason,
        "has_data": bool(data),
        "data": data,
        "has_arxiv_id": _first_arxiv_id(doc.get("identifier")) is not None,
    }


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Upstream status!=OK"}

    token = _get_ads_token()
    if not token:
        raise RuntimeError("ADS token not configured (set ADS_TOKEN or ADS_DEV_KEY).")

    # fetch docs_raw via Search API (same as before)
    docs_raw = _ads_request(_build_ads_query(_collect_names(event)), token)

    # Build SLIM records with resolver-based OA
    ctx = {"resolver_calls": 0}
    # Build SLIM records for the payload
    records_slim = [_to_slim_with_resolver(d, token, ctx) for d in docs_raw]
    names = _collect_names(event)
    query = _build_ads_query(names)

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