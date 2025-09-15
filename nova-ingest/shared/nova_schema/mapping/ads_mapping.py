# shared/nova_schema/mapping/ads_mapping.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from nova_schema.biblio import BiblioSource
# from nova_schema.harvest import HarvestCandidate  # if using Pydantic model
# import sys
# from pathlib import Path
# shared_path = Path(__file__).resolve().parent.parent.parent
# sys.path.append(str(shared_path))
# print("Added to PYTHONPATH:", shared_path)
# import importlib
# importlib.reload(nova_schema.biblio)

ADS_UI = "https://ui.adsabs.harvard.edu/abs/"

def merge_updates(b: BiblioSource, updates: Dict[str, Any]) -> BiblioSource:
    """
    Return a new Nova with updates applied. Prefer explicit overrides in 'updates'.
    (Use this when the new step wants to set/override certain fields.)
    """
    return b.model_copy(update=updates, deep=True)

def _as_list(x: Any) -> List[str]:
    if x is None: return []
    return x if isinstance(x, list) else [x]

def _collect_links(doc: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Extract (label, url) from ADS record.
    ADS often exposes a 'link' array: [{'title': 'ADS PDF', 'url': '...'}, ...]
    Be liberal in what you accept.
    """
    out: List[Tuple[str, str]] = []
    for link in _as_list(doc.get("link")):
        try:
            label = str(link.get("title") or link.get("label") or "").strip()
            url = str(link.get("url") or "").strip()
            if url:
                out.append((label.lower(), url))
        except Exception:
            continue
    # Some records also have 'identifier' (e.g., DOIs or arXiv) — you can add rules if needed.
    return out

def evaluate_open_access(doc: Dict[str, Any]) -> Tuple[bool, Optional[str], str]:
    """
    Your logic verbatim, slightly hardened. Returns (is_oa, best_url, reason).
    """
    props = {str(p).lower() for p in (doc.get("property") or [])}
    links = _collect_links(doc)

    # 1) arXiv PDF
    for label, url in links:
        u = (url or "").lower()
        if ("arxiv" in label or "arxiv.org" in u) and "pdf" in u:
            return True, url, "arxiv"

    # 2) ADS PDFs/scans
    for label, url in links:
        if "ads pdf" in label or "ads scanned" in label or "ads full text" in label:
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

def _ads_abs_url(bib: Optional[str]) -> Optional[str]:
    return f"{ADS_UI}{bib}" if bib else None

def _atel_link(bib: Optional[str]) -> Optional[str]:
    # Simple ATel helper; you may have a richer one:
    # ATel bibcodes look like "2004ATel....123....1S" — you can’t always form the direct ATel URL
    # so default to ADS abstract if unsure.
    return _ads_abs_url(bib)

def _is_nonempty_list(x: Any) -> bool:
    return isinstance(x, list) and len(x) > 0

def _priority_for(doctype: str, is_data: bool  = False) -> int:
    # Per-data entries will carry a single data item; overall entries may have a list or None
    # data_field = candidate.get("data")
    if is_data:  # per-data entry
        return 200
    if doctype in {"database","dataset", "catalog"}:
        return 150
    if doctype == "circular":
        return 75
    if doctype == "article":
        return 10
    return 0  # default low priority


def _doc_bibstem_bib(doc: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    bib = doc.get("bibcode")
    bibstem = doc.get("bibstem") or doc.get("bibstems")
    if isinstance(bibstem, list):  # ADS can return list
        bibstem = bibstem[0] if bibstem else None
    return bibstem, bib

def is_circular(doc: Dict[str, Any]) -> bool:
    doctype = (doc.get("doctype") or "").lower()
    bibstem, _ = _doc_bibstem_bib(doc)
    if doctype == "circular":
        return True
    if bibstem and bibstem in {"ATel", "CBET", "IAUC"}:
        return True
    return False

def map_ads_to_harvest(doc: Dict[str, Any], nova_id: str) -> Dict[str, Any]:
    """
    Minimal, harvest-focused mapping for one ADS doc.
    """
    bibstem, bib = _doc_bibstem_bib(doc)
    doctype = doc.get("doctype")
    is_oa, oa_url, oa_reason = evaluate_open_access(doc)

    # Circulars are public HTML
    if (doctype or "").lower() == "circular" and bib:
        if bibstem in {"ATel"}:
            is_oa, oa_url, oa_reason = True, _atel_link(bib), "circular_html"
        else:
            is_oa, oa_url, oa_reason = True, _ads_abs_url(bib), "circular_html"

    links = _collect_links(doc)

    data = _as_list(doc.get("data")) if doc.get("data") else []
    has_abstract = bool(doc.get("abstract"))
    status = "created"
    priority = _priority_for(doctype or "")
    author_count = doc.get("author_count", 0)
    # print(f"Authors count: {doc.get('author_count')}")
    # print(f"Author Try 2: {doc.get('authors', [])}")

    return {
        "nova_id": nova_id,
        "bibcode": bib,
        "bibstem": bibstem,
        "doctype": doctype,
        "is_open_access": bool(is_oa),
        "best_free_url": oa_url,
        "oa_reason": oa_reason,
        "author_count": author_count,
        "links": links,
        "ingest_source": "ads",
        "data": data,
        "priority": priority,
        "has_abstract": has_abstract,
        "status": status,
    }

def map_ads_response_to_harvest(resp: Dict[str, Any], nova_id: str) -> List[Dict[str, Any]]:
    docs = []
    if "response" in resp and isinstance(resp["response"], dict):
        docs = resp["response"].get("docs") or []
    elif "docs" in resp:
        docs = resp["docs"] or []

    out: List[Dict[str, Any]] = []
    for rec in docs:
        try:
            mapped_bib = map_ads_to_harvest(rec,nova_id)
            bib_source = BiblioSource(**mapped_bib)
            out.append(bib_source)
            # print("no issue")
            # new_entry = map_ads_to_harvest(rec,nova_id)
            # out.append(new_entry)
            if _is_nonempty_list(mapped_bib.get("data")):
                for data_item in mapped_bib["data"]:
                    if "simbad" not in data_item.lower():
                        updates = {}
                        updates["data"] = [data_item]
                        updates["doctype"] = "data"
                        updates["priority"] = _priority_for("data", True)
                        temp_entry = merge_updates(bib_source, updates)
                        bib_source = BiblioSource(**mapped_bib)
                        out.append(temp_entry)

            #             # data_entry = map_ads_to_harvest({**rec, "data": data_item})
            #         out.append(temp_entry)
        except Exception as e:
            print("Issue here:", e)
            continue
    return out
