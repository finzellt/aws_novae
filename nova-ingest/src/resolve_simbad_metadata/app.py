# app.py
"""
Lambda: resolve_simbad_metadata
- Input event: {"candidate_name": "<string>"}
- Output on success:
  {
    "status": "OK",
    "candidate_name": "...",
    "preferred_name": "<SIMBAD main id>",
    "name_norm": "<normalized>",
    "coords": {"ra_deg": <float>, "dec_deg": <float>},
    "object_types": ["<otype>"],
    "aliases": ["<main id>", "<alias 1>", ...],
    "simbad": {"main_id": "<...>", "raw_ids": ["<...>", ...]}
  }
- Output if not found: {"status": "NOT_FOUND", "candidate_name": "..."}
Raise exceptions for transient/system errors so Step Functions can retry.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

import astropy
import astroquery
from astroquery.simbad import Simbad
from astropy.table import Table

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configure a dedicated Simbad client (instance-local fields)
_simbad = Simbad()
# Degrees for easy downstream math
_simbad.add_votable_fields("ra(d)", "dec(d)", "otype")
# Tweak row limit just in case; query_object returns a single best match.
_simbad.ROW_LIMIT = 1

# Optional: network timeout (seconds)
try:
    from astroquery.utils.tap.core import TapPlus  # noqa: F401
    _simbad.TIMEOUT = int(os.getenv("SIMBAD_TIMEOUT_SEC", "15"))
except Exception:
    pass


def normalize_name(s: str) -> str:
    """Lowercase alnum-only for idempotent keys (e.g., DynamoDB/S3 markers)."""
    return re.sub(r"[^A-Za-z0-9]+", "", s).lower()


def _table_cell(row: Any, name: str) -> Optional[str]:
    # Safely extract a string cell from an Astropy row, handling masked values.
    if name not in row.colnames:
        return None
    val = row[name]
    if hasattr(val, "mask") and getattr(val, "mask", False):
        return None
    v = str(val).strip()
    return v if v and v.lower() != "nan" else None


def resolve_simbad(candidate_name: str) -> Dict[str, Any]:
    """Core resolver logic (pure function; easy to unit test)."""
    name = (candidate_name or "").strip()
    if not name:
        raise ValueError("candidate_name is required")

    logger.info("Querying SIMBAD: %s", name)
    tbl: Optional[Table] = _simbad.query_object(name)

    if tbl is None or len(tbl) == 0:
        logger.info("SIMBAD not found: %s", name)
        return {"status": "NOT_FOUND", "candidate_name": name}

    row = tbl[0]

    main_id = _table_cell(row, "MAIN_ID") or name
    # logger.warning(f"SIMBAD row: {row}")
    # logger.warning(f"Atropy version: {astropy.__version__}")
    # logger.warning(f"Astroquery version: {astroquery.__version__}")
    ra_s = _table_cell(row, "RA_d")
    dec_s = _table_cell(row, "DEC_d")
    if ra_s is None or dec_s is None:
        # Treat as transient/system failure so Step Functions retries
        raise RuntimeError("SIMBAD returned row without RA/Dec in degrees")

    try:
        ra_deg = float(ra_s)
        dec_deg = float(dec_s)
    except Exception as e:
        raise RuntimeError(f"Invalid RA/Dec values from SIMBAD: {ra_s}, {dec_s}") from e

    otype = _table_cell(row, "OTYPE")
    object_types: List[str] = [otype] if otype else []

    # Get aliases (second call)
    ids_tbl: Optional[Table] = _simbad.query_objectids(name)

    raw_ids: List[str] = []
    if ids_tbl is not None and "ID" in ids_tbl.colnames:
        for rec in ids_tbl:
            v = str(rec["ID"]).strip()
            if v:
                raw_ids.append(v)

    # Dedup & prefer main_id first, then all other aliases, then original input
    aliases: List[str] = []
    seen = set()

    def add_alias(a: Optional[str]):
        if not a:
            return
        a_norm = " ".join(a.split())
        if a_norm and a_norm not in seen:
            aliases.append(a_norm)
            seen.add(a_norm)

    add_alias(main_id)
    for a in raw_ids:
        add_alias(a)
    add_alias(name)

    result: Dict[str, Any] = {
        "status": "OK",
        "candidate_name": name,
        "preferred_name": main_id,
        "name_norm": normalize_name(main_id),
        "coords": {"ra_deg": ra_deg, "dec_deg": dec_deg},
        "object_types": object_types,
        "aliases": aliases,
        "simbad": {"main_id": main_id, "raw_ids": raw_ids},
    }
    return result


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entrypoint.
    Event example: {"candidate_name": "V606 Aql"}
    """
    try:
        candidate_name = event.get("candidate_name")
        return resolve_simbad(candidate_name)
    except ValueError as ve:
        # Bad input: let it surface as a 4xx-style failure (no retry)
        logger.exception("Bad input")
        return {"status": "BAD_REQUEST", "error": str(ve)}
    except Exception as e:
        # System/transient errors should throw to enable SFN retries
        logger.exception("Unhandled exception in resolve_simbad_metadata")
        raise
