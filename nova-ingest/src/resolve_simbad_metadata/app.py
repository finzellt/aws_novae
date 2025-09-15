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

import os, pathlib

def _bootstrap_astropy(base="/tmp"):
    os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/astropy/config")
    os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/astropy/cache")
    os.environ.setdefault("ASTROQUERY_CACHE_DIR", f"{base}/astroquery")
    # belt & suspenders: some code uses XDG or HOME
    os.environ.setdefault("XDG_CACHE_HOME", f"{base}/.cache")
    os.environ.setdefault("HOME", base)

    for p in (
        os.environ["ASTROPY_CONFIGDIR"],
        os.environ["ASTROPY_CACHE_DIR"],
        os.environ["ASTROQUERY_CACHE_DIR"],
        os.environ["XDG_CACHE_HOME"],
    ):
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)

_bootstrap_astropy()

import sys
print("ASTROPY_CONFIGDIR=", os.environ.get("ASTROPY_CONFIGDIR"))
print("ASTROPY_CACHE_DIR=", os.environ.get("ASTROPY_CACHE_DIR"))
print("ASTROQUERY_CACHE_DIR=", os.environ.get("ASTROQUERY_CACHE_DIR"))
print("HOME=", os.environ.get("HOME"))
print("sys.path=", sys.path[:3])


import json
import logging
import os
import re
from typing import Any, Dict, List, Optional,Iterable
from nova_schema.nova import Nova
from nova_schema.mapping.nova_mapping import from_simbad

# import astropy
# import astroquery
# from common.astropy_bootstrap import setup_astropy_cache
# setup_astropy_cache()

from astroquery.simbad import Simbad
from astropy.table import Table
from astropy.coordinates import SkyCoord, get_constellation
import astropy.units as u

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configure a dedicated Simbad client (instance-local fields)
_simbad = Simbad()
try:
    _simbad.cache_location = os.environ["ASTROQUERY_CACHE_DIR"]
except Exception:
    pass
# Degrees for easy downstream math
_simbad.add_votable_fields("ra(d)", "dec(d)", "otypes", "ids")
# Tweak row limit just in case; query_object returns a single best match.
_simbad.ROW_LIMIT = 1

# Optional: network timeout (seconds)
try:
    from astroquery.utils.tap.core import TapPlus  # noqa: F401
    _simbad.TIMEOUT = int(os.getenv("SIMBAD_TIMEOUT_SEC", "15"))
except Exception:
    pass

from pydantic import ValidationError
import json, logging, math, numpy as np

logger = logging.getLogger(__name__)
_EXPLICIT_NOVA_ALIASES = ["No?", "No*"]
# _EXPLICIT_NOVA_ALIASES = {
#     "No?", "No*"
# }
def is_nova(otypes: list[str]) -> bool:
    """Check if any of the object types match the explicit NOVA aliases."""
    # return any(otype in _EXPLICIT_NOVA_ALIASES for otype in otypes)
    return any(nova_alias in otypes for nova_alias in _EXPLICIT_NOVA_ALIASES)

def fetch_simbad_object(name: str) -> dict | None:
    """Pure fetch: return raw column values (strings/numbers) or None if not found."""
    tbl: Table | None = _simbad.query_object(name)
    if not tbl or len(tbl) == 0:
        return None
    row = tbl[0]
    # Minimal extraction: don't normalize; just make Python scalars
    def raw(col):
        v = row[col]
        try:
            # handle masked values
            if getattr(v, "mask", False):
                return None
        except Exception:
            pass
        return v.item() if hasattr(v, "item") else (str(v) if v is not None else None)

    return {
        "MAIN_ID": raw("MAIN_ID"),
        "RA_d": raw("RA_d"),
        "DEC_d": raw("DEC_d"),
        "OTYPES": raw("OTYPES"),
        "IDS": raw("IDS"),  # pipe-separated identifiers, if provided
    }

def handler(event, ctx):
    name = event["candidate_name"]
    raw = fetch_simbad_object(name)
    if not raw:
        return {"status": "NOT_FOUND", "candidate_name": name}
    if not is_nova(raw["OTYPES"]):
        logger.warning(f"Candidate {name} found in SIMBAD but not a nova: OTYPES={raw['OTYPES']}")
        return {"status": "NOT_A_NOVA", "candidate_name": name}

    mapped = from_simbad(raw)
    nova = Nova(**mapped)
    # if not is_nova(nova.obj_types):
    #     logger.warning(f"Candidate {name} found in SIMBAD but not a nova: OTYPES={nova.obj_types}")
    #     return {"status": "NOT_A_NOVA", "candidate_name": name}
    # nova = Nova(**{
    #     **mapped,
    #     # "ingest_run_id": event.get("ingest_run_id"),
    # })
    return {"status": "OK", "canonical": nova.model_dump(mode="json")}

# def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
#     """
#     AWS Lambda entrypoint.
#     Event example: {"candidate_name": "V606 Aql"}
#     """
#     try:
#         candidate_name = event.get("candidate_name")
#         return resolve_simbad(candidate_name)
#     except ValueError as ve:
#         # Bad input: let it surface as a 4xx-style failure (no retry)
#         logger.exception("Bad input")
#         return {"status": "BAD_REQUEST", "error": f"1 {str(ve)}"}
#     except Exception as e:
#         # System/transient errors should throw to enable SFN retries
#         logger.exception("Unhandled exception in resolve_simbad_metadata")
#         raise
