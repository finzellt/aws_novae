# app.py
"""
Lambda: validate_nova_and_coords
Input (from resolve_simbad_metadata):
{
  "status": "OK",
  "candidate_name": "...",
  "preferred_name": "...",
  "name_norm": "...",
  "coords": {"ra_deg": <float>, "dec_deg": <float>},
  "object_types": ["Nova", "Cataclysmic Variable", ...],
  "aliases": [...],
  ...
}

Output on success: input + the following fields
{
  "validation": {
    "nova_valid": true,
    "coords_valid": true
  },
  "constellation": {
    "short": "And",         # e.g., Andromeda
    "full": "Andromeda"
  },
  "coords_galactic": {
    "l_deg": <float>,       # 0..360
    "b_deg": <float>        # -90..90
  }
}

Failure (business-rule):
  {"status":"NOT_NOVA", "reason":"..."}  OR
  {"status":"BAD_COORDS", "reason":"..."}

System/transient errors are raised so Step Functions can retry.
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


import logging
import math
import os
from typing import Any, Dict, Iterable, Optional

# from common.astropy_bootstrap import setup_astropy_cache
# setup_astropy_cache()
from astropy.coordinates import SkyCoord, get_constellation
import astropy.units as u

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Acceptable labels for SIMBAD OTYPE that indicate a nova.
# We treat anything containing "nova" (case-insensitive) as nova,
# EXCEPT strings containing "supernova".
# You can harden this with an explicit allow-list if you prefer.
_EXPLICIT_NOVA_ALIASES = {
    "No?", "No*"
}


def _is_nova(object_types: Optional[Iterable[str]]) -> bool:
    if not object_types:
        return False
    for raw in object_types:
        t = (raw or "").strip()
        if not t:
            continue
        if t in _EXPLICIT_NOVA_ALIASES:
            return True
    return False


def _valid_coords(ra_deg: Any, dec_deg: Any) -> bool:
    try:
        ra = float(ra_deg)
        dec = float(dec_deg)
    except Exception:
        return False
    if not (math.isfinite(ra) and math.isfinite(dec)):
        return False
    if not (0.0 <= ra < 360.0):
        return False
    if not (-90.0 <= dec <= 90.0):
        return False
    return True


def validate_and_enrich(event: Dict[str, Any]) -> Dict[str, Any]:
    # Basic shape checks
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Upstream resolver did not return status=OK"}

    coords = event.get("coords") or {}
    ra_deg = coords.get("ra_deg")
    dec_deg = coords.get("dec_deg")
    if not _valid_coords(ra_deg, dec_deg):
        return {"status": "BAD_COORDS", "reason": f"Invalid RA/Dec: ra={ra_deg}, dec={dec_deg}"}

    obj_types = event.get("object_types", [])
    if not _is_nova(obj_types):
        return {"status": "NOT_NOVA", "reason": f"Object types do not indicate a nova: {obj_types}"}

    # Compute constellation (short and full) and Galactic coordinates
    c_icrs = SkyCoord(float(ra_deg) * u.deg, float(dec_deg) * u.deg, frame="icrs")
    const_short = get_constellation(c_icrs, short_name=True)
    const_full = get_constellation(c_icrs, short_name=False)

    l_deg = float(c_icrs.galactic.l.wrap_at(360 * u.deg).deg)
    b_deg = float(c_icrs.galactic.b.deg)

    # Enrich and return the same payload plus new fields
    out = dict(event)  # shallow copy
    out["validation"] = {"nova_valid": True, "coords_valid": True}
    out["constellation"] = {"short": const_short, "full": const_full}
    out["coords_galactic"] = {"l_deg": l_deg, "b_deg": b_deg}
    return out


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    try:
        return validate_and_enrich(event)
    except Exception:
        # Let Step Functions retry on unexpected failures
        logger.exception("Unhandled exception in validate_nova_and_coords")
        raise
