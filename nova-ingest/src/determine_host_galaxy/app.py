# app.py
"""
Lambda: determine_host_galaxy
Input (from previous step):
{
  "status": "OK",
  "candidate_name": "...",
  "preferred_name": "...",
  "aliases": ["...", ...],
  "coords": {"ra_deg": <float>, "dec_deg": <float>},
  ... (other fields)
}

Output (adds):
{
  "host_galaxy": "MW" | "<Galaxy Name>" | ["MW", "<Galaxy Name>"],
  "external_galaxy_confidence": "CONFIRMED" | "PROBABLE" | "POSSIBLE" | "MW",
  "nearest_galaxy": {"name": "<Galaxy Name>", "distance_deg": <float>, "ra_deg": <float>, "dec_deg": <float>},
  "host_method": "name_match" | "positional" | "none"
}
"""

from __future__ import annotations

import csv
import io
import os
import pathlib
from typing import Any, Dict, List, Optional, Tuple
from nova_schema.mappings import load_canonical, dump_canonical, merge_updates  # shared helpers
from pydantic import ValidationError

# import boto3
import os, boto3
# _s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL"))  # None in AWS, LocalStack URL locally
_endpoint = os.getenv("S3_ENDPOINT_URL")
_s3 = boto3.client("s3", **({"endpoint_url": _endpoint} if _endpoint else {}))

# --- Make astropy safe on Lambda (/tmp only) BEFORE import ---
def _bootstrap_astropy(base="/tmp"):
    os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/astropy/config")
    os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/astropy/cache")
    os.environ.setdefault("ASTROQUERY_CACHE_DIR", f"{base}/astroquery")
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

from astropy.coordinates import SkyCoord
import astropy.units as u
import logging

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# --- Config via env vars ---
GALAXY_LIST_URI = os.getenv(
    "GALAXY_LIST_URI",
    "s3://nova-data-bucket-finzell/reference/nearby_galaxies.csv",
)
CONFIRM_WITHIN_DEG = float(os.getenv("CONFIRM_WITHIN_DEG", "2.0"))     # name check gate
PROBABLE_WITHIN_DEG = float(os.getenv("PROBABLE_WITHIN_DEG", "0.5"))   # positional strong match

_s3 = boto3.client("s3")
_cached_galaxies: Optional[List[Tuple[str, float, float]]] = None  # (name, ra_deg, dec_deg)


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    _, _, rest = uri.partition("s3://")
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def _load_galaxies() -> List[Tuple[str, float, float]]:
    """
    Returns a list of (name, ra_deg, dec_deg). Cached across warm invocations.
    CSV columns: Galaxy, RA_deg, DEC_deg
    """
    global _cached_galaxies
    if _cached_galaxies is not None:
        return _cached_galaxies

    bucket, key = _parse_s3_uri(GALAXY_LIST_URI)
    obj = _s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    text = body.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows: List[Tuple[str, float, float]] = []
    for r in reader:
        try:
            name = (r.get("Primary_Name") or "").strip()
            ra = float(r.get("RA_deg"))
            dec = float(r.get("DEC_deg"))
        except Exception:
            continue
        if name:
            rows.append((name, ra, dec))
    if not rows:
        raise RuntimeError("Galaxy list is empty or malformed.")

    _cached_galaxies = rows
    return rows


# def _normalize(s: str) -> str:
#     return " ".join((s or "").lower().split())
def _normalize(s: str) -> str:
    return "".join((s or "").lower().split())

def _aliases_as_text(event: Dict[str, Any]) -> str:
    parts: List[str] = []
    for k in ("candidate_name", "preferred_name"):
        v = event.get(k)
        if isinstance(v, str):
            parts.append(v)
    aliases = event["aliases"] or []
    if isinstance(aliases, list):
        parts.extend([a for a in aliases if isinstance(a, str)])
    return " | ".join(parts)


def _nearest_galaxy(ra_deg: float, dec_deg: float) -> Tuple[Tuple[str, float, float], float]:
    """Return ((name, ra, dec), separation_deg) for the nearest galaxy."""
    target = SkyCoord(ra_deg * u.deg, dec_deg * u.deg, frame="icrs")
    best: Optional[Tuple[str, float, float]] = None
    best_sep: Optional[float] = None

    for name, gra, gdec in _load_galaxies():
        gc = SkyCoord(gra * u.deg, gdec * u.deg, frame="icrs")
        sep = target.separation(gc).deg
        if best_sep is None or sep < best_sep:
            best_sep = sep
            best = (name, gra, gdec)

    if best is None or best_sep is None:
        raise RuntimeError("Failed to compute nearest galaxy.")
    return best, best_sep


def _update_canonical(event: Dict[str, Any], out: Dict[str,any]):
    canonical_prev: Dict[str, Any] = event.get("canonical") or {}
    # 1) Load previous canonical (re-validates it)
    nova = load_canonical(canonical_prev)

    # 2) Map step-specific outputs -> schema fields
    host = out.get("host_galaxy")
    confidence = out.get("external_galaxy_confidence")

    updates = {}
    if host not in (None, ""):
        updates["host_gal"] = host
    if confidence is not None:
        updates["host_gal_confidence"] = confidence

    # 3) Apply updates and re-validate
    try:
        nova = merge_updates(nova, updates)     # overrides existing values; use fill_missing if you prefer add-only
    except ValidationError as e:
        # Log helpful details, then re-raise so Step Functions can retry/fail visibly
        logger.error("Nova validation failed after host-galaxy updates: %s", e)
        for err in e.errors():
            logger.error("field=%s msg=%s input=%s", ".".join(map(str, err.get("loc",[]))), err.get("msg"), err.get("input"))
        raise

    # 4) Serialize back for the next state
    return {"canonical": dump_canonical(nova)}


def determine_host(event: Dict[str, Any]) -> Dict[str, Any]:
    # Validate input
    if not isinstance(event, dict) or event.get("status") != "OK":
        return {"status": "BAD_REQUEST", "reason": "Upstream did not return status=OK"}

    coords = event.get("coords") or {}
    try:
        ra = float(coords["ra_deg"])
        dec = float(coords["dec_deg"])
    except Exception:
        return {"status": "BAD_COORDS", "reason": f"Invalid coords: {coords}"}

    # Find nearest galaxy by angular separation
    (gname, gra, gdec), sep_deg = _nearest_galaxy(ra, dec)

    # Name/alias check
    names_text = _normalize(_aliases_as_text(event))
    gnorm = _normalize(gname)
    name_contains = (gnorm in names_text) if (gnorm and names_text) else False

    # Decide confidence per your rules
    if sep_deg <= CONFIRM_WITHIN_DEG and name_contains:
        host = gname
        confidence = "CONFIRMED"
        method = "name_match"
    elif sep_deg <= PROBABLE_WITHIN_DEG:
        host = gname
        confidence = "PROBABLE"
        method = "positional"
    elif PROBABLE_WITHIN_DEG <= sep_deg <= CONFIRM_WITHIN_DEG:
        host = ["MW", gname]
        confidence = "POSSIBLE"
        method = "positional"
    else:
        host = "MW"
        confidence = "MW"
        method = "none"

    out = dict(event)  # shallow copy
    out["nearest_galaxy"] = {
        "name": gname,
        "distance_deg": round(sep_deg, 6),
        "ra_deg": gra,
        "dec_deg": gdec,
    }
    out["host_galaxy"] = host
    out["external_galaxy_confidence"] = confidence
    out["host_method"] = method

    out["canonical"] = _update_canonical(event,out)
    return out


def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    try:
        return determine_host(event)
    except Exception:
        # surface errors to Step Functions for retry/visibility
        raise
