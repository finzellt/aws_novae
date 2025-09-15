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
from nova_schema.mapping.nova_mapping import load_canonical, dump_canonical, merge_updates  # shared helpers
from pydantic import ValidationError
from nova_schema.nova import Nova  # your Pydantic model

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
CONFIRM_WITHIN_DEG = float(os.getenv("CONFIRM_WITHIN_DEG", "1.0"))     # name check gate
PROBABLE_WITHIN_DEG = float(os.getenv("PROBABLE_WITHIN_DEG", "0.25"))   # positional strong match

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

def _get_nearest_galaxy_score(
    ra_deg: float,
    dec_deg: float,
    names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
    """
    Score the nearest galaxy to (ra_deg, dec_deg).
    If names is provided, check if any match the nova's normalized name.
    Returns dict with keys:
      - host: str or [str, str] (galaxy name or ["MW", "Galaxy Name"])
      - confidence: "CONFIRMED" | "PROBABLE" | "POSSIBLE" | "MW"
      - method: "name_match" | "positional" | "none"
      - separation_deg: float
    """

    (gname, gra, gdec), sep_deg = _nearest_galaxy(ra_deg, dec_deg)

    gnorm = " ".join(gname.split()).strip().lower() if gname else None
    names_text = " ".join(names).lower() if names else ""
    name_contains = ((gnorm in names_text) or (gnorm in gname)) if (gnorm and names_text) else False

    # Decide confidence per your rules
    if name_contains:
        host = gname
        confidence = 1
        # method = "name_match"
    elif sep_deg <= CONFIRM_WITHIN_DEG:
        host = gname
        confidence = 0.75
        # method = "positional"
    elif sep_deg <= PROBABLE_WITHIN_DEG:
        host = gname
        confidence = 0.5
        # method = "positional"
    else:
        host = "MW"
        confidence = 1.0
        # method = "none"

    return host, confidence


def compute_host_galaxy(canonical: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    # Validate input

    ra_deg: Optional[float] = canonical.get("ra_angle")
    dec_deg: Optional[float] = canonical.get("dec_angle")
    aliases = canonical.get("aliases") or []  # list[str]
    primary_name = canonical.get("primary_name")

    # If you previously normalized names elsewhere, keep that:
    norm = lambda s: " ".join((s or "").split()).strip().lower()

    # 1) (example) name-based hints — keep your rules here
    names = {norm(primary_name)} | {norm(a) for a in aliases if a}
    # If your old code checked for e.g. "m31", "andromeda", etc., keep that logic:
    # if any("m31" in n or "andromeda" in n for n in names):
    #     return "external", 0.95

    # 2) coordinate-driven heuristic via your existing helper
    host_from_nearest: Optional[str] = None
    conf_from_nearest: Optional[float] = None
    if ra_deg is not None and dec_deg is not None:
        try:
            # ⬇️ Use your existing nearest-galaxy routine exactly as before
            # It might return (host_label, distance) — adapt mapping as needed
            host_from_nearest, conf_from_nearest = _get_nearest_galaxy_score(ra_deg, dec_deg, names)  # type: ignore[name-defined]
            # Example: map score/distance to a confidence in [0,1]; keep your logic
            # conf_from_nearest = max(0.0, min(1.0, some_function_of(score)))
        except Exception:
            logger.exception("nearest-galaxy lookup failed; continuing")
    
    # 3) Combine your signals into a final decision — KEEP your existing thresholds/logic
    # Below is a minimal, conservative fallback; replace with your code.
    if host_from_nearest:
        return host_from_nearest, conf_from_nearest

    # Fallback: unknown
    return None, None

def handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    Expects:
      { "canonical": { ... Nova dict ... } }
    Returns:
      { "canonical": { ... updated Nova dict ... } }
    """
    canonical_in: Dict[str, Any] = event.get("canonical") or {}
    if not canonical_in:
        logger.error("Missing 'canonical' in event")
        return {"status": "BAD_REQUEST", "error": "Missing 'canonical' in event"}

    # 1) Load & validate incoming canonical
    try:
        nova = Nova(**canonical_in)
    except ValidationError as e:
        logger.exception("Incoming canonical failed validation")
        return {"status": "BAD_CANONICAL", "errors": e.errors()}

    # 2) Compute host galaxy from canonical
    host, conf = compute_host_galaxy(canonical_in)

    # 3) If nothing to update, pass through
    if host is None and conf is None:
        return {"canonical": nova.model_dump(mode="json")}

    updates: Dict[str, Any] = {}
    if host is not None:
        updates["host_gal"] = host
    if conf is not None:
        updates["host_gal_confidence"] = float(conf)

    # 4) Apply updates and re-validate
    try:
        nova_updated = nova.model_copy(update=updates, deep=True)
        # re-construct to trigger validators (belt & suspenders)
        nova_updated = Nova(**nova_updated.model_dump())
    except ValidationError as e:
        logger.exception("Validation failed after host-galaxy updates")
        return {"status": "UPDATE_FAILED", "errors": e.errors()}

    # 5) Return updated canonical
    return {"canonical": nova_updated.model_dump(mode="json")}