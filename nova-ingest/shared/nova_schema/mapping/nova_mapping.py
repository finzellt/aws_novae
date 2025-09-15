from __future__ import annotations
from typing import Any, Callable, Dict, Tuple, Optional, List
from datetime import datetime, timezone
from pydantic import ValidationError
from nova_schema.nova import Nova
from nova_schema.identity import nova_id_from_fields
from astropy.coordinates import SkyCoord
import astropy.units as u

# Map spec: schema_field -> ("path.in.payload", transform | None)
# Supports dotted paths and callables.
MapSpec = Dict[str, Tuple[str, Callable[[Any], Any] | None]]


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        v = float(x)
        return None if (v != v or v in (float("inf"), float("-inf"))) else v
    except Exception:
        return None

def _norm_name(s: Optional[str]) -> Optional[str]:
    if not s: return None
    return " ".join(s.split()).strip().lower()

def _split_pipe_list(s: Optional[str]) -> List[str]:
    if not s: return []
    parts = [p.strip() for p in s.split("|")]
    seen, out = set(), []
    for p in parts:
        if p and p not in seen:
            seen.add(p); out.append(p)
    return out

def from_simbad(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map raw SIMBAD columns -> canonical Nova dict (schema field names)."""
    main_id = raw.get("MAIN_ID") or ""
    if main_id.startswith("V* "):
        main_id = main_id[3:]
    name_norm = _norm_name(main_id)
    ra = _as_float(raw.get("RA_d"))
    dec = _as_float(raw.get("DEC_d"))
    aliases = _split_pipe_list(raw.get("IDS"))
    # OTYPES can be pipe/space/semicolon separated; treat like IDS if needed
    otypes = _split_pipe_list(raw.get("OTYPES")) or []

    gal_l = gal_b = const = None
    if ra is not None and dec is not None:
        c = SkyCoord(ra=ra*u.deg, dec=dec*u.deg, frame="icrs")
        gal = c.galactic
        gal_l = float(gal.l.deg)
        gal_b = float(gal.b.deg)
        const  = c.get_constellation(short_name=True)
    
    mapped: Dict[str, Any] = {
        "primary_name": main_id or None,
        "name_norm": name_norm,
        "ra_angle": ra,
        "dec_angle": dec,
        "gal_coords_l": gal_l,
        "gal_coords_b": gal_b,
        "constellation": const,
        "aliases": ([main_id] + aliases) if main_id else aliases,
        "obj_types": otypes or None,
        "ingest_source": "simbad",
    }

    # Only assign an ID if one is not already present from upstream
    if "nova_id" not in raw and "nova_id" not in mapped:
        nova_id, identity, guid = nova_id_from_fields(
            name_norm=name_norm, ra_deg=ra, dec_deg=dec
        )
        mapped["nova_id"] = nova_id

    return mapped



def load_canonical(d: Dict[str, Any]) -> Nova:
    """Deserialize + validate a canonical dict into the Nova model."""
    return Nova(**d)

def dump_canonical(n: Nova) -> Dict[str, Any]:
    """Serialize to JSON-safe dict for Step Functions / logs."""
    return n.model_dump(mode="json")

def merge_updates(n: Nova, updates: Dict[str, Any]) -> Nova:
    """
    Return a new Nova with updates applied. Prefer explicit overrides in 'updates'.
    (Use this when the new step wants to set/override certain fields.)
    """
    return n.model_copy(update=updates, deep=True)

def fill_missing(n: Nova, additions: Dict[str, Any]) -> Nova:
    """
    Only fill fields that are currently None/empty. (Use this for “add-only” steps.)
    """
    current = n.model_dump()
    take = {}
    for k, v in additions.items():
        if v in (None, "", [], {}):
            continue
        if current.get(k) in (None, "", [], {}):
            take[k] = v
    return n.model_copy(update=take, deep=True)
