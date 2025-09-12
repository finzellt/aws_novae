from __future__ import annotations
from typing import Any, Callable, Dict, Tuple
from pydantic import ValidationError
from nova_schema.models import Nova

# Map spec: schema_field -> ("path.in.payload", transform | None)
# Supports dotted paths and callables.
MapSpec = Dict[str, Tuple[str, Callable[[Any], Any] | None]]

SIMBAD_MAP: MapSpec = {
    "nova_id": ("id", int),
    "primary_name": ("main_id", None),
    "name_norm": ("main_id", lambda s: s.lower().strip() if s else None),
    "ra_angle": ("ra_deg", float),
    "dec_angle": ("dec_deg", float),
    "first_observed": ("first_obs_date", None),       # ISO date string ok; Nova will validate
    "constellation": ("constellation_code", None),
    "aliases": ("aliases", None),
}

# shared/nova_schema/mapping.py
from typing import Dict, Any
from nova_schema.models import Nova

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
