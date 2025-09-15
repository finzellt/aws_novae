# shared/nova_schema/identity.py
from __future__ import annotations
import uuid
from typing import Optional

# Stable namespace for all nova IDs in *your* system (don’t change once set).
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://your-org.example/nova")

def _round_coord(x: Optional[float], places: int = 6) -> Optional[str]:
    if x is None:
        return None
    # Gentle rounding; avoid "-0.0"
    r = round(float(x), places)
    r = 0.0 if r == -0.0 else r
    return f"{r:.{places}f}"

def build_identity_v1(
    *,
    name_norm: Optional[str],
    ra_deg: Optional[float],
    dec_deg: Optional[float],
    ) -> str:
    """
    Deterministic identity string. Freeze this recipe once in production.
    - Prefer normalized name when present (SIMBAD main_id normalized).
    - Fallback to coords (rounded) if name unknown.
    - No first_observed used (can be unknown in early steps).
    """
    name_part = (name_norm or "").strip().lower()
    ra_part   = _round_coord(ra_deg) or ""
    dec_part  = _round_coord(dec_deg) or ""
    # NOTE: keep order/fields/version stable forever
    return "|".join(["v1", name_part, ra_part, dec_part])

def uuid5_from_identity(identity: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, identity)

def u64_from_uuid(u: uuid.UUID) -> int:
    """Take the first 8 bytes big-endian for a 64-bit surrogate (unsigned)."""
    return int.from_bytes(u.bytes[:8], "big", signed=False)

def nova_id_from_fields(
    *, name_norm: Optional[str], ra_deg: Optional[float], dec_deg: Optional[float]
    ) -> tuple[int, str, str]:
    """
    Returns (nova_id_u64, identity_string, uuid_string).
    Raises if *all* identity inputs are missing.
    """
    if not name_norm and ra_deg is None and dec_deg is None:
        raise ValueError("Cannot derive nova_id: need name_norm or (ra, dec)")
    ident = build_identity_v1(name_norm=name_norm, ra_deg=ra_deg, dec_deg=dec_deg)
    guid = uuid5_from_identity(ident)
    return u64_from_uuid(guid), ident, str(guid)
