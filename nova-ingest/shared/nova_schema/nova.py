from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic import AnyUrl
from typing import Optional, List
from datetime import date, datetime, timezone
# import from sibling module
from .consts import BANDS, IAU_CODES

class Nova(BaseModel):
    model_config = {
        "extra": "forbid",          # reject unknown fields
        "validate_assignment": True # re-validate on attribute set
    }
    nova_id: int = Field(..., description="Stable unique identifier")
    primary_name: str = Field(..., description="Most frequently used name in the literature.")
    name_norm: str = Field(..., description="Normalized name for matching (lowercase ASCII, no spaces).")
    data_table_id: Optional[int] = Field(None, description="ID linking to the raw ingest record or source table.")

    ra_angle: float = Field(..., description="RA [deg] ICRS J2000")
    dec_angle: float = Field(..., description="Dec [deg] ICRS J2000")
    gal_coords_l: float = Field(..., description="Galactic l [deg]")
    gal_coords_b: float = Field(..., description="Galactic b [deg]")

    host_gal: Optional[str] = Field("MW", description="Host Galaxy of the nova")
    host_gal_confidence: Optional[float] = Field(None, description="Level of confidence in assigned host galaxy")
    first_observed: Optional[date] = Field(None, description="Date of discovery")
    obs_year: Optional[int] = Field(None, description="Year in which nova was discovered")
    constellation: str = Field(..., description="IAU 3-letter code")

    max_mag: Optional[float] = Field(None, description="Maximum/peak magnitude")
    max_mag_band: Optional[str] = Field(None, min_length=1, max_length=5,
                                        description="Photometric band of peak magnitude (e.g., V, g, r).")
    max_mag_date: Optional[date] = Field(None, description="Date fo maximum magnitude")
    max_mag_source: Optional[str] = Field(None, description="Source for maximum magnitude")

    obj_types: Optional[List[str]] = Field(None, description="List of Simbad Object Types")
    bib_sources: Optional[List[str]] = Field(None, description="List of bibcode biblographic sources")
    ads_snap_uri: Optional[AnyUrl] = Field(None, description="URI location of data from ADS query")
    simbad_query: Optional[str] = Field(None, description="Query used to get metadata from Simbad")
    aliases: Optional[List[str]] = Field(None, description="Other names used for the nova")

    ingest_source: str = "unknown"
    ingest_run_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When entry was first made")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When entry was last updated")

    @field_validator("ra_angle", "gal_coords_l")
    @classmethod
    def _wrap_0_360(cls, v):
        if v is not None and not (0 <= v < 360):
            raise ValueError("value must be in [0, 360)")
        return v

    @field_validator("dec_angle", "gal_coords_b")
    @classmethod
    def _wrap_lat(cls, v):
        if v is not None and not (-90 <= v <= 90):
            raise ValueError("value must be in [-90, 90]")
        return v

    @field_validator("max_mag_band")
    @classmethod
    def _band_ok(cls, v):
        if v is None:
            return v
        if v not in BANDS:
            raise ValueError(f"max_mag_band must be one of {sorted(BANDS)}")
        return v

    @field_validator("constellation")
    @classmethod
    def _iau_ok(cls, v):
        if v is not None and v not in IAU_CODES:
            raise ValueError("Invalid IAU constellation code")
        return v

    @field_validator("aliases", "obj_types", "bib_sources")
    @classmethod
    def _dedupe_lists(cls, v):
        if v is None:
            return v
        seen, out = set(), []
        for x in v:
            if x not in seen:
                seen.add(x); out.append(x)
        return out

    @field_validator("first_observed", "max_mag_date")
    @classmethod
    def _realistic_date(cls, v):
        if v is None:
            return v
        from datetime import date as _date
        if not (_date(1750, 1, 1) <= v <= _date.today()):
            raise ValueError("Date must be between 1750-01-01 and today")
        return v

    @model_validator(mode="after")
    def _derive_obs_year(self):
        if self.first_observed and not self.obs_year:
            self.obs_year = self.first_observed.year
        return self

def nova_json_schema() -> dict:
    return Nova.model_json_schema(
        title="Nova",
        description="Canonical nova record",
        ref_template="#/components/schemas/{model}"
    )
