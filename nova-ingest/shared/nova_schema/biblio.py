from __future__ import annotations

from datetime import date, datetime, timezone
# from datetime import datetime as datetime
# import datetime
from typing import List, Optional, Sequence
from pydantic import BaseModel, HttpUrl, Field, computed_field, field_validator
from pydantic.config import ConfigDict
import hashlib
import logging
import re,json

logger = logging.getLogger(__name__)

ADS_BIBCODE_RE = re.compile(r"^\S{4}\S*$")  # (placeholder) keep loose unless you want strict ADS format


# ---------- Helpers ----------
def _first_arxiv_id(identifiers: Optional[Sequence[str]]) -> Optional[str]:
    """Return the first arXiv-like identifier, if any."""
    if not identifiers:
        return None
    for s in identifiers:
        if not s:
            continue
        t = s.strip()
        if "arxiv" in t.lower():
            return t
    return None

def _coerce_date(v, *, field_name: str) -> Optional[date]:
    """
    Accepts date, datetime, or ISO-like strings: 'YYYY', 'YYYY-MM', 'YYYY-MM-DD'.
    Pads partial dates to the first day where needed.
    """
    # print("Yes, I can print from here.")
    if v is None or v == "":
        return None

    try:
        if isinstance(v, date) and not isinstance(v, datetime):
            return v
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            if len(s) == 4:
                return date(int(s), 1, 1)
            if len(s) == 7:
                y, m = map(int, s.split("-"))
                return date(y, m, 1)
            return datetime.fromisoformat(s).date()
    except Exception as exc:
        logger.warning("Date parse failed for %s=%r: %s", field_name, v, exc)
        raise ValueError(
            f"{field_name}: unrecognized date format {v!r}. "
            "Expected 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', a date, or a datetime."
        ) from exc

    logger.warning("Date parse failed (unknown branch) for %s=%r", field_name, v)
    raise ValueError(
        f"{field_name}: unrecognized date format {v!r}. "
        "Expected 'YYYY', 'YYYY-MM', 'YYYY-MM-DD', a date, or a datetime."
    )

# ---------- Model ----------
class BiblioSource(BaseModel):
    """
    Bibliographic source record with explicit, documented fields.
    Add or edit descriptions inline with each Field(...) call below.
    """

    # --- Core identifiers & metadata ---
    nova_id: str = Field(..., description="Stable unique nova identifier.")
    bibcode: str = Field(..., description="Unique bibliographic code (ADS bibcode).")
    bibstem: str = Field(..., description="Journal or series short name (bibstem).")

    @computed_field(return_type=str, description="SHA-256 fingerprint of nova_id and bibcode")
    def fp(self) -> str:
        try:
            key = f"{(self.bibcode or '').lower()}|{(self.nova_id or '').lower()}".encode("utf-8")
            return hashlib.sha256(key).hexdigest()
        except Exception as e:
            logger.exception(f"Error occurred while generating fingerprint for {self.bibcode}; Error: {e}")
            return "ERROR"

    @computed_field(return_type=str, description="Primary key for DynamoDB (SNAP#...)")
    def pk(self) -> str:
        try:
            return f"SNAP#{self.candidate_id}"
            # return f"SNAP#{self.fp}"
        except Exception as e:
            logger.exception(f"Error occurred while generating primary key for {self.bibcode}; Error: {e}")
            return "ERROR"

    @computed_field(return_type=str, description="Secondary key for DynamoDB (NOVA#...#BIB#...)")
    def sk(self) -> str:
        try:
            return f"NOVA#{self.nova_id or 'UNKNOWN'}#BIB#{self.bibcode or 'UNKNOWN'}"
        except Exception as e:
            logger.exception(f"Error occurred while generating secondary key for {self.bibcode}; Error: {e}")
            return "ERROR"

    doctype: Optional[str] = Field(
        None,
        description="Document type (e.g., 'article', 'circular', 'database').",
    )

    # --- Dates ---
    date: Optional[datetime] = Field(
        None,
        description="Publication date. Accepts 'YYYY', 'YYYY-MM', or 'YYYY-MM-DD'.",
    )
    entry_date: Optional[datetime] = Field(
        None,
        description="Ingestion/entry date into the index or database.",
    )

    # --- Counts & simple flags ---
    author_count: int = Field(
        None,
        ge=0,
        description="Number of authors on the work.",
    )
    has_abstract: bool = Field(
        ...,
        description="True if an abstract is present.",
    )
    is_open_access: bool = Field(
        ...,
        description="True if the work is open access.",
    )

    # --- Open Access details ---
    open_access_url: Optional[HttpUrl] = Field(
        None,
        description="Canonical OA landing page or PDF URL, if available.",
    )
    oa_reason: Optional[str] = Field(
        None,
        description="Reason or route for OA (e.g., 'arXiv', 'publisher policy').",
    )

    # --- Data links/tags ---
    data: List[str] = Field(
        default_factory=list,
        description="List of associated data resource identifiers/links/tags.",
    )

    @computed_field(return_type=str)
    @property
    def candidate_id(self) -> str:
        data_id = ""
        try:
            if isinstance(self.data, list) and self.data:
                # print("Data ID found:")
                data_id = (
                    str(" ".join(self.data))
                    or str(self.data.get("name"))
                    or json.dumps(self.data, sort_keys=True, ensure_ascii=False)
                )
                # print("Data ID found:", data_id)
            raw = "|".join([
                self.bibcode or "",
                (self.doctype or ""),
                (str(self.open_access_url) if self.open_access_url else ""),
                data_id,
            ])
        except Exception as e:
            logger.exception(f"Error occurred while generating candidate ID for {self.bibcode}; Error: {e}")
            # print("Error occurred while generating candidate ID:", e)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    status: str = Field("created", description="Ingestion status.")
    priority: int = Field(0, ge=0, description="Ingestion priority.")

    updated_at: datetime = Field(default_factory= lambda: datetime.now(timezone.utc), description="Last updated timestamp.")

    @computed_field(return_type=str, description="Date of file creation")
    def created_at(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    @computed_field(return_type=str, description="GSI keys for DynamoDB")
    def gsi1_pk(self) -> str:
        return f"STATUS#{self.status}"
    
    @computed_field(return_type=str, description="GSI keys for DynamoDB")
    def gsi1_sk(self) -> str:
        return f"{self.priority:03d}|{self.pk}"
        # return f"{self.priority:03d}|{(self.date.isoformat() or '')[:10] or '0000-00-00'}|{self.pk}"

    @computed_field(return_type=str, description="GSI keys for DynamoDB")
    def gsi2_pk(self) -> str:
        return f"BIBLIO#{self.candidate_id or 'UNKNOWN'}"

    @computed_field(return_type=str, description="GSI keys for DynamoDB")
    def gsi2_sk(self) -> str:
        return f"{self.priority:03d}|{self.pk}|{self.updated_at.isoformat()}"

    # --- Derived/denormalized flags (stored) ---
    has_arxiv_id: bool = Field(
        False,
        description="True if any identifier appears to be an arXiv ID.",
    )

    # --- Model config ---
    model_config = ConfigDict(extra="ignore", populate_by_name=True, validate_assignment=True)

    # --- Validators ---
    @field_validator("date", "entry_date", mode="before")
    @classmethod
    def _parse_dates(cls, v):
        return _coerce_date(v, field_name="date/entry_date")
    
    @field_validator("bibcode", mode="before")
    @classmethod
    def _clean_bibcode(cls, v):
        if v is None:
            raise ValueError("bibcode is required.")
        s = str(v).strip()
        if not s:
            raise ValueError("bibcode cannot be empty or whitespace.")
        # keep pattern loose unless you want to enforce the exact ADS bibcode grammar
        if not ADS_BIBCODE_RE.match(s):
            logger.info("bibcode %r did not match ADS_BIBCODE_RE; keeping as-is", s)
        return s

    @field_validator("nova_id", mode="before")
    @classmethod
    def _clean_nova_id(cls, v):
        if v is None:
            raise ValueError("nova_id is required.")
        s = str(v).strip()
        if not s:
            raise ValueError("nova_id cannot be empty or whitespace.")
        return s

    @field_validator("date", mode="before")
    @classmethod
    def _parse_date_field(cls, v):
        return _coerce_date(v, field_name="date")

    @field_validator("entry_date", mode="before")
    @classmethod
    def _parse_entry_date_field(cls, v):
        return _coerce_date(v, field_name="entry_date")

    @field_validator("status", mode="before")
    @classmethod
    def _validate_status(cls, v):
        if v not in {"created", "queued", "processing", "completed", "failed"}:
            raise ValueError("Invalid status.")
        return v

    # --- Derived/computed (not stored) ---
    @computed_field(description="True if 'data' is non-empty.")
    def has_data(self) -> bool:
        return bool(self.data)

    # --- Factory ---
    @classmethod
    def from_doc(
        cls,
        *,
        doc: dict,
        bib: str,
        bibstem: str,
        has_abs: bool,
        is_oa: bool,
        oa_url: Optional[str] = None,
        oa_reason: Optional[str] = None,
        data: Optional[Sequence[str]] = None,
        author_count: Optional[int] = None,
        candidate_id: Optional[str] = None,
        priority: Optional[int] = None,
        status: Optional[str] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        gsi1_pk: Optional[str] = None,
        gsi1_sk: Optional[str] = None,
        gsi2_pk: Optional[str] = None,
        gsi2_sk: Optional[str] = None,
    ) -> "BiblioSource":
        """
        Build a BiblioSource from raw inputs and a source 'doc' mapping.

        Mappings:
          - bibcode <- bib
          - bibstem <- bibstem
          - doctype <- doc.get('doctype')
          - date <- doc.get('date')
          - entry_date <- doc.get('entry_date')
          - author_count <- len(authors)
          - has_abstract <- has_abs
          - is_open_access <- is_oa
          - open_access_url <- oa_url
          - oa_reason <- oa_reason
          - data <- data
          - has_arxiv_id <- _first_arxiv_id(doc.get('identifier')) is not None
        """
        identifiers = doc.get("identifier")
        has_arxiv = _first_arxiv_id(identifiers) is not None

        return cls(
            bibcode=bib,
            bibstem=bibstem,
            doctype=doc.get("doctype"),
            date=doc.get("date"),
            entry_date=doc.get("entry_date"),
            author_count=author_count,
            has_abstract=has_abs,
            is_open_access=is_oa,
            open_access_url=oa_url,
            oa_reason=oa_reason,
            data=list(data or []),
            has_arxiv_id=has_arxiv,
            candidate_id=candidate_id,
            priority=priority,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            gsi1_pk=gsi1_pk,
            gsi1_sk=gsi1_sk,
            gsi2_pk=gsi2_pk,
            gsi2_sk=gsi2_sk,
        )
