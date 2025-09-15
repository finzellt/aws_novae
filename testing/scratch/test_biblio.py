from pydantic import BaseModel, HttpUrl, Field, computed_field, field_validator
import hashlib,json
from typing import List, Optional, Sequence


class TestBib(BaseModel):
    nova_id: str = Field("ABCDE", description="Stable unique nova identifier.")
    bibcode: str = Field("1234", description="Unique bibliographic code (ADS bibcode).")
    bibstem: str = Field("ZYXW", description="Journal or series short name (bibstem).")
    
    @computed_field(return_type=str, description="SHA-256 fingerprint of nova_id and bibcode")
    def fp(self) -> str:
        key = f"{(self.bibcode or '').lower()}|{(self.nova_id or '').lower()}".encode("utf-8")
        return hashlib.sha256(key).hexdigest()

    @computed_field(return_type=str, description="Primary key for DynamoDB (SNAP#...)")
    def pk(self) -> str:
        # logger.warning("PK computed field called")
        return f"SNAP#{self.fp}"

    @computed_field(return_type=str, description="Secondary key for DynamoDB (NOVA#...#BIB#...)")
    def sk(self) -> str:
        return f"NOVA#{self.nova_id or 'UNKNOWN'}#BIB#{self.bibcode or 'UNKNOWN'}"

    nova_id: List[str] = Field(["V1324","Sco"], description="Stable unique nova identifier.")
    source_type: List[str] = Field(["V1324","Sco"], description="Stable unique nova identifier.")

    @computed_field(return_type=str)
    @property
    def candidate_id(self) -> str:
        data_id = ""
        try:
            if isinstance(self.data, dict) and self.data:
                print("Data ID found:")
                data_id = (
                    str(" ".join(self.data))
                    or str(self.data.get("name"))
                    or json.dumps(self.data, sort_keys=True, ensure_ascii=False)
                )
                print("Data ID found:", data_id)
            raw = "|".join([
                self.bibcode or "",
                self.source_type or "",
                (self.doctype or ""),
                (str(self.best_free_url) if self.best_free_url else ""),
                data_id,
            ])
            print("Data ID found:", data_id)
        except Exception as e:
            print("Error occurred while generating candidate ID:", e)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()