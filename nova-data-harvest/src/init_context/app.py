import os
import uuid
import json
from datetime import datetime, timezone
from pydantic import BaseModel, Field, ValidationError

ISO8601 = "%Y-%m-%dT%H:%M:%SZ"

# --------- Models ---------
class Nova(BaseModel):
    '''
    Class for storing ID, name, and aliases of a Nova entity.
    '''
    id: str
    name_norm: str | None = None
    aliases: list[str] = Field(default_factory=list)

class Event(BaseModel):
    '''
    Class for storing nova and corresponding event data.
    '''
    nova: Nova
    # Optional; if not provided, we derive it from env + nova.id
    ads_snapshot_uri: str | None = None

# --------- Helpers ---------
def env(name: str, default: str | None = None) -> str:
    '''
    Get an environment variable or raise an error.
    '''
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def build_ads_uri(nova_id: str) -> str:
    '''
    Build the S3 URI for the ADS snapshot.
    '''
    bucket = env("S3_BUCKET")
    prefix = env("ADS_SNAPSHOT_PREFIX", "harvest/snapshots/ads/")
    return f"s3://{bucket}/{prefix}{nova_id}.json"

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO8601)

# --------- Handler ---------
def handler(event, _context):
    """
    Initializes a harvest run:
      - validates input
      - fills defaults from env
      - returns derived S3 paths + run metadata
    """
    try:
        parsed = Event(**event)
    except ValidationError as e:
        return {
            "ok": False,
            "error": "ValidationError",
            "details": json.loads(e.json())
        }

    s3_bucket = env("S3_BUCKET")
    manifest_prefix = env("MANIFEST_PREFIX", "harvest/manifests/")
    recency_days = int(env("RECENCY_BOOST_DAYS", "90"))

    run_id = str(uuid.uuid4())
    started_at = now_iso()

    ads_uri = parsed.ads_snapshot_uri or build_ads_uri(parsed.nova.id)

    result = {
        "ok": True,
        "run": {
            "id": run_id,
            "started_at": started_at,
            "app": env("APP_NAME", "nova-data-harvest")
        },
        "nova": parsed.nova.model_dump(),
        "config": {
            "s3_bucket": s3_bucket,
            "ads_snapshot_uri": ads_uri,
            "manifest_prefix": manifest_prefix,
            "recency_boost_days": recency_days
        }
    }
    return result
