from unittest.mock import patch
import os, sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)
import app
import json

EVENT = {
  "status":"OK",
  "candidate_name":"V1324 Sco",
  "preferred_name":"V1324 Sco",
  "coords":{"ra_deg":10.0,"dec_deg":-30.0},
  "ads":{"bibcodes":["2012ATel...."], "earliest_bibcode":"1998IAUC....", "discovery_date":"1998-06-02"}
}

class _PutRes:
    def __init__(self): self.ETag='"etag123"'
    def get(self,k): return getattr(self,k,None)

@patch.object(app, "s3")
def test_stage_write_basic(mock_s3, monkeypatch):
    mock_s3.put_object.return_value = {"ETag": '"etag123"'}
    out = app.handler(dict(EVENT), None)

    stg = out["staging"]
    assert stg["bucket"]
    assert stg["snapshot_key"].startswith("staging/metadata/")
    assert stg["snapshot_key"].endswith(".json")
    # ensure latest pointer was written too
    assert stg["latest_key"].startswith("staging/metadata/latest/")
    # and that the document includes stage_written_at
    assert "stage_written_at" in out
