import json
from unittest.mock import patch
import os, sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)
import app

SAMPLE = {
  "response": {"docs": [
    {"bibcode":"1998IAUC.1234....1A","date":"1998-06-02","entry_date":"1998-06-02T00:00:00Z","data":[], "bibstem":"IAUC"},
    {"bibcode":"2012ATel.4321....1B","date":"2012-05-01","entry_date":"2012-05-01T12:34:56Z","data":["D"], "bibstem":"ATel"},
    {"bibcode":"2013yCat.1234....1C","date":"2013-07-01","entry_date":"2013-07-02T00:00:00Z","data":[], "bibstem":"yCat"}
  ]}
}

@patch("app.requests.get")
def test_ads_query_and_discovery(mock_get, monkeypatch):
    monkeypatch.setenv("ADS_TOKEN", "fake")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = SAMPLE

    event = {
        "status":"OK",
        "candidate_name":"V1324 Sco",
        "preferred_name":"V1324 Sco",
        "aliases":["Nova Sco 2012"]
    }
    out = app.handler(event, None)

    assert out["ads"]["has_fast_notice"] is True
    assert out["ads"]["earliest_bibcode"] == "1998IAUC.1234....1A"
    # 1998 uses "date"; post-2000 records would use "entry_date"
    assert out["ads"]["discovery_basis"] == "date"
    assert len(out["ads"]["priority_citations"]) == 3  # IAUC + ATel + yCat
    assert "full:(" in out["ads"]["query"] and "V1324 Sco" in out["ads"]["query"]
