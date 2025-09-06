# tests/unit/test_query_ads_bibcodes_slim.py
import os, sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)
import app
from unittest.mock import patch

SAMPLE = {
  "response": {"docs": [
    {
      "bibcode":"2012ATel.4321....1B",
      "bibstem":["ATel","Supp"],
      "doctype":"circular",
      "property":["EPRINT_OPENACCESS"],
      "links_data":[{"title":"arXiv e-print","url":"https://arxiv.org/pdf/1205.00001.pdf"}],
      "identifier":["arXiv:1205.00001"],
      "date":"2012-05-01",
      "entry_date":"2012-05-01T12:34:56Z",
      "data":[],
      "author":["A. Author","B. Author"],
      "abstract":"Short notice.",
      "title":"A fast notice"
    },
    {
      "bibcode":"1998IAUC.1234....1A",
      "bibstem":"IAUC",
      "doctype":"circular",
      "property":[],
      "links_data":[],
      "identifier":[],
      "date":"1998-06-02",
      "entry_date":"1998-06-02T00:00:00Z",
      "data":[],
      "author":["C. Author"],
      "abstract":"Earlier circular.",
      "title":"Earlier notice"
    }
  ]}
}

@patch("app.requests.get")
def test_slim_fields_and_oa(mock_get, monkeypatch):
    monkeypatch.setenv("ADS_TOKEN", "fake")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = SAMPLE

    event = {"status":"OK","candidate_name":"V1324 Sco","preferred_name":"V1324 Sco","aliases":["Nova Sco 2012"]}
    out = app.handler(event, None)

    recs = out["ads"]["records"]
    assert set(recs[0].keys()) >= {
        "bibcode","bibstem","doctype","date","entry_date",
        "authors_count","has_abstract","is_open_access","open_access_url","oa_reason",
        "has_data_links","has_arxiv_id"
    }
    assert "author" not in recs[0] and "abstract" not in recs[0]
    # OA computed
    atel = next(r for r in recs if r["bibstem"]=="ATel")
    assert atel["is_open_access"] is True
    assert "arxiv.org" in (atel["open_access_url"] or "")
    # Discovery from 1998 record
    assert out["ads"]["earliest_bibcode"] == "1998IAUC.1234....1A"
    assert out["ads"]["discovery_basis"] == "date"
