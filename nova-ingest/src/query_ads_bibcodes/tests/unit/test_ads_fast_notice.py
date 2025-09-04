from unittest.mock import patch
import os, sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)
import app

BASE_EVENT = {
    "status": "OK",
    "candidate_name": "V1324 Sco",
    "preferred_name": "V1324 Sco",
    "aliases": ["Nova Sco 2012"]
}

def _mock_ads_response(docs):
    return {"response": {"docs": docs}}

@patch("app.requests.get")
def test_fast_notice_true_with_ATel(mock_get, monkeypatch):
    monkeypatch.setenv("ADS_TOKEN", "fake")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = _mock_ads_response([
        {"bibcode":"2012ATel.4321....1B","date":"2012-05-01","entry_date":"2012-05-01T12:34:56Z","data":[], "bibstem":"ATel"},
        {"bibcode":"2013ApJ...1234....1C","date":"2013-07-01","entry_date":"2013-07-02T00:00:00Z","data":[], "bibstem":"ApJ"}
    ])

    out = app.handler(dict(BASE_EVENT), None)
    assert out["ads"]["has_fast_notice"] is True
    assert any(r["bibstem"] == "ATel" for r in out["ads"]["records"])

# @patch("app.requests.get")
# def test_fast_notice_true_with_CBET(mock_get, monkeypatch):
#     monkeypatch.setenv("ADS_TOKEN", "fake")
#     mock_get.return_value.status_code = 200
#     mock_get.return_value.json.return_value = _mock_ads_response([
#         {"bibcode":"2008CBET.1234....1A","date":"2008-06-02","entry_date":"2008-06-02T00:00:00Z","data":[], "bibstem":"CBET"},
#         {"bibcode":"2009MNRAS.1234....2D","date":"2009-02-10","entry_date":"2009-02-11T00:00:00Z","data":[], "bibstem":"MNRAS"}
#     ])

#     out = app.handler(dict(BASE_EVENT), None)
#     assert out["ads"]["has_fast_notice"] is True
#     assert any(r["bibstem"] == "CBET" for r in out["ads"]["records"])

@patch("app.requests.get")
def test_fast_notice_false_when_absent(mock_get, monkeypatch):
    monkeypatch.setenv("ADS_TOKEN", "fake")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = _mock_ads_response([
        {"bibcode":"1998IAUC.1234....1A","date":"1998-06-02","entry_date":"1998-06-02T00:00:00Z","data":[], "bibstem":"IAUC"},
        {"bibcode":"2013yCat.1234....1C","date":"2013-07-01","entry_date":"2013-07-02T00:00:00Z","data":[], "bibstem":"yCat"},
        {"bibcode":"2015ApJ...5678....9Z","date":"2015-03-05","entry_date":"2015-03-06T00:00:00Z","data":[], "bibstem":"ApJ"}
    ])

    out = app.handler(dict(BASE_EVENT), None)
    assert out["ads"]["has_fast_notice"] is False
    # priority_citations should still include IAUC/yCat even if no fast notice
    stems = {r["bibstem"] for r in out["ads"]["priority_citations"]}
    assert stems.issuperset({"IAUC","yCat"})

@patch("app.requests.get")
def test_bibstem_list_is_normalized(mock_get, monkeypatch):
    monkeypatch.setenv("ADS_TOKEN", "fake")
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "response": {"docs": [
            {"bibcode":"2012ATel.4321....1B","date":"2012-05-01","entry_date":"2012-05-01T12:34:56Z",
             "data":[], "bibstem":["ATel","ATelSupp"]},
        ]}
    }
    event = {"status":"OK","candidate_name":"V1324 Sco","preferred_name":"V1324 Sco","aliases":[]}
    out = app.handler(event, None)
    rec = out["ads"]["records"][0]
    assert rec["bibstem"] == "ATel"            # normalized
    assert out["ads"]["has_fast_notice"] is True
