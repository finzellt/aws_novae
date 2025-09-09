# import os
# from src.init_context import app
import sys
import os

# Get the absolute path two directories up
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

# Add it to the Python path
sys.path.append(parent_dir)
import app

def setup_module(_m):
    # Provide env vars for the handler
    os.environ["APP_NAME"] = "nova-data-harvest"
    os.environ["S3_BUCKET"] = "nova-data-bucket-finzell"
    os.environ["ADS_SNAPSHOT_PREFIX"] = "harvest/snapshots/ads/"
    os.environ["MANIFEST_PREFIX"] = "harvest/manifests/"
    os.environ["RECENCY_BOOST_DAYS"] = "90"

def test_handler_minimal_event_builds_defaults():
    event = {"nova": {"id": "V1324 Sco", "name_norm": "v1324_sco"}}
    out = app.handler(event, None)
    assert out["ok"] is True
    assert out["run"]["app"] == "nova-data-harvest"
    assert out["nova"]["id"] == "V1324 Sco"
    assert out["config"]["s3_bucket"] == "nova-data-bucket-finzell"
    # ads uri derived from env + nova id
    assert out["config"]["ads_snapshot_uri"].endswith("/harvest/snapshots/ads/V1324 Sco.json")

def test_handler_explicit_ads_uri_respected():
    event = {
        "nova": {"id": "V1324 Sco", "name_norm": "v1324_sco"},
        "ads_snapshot_uri": "s3://custom-bucket/custom/key.json"
    }
    out = app.handler(event, None)
    assert out["ok"] is True
    assert out["config"]["ads_snapshot_uri"] == "s3://custom-bucket/custom/key.json"

def test_handler_validation_error():
    # Missing 'nova' should trigger validation error
    out = app.handler({}, None)
    assert out["ok"] is False
    assert out["error"] == "ValidationError"
