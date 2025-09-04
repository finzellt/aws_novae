import json
from unittest.mock import patch
import sys
import os

# Get the absolute path two directories up
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

# Add it to the Python path
sys.path.append(parent_dir)

# Now you can import
import app


CSV = """Primary_Name,RA_deg,DEC_deg
M31,10.6847083,41.26875
NGC 253,11.888,-25.288
"""

EVENT_BASE = {
    "status": "OK",
    "candidate_name": "V1324 Sco",
    "preferred_name": "V1324 Sco",
    "aliases": ["V1324 Sco"],
    "coords": {"ra_deg": 10.6847083, "dec_deg": 41.27}  # near M31
}

class FakeS3Body:
    def __init__(self, b): self._b=b
    def read(self): return self._b

def fake_get_object(**kwargs):
    return {"Body": FakeS3Body(CSV.encode("utf-8"))}

@patch.object(app, "_s3")
def test_confirmed_by_name(mock_s3):
    """
    Tests that the determine_host function correctly identifies and confirms the host galaxy
    based on the event's alias name. Specifically, verifies that when the alias contains
    a known galaxy name (e.g., "M31"), the returned host galaxy is set to that name and
    the external galaxy confidence is marked as "CONFIRMED".

    Mocks the S3 get_object method to use a fake implementation for testing.
    """
    mock_s3.get_object.side_effect = fake_get_object
    e = dict(EVENT_BASE)
    e["aliases"] = ["M31N 2025-08a"]  # contains "M31"
    out = app.determine_host(e)
    assert out["host_galaxy"] == "M31"
    assert out["external_galaxy_confidence"] == "CONFIRMED"

@patch.object(app, "_s3")
def test_probable_by_position(mock_s3):
    mock_s3.get_object.side_effect = fake_get_object
    e = dict(EVENT_BASE)
    e["aliases"] = ["AT 2025xyz"]  # no M31 text
    out = app.determine_host(e)
    # separation is ~0.001-0.002 deg, should be PROBABLE (< 0.5)
    assert out["host_galaxy"] == "M31"
    assert out["external_galaxy_confidence"] == "PROBABLE"

@patch.object(app, "_s3")
def test_mw_when_far(mock_s3):
    mock_s3.get_object.side_effect = fake_get_object
    e = dict(EVENT_BASE)
    e["coords"] = {"ra_deg": 200.0, "dec_deg": 0.0}  # far from both
    out = app.determine_host(e)
    assert out["host_galaxy"] == "MW"
    assert out["external_galaxy_confidence"] == "MW"
