# test_app.py
import types
from unittest.mock import patch

import pytest
from astropy.table import Table

import app


def _mk_main_row(main_id="M31", ra=10.68470833, dec=41.26875, otype="Galaxy"):
    return Table(
        rows=[(main_id, ra, dec, otype)],
        names=("MAIN_ID", "RA_d", "DEC_d", "OTYPE"),
    )


def _mk_ids(*ids):
    return Table(rows=[(i,) for i in ids], names=("ID",))


def test_success_basic(monkeypatch):
    # Patch the module-level _simbad instance's methods
    class FakeSimbad:
        def query_object(self, name):
            assert name == "M31"
            return _mk_main_row(main_id="M31", ra=10.68470833, dec=41.26875, otype="Galaxy")

        def query_objectids(self, name):
            return _mk_ids("NGC 224", "Andromeda", "M 31")

    monkeypatch.setattr(app, "_simbad", FakeSimbad())

    out = app.resolve_simbad("M31")
    assert out["status"] == "OK"
    assert out["preferred_name"] == "M31"
    assert out["coords"]["ra_deg"] == pytest.approx(10.68470833)
    assert out["coords"]["dec_deg"] == pytest.approx(41.26875)
    assert out["object_types"] == ["Galaxy"]
    # Main id first, aliases deduped, candidate included
    assert out["aliases"][0] == "M31"
    assert "NGC 224" in out["aliases"]
    assert "Andromeda" in out["aliases"]
    assert out["name_norm"] == "m31"


def test_not_found(monkeypatch):
    class FakeSimbad:
        def query_object(self, name):
            return None

        def query_objectids(self, name):
            raise AssertionError("should not be called")

    monkeypatch.setattr(app, "_simbad", FakeSimbad())

    out = app.resolve_simbad("TotallyNotAStar")
    assert out["status"] == "NOT_FOUND"
    assert out["candidate_name"] == "TotallyNotAStar"


def test_missing_coords_raises(monkeypatch):
    # Simulate a row without RA/Dec degrees -> should raise to trigger retry
    tbl = Table(rows=[("X", None, None, "Nova")], names=("MAIN_ID", "RA_d", "DEC_d", "OTYPE"))

    class FakeSimbad:
        def query_object(self, name):
            return tbl

        def query_objectids(self, name):
            return _mk_ids("X 2025")

    monkeypatch.setattr(app, "_simbad", FakeSimbad())

    with pytest.raises(RuntimeError):
        app.resolve_simbad("X")


def test_alias_dedup_and_order(monkeypatch):
    class FakeSimbad:
        def query_object(self, name):
            return _mk_main_row(main_id="V606 Aql", ra=290.123, dec=-5.4321, otype="Nova")

        def query_objectids(self, name):
            # Include duplicates and spacing variants
            return _mk_ids("V606 Aql", "Nova Aql 1899", "Nova  Aql   1899")

    monkeypatch.setattr(app, "_simbad", FakeSimbad())

    out = app.resolve_simbad("V606 Aql")
    assert out["aliases"][0] == "V606 Aql"            # main id first
    assert out["aliases"].count("V606 Aql") == 1      # deduped
    assert "Nova Aql 1899" in out["aliases"]
    assert out["object_types"] == ["Nova"]
    assert out["name_norm"] == "v606aql"


def test_bad_input_returns_bad_request():
    out = app.handler({}, None)
    assert out["status"] == "BAD_REQUEST"
    assert "candidate_name is required" in out["error"]
