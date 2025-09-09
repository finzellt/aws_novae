# tests/unit/test_stage_write_metadata_queue.py
import json
import types
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import importlib
import os
import pytest
import os, sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)

# ------------------ Fakes ------------------

class FakeS3:
    def __init__(self):
        self.calls = []
        self.objects = {}  # (Bucket, Key) -> json string

    def put_object(self, Bucket, Key, Body, ContentType):
        self.calls.append({"Bucket": Bucket, "Key": Key, "Body": Body, "ContentType": ContentType})
        # store last JSON for convenience (best-effort decode)
        try:
            self.objects[(Bucket, Key)] = Body.decode("utf-8")
        except Exception:
            self.objects[(Bucket, Key)] = Body
        return {"ETag": '"fake-etag"'}

class ConditionalCheckFailed(Exception):
    pass

class FakeTable:
    """
    Tiny in-memory table that supports:
      - put_item(Item, ConditionExpression="attribute_not_exists(pk)")
      - update_item(... ConditionExpression="attribute_not_exists(#p) OR #p > :new_prio")
    Stores items keyed by (pk, sk).
    """
    def __init__(self):
        self.items = {}

    def put_item(self, Item, ConditionExpression=None):
        pk, sk = Item["pk"], Item["sk"]
        exists = (pk, sk) in self.items
        if ConditionExpression and "attribute_not_exists(pk)" in ConditionExpression and exists:
            # Mimic botocore ClientError shape we check in code
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[(pk, sk)] = dict(Item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def update_item(self, Key, UpdateExpression, ConditionExpression,
                    ExpressionAttributeNames, ExpressionAttributeValues, ReturnValues):
        from botocore.exceptions import ClientError

        pk, sk = Key["pk"], Key["sk"]
        if (pk, sk) not in self.items:
            raise ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "UpdateItem")

        cur = self.items[(pk, sk)]
        cur_prio = cur.get("priority")

        # Evaluate condition: "attribute_not_exists(#p) OR #p > :new_prio"
        name_for_p = ExpressionAttributeNames.get("#p")
        new_prio = ExpressionAttributeValues[":new_prio"]
        cond_ok = (name_for_p not in cur) or (cur_prio is None) or (cur_prio > new_prio)
        if not cond_ok:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")

        # Apply minimal parts of UpdateExpression we use
        # SET updated_at=:t, ads_snapshot_uri=:u, reason=:r, #p=:new_prio, gsi1_sk=:g1, gsi2_sk=:g2
        cur["updated_at"] = ExpressionAttributeValues[":t"]
        cur["ads_snapshot_uri"] = ExpressionAttributeValues[":u"]
        cur["reason"] = ExpressionAttributeValues[":r"]
        cur[name_for_p] = int(new_prio)
        cur["gsi1_sk"] = ExpressionAttributeValues[":g1"]
        cur["gsi2_sk"] = ExpressionAttributeValues[":g2"]

        self.items[(pk, sk)] = cur
        return {"Attributes": {"priority": cur["priority"]}}


# ------------------ Test helpers ------------------

def _recent_iso(days_ago=0):
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat().replace("+00:00", "Z")

def _base_event():
    return {
        "status": "OK",
        "preferred_name": "V1324 Sco",
        "nova_id": "nova-001",
        "ads": {
            "query": 'full:("V1324 Sco" OR "Nova Sco 2012") AND collection:astronomy',
            "bibcodes": ["2012ATel.4321....1B", "2025MNRAS.0000..123X", "2010proposal....ZZ"],
            "has_fast_notice": True,
            "num_open_access": 2,
            "earliest_bibcode": "1998IAUC.1234....1A",
            "discovery_date": "1998-06-02",
            "discovery_basis": "date",
            "records": [
                # P0 eligible: circular (ATel), authors ok, OA or abstract ok
                {
                    "bibcode": "2012ATel.4321....1B",
                    "bibstem": "ATel",
                    "doctype": "circular",
                    "date": "2012-05-01",
                    "entry_date": "2012-05-01T12:34:56Z",
                    "authors_count": 2,
                    "has_abstract": False,
                    "has_data_links": False,
                    "is_open_access": True,
                    "open_access_url": "https://ui.adsabs.harvard.edu/abs/2012ATel.4321....1B/abstract",
                    "oa_reason": "circular_html",
                    "has_arxiv_id": False,
                },
                # P1 eligible: big journal article (MNRAS), OA, authors ok; recent for bonus
                {
                    "bibcode": "2025MNRAS.0000..123X",
                    "bibstem": "MNRAS",
                    "doctype": "article",
                    "date": "2025-08-20",
                    "entry_date": _recent_iso(5),  # recent to trigger bonus
                    "authors_count": 3,
                    "has_abstract": True,
                    "has_data_links": True,
                    "is_open_access": True,
                    "open_access_url": "https://arxiv.org/pdf/2508.01234.pdf",
                    "oa_reason": "arxiv",
                    "has_arxiv_id": True,
                },
                # Excluded doctype: should not enqueue
                {
                    "bibcode": "2010proposal....ZZ",
                    "bibstem": "NONE",
                    "doctype": "proposal",
                    "date": "2010-01-01",
                    "entry_date": "2010-01-01T00:00:00Z",
                    "authors_count": 5,
                    "has_abstract": True,
                    "has_data_links": False,
                    "is_open_access": True,
                    "open_access_url": None,
                    "oa_reason": "property_only",
                    "has_arxiv_id": False,
                },
            ],
        },
    }

# ------------------ Tests ------------------

@patch.dict(os.environ, {
    "STAGING_BUCKET": "nova-catalog",
    "STAGING_PREFIX": "staging/metadata",
    "ADS_STAGING_PREFIX": "staging/ads",
    "WRITE_LATEST": "true",
    "DROP_ADS_FROM_PAYLOAD": "true",
    "ADS_SNAPSHOT_MODE": "slim",
    "TOP_DOCTYPES": "circular,catalog,dataset",
    "BIG_JOURNAL_BIBSTEMS": "mnras,aj,apj,a&a,pasp",
    "EXCLUDED_DOCTYPES": "proposal,book,bookreview,editorial,inbook,obituary,inproceedings,phdthesis,talk,software",
    "PRIORITY_P0": "10",
    "PRIORITY_P1": "50",
    "PRIORITY_P2": "90",
    "PRIORITY_RECENCY_MAX_BONUS": "5",
    "PRIORITY_RECENCY_WINDOW_DAYS": "365",
    "ELIGIBILITY_RULE_VERSION": "2025-09-07",
    "AWS_REGION": "us-east-1",
})
def test_stage_write_writes_snapshots_and_enqueues(monkeypatch):
    # Import the module fresh after env is set
    import app as stage
    importlib.reload(stage)

    # Patch S3 + Dynamo table
    fake_s3 = FakeS3()
    fake_table = FakeTable()
    stage.s3 = fake_s3
    stage.queue_table = fake_table
    stage.ADS_QUEUE_TABLE = "nova-ingest-ads-queue"

    event = _base_event()
    out = stage.handler(event, None)

    # --- S3 writes: metadata, latest, slim ADS snapshot ---
    keys = [c["Key"] for c in fake_s3.calls]
    meta_keys = [k for k in keys if k.startswith("staging/metadata/") and "/latest/" not in k]
    latest_keys = [k for k in keys if k.startswith("staging/metadata/latest/")]
    ads_keys = [k for k in keys if k.startswith("staging/ads/")]

    assert len(meta_keys) == 1, "Metadata snapshot should be written once"
    assert len(latest_keys) == 1, "Latest pointer should be written once"
    assert len(ads_keys) == 1, "Slim ADS snapshot should be written once"

    # Snapshot record fields are restricted to the 12 allowed
    ads_key = ads_keys[0]
    snap = json.loads(fake_s3.objects[("nova-catalog", ads_key)])
    assert "records" in snap and len(snap["records"]) == 3
    allowed = {
        "bibcode","bibstem","doctype","date","entry_date",
        "authors_count","has_abstract","has_data_links",
        "is_open_access","open_access_url","oa_reason","has_arxiv_id",
    }
    for r in snap["records"]:
        assert set(r.keys()) <= allowed

    # --- Enqueue: only 2 eligible items (P0 circular + P1 big journal OA) ---
    # Check items stored in fake table
    all_items = list(fake_table.items.values())
    assert len(all_items) == 2
    by_bib = {it["bibcode"]: it for it in all_items}
    assert "2012ATel.4321....1B" in by_bib
    assert "2025MNRAS.0000..123X" in by_bib
    # Excluded proposal not enqueued
    assert "2010proposal....ZZ" not in by_bib

    # P0 circular priority = 10 (no recency bonus for 2012)
    assert by_bib["2012ATel.4321....1B"]["priority"] == 10
    assert by_bib["2012ATel.4321....1B"]["status"] == "READY"
    assert by_bib["2012ATel.4321....1B"]["reason"].lower().startswith("p0")

    # P1 big journal OA with recent entry_date => bonus lowers numeric priority
    # Base 50 - bonus 5 = 45
    assert by_bib["2025MNRAS.0000..123X"]["priority"] == 45
    assert by_bib["2025MNRAS.0000..123X"]["reason"].lower().startswith("p1")

    # --- Returned payload trimmed ads + pointers present ---
    assert out["staging"]["ads_snapshot_key"] == ads_key
    assert set(out["ads"].keys()) == {
        "query","bibcodes","has_fast_notice","num_open_access",
        "earliest_bibcode","discovery_date","discovery_basis","ads_snapshot_key"
    }

@patch.dict(os.environ, {
    "STAGING_BUCKET": "nova-catalog",
    "STAGING_PREFIX": "staging/metadata",
    "ADS_STAGING_PREFIX": "staging/ads",
    "WRITE_LATEST": "true",
    "DROP_ADS_FROM_PAYLOAD": "true",
    "ADS_SNAPSHOT_MODE": "slim",
    "TOP_DOCTYPES": "circular,catalog,dataset",
    "BIG_JOURNAL_BIBSTEMS": "mnras,aj,apj,a&a,pasp",
    "EXCLUDED_DOCTYPES": "proposal,book,bookreview,editorial,inbook,obituary,inproceedings,phdthesis,talk,software",
    "PRIORITY_P0": "10",
    "PRIORITY_P1": "50",
    "PRIORITY_P2": "90",
    "PRIORITY_RECENCY_MAX_BONUS": "5",
    "PRIORITY_RECENCY_WINDOW_DAYS": "365",
    "ELIGIBILITY_RULE_VERSION": "2025-09-07",
    "AWS_REGION": "us-east-1",
})
def test_priority_update_only_when_better(monkeypatch):
    import app as stage
    importlib.reload(stage)

    fake_s3 = FakeS3()
    fake_table = FakeTable()
    stage.s3 = fake_s3
    stage.queue_table = fake_table
    stage.ADS_QUEUE_TABLE = "nova-ingest-ads-queue"

    event = _base_event()

    # 1) First run enqueues both; MNRAS priority 45
    out1 = stage.handler(event, None)
    items1 = list(fake_table.items.values())
    p_mnras_1 = next(i["priority"] for i in items1 if i["bibcode"] == "2025MNRAS.0000..123X")
    assert p_mnras_1 == 45

    # 2) Second run: same record but pretend rules improved it to better priority
    #    Simulate by temporarily tweaking priority envs in module
    stage.PRIORITY_P1 = 40  # base 40 - 5 bonus = 35 (better)
    out2 = stage.handler(event, None)
    items2 = list(fake_table.items.values())
    p_mnras_2 = next(i["priority"] for i in items2 if i["bibcode"] == "2025MNRAS.0000..123X")
    assert p_mnras_2 == 35, "Priority should update only when new priority is better (numerically lower)"

@patch.dict(os.environ, {
    "STAGING_BUCKET": "nova-catalog",
    "STAGING_PREFIX": "staging/metadata",
    "ADS_STAGING_PREFIX": "staging/ads",
    "WRITE_LATEST": "true",
    "DROP_ADS_FROM_PAYLOAD": "true",
    "ADS_SNAPSHOT_MODE": "slim",
    "AWS_REGION": "us-east-1",
})
def test_handles_no_ads_records_gracefully(monkeypatch):
    import app as stage
    importlib.reload(stage)

    fake_s3 = FakeS3()
    stage.s3 = fake_s3
    stage.queue_table = None  # no queue configured

    event = {
        "status": "OK",
        "preferred_name": "V1324 Sco",
        "nova_id": "nova-001",
        "ads": { "query":"q", "bibcodes": [], "records": [] }
    }
    out = stage.handler(event, None)

    # Should still write metadata (and latest), but skip ADS snapshot + enqueues
    keys = [c["Key"] for c in fake_s3.calls]
    assert any(k.startswith("staging/metadata/") and "/latest/" not in k for k in keys)
    assert any(k.startswith("staging/metadata/latest/") for k in keys)
    assert not any(k.startswith("staging/ads/") for k in keys)

    # Enqueue summary present but zeros
    assert out.get("enqueue", {}).get("enqueued", 0) == 0
