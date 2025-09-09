import importlib
import json
import os
from pathlib import Path

import boto3
from moto import mock_aws
import pytest
from botocore.exceptions import ClientError
import os, sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(parent_dir)

# Important: this module creates boto3 clients at import-time.
# So we must set env vars _before_ importing the module.

@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("STAGING_BUCKET", "nova-catalog")
    monkeypatch.setenv("STAGING_PREFIX", "staging/metadata")
    monkeypatch.setenv("ADS_STAGING_PREFIX", "staging/ads")
    monkeypatch.setenv("WRITE_LATEST", "true")
    monkeypatch.setenv("DROP_ADS_FROM_PAYLOAD", "true")
    monkeypatch.setenv("ADS_SNAPSHOT_MODE", "slim")
    monkeypatch.setenv("ADS_QUEUE_TABLE", "ads-priority-queue-test")
    # optional S3 endpoint for non-aws runners left blank; moto intercepts anyway
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    return tmp_path

@mock_aws
def test_handler_happy_path(env, monkeypatch):
    # create mocked infra
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="nova-catalog")

    ddb = boto3.client("dynamodb", region_name="us-east-1")
    ddb.create_table(
        TableName="ads-priority-queue-test",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi1_pk", "AttributeType": "S"},
            {"AttributeName": "gsi1_sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gsi1_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi1_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            }
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )

    # Now import the module after env + moto are configured
    mod = importlib.reload(importlib.import_module("test_new_stage_write"))

    event = {
        "status": "OK",
        "preferred_name": "V1324 Sco",
        "nova_id": "nova-0001",
        "ads": {
            "records": [
                {
                    "bibcode": "2025MNRAS.000..001A",
                    "bibstem": "mnras",
                    "doctype": "catalog",
                    "entry_date": "2025-06-01",
                    "authors_count": 3,
                    "has_abstract": True,
                    "is_open_access": True,
                }
            ]
        },
    }

    out = mod.handler(event, None)
    assert out.get("status") != "BAD_REQUEST"
    assert out["staging"]["bucket"] == "nova-catalog"
    # verify S3 writes happened
    s3.list_objects_v2(Bucket="nova-catalog")
    # verify enqueue summary exists
    assert "enqueue" in out
    assert out["enqueue"]["eligible"] >= 1
