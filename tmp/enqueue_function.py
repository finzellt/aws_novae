def _enqueue_snapshot(nova_id: str, bibcode: str, bibstem: str, doctype: str,
                      entry_date: Optional[str], ads_snapshot_key: Optional[str],
                      priority: int, reason: str,
                      data: Optional[List[Any]] = None,
                      open_access_url: Optional[str] = None,
                      oa_reason: Optional[str] = None) -> Dict[str, Any]:
    """
    Upsert into ADS_QUEUE_TABLE priority queue.
    Keys:
      PK = SNAP#<fingerprint>
      SK = NOVA#<nova_id>#BIB#<bibcode>
    GSIs:
      gsi1_pk = STATUS#<status>
      gsi1_sk = {priority:03d}|{entry_date}|{PK}
      gsi2_pk = NOVA#<nova_id>
      gsi2_sk = {status}|{priority:03d}|{updated_at}
    """
    if not queue_table:
        return {"enqueued": False, "reason": "ADS_QUEUE_TABLE not configured"}

    fp = _fingerprint(nova_id or "unknown", bibcode or "unknown")
    pk = f"SNAP#{fp}"
    sk = f"NOVA#{nova_id or 'UNKNOWN'}#BIB#{bibcode or 'UNKNOWN'}"

    status = "READY"
    created_at = _now_iso()
    updated_at = created_at
    attempts = 0
    lease_expires_at = 0
    entry_day = (entry_date or "")[:10] or "0000-00-00"

    item = {
        "pk": pk,
        "sk": sk,
        "status": status,
        "priority": int(priority),
        "ads_snapshot_key": ads_snapshot_key,   # store just the key; bucket is known at runtime
        "eligibility_rule_version": ELIGIBILITY_RULE_VERSION,
        "reason": reason,
        "bibcode": bibcode,
        "nova_id": nova_id,
        "bibstem": bibstem,
        "doctype": doctype,
        "entry_date": entry_date,
        "open_access_url": open_access_url,     # <-- NEW
        "oa_reason": oa_reason,                 # <-- NEW
        "has_data": bool(data),
        "data": data,
        "attempts": attempts,
        "lease_expires_at": lease_expires_at,
        "created_at": created_at,
        "updated_at": updated_at,
        # optional query keys for a priority view (if you’re using them):
        "gsi1_pk": f"STATUS#{status}",
        "gsi1_sk": f"{priority:03d}|{(entry_date or '')[:10] or '0000-00-00'}|{pk}",
        "gsi2_pk": f"NOVA#{nova_id or 'UNKNOWN'}",
        "gsi2_sk": f"{status}|{priority:03d}|{updated_at}",
    }
    # strip None (DynamoDB can’t store None)
    item = {k: v for k, v in item.items() if v is not None}

    try:
        queue_table.put_item(Item=item, ConditionExpression="attribute_not_exists(pk)")
        return {"enqueued": True, "created": True, "pk": pk, "sk": sk, "priority": priority}
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # update existing to (a) lower priority if better, and (b) refresh OA fields / pointer
        try:
            queue_table.update_item(
                Key={"pk": pk, "sk": sk},
                UpdateExpression=(
                    "SET #p = :new_prio, "
                    "updated_at = :t, ads_snapshot_key = :k, reason = :r, gsi1_sk = :g1"
                ),
                ConditionExpression="attribute_not_exists(#p) OR #p > :new_prio",
                ExpressionAttributeNames={"#p": "priority"},
                ExpressionAttributeValues={
                    ":new_prio": int(priority),
                    ":t": _now_iso(),
                    ":k": ads_snapshot_key,
                    ":r": reason,
                    ":g1": f"{priority:03d}|{(entry_date or '')[:10] or '0000-00-00'}|{pk}",
                },
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        # 2) Always refresh OA fields & pointer (NOTE: do NOT touch priority here)
        queue_table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression="SET open_access_url=:o, oa_reason=:or, ads_snapshot_key=:k, updated_at=:t",
            ExpressionAttributeValues={
                ":o": (open_access_url or ""),
                ":or": (oa_reason or ""),
                ":k": ads_snapshot_key,
                ":t": _now_iso(),
            },
        )
        return {"enqueued": True, "created": False, "pk": pk, "sk": sk, "priority": priority, "updated_priority": True}
# ---------- Handler ----------