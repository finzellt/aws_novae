# import logging
from typing import Any, Dict, List
from urllib.parse import urlencode
from pydantic import ValidationError
from datetime import datetime, timezone
# from nova_schema.nova import Nova
# # from nova_schema.harvest import HarvestCandidate
# from nova_schema.mapping.ads_mapping import map_ads_response_to_harvest
import os,boto3,json,sys
from pathlib import Path

# Example: add ../shared to sys.path (relative to current file)
shared_path = Path(__file__).resolve().parent.parent.parent / "nova-ingest/shared"
sys.path.append(str(shared_path))
print("Added to PYTHONPATH:", shared_path)
# from nova_schema.nova import Nova
from nova_schema.nova import Nova
from nova_schema.mapping.ads_mapping import map_ads_response_to_harvest,merge_updates
from nova_schema.biblio import BiblioSource

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

ADS_API_URL = os.getenv("ADS_API_URL", "https://api.adsabs.harvard.edu/v1/search/query")

# # We still request these (to derive OA & heuristics), but won't return the heavy ones in payload
# ADS_FIELDS = [
#     "bibcode","bibstem","doctype","property","identifier","data",
#     "date","entry_date","data","author","abstract","title"
#]
ADS_FIELDS = [
    "bibcode","bibstem","doctype","property","identifier","data","abstract",
    "date","entry_date","author_count"
]
# def _build_ads_query(canonical: Dict[str, Any]) -> Dict[str, Any]:
#     name = canonical.get("primary_name")
#     q_parts = []
#     if name:
#         q_parts.append(f'object:"{name}"')
#     # Add other heuristics (aliases, coordinates windows) if you like
#     q = " AND ".join(q_parts) if q_parts else "*:*"
#     # Request only fields you actually need
#     fl = ",".join(ADS_FIELDS)
#     return urlencode({"q": q, "fl": fl, "rows": 50, "sort": "date desc"})

def _collect_names(canonical: Dict[str, Any]) -> List[str]:
    '''
    Collects names from the event dictionary. Includes candidate, preferred names
    and aliases. Ensures that names are strings with no extra whitespace, and are unique.

    inputs:
        event (Dict[str, Any]): The event dictionary containing name information.

    outputs:
        List[str]: A list of collected names.
    '''
    names: List[str] = []
    for k in ("candidate_name", "preferred_name"):
        v = canonical.get(k)
        if isinstance(v, str) and v.strip():
            names.append(v.strip())
    aliases = canonical.get("aliases")
    if isinstance(aliases, list):
        for a in aliases:
            if isinstance(a, str) and a.strip():
                names.append(a.strip())
    print(names)
    return names

def _quote(s: str) -> str:
    '''
    Ensure output is a string, regardless of input type. Quotes a string for inclusion in an ADS 
    query. Adds escape character for internal quotes. 

    inputs:
        s (str): The input string to quote.

    outputs:
        str: The quoted string.
    '''
    s = (s or "").strip()
    return '"' + s.replace('"', r'\"') + '"' if s else ""

# def _ads_request(params: Dict[str, Any]) -> Dict[str, Any]:
def _ads_request(canonical: Dict[str, Any]) -> Dict[str, Any]:
    import os, requests
    token ="7eYEFm24avvj5QHN9bcNQlCs5AmVulwwxFqYElry"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    names = [q for q in (_quote(n) for n in _collect_names(canonical)) if q]
    params = {"q": " OR ".join(names), "fl": ",".join(ADS_FIELDS), "rows": 50, "sort": "date asc"}
    # print(params)
    r = requests.get(ADS_API_URL, headers=headers, params=params, timeout=25)
    # headers = {"Authorization": f"Bearer {token}"} if token else {}
    # r = requests.get("https://api.adsabs.harvard.edu/v1/search/query?{}".format(params),
    #                  headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def _utc_now():
    return datetime.now(timezone.utc)

def _test_upsert(table: str, candidates: List[Dict[str, Any]]) -> int:
    """
    Upsert candidates individually. If you want BatchWrite, you can group in 25s,
    but PutItem is simplest and idempotent with stable candidate_id.
    """
    count = 0
    new_candidates = []
    for c in candidates:
        new_candidate = merge_updates(c,{"status": "queued", "updated_at": _utc_now()})
        item = {**new_candidate.model_dump(mode="json")}
        # Ensure candidate_id exists and is part of the key
        # new_candidate = 
        # c["status"] = "queued"
        # c.setdefault("status", "queued")
        # cid = item.get("candidate_id")
        # item["candidate_id"] = cid
        # Upsert (PutItem overwrites same key)
        try:
            # ddb.put_item(
            #     TableName=table,
            #     Item=_to_dynamo(item),
            # )
            new_candidates.append(new_candidate)
        except Exception as e:
            # logger.error("Failed to upsert item %s: %s", item, e)
            new_candidates.append(c)

        count += 1
    return count,new_candidates

def _get_biblio_objs(candidates: List[Dict[str, Any]]) -> List[BiblioSource]:
    """
    Convert a list of candidate dictionaries into BiblioSource objects.
    """
    return [BiblioSource(**c) for c in candidates]

def main():
    test_event = "/Users/tfinzell/Git/aws_novae/nova-ingest/.out_resolve.json"
    with open(test_event, "r") as f:
        event = json.load(f)

    canonical_in: Dict[str, Any] = event.get("canonical") or {}

    print(f"type(canonical_in): {type(canonical_in)}")
    nova = Nova(**canonical_in)
    print(f"type(nova): {type(nova)}")
    # params = _build_ads_query(nova.model_dump(mode="json"))
    raw = _ads_request(nova.model_dump(mode="json"))
    candidates: List[Dict[str, Any]] = map_ads_response_to_harvest(raw, nova.model_dump(mode="json").get("nova_id"))
    # candidates: List[BiblioSource] = map_ads_response_to_harvest(raw, nova.model_dump(mode="json").get("nova_id"))


    for can in candidates:
        print(can.bibcode, can.doctype, can.priority, can.author_count)
        # print(type(can))
    # decimal_value = 255
    # hex_representation = hex(decimal_value)
    # print(candidates)

    counts,new_candidates = _test_upsert("nova-harvest-candidates-dev", candidates)
    # print(f"type(new_candidates[0]): {type(new_candidates[0])}")
    serial_candidates = [m.model_dump(mode="json") for m in new_candidates]
    # print(f"type(serial_candidates[0]): {type(serial_candidates[0])}")
    # print(serial_candidates[0])
    # candidates_bib = _get_biblio_objs(serial_candidates)
    
    # updates = {}
    # if candidates_bib:
    #     bibcodes = []
    #     for candidate in candidates_bib:
    #         bibcodes.append(candidate.bibcode)
    #     # bs = set(nova.bib_sources or [])
    #     # bs.add("ads")
    #     updates["bib_sources"] = sorted(bibcodes)
    #     nova = merge_updates(nova, updates=updates)

    # for can in candidates_bib:
    #     print(type(can.candidate_id))
    # for candidate in candidates:
    # for candidate in new_candidates:
    #     temp_candidate = candidate.model_dump(mode="json")
    #     # try:
    #     # bc = candidate.get("bibcode")
    #     # hr = hex(candidate["candidate_id"])
    #     hr = temp_candidate["updated_at"]
    #     # if not bc:
    #     #     continue
    #     print(f"\t Candidate hex_representation: {hr}, Candidate Doctype: {temp_candidate['doctype']}, Candidate status: {temp_candidate['status']}")

        # if temp_candidate["data"]:
        #     print(f"\t Candidate hex_representation: {hr}, Candidate Doctype: {temp_candidate['doctype']}, Candidate status: {temp_candidate['status']}")
        # else:
        #     print(f"Candidate hex_representation: {hr}, Candidate Doctype: {temp_candidate['doctype']}")
    return {
    "canonical": nova.model_dump(mode="json"),
    "harvest_candidates": serial_candidates,   # pass to the next step that writes/harvests
    }

if __name__ == "__main__":
    a = main()
    print(a["canonical"])
    # print(a)
#         biblio = BiblioSource.from_doc(
#             doc=candidate,
#             bib=bc,
#             bibstem=candidate.get("bibstem") or "",
#             authors=candidate.get("author") or [],
#             has_abs=bool(candidate.get("abstract")),
#             is_oa=bool(candidate.get("is_open_access")),
#             oa_url=candidate.get("best_free_url"),
#             oa_reason=candidate.get("oa_reason"),
#             data=candidate.get("data") if candidate.get("data") else None,
#         )
#         print("  BiblioSource:", biblio.model_dump(mode="json"))
#     except ValidationError as ve:
#         print("  ValidationError:", ve)
#     except Exception as e:
#         print("  Exception:", e)
# # print(raw["response"]["docs"])
# for val in raw["response"]["docs"]:
#     print(val["bibcode"])
#     if 'data' in val:
#         print(val["bibcode"], val['data'])
#         for data in val['data']:
#             print(f"\t {data}")
        # print()
# for val in raw.values():
#     print(val)
#     print()
# print("Raw ADS response:", raw)