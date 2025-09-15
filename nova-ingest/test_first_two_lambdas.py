from src.resolve_simbad_metadata.app import handler as resolve
from src.determine_host_galaxy.app import handler as host

event1 = {"candidate_name": "V1324 Sco"}
res1 = resolve(event1, None)
res2 = host({"canonical": res1["canonical"]}, None)
print(res2)