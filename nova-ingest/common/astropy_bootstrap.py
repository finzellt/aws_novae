# common/astropy_bootstrap.py
import os, pathlib

def setup_astropy_cache(base="/tmp"):
    # Only set if not already provided via env
    conf = os.environ.setdefault("ASTROPY_CONFIGDIR", f"{base}/astropy/config")
    cache = os.environ.setdefault("ASTROPY_CACHE_DIR", f"{base}/astropy/cache")
    aq   = os.environ.setdefault("ASTROQUERY_CACHE_DIR", f"{base}/astroquery")

    # (Optional) only set HOME if you still see ~/.astropy lookups
    # os.environ.setdefault("HOME", base)

    for p in (conf, cache, aq):
        pathlib.Path(p).mkdir(parents=True, exist_ok=True)