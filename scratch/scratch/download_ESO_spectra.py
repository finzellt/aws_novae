#!/usr/bin/env python
# ------------------------------------------------------------------------------
# Python 3 example on how to query by position the ESO SSA service
# and retrieve the spectra science files with SNR higher than a given threshold.
#
# Name: eso_ssa.py
# Version: 2018-04-09
# 
# In case of questions, please send an email to: usd-help@eso.org 
# with the following subject: ASG programmatic access (eso_ssa.py)
#
# Useful to read: http://www.astropy.org/astropy-tutorials/Coordinates.html
# ------------------------------------------------------------------------------

import sys

import pyvo as vo
from astropy.coordinates import SkyCoord
from astropy.units import Quantity

# --------------------------------------------------
# Define the end point and the SSA service to query:
# --------------------------------------------------

ssap_endpoint = "http://archive.eso.org/ssap"

ssap_service = vo.dal.SSAService(ssap_endpoint)

# A typical SSA invocation: ssap_endpoint + '?' + 'REQUEST=queryData&POS=197.44888,-23.38333&SIZE=0.5'
# where
#   197.44888 is the right ascension J2000 (decimal degrees),
#   -23.38333 is the declination J2000 (decimal degrees),
#   0.5       is the diameter of the cone to search in (decimal degrees).
# Within astroquery this is achieved by the following lines:
#   mytarget = SkyCoord(197.44888, -23.38333, unit='deg')
#   mysize   = Quantity(0.5, unit="deg")
#   ssap_resultset = ssap_service.search(pos=mytarget, diameter=mysize)
# Really the coordinates are ICRS, but for all practical effects when searching, there is no difference with J2000.


# --------------------------------------------------
# Prepare search parameters
# searching by cone around NGC 4993,
# with a diameter or 0.5 deg (radius=0.25 deg).
# --------------------------------------------------
target = "V5668 Sgr"
diameter = 0.001

print()
print("Looking for spectra around target %s in a cone of diameter %f deg."   %(target, diameter))
print("Querying the ESO SSAP service at %s" %(ssap_endpoint))

# --------------------------------------------------
# The actual position of the selected target
# is queried by the from_name() function,
# which queries the CDS SESAME service
# (http://cdsweb.u-strasbg.fr/cgi-bin/Sesame).
# --------------------------------------------------

print("The provided target is being resolved by SESAME...")
pos = SkyCoord.from_name(target)
size = Quantity(diameter, unit="deg")
print("SESAME coordinates for %s: %s" % (target, pos.to_string()))

# see: http://docs.astropy.org/en/stable/coordinates/skycoord.html
# In case you know better the coordinates, then use:
# my_icrs_pos = SkyCoord(197.44888, -23.38333, unit='deg')    
#
# Or in case you know the galactic coordinates instead:
# my_gal_pos=SkyCoord(308.37745107, 39.29423547, frame='galactic', unit='deg')
#    in which case you will have to use:
# ssap_service.search(pos=my_gal_pos.fk5, diameter=size)
#    given that my_fk5_pos = my_gal_pos.fk5

# --------------------------------------------------
# Query in that defined cone (pos, size):
# --------------------------------------------------
print("Performing a Simple Spectral Access query...")
ssap_resultset = ssap_service.search(pos=pos.fk5, diameter=size)

# NOTE: The ESO coordinate system is: FK5. You would not be off by more than 20mas by querying by pos==pos.icrs (or simply pos=pos) instead.

# --------------------------------------------------
# define the output fields you are interested in;
# uppercase fields are the one defined by the SSAP standard as valid input fields
# --------------------------------------------------
fields = ["COLLECTION", "TARGETNAME", "s_ra", "s_dec", "APERTURE",
          "em_min", "em_max", "SPECRP", "SNR", "t_min", "t_max",
          "CREATORDID", "access_url"]

# --------------------------------------------------
# Print the blank-separated list of fields
# (one line for each of the spectra)
# with the following formatting rules:
# - Do not show the CREATORDID in the stdout:
# - In Python 3, to pretty-print a 'bytes' value, this must be decoded
# - Wavelengths are expressed in meters, for display they are converted to nanometers
# Also, count how many spectra have a SNR > min_SNR
# --------------------------------------------------
min_SNR = 70
count_high_SNR_files=0
separator=' '
for row in ssap_resultset:
   if row["SNR"] > min_SNR:
       count_high_SNR_files += 1
   for field in fields:
       if field == "CREATORDID":
          continue
       value = row[field]
       if isinstance(value, bytes):
          print(value.decode().rjust(16), end=separator)
       elif isinstance(value, str):
          print(value.rjust(16), end=separator)
       else:
          if (field == "em_min" or field == "em_max"): 
             value *= 1E9
          print('%16.10f' % (value), end=separator)
   print()

print()

# # --------------------------------------------------
# # Download those spectra that have SNR > min_SNR
# # The name of the file on disk will be the file name
# # defined by the creator of such file (field: CREATORDID).
# # --------------------------------------------------
# import urllib
# prompt = "Of the above spectra, download the (%d) ones with SNR > %d, [y|n]:" % (count_high_SNR_files, min_SNR)
# shall_I = input(prompt) # Python 3

# if shall_I != "y":
#     print("Stopping here, without downloading any file")
#     sys.exit(0)

# print("Downloading files with SNR > %d (if any)" % (min_SNR))

# id={}
# # for row in ssap_resultset:
# #    if row["SNR"] > min_SNR:
# #       dp_id = row["dp_id"].decode()
# #       origfile = row["CREATORDID"].decode()[23:]
# #       id[origfile] = dp_id
# #       # The downloaded file is saved with the name provided by the creator of the file: origfile.
# #       # Though, care should be taken, because reduced products
# #       # generated by external users might have colliding CREATORDID!
# #       # This "demo" script does not take into consideration this risk.
# #       print("Fetching file with SNR=%f: %s.fits renamed to %s" %(row["SNR"], dp_id, origfile))
# #       urllib.request.urlretrieve(row["access_url"].decode(), row["CREATORDID"].decode()[23:])

# for row in ssap_resultset:
#    if row["SNR"] > min_SNR:
#       dp_id = row["dp_id"]
#       origfile = row["CREATORDID"]
#       id[origfile] = dp_id
#       print("Fetching file with SNR=%f: %s.fits renamed to %s" %(row["SNR"], dp_id, origfile))
#       # urllib.request.urlretrieve(row["access_url"], row["CREATORDID"])
#       _ = urllib.request.urlretrieve(row["access_url"], dp_id+"fits")
#       print(row["access_url"])
#       # urllib.request.urlretrieve(row["access_url"], row["CREATORDID"])


# print("End of execution")
















# # """
# # Utilities to query ESO's SSA service and download spectral data files.

# # Public entry point:
# #     download_eso_ssa_spectra(target, radius_arcsec, min_snr=0, outdir='.', ...)

# # `target` may be either:
# # - a tuple (ra, dec) in degrees, or
# # - a string target name resolvable by SESAME/Simbad.

# # Example
# # -------
# # >>> download_eso_ssa_spectra("Vega", 30, min_snr=10)
# # >>> download_eso_ssa_spectra((150.057, 2.193), 5, min_snr=10)
# # """
# # from __future__ import annotations

# # import os
# # import pathlib
# # import urllib.parse
# # import urllib.request
# # import xml.etree.ElementTree as ET
# # from typing import Dict, List, Optional, Tuple, Union
# # from astropy.coordinates import SkyCoord
# # import astropy.units as u
# # import pyvo as vo
# # import json

# # from math import radians, sin, cos, sqrt, atan2

# # # --- Constants ----------------------------------------------------------------
# # SSAP_ENDPOINT = "https://archive.eso.org/ssa/eso/ssa"  # documented ESO SSA base
# # DEFAULT_MAXREC = 10000
# # SESAME_URL = "https://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame/-oxp/~SNV?"  # returns XML

# # ssap_endpoint = "http://archive.eso.org/ssap"

# # ssap_service = vo.dal.SSAService(ssap_endpoint)

# # # --- Helpers ------------------------------------------------------------------

# # # def _resolve_with_astroquery(name: str) -> Optional[Tuple[float, float]]:
# # #     """Try resolving with astroquery.simbad if available.
# # #     Returns (ra_deg, dec_deg) or None if not available/failed.
# # #     """
# # #     try:
# # #         from astroquery.simbad import Simbad  # type: ignore
# # #         Simbad.ROW_LIMIT = 1
# # #         res = Simbad.query_object(name)
# # #         if res is None or len(res) == 0:
# # #             return None
# # #         # RA/DEC given as sexagesimal strings; convert to degrees via astropy if present
# # #         try:
# # #             from astropy.coordinates import Angle  # type: ignore
# # #             ra_deg = Angle(res['RA'][0] + ' hours').degree
# # #             dec_deg = Angle(res['DEC'][0] + ' degrees').degree
# # #             return float(ra_deg), float(dec_deg)
# # #         except Exception:
# # #             # Fallback quick parser for sexagesimal without astropy (best-effort)
# # #             def _sexa_to_deg(ra_str: str, dec_str: str) -> Tuple[float, float]:
# # #                 def hms_to_deg(hms: str) -> float:
# # #                     parts = hms.replace(':', ' ').split()
# # #                     h, m, s = [float(p) for p in parts[:3]]
# # #                     return (h + m/60 + s/3600) * 15.0
# # #                 def dms_to_deg(dms: str) -> float:
# # #                     parts = dms.replace(':', ' ').split()
# # #                     sign = -1.0 if parts[0].strip().startswith('-') else 1.0
# # #                     d = abs(float(parts[0])); m = float(parts[1]); s = float(parts[2])
# # #                     return sign * (d + m/60 + s/3600)
# # #                 return hms_to_deg(ra_str), dms_to_deg(dec_str)
# # #             ra_deg, dec_deg = _sexa_to_deg(res['RA'][0], res['DEC'][0])
# # #             return float(ra_deg), float(dec_deg)
# # #     except Exception:
# # #         return None


# # # def _resolve_with_sesame(name: str) -> Optional[Tuple[float, float]]:
# # #     """Resolve a target name to (RA, Dec) in degrees using CDS Sesame JSON.
# # #     Returns None if not found.
# # #     """
# # #     # See: https://cds.unistra.fr/cgi-bin/nph-sesame
# # #     base = "https://cds.unistra.fr/cgi-bin/nph-sesame/-oJ"
# # #     url = f"{base}?{urllib.parse.quote(name)}"
# # #     try:
# # #         with urllib.request.urlopen(url, timeout=15) as resp:
# # #             data = json.loads(resp.read().decode('utf-8', errors='replace'))
# # #     except Exception:
# # #         return None

# # #     # Sesame JSON can be nested variably. Search for keys 'jradeg'/'jdedeg'.
# # #     def _walk(obj):
# # #         if isinstance(obj, dict):
# # #             # Direct hit
# # #             if 'jradeg' in obj and 'jdedeg' in obj:
# # #                 yield obj['jradeg'], obj['jdedeg']
# # #             for v in obj.values():
# # #                 yield from _walk(v)
# # #         elif isinstance(obj, list):
# # #             for it in obj:
# # #                 yield from _walk(it)
# # #     for ra_deg, dec_deg in _walk(data):
# # #         try:
# # #             return float(ra_deg), float(dec_deg)
# # #         except Exception:
# # #             continue
# # #     return None


# # # def resolve_target(name: str) -> Tuple[float, float]:
# # #     from astroquery.simbad import Simbad
# # #     from astropy.table import Table
# # #     """Resolve a target name to ICRS (ra_deg, dec_deg).

# # #     Tries astroquery/Simbad first (if installed), then CDS Sesame JSON.
# # #     Raises ValueError if resolution fails.
# # #     """
# # #     # for resolver in (_resolve_with_astroquery, _resolve_with_sesame):
# # #     # for resolver in (_resolve_with_sesame):
# # #     _simbad = Simbad()
# # #     _simbad.add_votable_fields("dec", "dec", "otypes", "ids")
# # #     tbl: Table | None = _simbad.query_object(name)
# # #     # result = _resolve_with_sesame(name)
# # #     if tbl is not None:
# # #         ra,dec = tbl["ra"], tbl["dec"]
# # #         return ra,dec
# # #     else:
# # #         raise ValueError(f"Could not resolve target name: {name}")


# # # --- Helpers (geometry, queries, parsing) -------------------------------------

# # def _angular_sep_deg(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
# #     ra1r, dec1r, ra2r, dec2r = map(radians, (ra1, dec1, ra2, dec2))
# #     d_ra = ra2r - ra1r
# #     d_dec = dec2r - dec1r
# #     a = sin(d_dec/2)**2 + cos(dec1r) * cos(dec2r) * sin(d_ra/2)**2
# #     c = 2 * atan2(sqrt(a), sqrt(1-a))
# #     return c * 180.0 / 3.141592653589793


# # # def _resolve_target(target: Union[str, Tuple[float, float]]) -> Tuple[float, float]:
# # #     """Resolve a target name via SESAME or pass through (ra, dec)."""
# # #     if isinstance(target, (tuple, list)) and len(target) == 2:
# # #         return float(target[0]), float(target[1])
# # #     if isinstance(target, str):
# # #         url = SESAME_URL + urllib.parse.quote(target)
# # #         with urllib.request.urlopen(url) as resp:
# # #             xml_bytes = resp.read()
# # #         root = ET.fromstring(xml_bytes)
# # #         ra_el = root.find("//Resolver/jradeg")
# # #         dec_el = root.find("//Resolver/jdedeg")
# # #         if ra_el is None or dec_el is None:
# # #             raise ValueError(f"Could not resolve target name '{target}' via SESAME.")
# # #         return float(ra_el.text), float(dec_el.text)
# # #     raise TypeError("target must be a (ra, dec) tuple or a string target name")


# # def _build_ssa_query(
# #     ra: float,
# #     dec: float,
# #     radius_arcsec: float,
# #     *,
# #     maxrec: int = DEFAULT_MAXREC,
# #     extra_params: Optional[Dict[str, str]] = None,
# # ) -> str:
# #     size_deg = float(radius_arcsec) / 3600.0
# #     params = {
# #         "REQUEST": "queryData",
# #         "POS": f"{ra},{dec}",
# #         "SIZE": f"{size_deg}",
# #         "FORMAT": "application/x-votable+xml",
# #         "MAXREC": str(maxrec),
# #     }
# #     if extra_params:
# #         params.update({k.upper(): str(v) for k, v in extra_params.items()})
# #     return SSAP_ENDPOINT + "?" + urllib.parse.urlencode(params)


# # def _parse_votable(xml_bytes: bytes) -> List[Dict[str, str]]:
# #     root = ET.fromstring(xml_bytes)
# #     ns = {
# #         "v": "http://www.ivoa.net/xml/VOTable/v1.3",
# #         "v1": "http://www.ivoa.net/xml/VOTable/v1.2",
# #         "v2": "http://www.ivoa.net/xml/VOTable/v1.1",
# #         "v0": "http://www.ivoa.net/xml/VOTable/v1.0",
# #     }
# #     table = None
# #     for prefix in ("v", "v1", "v2", "v0", ""):
# #         path = f".//{prefix + ':' if prefix else ''}TABLE"
# #         table = root.find(path, ns) if ns else root.find(path)
# #         if table is not None:
# #             break
# #     if table is None:
# #         raise ValueError("No TABLE element found in VOTable response.")
# #     fields: List[str] = []
# #     for field in table.findall(".//v:FIELD", ns) or table.findall("FIELD"):
# #         name = field.get("name") or field.get("ID") or field.get("ucd") or "col"
# #         fields.append(name)
# #     data_rows: List[Dict[str, str]] = []
# #     for tr in table.findall(".//v:TR", ns) or table.findall("TR"):
# #         values = [td.text or "" for td in (tr.findall("v:TD", ns) or tr.findall("TD"))]
# #         if len(values) < len(fields):
# #             values += [""] * (len(fields) - len(values))
# #         row = {fields[i]: values[i] for i in range(min(len(fields), len(values)))}
# #         data_rows.append(row)
# #     return data_rows


# # def _ensure_outdir(outdir: str | os.PathLike) -> pathlib.Path:
# #     p = pathlib.Path(outdir).expanduser().resolve()
# #     p.mkdir(parents=True, exist_ok=True)
# #     return p


# # # --- Public API ----------------------------------------------------------------

# # def download_eso_ssa_spectra_for_target(
# #     target: str,
# #     radius_arcsec: float,
# #     *,
# #     min_snr: float = 0.0,
# #     outdir: str | os.PathLike = ".",
# #     maxrec: int = DEFAULT_MAXREC,
# #     overwrite: bool = False,
# #     name_by: str = "ssa_pubDID",
# #     extra_params: Optional[Dict[str, str]] = None,
# #     limit: Optional[int] = None,
# # ) -> List[pathlib.Path]:
# #     """Resolve a target name via Simbad/Sesame and download matching spectra."""
# #     ra_deg, dec_deg = SkyCoord.from_name(target)
# #     # ra_deg, dec_deg = resolve_target(target)
# #     return download_eso_ssa_spectra(
# #         (ra_deg, dec_deg), radius_arcsec,
# #         min_snr=min_snr,
# #         outdir=outdir,
# #         maxrec=maxrec,
# #         overwrite=overwrite,
# #         name_by=name_by,
# #         extra_params=extra_params,
# #         limit=limit,
# #     )


# # # --- Public API (existing) ----------------------------------------------------

# # def search_eso_ssa(
# #     ra: float,
# #     dec: float,
# #     radius_arcsec: float,
# #     *,
# #     maxrec: int = DEFAULT_MAXREC,
# #     extra_params: Optional[Dict[str, str]] = None,
# # ) -> List[Dict[str, str]]:
# #     url = _build_ssa_query(ra, dec, radius_arcsec, maxrec=maxrec, extra_params=extra_params)
# #     print(url)
# #     with urllib.request.urlopen(url) as resp:
# #         xml_bytes = resp.read()
# #     return _parse_votable(xml_bytes)


# # def download_eso_ssa_spectra(
# #     # target: Tuple[float, float],
# #     ra: float,
# #     dec: float,
# #     radius_arcsec: float,
# #     *,
# #     min_snr: float = 0.0,
# #     outdir: str | os.PathLike = ".",
# #     maxrec: int = DEFAULT_MAXREC,
# #     overwrite: bool = False,
# #     name_by: str = "ssa_pubDID",
# #     extra_params: Optional[Dict[str, str]] = None,
# #     limit: Optional[int] = None,
# # ) -> List[pathlib.Path]:
# #     # ra, dec = resolve_target(target)
# #     rows = search_eso_ssa(ra, dec, radius_arcsec, maxrec=maxrec, extra_params=extra_params)
# #     out = _ensure_outdir(outdir)
# #     downloaded: List[pathlib.Path] = []
# #     n_match = 0
# #     for row in rows:
# #         snr_val = None
# #         for key in ("SNR", "snr", "ssa_snr", "SsaSNR"):
# #             if key in row and row[key] not in (None, ""):
# #                 try:
# #                     snr_val = float(row[key])
# #                 except ValueError:
# #                     snr_val = None
# #                 break
# #         if snr_val is None or snr_val < float(min_snr):
# #             continue
# #         try:
# #             row_ra = float(row.get("s_ra") or row.get("RA") or row.get("ra") or "nan")
# #             row_dec = float(row.get("s_dec") or row.get("DEC") or row.get("dec") or "nan")
# #         except ValueError:
# #             continue
# #         sep_deg = _angular_sep_deg(ra, dec, row_ra, row_dec)
# #         if sep_deg * 3600.0 > radius_arcsec:
# #             continue
# #         access_url = row.get("access_url") or row.get("accessURL") or row.get("accessRef")
# #         if not access_url:
# #             continue
# #         base = row.get(name_by) or row.get("CREATORDID") or row.get("ssa_pubDID")
# #         if not base:
# #             base = urllib.parse.unquote(urllib.parse.urlparse(access_url).path.split("/")[-1])
# #         safe_base = base.replace(":", "_").replace("/", "_").replace(" ", "_")
# #         suffix = pathlib.Path(safe_base).suffix
# #         if not suffix:
# #             safe_base += ".fits"
# #         dest = out / safe_base
# #         if dest.exists() and not overwrite:
# #             downloaded.append(dest)
# #         else:
# #             tmp_dest, _ = urllib.request.urlretrieve(access_url, filename=str(dest))
# #             downloaded.append(pathlib.Path(tmp_dest))
# #         n_match += 1
# #         if limit is not None and n_match >= limit:
# #             break
# #     return downloaded


# # if __name__ == "__main__":
# #     import argparse
# #     p = argparse.ArgumentParser(description="Download ESO SSA spectra around a sky position or named target.")

# #     # Allow either a --target name OR explicit --ra/--dec
# #     # Optional: target name OR explicit coordinates
# #     p.add_argument("--target", help="Target name to resolve via Simbad/Sesame (e.g., 'V1324 Sco', 'M 31')")
# #     p.add_argument("--ra", type=float, help="Right Ascension in degrees (ICRS)")
# #     p.add_argument("--dec", type=float, help="Declination in degrees (ICRS)")


# #     p.add_argument("--radius", type=float, required=True, help="Search radius in arcseconds")
# #     p.add_argument("--min-snr", type=float, default=0.0, help="Minimum SNR to keep")
# #     p.add_argument("--outdir", default=".", help="Destination directory")
# #     p.add_argument("--maxrec", type=int, default=DEFAULT_MAXREC, help="MAXREC for SSA query")
# #     p.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
# #     p.add_argument("--name-by", default="ssa_pubDID", choices=["ssa_pubDID", "CREATORDID", "dp_id"], help="Which column to use for naming downloaded files")
# #     p.add_argument("--limit", type=int, default=None, help="Stop after this many files")

# #     args = p.parse_args()

# #     args = p.parse_args()

# # if args.target:
# #     # resolve name
# #     paths = download_eso_ssa_spectra_for_target(
# #         args.target,
# #         args.radius,
# #         min_snr=args.min_snr,
# #         outdir=args.outdir,
# #         maxrec=args.maxrec,
# #         overwrite=args.overwrite,
# #         name_by=args.name_by,
# #         limit=args.limit,
# #     )
# # elif args.ra is not None and args.dec is not None:
# #     paths = download_eso_ssa_spectra(args.ra, args.dec,
# #                                     args.radius,
# #                                     min_snr=args.min_snr,
# #                                     outdir=args.outdir,
# #                                     maxrec=args.maxrec,
# #                                     overwrite=args.overwrite,
# #                                     name_by=args.name_by,
# #                                     limit=args.limit,
# #                                     )
# # else:
# #     p.error("Either --target OR both --ra and --dec must be provided.")
