
"""
fix_odim_vol2bird_strict.py
---------------------------
Make an ODIM PVOL HDF5 file *strictly* vol2bird-compatible, producing
both a dual-pol (keeps ZDR/RHOHV/PHIDP) and a single-pol (DBZH+VRADH)
variant. Files are rewritten with old HDF5 compatibility and a classic
ODIM header (H5rad 2.2). By default outputs mirror the raw file
structure from /gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/raw_h5_data_final
into /gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/vol2bird_dualpol
and /gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/vol2bird_singlepol.

What it does:
- Writes a fresh HDF5 with libver='earliest' (better compatibility).
- Root /what: object=PVOL, version=H5rad 2.2, date/time from source,
  source="RAD:<rad_code>,PLC:<lat>,<lon>,NOD:<nod_code>",
  Conventions="ODIM_H5/V2_2".
- Root /where: lat/lon/height as float64 (like many working PVOLs).
- Each /datasetX:
  * /what: object=SCAN, product=SCAN, copies date/time and start/end.
  * /where: casts required attributes to the right types and ensures
    data arrays are oriented (nrays, nbins).
  * /how: NI (from source how:nyquist_velocity if present, else default),
           polarization dualpol/singlepol.
  * data fields are decoded using existing gain/offset (if any) and
    re-encoded to robust integer encodings:
      - DBZH (data1) -> uint16, gain=0.1, offset=0, nodata=65535, undetect=0
      - VRADH (data5) -> int16, gain=1, offset=0, nodata=undetect=-32768
        duplicated to data2 as VRADH
      - Dual-pol fields (dual mode only):
         ZDR (data8)   -> uint16, gain=0.01, offset=-100
         RHOHV (data9) -> uint16, gain=1/4095, offset=0, clipped to [0,1]
         PHIDP (data10)-> uint16, gain=0.01, offset=0, modulo 360 to [0,360)
        Also duplicated into classic slots:
         data3=RHOHV, data4=ZDR, data6=PHIDP

Usage
-----
python fix_odim_vol2bird_strict.py \
  -i /path/to/input.h5 \
  --out-dual /path/to/output_dual.h5 \
  --out-single /path/to/output_single.h5 \
  --raw-root /raw/root/to/mirror \
  --dual-root /base/dual/output/root \
  --single-root /base/single/output/root \
  --nyquist 32 \
  --rad-code NIMROD05 --nod-code NIMROD05

If --out-dual / --out-single are not given, filenames are derived by
mirroring the input's path under raw-root into the dual/single roots
and appending suffixes; if the input is outside raw-root they are
written alongside the input.

Requires: Python 3.8+, h5py, numpy
"""
import argparse
import os
import sys
import numpy as np
import h5py

DEFAULT_RAW_ROOT = "/gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/raw_h5_data_final"
DEFAULT_DUAL_ROOT = "/gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/vol2bird_dualpol"
DEFAULT_SINGLE_ROOT = "/gws/nopw/j04/ncas_radar_vol2/avocet/ukmo-nimrod/vol2bird_singlepol"

def _dec(x, default=""):
    if x is None:
        return default
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("ascii", "ignore")
        except Exception:
            return default
    return str(x)

def _as_ascii(x: str):
    return np.string_(str(x))

def _decode(arr: np.ndarray, what_grp: h5py.Group, default_gain=1.0, default_offset=0.0):
    g = float(what_grp.attrs.get("gain", default_gain))
    o = float(what_grp.attrs.get("offset", default_offset))
    return arr.astype(np.float32) + 0.0 if (g == 1.0 and o == 0.0) else (arr.astype(np.float32) * g + o)

def _encode_uint16(decoded: np.ndarray, gain: float, offset: float):
    code = np.rint((decoded - offset) / gain)
    return np.clip(code, 0, 65535).astype(np.uint16)

def _encode_int16(decoded: np.ndarray, gain: float, offset: float):
    code = np.rint((decoded - offset) / gain)
    return np.clip(code, -32768, 32767).astype(np.int16)

def _ensure_shape(arr: np.ndarray, nrays: int, nbins: int) -> np.ndarray:
    if arr.shape == (nbins, nrays):
        return arr.T
    if arr.shape == (nrays, 1):
        return np.repeat(arr, nbins, axis=1)
    if arr.shape == (1, nrays):
        return np.repeat(arr, nbins, axis=0).T
    return arr

def _int32(v): return np.int32(int(v))
def _float32(v): return np.float32(float(v))

def _root_latlonheight(src: h5py.File):
    lat = lon = height = 0.0
    if "where" in src:
        la = src["where"].attrs.get("lat", None)
        lo = src["where"].attrs.get("lon", None)
        he = src["where"].attrs.get("height", None)
        lat = float(la) if la is not None else 0.0
        lon = float(lo) if lo is not None else 0.0
        height = float(he) if he is not None else 0.0
    return lat, lon, height

def _derive_dt(src: h5py.File):
    # prefer root what date/time; else dataset1 what
    date = time = None
    if "what" in src:
        date = _dec(src["what"].attrs.get("date", None), None)
        time = _dec(src["what"].attrs.get("time", None), None)
    if (not date or not time) and "dataset1" in src:
        ww = src["dataset1"]["what"]
        date = _dec(ww.attrs.get("date", date or "19700101"))
        time = _dec(ww.attrs.get("time", time or "000000"))
    if not date: date = "19700101"
    if not time: time = "000000"
    return date, time

def _dataset_start_end(src_ds_what: h5py.Group, fallback_date: str, fallback_time: str):
    sd = _dec(src_ds_what.attrs.get("startdate", fallback_date))
    st = _dec(src_ds_what.attrs.get("starttime", fallback_time))
    ed = _dec(src_ds_what.attrs.get("enddate", sd))
    et = _dec(src_ds_what.attrs.get("endtime", st))
    return sd, st, ed, et

def _ensure_ds_where(dst_where: h5py.Group, src_where: h5py.Group, nrays_guess=None, nbins_guess=None):
    # copy or default attrs, cast types
    # integer attrs
    for k in ["nbins", "nrays", "a1gate"]:
        if k in src_where.attrs:
            dst_where.attrs.create(k, _int32(src_where.attrs[k]))
        else:
            # fallback guesses or zeros
            if k == "nbins" and nbins_guess is not None:
                dst_where.attrs.create(k, _int32(nbins_guess))
            elif k == "nrays" and nrays_guess is not None:
                dst_where.attrs.create(k, _int32(nrays_guess))
            else:
                dst_where.attrs.create(k, _int32(0))
    # float attrs
    for k in ["rscale", "rstart", "elangle", "startaz", "stopaz"]:
        if k in src_where.attrs:
            dst_where.attrs.create(k, _float32(src_where.attrs[k]))
        else:
            dst_where.attrs.create(k, _float32(0.0))
    # azimuth dataset (if present)
    if "azimuth" in src_where:
        az = np.asarray(src_where["azimuth"][()], dtype=np.float32)
        dst_where.create_dataset("azimuth", data=az)

def _write_field(dst_parent: h5py.Group, name: str, qty: str, data: np.ndarray,
                 dtype, gain: float, offset: float, nodata: float, undetect: float):
    g = dst_parent.create_group(name)
    w = g.create_group("what")
    w.attrs.create("quantity", _as_ascii(qty))
    w.attrs.create("gain", float(gain))
    w.attrs.create("offset", float(offset))
    w.attrs.create("nodata", float(nodata))
    w.attrs.create("undetect", float(undetect))
    g.create_dataset("data", data=data.astype(dtype))
    return g

def _process(in_path: str, out_path: str, keep_dual: bool, nyquist_default: float, rad_code: str, nod_code: str):
    with h5py.File(in_path, "r") as src, h5py.File(out_path, "w", libver="earliest", track_order=True) as dst:
        # Root /where
        lat, lon, height = _root_latlonheight(src)
        r_where = dst.create_group("where")
        r_where.attrs.create("lat", float(lat))
        r_where.attrs.create("lon", float(lon))
        r_where.attrs.create("height", float(height))

        # Root /what
        date, time = _derive_dt(src)
        source_str = f"RAD:{rad_code},PLC:{lat:.3f},{lon:.3f},NOD:{nod_code}"
        r_what = dst.create_group("what")
        r_what.attrs.create("object", _as_ascii("PVOL"))
        r_what.attrs.create("version", _as_ascii("H5rad 2.2"))
        r_what.attrs.create("date", _as_ascii(date))
        r_what.attrs.create("time", _as_ascii(time))
        r_what.attrs.create("source", _as_ascii(source_str))
        r_what.attrs.create("Conventions", _as_ascii("ODIM_H5/V2_2"))

        # Root /how
        nyq = nyquist_default
        if "how" in src and "nyquist_velocity" in src["how"].attrs:
            try:
                nyq = float(src["how"].attrs["nyquist_velocity"])
            except Exception:
                nyq = nyquist_default
        r_how = dst.create_group("how")
        r_how.attrs.create("nyquist_velocity", float(nyq))

        # Datasets
        idx = 1
        while f"dataset{idx}" in src:
            sds = src[f"dataset{idx}"]
            dds = dst.create_group(f"dataset{idx}")

            # dataset what
            dsw = dds.create_group("what")
            src_w = sds["what"]
            # base date/time + start/end
            dsw.attrs.create("object", _as_ascii("SCAN"))
            dsw.attrs.create("product", _as_ascii("SCAN"))
            d_date = _dec(src_w.attrs.get("date", date), date)
            d_time = _dec(src_w.attrs.get("time", time), time)
            dsw.attrs.create("date", _as_ascii(d_date))
            dsw.attrs.create("time", _as_ascii(d_time))
            sd, st, ed, et = _dataset_start_end(src_w, d_date, d_time)
            dsw.attrs.create("startdate", _as_ascii(sd))
            dsw.attrs.create("starttime", _as_ascii(st))
            dsw.attrs.create("enddate", _as_ascii(ed))
            dsw.attrs.create("endtime", _as_ascii(et))

            # dataset where
            dwr = dds.create_group("where")
            # guess dims from first present field
            nb_guess = nr_guess = None
            for cand in ["data1","data5","data8","data9","data10"]:
                if cand in sds and "data" in sds[cand]:
                    shape = sds[cand]["data"].shape
                    # Decide which is nrays/nbins: we expect either (nrays, nbins) or (nbins, nrays)
                    # Heuristic: nrays ~ 360, nbins > nrays typically.
                    if len(shape) == 2:
                        a, b = shape
                        if a in (360, 361) or a < b:
                            nr_guess, nb_guess = a, b
                        else:
                            nr_guess, nb_guess = b, a
                    break
            _ensure_ds_where(dwr, sds["where"], nr_guess, nb_guess)
            nrays = int(dwr.attrs.get("nrays"))
            nbins = int(dwr.attrs.get("nbins"))

            # dataset how
            dh = dds.create_group("how")
            # Prefer dataset NI if present
            NI = None
            if "how" in sds and "NI" in sds["how"].attrs:
                try:
                    NI = float(sds["how"].attrs["NI"])
                except Exception:
                    NI = None
            if NI is None:
                NI = nyq
            dh.attrs.create("NI", float(NI))
            dh.attrs.create("polarization", _as_ascii("dualpol" if keep_dual else "singlepol"))

            # Helpers to process fields
            def read_and_orient(sgroup):
                arr = sgroup["data"][()]
                return _ensure_shape(arr, nrays, nbins)

            # --- DBZH (data1) ---
            if "data1" in sds and "data" in sds["data1"]:
                a = read_and_orient(sds["data1"])
                dec = _decode(a, sds["data1"]["what"], 1.0, 0.0)
                enc = _encode_uint16(dec, 0.1, 0.0)
                _write_field(dds, "data1", "DBZH", enc, np.uint16, 0.1, 0.0, 65535.0, 0.0)

            # --- VRADH (data5) + duplicate to data2 ---
            vr_enc = None
            if "data5" in sds and "data" in sds["data5"]:
                a = read_and_orient(sds["data5"])
                dec = _decode(a, sds["data5"]["what"], 1.0, 0.0)
                vr_enc = _encode_int16(dec, 1.0, 0.0)
                _write_field(dds, "data5", "VRADH", vr_enc, np.int16, 1.0, 0.0, -32768.0, -32768.0)
                # duplicate to data2
                _write_field(dds, "data2", "VRADH", vr_enc, np.int16, 1.0, 0.0, -32768.0, -32768.0)

            # --- Dual-pol (only if keep_dual=True) ---
            zdr_enc = rho_enc = ph_enc = None
            if keep_dual and "data8" in sds and "data" in sds["data8"]:
                a = read_and_orient(sds["data8"])
                dec = _decode(a, sds["data8"]["what"], 1.0, 0.0)
                zdr_enc = _encode_uint16(dec, 0.01, -100.0)
                _write_field(dds, "data8", "ZDR", zdr_enc, np.uint16, 0.01, -100.0, 65535.0, 0.0)
            if keep_dual and "data9" in sds and "data" in sds["data9"]:
                a = read_and_orient(sds["data9"])
                dec = _decode(a, sds["data9"]["what"], 1.0, 0.0)
                dec = np.clip(dec, 0.0, 1.0)
                rho_enc = _encode_uint16(dec, 1.0/4095.0, 0.0)
                _write_field(dds, "data9", "RHOHV", rho_enc, np.uint16, 1.0/4095.0, 0.0, 65535.0, 0.0)
            if keep_dual and "data10" in sds and "data" in sds["data10"]:
                a = read_and_orient(sds["data10"])
                dec = _decode(a, sds["data10"]["what"], 1.0, 0.0)
                dec = np.mod(dec, 360.0)
                ph_enc = _encode_uint16(dec, 0.01, 0.0)
                _write_field(dds, "data10", "PHIDP", ph_enc, np.uint16, 0.01, 0.0, 65535.0, 0.0)

            # Classic duplicates
            if keep_dual and rho_enc is not None:
                _write_field(dds, "data3", "RHOHV", rho_enc, np.uint16, 1.0/4095.0, 0.0, 65535.0, 0.0)
            if keep_dual and zdr_enc is not None:
                _write_field(dds, "data4", "ZDR", zdr_enc, np.uint16, 0.01, -100.0, 65535.0, 0.0)
            if keep_dual and ph_enc is not None:
                _write_field(dds, "data6", "PHIDP", ph_enc, np.uint16, 0.01, 0.0, 65535.0, 0.0)

            idx += 1

def build_outputs(in_path: str, out_dual: str, out_single: str, nyquist: float, rad_code: str, nod_code: str):
    os.makedirs(os.path.dirname(os.path.abspath(out_dual)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(out_single)), exist_ok=True)
    _process(in_path, out_dual, keep_dual=True, nyquist_default=nyquist, rad_code=rad_code, nod_code=nod_code)
    _process(in_path, out_single, keep_dual=False, nyquist_default=nyquist, rad_code=rad_code, nod_code=nod_code)
    print("Wrote:", out_dual)
    print("Wrote:", out_single)

def derive_default_names(in_path: str, raw_root: str, dual_root: str, single_root: str):
    base = os.path.splitext(os.path.basename(in_path))[0]
    dname = f"{base}_dualpol_fixed_strict.h5"
    sname = f"{base}_singlepol_fixed_strict.h5"

    abs_in = os.path.abspath(in_path)
    abs_raw_root = os.path.abspath(raw_root)
    rel_dir = None
    try:
        if os.path.commonpath([abs_in, abs_raw_root]) == abs_raw_root:
            rel_dir = os.path.relpath(os.path.dirname(abs_in), abs_raw_root)
    except ValueError:
        rel_dir = None
    # If the input is not under raw_root, fall back to writing beside input
    if rel_dir is None or rel_dir.startswith(".."):
        out_dual = os.path.join(os.path.dirname(abs_in), dname)
        out_single = os.path.join(os.path.dirname(abs_in), sname)
    else:
        out_dual = os.path.join(os.path.abspath(dual_root), rel_dir, dname)
        out_single = os.path.join(os.path.abspath(single_root), rel_dir, sname)
    return out_dual, out_single

def main():
    ap = argparse.ArgumentParser(description="Make ODIM PVOL vol2bird-ready (strict, earliest HDF5), producing dual-pol and single-pol outputs.")
    ap.add_argument("-i", "--input", required=True, help="Input ODIM PVOL .h5 file")
    ap.add_argument("--out-dual", help="Output dual-pol file path")
    ap.add_argument("--out-single", help="Output single-pol file path")
    ap.add_argument("--raw-root", default=DEFAULT_RAW_ROOT, help=f"Root of raw HDF5 tree to mirror for default outputs (default {DEFAULT_RAW_ROOT})")
    ap.add_argument("--dual-root", default=DEFAULT_DUAL_ROOT, help=f"Base output directory for dual-pol defaults (default {DEFAULT_DUAL_ROOT})")
    ap.add_argument("--single-root", default=DEFAULT_SINGLE_ROOT, help=f"Base output directory for single-pol defaults (default {DEFAULT_SINGLE_ROOT})")
    ap.add_argument("--nyquist", type=float, default=32.0, help="Default Nyquist velocity if missing (m/s)")
    ap.add_argument("--rad-code", default="NIMROD05", help="RAD code used in root what/source (default NIMROD05)")
    ap.add_argument("--nod-code", default="NIMROD05", help="NOD code used in root what/source (default NIMROD05)")
    args = ap.parse_args()

    in_path = args.input
    if not os.path.exists(in_path):
        print("Input file does not exist:", in_path, file=sys.stderr)
        sys.exit(1)
    if not args.out_dual or not args.out_single:
        default_dual, default_single = derive_default_names(
            in_path,
            raw_root=args.raw_root,
            dual_root=args.dual_root,
            single_root=args.single_root,
        )
        out_dual = args.out_dual or default_dual
        out_single = args.out_single or default_single
    else:
        out_dual = args.out_dual
        out_single = args.out_single

    build_outputs(in_path, out_dual, out_single, nyquist=args.nyquist, rad_code=args.rad_code, nod_code=args.nod_code)

if __name__ == "__main__":
    main()
