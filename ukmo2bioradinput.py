"""
ukmo2bioradinput.py
-------------------
Take a UKMO aggregated HDF5 file and split it into one output per pulse
type (`lp` and `sp`). Under each pulse type, the child groups are time
slots (e.g., 1700). For every pulse/time pair we copy that group's
contents into a standalone HDF5 with the hierarchy flattened to the
root and attributes preserved.

Output naming: <base>_<pulse>_<time>.h5 where <time> is the child group
name under the pulse type. By default outputs are written under
/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput,
mirroring the raw input tree, then adding a day folder (from the leading
date in the filename) and a pulse-type folder (lp/sp).
"""
import argparse
import h5py
import os
import sys
import re

DEFAULT_RAW_ROOT = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/raw_h5_data_final"
DEFAULT_OUT_ROOT = "/gws/ssde/j25a/ncas_radar/vol2/avocet/ukmo-nimrod/vol2birdinput"
DATASET_RE = re.compile(r"^dataset[0-9]+$")


def copy_group_contents_to_root(src: h5py.File, group_path: str, dst: h5py.File):
    """
    Copy everything under group_path into dst root, preserving attributes.
    This keeps the internal structure of the pulse/time group intact while
    flattening it to the destination root.
    """
    grp = src[group_path]
    # Preserve group-level attributes
    for attr, val in grp.attrs.items():
        dst.attrs[attr] = val
    # Copy all child datasets/groups as-is into the destination root
    for name, obj in grp.items():
        src.copy(obj, dst, name=name)


def process_pulse_type(src: h5py.File, pulse: str, base_name: str, output_dir: str):
    """
    Process one pulse-type group (lp/sp), emitting one file per time group.
    Child keys are assumed to be time codes (e.g., 1700).
    """
    if pulse not in src:
        return []
    child_keys = sorted(src[pulse].keys())
    expected_names = {f"{base_name}_{pulse}_{key}.h5" for key in child_keys}
    if os.path.isdir(output_dir):
        prefix = f"{base_name}_{pulse}_"
        for existing_name in os.listdir(output_dir):
            if (
                existing_name.startswith(prefix)
                and existing_name.endswith(".h5")
                and existing_name not in expected_names
            ):
                os.unlink(os.path.join(output_dir, existing_name))
                print(f"Removed stale pvol output: {os.path.join(output_dir, existing_name)}", file=sys.stderr)
    outputs = []
    for key in child_keys:
        group_path = f"{pulse}/{key}"
        out_name = f"{base_name}_{pulse}_{key}.h5"
        out_path = os.path.join(output_dir, out_name)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        group = src[group_path]
        if "dataset1" not in group or not any(DATASET_RE.match(name) for name in group.keys()):
            if os.path.exists(out_path):
                os.unlink(out_path)
            print(
                f"Skipped invalid pvol group without dataset1: {group_path}",
                file=sys.stderr,
            )
            continue
        tmp_path = os.path.join(
            os.path.dirname(out_path),
            f".{os.path.basename(out_path)}.{os.getpid()}.tmp",
        )
        try:
            with h5py.File(tmp_path, "w") as dst:
                copy_group_contents_to_root(src, group_path, dst)
                dst.flush()
            with open(tmp_path, "rb") as handle:
                os.fsync(handle.fileno())
            os.replace(tmp_path, out_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
        outputs.append(out_path)
    return outputs


def main():
    ap = argparse.ArgumentParser(
        description="Split lp/sp groups into single HDF5 files (one per child group) with contents copied to root."
    )
    ap.add_argument("-i", "--input", required=True, help="Input aggregated HDF5 file")
    ap.add_argument("-o", "--output-dir", help="Directory to write outputs (default: derived under output-root)")
    ap.add_argument("--raw-root", default=DEFAULT_RAW_ROOT, help=f"Raw tree root to mirror (default {DEFAULT_RAW_ROOT})")
    ap.add_argument("--output-root", default=DEFAULT_OUT_ROOT, help=f"Base output root (default {DEFAULT_OUT_ROOT})")
    args = ap.parse_args()

    input_file = args.input
    if not os.path.exists(input_file):
        print(f"Input file does not exist: {input_file}", file=sys.stderr)
        sys.exit(1)

    base_name = os.path.splitext(os.path.basename(input_file))[0]
    # Derive output directory: mirror input path under output-root, append day and pulse type
    if args.output_dir:
        base_output_dir = args.output_dir
    else:
        abs_in = os.path.abspath(input_file)
        abs_raw_root = os.path.abspath(args.raw_root)
        try:
            if os.path.commonpath([abs_in, abs_raw_root]) == abs_raw_root:
                rel_parent = os.path.relpath(os.path.dirname(abs_in), abs_raw_root)
            else:
                rel_parent = ""
        except ValueError:
            rel_parent = ""
        # Derive day folder from leading digits of filename (expected YYYYMMDD prefix)
        day = base_name.split("_")[0]
        base_output_dir = os.path.join(os.path.abspath(args.output_root), rel_parent, day)

    outputs = []
    with h5py.File(input_file, "r") as src:
        for pulse in ("lp", "sp"):
            # Each pulse type gets its own directory and one file per time key
            pulse_dir = os.path.join(base_output_dir, pulse)
            outputs.extend(process_pulse_type(src, pulse, base_name, pulse_dir))

    if not outputs:
        print("No lp/sp groups found; nothing written.", file=sys.stderr)
        sys.exit(2)
    for path in outputs:
        print(f"Wrote: {path}")


if __name__ == "__main__":
    main()
