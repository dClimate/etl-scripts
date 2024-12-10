import os
from pathlib import Path
import sys

import numpy as np
import xarray as xr
import numcodecs


def fix_fill_missing_value(ds: xr.Dataset) -> tuple[xr.Dataset, dict]:
    for var in ds.data_vars:
        fill_value = ds[var].encoding.get("_FillValue")
        missing_value = ds[var].attrs.get("missing_value")
        # If both are present and different, prefer _FillValue
        if (
            fill_value is not None
            and missing_value is not None
            and fill_value != missing_value
        ):
            print(
                f"Warning: {var} has different _FillValue and missing_value. Using _FillValue."
            )

        # Use _FillValue if present, otherwise use missing_value
        actual_fill_value = fill_value if fill_value is not None else missing_value

        if actual_fill_value is not None:
            # Replace fill value with NaN for floating point data
            if np.issubdtype(ds[var].dtype, np.floating):
                ds[var] = ds[var].where(ds[var] != actual_fill_value, np.nan)
                ds[var].encoding["_FillValue"] = np.nan
            else:
                # For non-floating point data, we keep the original fill value
                ds[var].encoding["_FillValue"] = actual_fill_value

        # Remove missing_value attribute to avoid conflicts
        ds[var].attrs.pop("missing_value", None)

    # Ensure consistent encoding across all variables
    encoding = {
        var: {"_FillValue": ds[var].encoding.get("_FillValue", np.nan)}
        for var in ds.data_vars
    }

    return (ds, encoding)


def compress_all_vars(ds: xr.Dataset):
    """Doesn't return the Dataset since this mutates the Dataset."""
    for var in ds.data_vars.keys():
        ds[var].encoding["compressor"] = numcodecs.Blosc()


def exit_if_zarr_uptodate(zarr_path: Path, nc_files: list[Path]):
    # If Zarr store already exists and is cached, get its modification time
    zarr_mtime = os.path.getmtime(zarr_path) if os.path.exists(zarr_path) else 0

    # Get the most recent modification time of .nc files
    latest_nc_mtime = max(os.path.getmtime(file) for file in nc_files)

    zarr_newer = zarr_mtime > latest_nc_mtime
    if zarr_newer:
        print(f"Skipping generation of Zarr {zarr_path}, it is newer than .nc files")
        sys.exit(0)


def eprint(out: str):
    print(out, file=sys.stderr)


def print_usage():
    script_call_path = sys.argv[0]
    eprint(f"Usage: python {script_call_path} <path to directory containing .nc files>")
    eprint(f"Example: python {script_call_path} cpc/precip-conus/")


def check_only_one_argument():
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        eprint("Error: Script did not receive only one argument")
        print_usage()
        sys.exit(1)


def validate_nc_dir_path(path_arg: str) -> Path:
    """Returns the parsed path if it actually exists"""
    nc_dir_path = Path(path_arg)
    if not nc_dir_path.is_dir():
        eprint(f"Error: Directory {nc_dir_path} does not exist")
        sys.exit(1)

    return nc_dir_path
