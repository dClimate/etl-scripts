import multiprocessing
import os
from pathlib import Path
import sys

from dask.distributed import Client, LocalCluster
import numpy as np
import xarray as xr
import numcodecs


def normalize_longitudes(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))

    # After converting, the longitudes may still start at zero. This reorders the longitude coordinates from -180
    # to 180 if necessary.
    ds.sortby(["latitude", "longitude"])

    return ds


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


def print_usage():
    script_call_path = sys.argv[0]
    print(
        f"Usage: python {script_call_path} <path to directory containing .nc files> <whether or not to fix fill values: true|false>"
    )
    print(f"Example: python {script_call_path} cpc/precip-conus/ false")
    print(f"Example: python {script_call_path} chirps/final-p25/ true")


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 2:
        print("Error: Script did not receive two arguments")
        print_usage()
        sys.exit(1)

    # Get path to directory with all netCDF files
    nc_directory_path = Path(sys.argv[1])
    if not nc_directory_path.is_dir():
        print(f"Error: Directory {nc_directory_path} does not exist!")
        sys.exit(1)

    # Get all netCDF files
    nc_files = list(nc_directory_path.glob("*.nc"))
    if not nc_files:
        print(f"Error: No .nc files found in {nc_directory_path}")
        sys.exit(1)

    should_fix_fill_missing_arg = sys.argv[2]
    should_fix_fill_missing = False
    match should_fix_fill_missing_arg:
        case "true":
            should_fix_fill_missing = True
        case "false":
            should_fix_fill_missing = False
        case _:
            print(
                "Error: Argument can only be 'true' or 'false' for whether or not to fix fill values"
            )
            print_usage()
            sys.exit(1)

    dataset_name = nc_directory_path.stem
    zarr_path = nc_directory_path / f"{dataset_name}.zarr"

    # If Zarr store already exists and is cached, get its modification time
    zarr_mtime = os.path.getmtime(zarr_path) if os.path.exists(zarr_path) else 0

    # Get the most recent modification time of .nc files
    latest_nc_mtime = max(os.path.getmtime(file) for file in nc_files)

    # If Zarr store doesn't exist or any of the data is newer than the last time we created the zarr, generate the zarr
    if zarr_mtime >= latest_nc_mtime:
        print(f"Skipping generation of Zarr {zarr_path}, it is newer than .nc files")
        return

    # Set up a dask cluster with memory limits before computing
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="6GB")
    dask_client = Client(cluster)

    try:
        ds = xr.open_mfdataset(
            nc_files,
            combine="by_coords",
            parallel=True,
        )

        # Rename dimensions if needed
        if "lat" in ds.dims:
            ds = ds.rename({"lat": "latitude"})
        if "lon" in ds.dims:
            ds = ds.rename({"lon": "longitude"})

        ds = normalize_longitudes(ds)

        # Apply compression to all data variables
        data_vars = list(ds.data_vars.keys())
        for var in data_vars:
            ds[var].encoding["compressor"] = numcodecs.Blosc()

        # Rechunk
        chunk_sizes = {"time": "auto", "latitude": "auto", "longitude": "auto"}
        ds = ds.chunk(chunk_sizes)

        if should_fix_fill_missing:
            ds, encoding = fix_fill_missing_value(ds)

        print(f"Writing zarr to {zarr_path}")
        with dask_client:
            if should_fix_fill_missing:
                ds.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)
            else:
                ds.to_zarr(zarr_path, mode="w", consolidated=True)

    finally:
        dask_client.close()
        cluster.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
