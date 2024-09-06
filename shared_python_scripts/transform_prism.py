import multiprocessing
from pathlib import Path
import os
import sys

from dask.distributed import Client, LocalCluster
import pandas as pd
import xarray as xr

from transform_nc import should_zarr_be_regenerated, perform_transformations


def print_usage():
    script_call_path = sys.argv[0]
    print(f"Usage: python {script_call_path} precip-4km|tmax-4km|tmin-4km")
    print(f"Example: python {script_call_path} prism/precip-4km/")


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print("Error: Script did not receive only one argument")
        print_usage()
        sys.exit(1)

    dataset = sys.argv[1]
    datatype_name = ""
    match dataset:
        case "precip-4km":
            datatype_name = "precip"
        case "tmax-4km" | "tmin-4km":
            datatype_name = "temp"
        case _:
            print("Error: Invalid dataset specified")
            print_usage()
            sys.exit(1)

    # Get path to directory with all netCDF files
    dataset_dir = (Path(__file__).parent.parent / "prism" / dataset).resolve()

    # Get all netCDF files
    nc_files = list(dataset_dir.glob("*.nc"))
    if not nc_files:
        print(f"Error: No .nc files found in {dataset_dir}")
        sys.exit(1)

    zarr_path = dataset_dir / f"{dataset}.zarr"
    # If Zarr store doesn't exist or any of the data is newer than the last time we created the zarr, generate the zarr
    if not should_zarr_be_regenerated(zarr_path, nc_files):
        print(f"Skipping generation of Zarr {zarr_path}, it is newer than .nc files")
        return

    # Set up a dask cluster with memory limits before computing
    cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit="16GB")
    dask_client = Client(cluster)

    try:
        years_set = set()
        year_to_latest_mtime = {}
        for nc_file in nc_files:
            # Ignore previously generated year files
            if len(nc_file.name) != 4:
                # Add to list of years
                nc_year = int(nc_file.name[0:4])
                years_set.add(nc_year)

                # Keep track of the latest mtimes wee see among the netCDF files, for each year
                nc_mtime = os.path.getmtime(nc_file)
                newest_mtime = (
                    year_to_latest_mtime[nc_year]
                    if nc_year in year_to_latest_mtime
                    else 0
                )

                if nc_mtime > newest_mtime:
                    year_to_latest_mtime[nc_year] = nc_mtime

        years = list(years_set)

        # Process year by year, and save to a netCDF file
        for year in years:
            # First, check if we need to regenerate the file at all
            year_nc_file_path = dataset_dir / f"{year}.nc"
            if year_nc_file_path.exists():
                year_mtime = os.path.getmtime(year_nc_file_path)
                latest_nc_mtime = year_to_latest_mtime[year]
                # If the year.nc file is newer, then we can skip regenerating the year.nc file
                if year_mtime > latest_nc_mtime:
                    print(f"Skipping generating {year}.nc for {dataset}")
                    continue

            # Generate a netCDF containing all the data for a year
            print(f"Combining all nc files for year {year}")
            year_nc_files = list(dataset_dir.glob(f"{year}-*.nc"))
            year_nc_files.sort()  # Ensures we add 01-02 after 01-01 since shell globbing returns random order
            year_datasets = []
            for nc_file in year_nc_files:
                ds = xr.open_dataset(nc_file)

                date_string = nc_file.name[0:10]
                date = pd.to_datetime(date_string)
                ds = ds.expand_dims(time=[date])

                if "crs" in ds:
                    ds = ds.drop_vars("crs")

                ds = ds.rename({"Band1": datatype_name})

                year_datasets.append(ds)

            year_nc_file_path = dataset_dir / f"{year}.nc"
            print(f"Saving {year_nc_file_path}")
            # Finally, concatenate the yearly nc files
            ds = xr.concat(year_datasets, dim="time")
            ds.to_netcdf(year_nc_file_path)

        # Redo nc_files to only be the yearly nc_files
        year_nc_files = list(dataset_dir.glob("????.nc"))
        ds = xr.open_mfdataset(
            year_nc_files,
            combine="by_coords",
            parallel=True,
        )
        ds, encoding = perform_transformations(ds)
        print(f"Writing zarr to {zarr_path}")
        with dask_client:
            ds.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)

    finally:
        dask_client.close()
        cluster.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
