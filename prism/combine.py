from pathlib import Path
import os
import sys

import pandas as pd
import xarray as xr


def print_usage():
    script_call_path = sys.argv[0]
    print(f"Usage: python {script_call_path} precip-4km|tmax-4km|tmin-4km")
    print(f"Example: python {script_call_path} prism/precip-4km/")


def is_dataset_valid(dataset: str):
    match dataset:
        case "precip-4km" | "tmax-4km" | "tmin-4km":
            return True
        case _:
            return False


def get_available_years(nc_files: list[Path]) -> list[int]:
    years_set = set()
    for nc_file in nc_files:
        # Ignore YYYY.nc files
        if len(nc_file.stem) != 4:
            nc_year = int(nc_file.stem[0:4])
            years_set.add(nc_year)

    return list(years_set)


def find_latest_mtime_per_year(nc_files: list[Path]):
    year_to_latest_mtime = {}
    for nc_file in nc_files:
        # Ignore YYYY.nc files
        if len(nc_file.stem) != 4:
            nc_year = int(nc_file.stem[0:4])

            nc_mtime = os.path.getmtime(nc_file)

            previous_latest_mtime = 0
            if nc_year in year_to_latest_mtime:
                previous_latest_mtime = year_to_latest_mtime[nc_year]

            if nc_mtime > previous_latest_mtime:
                year_to_latest_mtime[nc_year] = nc_mtime

    return year_to_latest_mtime


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print("Error: Script did not receive only one argument")
        print_usage()
        sys.exit(1)

    dataset = sys.argv[1]
    if not is_dataset_valid(dataset):
        print("Error: Invalid dataset specified")
        print_usage()
        sys.exit(1)

    dataset_to_datatype = {
        "precip-4km": "precip",
        "tmax-4km": "temp",
        "tmin-4km": "temp",
    }
    datatype_name = dataset_to_datatype[dataset]

    dataset_dir = (Path(__file__).parent / dataset).resolve()
    nc_files = list(dataset_dir.glob("*.nc"))
    if not nc_files:
        print(f"Error: No .nc files found in {dataset_dir}")
        sys.exit(1)

    year_to_latest_mtime = find_latest_mtime_per_year(nc_files)
    years = get_available_years(nc_files)

    for year in years:
        # If all the daily data files are older than the pre-existing year.nc file, then we can skip regenerating the year.nc file
        year_nc_file_path = dataset_dir / f"{year}.nc"
        if year_nc_file_path.exists():
            year_mtime = os.path.getmtime(year_nc_file_path)
            latest_nc_mtime = year_to_latest_mtime[year]

            if year_mtime > latest_nc_mtime:
                print(f"Skipping generating {year}.nc for {dataset}")
                continue

        print(f"Combining to create {year}.nc")
        year_nc_files = list(dataset_dir.glob(f"{year}-*.nc"))
        year_nc_files.sort()  # Ensures we add 01-02 after 01-01 since shell globbing returns random order
        year_datasets = []
        for nc_file in year_nc_files:
            ds = xr.open_dataset(nc_file)

            date_string = nc_file.name[0:10]
            date = pd.to_datetime(date_string)
            ds = ds.expand_dims(time=[date])

            # Drop a useless variable
            if "crs" in ds:
                ds = ds.drop_vars("crs")

            # Band1 is the name for the actual data we want
            ds = ds.rename({"Band1": datatype_name})

            year_datasets.append(ds)

        # Combine across the time dimension
        ds = xr.concat(year_datasets, dim="time")

        year_nc_file_path = dataset_dir / f"{year}.nc"
        print(f"Saving {year_nc_file_path}")
        ds.to_netcdf(year_nc_file_path)


if __name__ == "__main__":
    main()
