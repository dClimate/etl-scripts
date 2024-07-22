import os
import subprocess
import sys
from glob import glob
import xarray as xr


def combine_nc_to_zarr(dataset_name: str):
    def get_latest_modification_time(files):
        return max(os.path.getmtime(file) for file in files)

    # Make sure the dataset files are available
    print("Making sure dataset files are available")
    subprocess.run(["sh", "download.sh", dataset_name], check=True)

    data_dir = f"./{dataset_name}"

    # Get the cpc files which are all netcdf files
    nc_files = glob(os.path.join(data_dir, "*.nc"))
    if not nc_files:
        print(f"no .nc files found in {data_dir}")
        return

    # Where we store our combined zarr for caching
    zarr_path = os.path.join(data_dir, f"{dataset_name}.zarr")

    # If Zarr store already exists and is cached, get its modification time
    zarr_mtime = os.path.getmtime(zarr_path) if os.path.exists(zarr_path) else 0

    # Get the most recent modification time of .nc files
    latest_nc_mtime = get_latest_modification_time(nc_files)

    # If Zarr store doesn't exist or any of the data is newer than the last time we created the zarr, generate the zarr
    if zarr_mtime < latest_nc_mtime:
        print(f"Generating/updating Zarr store for {dataset_name}")
        # Open all .nc files as a single dataset
        ds = xr.open_mfdataset(
            nc_files,
            combine="by_coords",
        )

        # Save to Zarr format
        ds.to_zarr(zarr_path, mode="w", consolidated=True)

        print(f"Combined dataset {dataset_name} saved to {zarr_path}")
    else:
        print(f"Zarr store for {dataset_name} is up to date. Skipping regeneration.")


def main():
    # First, change directory to the location of this file, which should be in the cpc folder, since everything else is relative from this
    # This way, this python script can be called from anywhere
    absolute_script_dir = os.path.dirname(os.path.abspath(__file__))
    print(absolute_script_dir)
    os.chdir(absolute_script_dir)

    # Skip the script name (sys.argv[0]), so start at index 1
    for arg in sys.argv[1:]:
        if arg == "precip-conus":
            print("Processing precip-conus dataset")
            combine_nc_to_zarr("precip-conus")
        elif arg == "precip-global":
            print("Processing precip-global dataset")
            combine_nc_to_zarr("precip-global")
        elif arg == "tmax":
            print("Processing tmax dataset")
            combine_nc_to_zarr("tmax")
        elif arg == "tmin":
            print("Processing tmin dataset")
            combine_nc_to_zarr("tmin")
        else:
            print(f"Unknown argument: {arg}", file=sys.stderr)


if __name__ == "__main__":
    main()
