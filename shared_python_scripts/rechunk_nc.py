from pathlib import Path
import sys

import xarray as xr


def print_usage():
    script_call_path = sys.argv[0]
    print(f"Usage: python {script_call_path} <path to .nc file to rechunk>")
    print(f"Example: python {script_call_path} chirps/prelim-p25")


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print("Error: Script received more than one argument")
        print_usage()
        sys.exit(1)

    # Verify the path is a directory
    nc_file = Path(sys.argv[1])
    if not nc_file.exists():
        print(f"Error: No file found at path to netCDF file: {nc_file}")
        sys.exit(1)

    ds = xr.open_dataset(nc_file)

    # The choice of chunk length here is fairly arbitrary and is taken from typical values seen in CHIRPS
    # What matters is that the chunk sizing is consistent for datasets where ther is inconsistent
    chunk_size = {"time": 365, "latitude": 400, "longitude": 1440}
    print(f"Rechunking {nc_file}")
    ds_rechunked = ds.chunk(chunk_size)
    output_path = nc_file.with_stem(nc_file.stem + "-rechunked")
    print(f"Saving newly chunked netCDF to {output_path}")
    ds_rechunked.to_netcdf(output_path)


if __name__ == "__main__":
    main()
