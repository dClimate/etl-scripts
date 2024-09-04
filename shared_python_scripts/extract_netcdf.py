from pathlib import Path
from kerchunk import hdf
from msgspec import json


def extract_and_write_zarr_json(nc_path: Path):
    """Serializes a zarr json for a specified .nc file in the same directory.

    Parameters
    ----------
    nc_path
        The path to the .nc file to create a Zarr JSON for.
    """

    print(f"Converting {nc_path}")
    # Create the output JSON path, with suffix .nc.json
    json_path = nc_path.with_suffix(nc_path.suffix + ".json")
    print(f"Outputting to {json_path}")

    # Create a SingleHdf5ToZarr object
    h5_to_zarr = hdf.SingleHdf5ToZarr(str(nc_path))

    # Translate the NetCDF file to Zarr format
    zarr_dict = h5_to_zarr.translate()

    # Write the Zarr JSON to file
    with open(json_path, "wb") as f:
        json_bytes = json.encode(zarr_dict)
        f.write(json_bytes)

    print(f"Zarr JSON has been written to {json_path}")


if __name__ == "__main__":
    import sys

    script_call_path = sys.argv[0]

    def print_usage():
        print(f"Usage: python {script_call_path} path")
        print(
            f"Example: python {script_call_path} cpc/precip-conus/precip.V1.0.2007.nc"
        )

    # Check that there is only one argument
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print(
            f"Error: Script only takes 1 argument, was provided {num_arguments} arguments"
        )
        print_usage()
        sys.exit(1)

    input_path = sys.argv[1]
    nc_path = Path(input_path)
    extract_and_write_zarr_json(nc_path)
