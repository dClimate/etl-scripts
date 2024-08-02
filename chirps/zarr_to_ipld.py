import os
import sys
from glob import glob
from ipldstore import get_ipfs_mapper
import xarray as xr


def zarr_to_ipld(dataset_name: str):
    # Get all zarr directories
    data_dir = f"./{dataset_name}"
    zarr_dirs = glob(os.path.join(data_dir, "*.zarr"))
    if not zarr_dirs:
        print(f"No zarr directories found in {data_dir}")
        return

    for zarr_path in zarr_dirs:
        # Path to our zarr file, and name of our CAR file
        cid_path = f"{zarr_path}.hamt.cid"

        # Check if the CAR file needs to be regenerated
        zarr_mtime = os.path.getmtime(zarr_path)
        car_mtime = os.path.getmtime(cid_path) if os.path.exists(cid_path) else 0

        if zarr_mtime > car_mtime:
            print(f"Using {zarr_path} to create {cid_path}")

            # Create an IPLDStore instance
            ipld_store = get_ipfs_mapper(host="http://127.0.0.1:5001")

            # Open the Zarr dataset using xarray
            try:
                ds = xr.open_zarr(zarr_path)
            except Exception as e:
                print(
                    f"Error: Unable to open Zarr dataset at {zarr_path} due to exception: {e}"
                )
                continue

            # Create a new Zarr dataset in the IPLDStore
            try:
                ds.to_zarr(ipld_store, mode="w", consolidated=True)
            except Exception as e:
                print(f"Error: Unable to write dataset to IPLDStore: {e}")
                continue

            # Freeze the current state of the HAMT and get the root CID
            root_cid = ipld_store.freeze()

            # Write the HAMT CID to a file
            with open(cid_path, "w") as file:
                file.write(str(root_cid))

        else:
            print(
                f"CID {cid_path} for {zarr_path} is up to date. Skipping regeneration."
            )


def main():
    # First, change directory to the location of this file, which should be in the cpc folder, since everything else is relative from this
    # This way, this python script can be called from anywhere
    absolute_script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(absolute_script_dir)

    # Skip the script name (sys.argv[0]), so start at index 1
    for arg in sys.argv[1:]:
        if arg in ["final-p05", "final-p25", "prelim-p05"]:
            dataset_name = arg
            print(f"Converting {dataset_name}.zarr to HAMT")
            zarr_to_ipld(dataset_name)
        else:
            print(f"Unknown argument: {arg}", file=sys.stderr)


if __name__ == "__main__":
    main()
