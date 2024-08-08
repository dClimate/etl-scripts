import os
import sys
from glob import glob
from ipldstore import get_ipfs_mapper
import xarray as xr
import subprocess


def zarr_to_ipld(dataset_name: str):
    # Get all zarr directories
    data_dir = f"./{dataset_name}"
    zarr_dirs = glob(os.path.join(data_dir, "*.zarr"))
    if not zarr_dirs:
        print(f"No zarr directories found in {data_dir}")
        return

    # There should only be one zarr, so just get the first element from the array
    zarr_path = zarr_dirs[0]
    # Path to our zarr file, and name of the file containing the CID
    cid_path = f"{zarr_path}.hamt.cid"

    # Check if the CID needs to be regenerated if the zarr is newer
    zarr_mtime = os.path.getmtime(zarr_path)
    cid_mtime = os.path.getmtime(cid_path) if os.path.exists(cid_path) else 0

    # If our CID is newer than our Zarr data, don't process this and just exit'
    if cid_mtime > zarr_mtime:
        print(
            f"Zarr {zarr_path} is older than last generated CID {cid_path}, skipping sending to IPLD"
        )
        return

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
        return

    # Create a new Zarr dataset in the IPLDStore
    try:
        ds.to_zarr(ipld_store, mode="w", consolidated=True)
    except Exception as e:
        print(f"Error: Unable to write dataset to IPLDStore: {e}")
        return

    # Freeze the current state of the HAMT and get the root CID
    root_cid = ipld_store.freeze()

    # Store the previous CID so that we can tell IPFS daemon to unpin it once done creating the new CID
    previous_cid = None
    if os.path.exists(cid_path):
        with open(cid_path, "r") as file:
            previous_cid = file.read().strip()

    # Write the CID of the HAMT to a file
    # This will also implicitly pin the new root of the HAMT
    with open(cid_path, "w") as file:
        file.write(str(root_cid))

    # Pin the new CID
    subprocess.run(
        [
            "ipfs-cluster-ctl",
            "pin",
            "add",
            "--name",
            f"chirps-{dataset_name}",
            str(root_cid),
        ],
        check=True,
    )

    # Unpin the old CID
    if previous_cid:
        print("Unpinning previous CID")
        # Don't check the result of this, in case the old CID was already unpinned
        subprocess.run(["ipfs-cluster-ctl", "pin", "rm", previous_cid], check=False)

    # Cleanup the intermediate IPFS objects left over from unused HAMT nodes on this ipfs node
    print("Performing IPFS garbge collection")
    try:
        subprocess.run(
            ["ipfs", "repo", "gc"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print("IPFS garbage collection completed successfully.")
    except subprocess.CalledProcessError as e:
        print(
            f"Error: IPFS garbage collection failed with exit code {e.returncode}",
            file=sys.stderr,
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
