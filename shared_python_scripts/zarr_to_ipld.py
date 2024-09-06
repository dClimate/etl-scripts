from pathlib import Path
import subprocess

from ipldstore import get_ipfs_mapper
import xarray as xr


def load_zarr_to_ipld(zarr_path: Path):
    ipld_store = get_ipfs_mapper(host="http://127.0.0.1:5001")
    ds = xr.open_zarr(zarr_path)

    print("Writing zarr to IPLD")
    ds.to_zarr(ipld_store, mode="w")

    print("Writing CID and unpinning old one if it exists")
    root_cid = ipld_store.freeze()
    cid_path = zarr_path.with_suffix(".cid")

    # Store the previous CID so that we can unpin it after writing the new CID
    previous_cid = None
    if cid_path.exists():
        with cid_path.open("r") as f:
            previous_cid = f.read().strip()

    # Write the CID of the HAMT root to a file
    with cid_path.open("w") as f:
        f.write(str(root_cid))
        f.write("\n")

    # Unpin old CID
    ipns_key_name_path = cid_path.parent / "ipns-key-name.txt"
    with ipns_key_name_path.open("r") as f:
        ipns_key_name = f.read()
        subprocess.run(
            [
                "ipfs-cluster-ctl",
                "pin",
                "add",
                "--name",
                ipns_key_name,
                str(root_cid),
            ],
            check=True,
        )

    # Unpin the old CID
    if previous_cid:
        print("Unpinning previous CID")
        try:
            subprocess.run(["ipfs-cluster-ctl", "pin", "rm", previous_cid], check=True)
        except:  # noqa E722
            pass


if __name__ == "__main__":
    import sys

    def print_usage():
        script_call_path = sys.argv[0]
        print(f"Usage: python {script_call_path} <path to zarr>")
        print(f"Example: python {script_call_path} cpc/precip-conus/precip-conus.zarr")

    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print(
            f"Error: Script received more than one argument, was provided {num_arguments} arguments"
        )
        print_usage()
        sys.exit(1)

    zarr_path = Path(sys.argv[1])
    if not zarr_path.is_dir():
        print(f"Zarr does not exist at {zarr_path}")
        print("Quitting zarr to ipld loading")
        sys.exit(1)

    load_zarr_to_ipld(zarr_path)
