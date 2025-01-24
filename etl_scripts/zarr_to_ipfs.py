from pathlib import Path

import xarray as xr
from py_hamt import HAMT, IPFSStore


def load_zarr_to_ipfs(zarr_path: Path):
    ds = xr.open_zarr(zarr_path)

    print(f"Writing zarr {zarr_path} to HAMT on IPFS")
    hamt = HAMT(store=IPFSStore())
    ds.to_zarr(store=hamt, mode="w")

    print("Writing CID")
    root_cid = hamt.root_node_id
    cid_path = zarr_path.with_suffix(".cid")
    with cid_path.open("w") as f:
        f.write(str(root_cid))
        f.write("\n")


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

    load_zarr_to_ipfs(zarr_path)
