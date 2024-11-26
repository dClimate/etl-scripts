from pathlib import Path
import subprocess

from py_hamt import HAMT, IPFSStore
import xarray as xr

def load_zarr_to_ipfs(zarr_path: Path):
    ds = xr.open_zarr(zarr_path)

    print(f"Writing zarr {zarr_path} to HAMT on IPLD")
    hamt = HAMT(store=IPFSStore())
    ds.to_zarr(hamt, mode="w")

    print("Writing CID")
    root_cid = hamt.root_node_id
    cid_path = zarr_path.with_suffix(".cid")
    with cid_path.open("w") as f:
        f.write(str(root_cid))
        f.write("\n")

    # Pin new CID across the cluster
    # ipns_key_name_path = cid_path.parent / "ipns-key-name.txt"
    # with ipns_key_name_path.open("r") as f:
    #     ipns_key_name = f.read()
    #     subprocess.run(
    #         [
    #             "ipfs-cluster-ctl",
    #             "pin",
    #             "add",
    #             "--name",
    #             ipns_key_name,
    #             str(root_cid),
    #         ],
    #         check=True,
    #     )

    # Unpin the old CID
    # if previous_cid:
    #     print("Unpinning previous CID")
    #     try:
    #         subprocess.run(["ipfs-cluster-ctl", "pin", "rm", previous_cid], check=True)
    #     except:  # noqa E722
    #         pass


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
