import sys
from ipldstore import get_ipfs_mapper
import xarray as xr
from multiformats import CID

if len(sys.argv) != 2:
    script_name = sys.argv[0]
    print(f"Usage: python {script_name} <zarr_cid>")
    sys.exit(1)

zarr_cid = sys.argv[1]

ipld_store = get_ipfs_mapper(host="http://127.0.0.1:5001")

cid = CID.decode(zarr_cid)
ipld_store.set_root(cid)

ds = xr.open_zarr(ipld_store)

print(ds)
