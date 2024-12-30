import sys

import xarray as xr
from py_hamt import HAMT, IPFSStore
from multiformats import CID

if len(sys.argv) != 2:
    script_name = sys.argv[0]
    print(f"Usage: python {script_name} <cid>")
    sys.exit(1)

cid = CID.decode(sys.argv[1])
# For reading from a remote gateway, e.g. "https://ipfs.io"
# hamt = HAMT(store=IPFSStore(gateway_uri_stem="https://ipfs.io"), root_node_id=cid)
hamt = HAMT(store=IPFSStore(), root_node_id=cid)
ds = xr.open_zarr(store=hamt)
print(ds)
