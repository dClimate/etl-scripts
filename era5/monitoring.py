# This script verifies that a random selection of the Zarr on IPFS is identical to the cloud storage source
import os

import xarray as xr
from py_hamt import HAMT, IPFSStore
from multiformats import CID

google_ds = xr.open_zarr(
    "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    chunks=None,  # type: ignore
    storage_options=dict(token="anon"),
)
print(google_ds)

cid_env: str | None = os.getenv("ERA5_TOTAL_PRECIPITATION_CID")
if cid_env is None:
    print("Error: no CID found through the environment")
cid_str: str = cid_env  # type: ignore
hamt = HAMT(store=IPFSStore(), read_only=False, root_node_id=CID.decode(cid_str))
ds = xr.open_zarr(store=hamt)
print(ds)
