import sys

import xarray as xr
import numcodecs
import numpy as np

from py_hamt import HAMT, IPFSStore
from multiformats import CID
from xarray.core.utils import V

if len(sys.argv) != 3:
    script_name = sys.argv[0]
    print(f"Usage: python {script_name} <variable_to_check> <cid>")
    sys.exit(1)

variable_to_check = sys.argv[1]
cid = CID.decode(sys.argv[2])

google_ds = xr.open_zarr(
    "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    chunks=None, # type: ignore
    storage_options=dict(token="anon"),
)
if variable_to_check not in google_ds:
    print(f"Variable {variable_to_check} is not in the Google Dataset")
    sys.exit(1)

google_da = google_ds[variable_to_check]
print(google_da)

hamt = HAMT(store=IPFSStore(), root_node_id=cid, read_only=True)
ipfs_ds = xr.open_zarr(store=hamt)
da = ipfs_ds[variable_to_check]
print(da)

# This inherently assumes that the dClimate ETL starts from the same timepoint beginning as the Google one
n_samples = int(len(da.time) * 0.0001)  # 0.01%
random_time_indices = np.random.choice(len(da), size=n_samples, replace=False)

google_subset = google_da.isel(time=random_time_indices)
da_subset = da.isel(time=random_time_indices)
print("Google subset")
print(google_subset)
print("IPFS data subset")
print(da_subset)
xr.testing.assert_identical(google_subset, da_subset)
