import sys

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

from py_hamt import HAMT, IPFSStore
from multiformats import CID

if len(sys.argv) != 2:
    script_name = sys.argv[0]
    print(f"Usage: python {script_name} <cid>")
    sys.exit(1)

cid = CID.decode(sys.argv[1])
hamt = HAMT(store=IPFSStore(), root_node_id=cid, read_only=True)
ds = xr.open_zarr(store=hamt)
print(ds)

random_time = np.random.choice(ds.time)
ds_slice = ds.sel(time=random_time)
for var in ds.data_vars:
    ds_slice[var].plot()  # type: ignore
    plt.savefig("/tmp/plot.png", dpi=300, bbox_inches="tight")
