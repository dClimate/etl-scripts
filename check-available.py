import click
from click import Context

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

from py_hamt import HAMT, IPFSStore
from multiformats import CID


@click.command
@click.option("--cid")
@click.pass_context
def check_available(ctx: Context, cid: str):
    hamt = HAMT(store=IPFSStore(), root_node_id=CID.decode(cid), read_only=True)
    ds = xr.open_zarr(store=hamt)
    print(ds)

    random_time = np.random.choice(ds.time)
    ds_slice = ds.sel(time=random_time)
    for var in ds.data_vars:
        ds_slice[var].plot()  # type: ignore
        plt.savefig(f"/tmp/plot-{cid}-{var}.png", dpi=300, bbox_inches="tight")


if __name__ == "__main__":
    check_available()
