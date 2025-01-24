import click

from pathlib import Path
import sys

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from py_hamt import HAMT, IPFSStore
from multiformats import CID


@click.command
@click.argument("cid")
@click.argument(
    "out-dir",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
def plot_random_point(cid: str, out_dir: Path):
    """
    Plot a random timepoint for each data variable in the zarr at the CID. The zarr should have a variable "time" for this to work.

    This script will save the files with a naming scheme of "plot-{cid}-{data_var}.png" inside the OUT_DIR, which is the output directory.
    """
    hamt = HAMT(store=IPFSStore(), root_node_id=CID.decode(cid), read_only=True)
    ds = xr.open_zarr(store=hamt)
    print(ds, file=sys.stderr)

    random_time = np.random.choice(ds.time)
    ds_slice = ds.sel(time=random_time)
    for data_var in ds.data_vars:
        ds_slice[data_var].plot()  # type: ignore
        plt.savefig(
            out_dir / f"plot-{cid}-{data_var}.png", dpi=300, bbox_inches="tight"
        )


if __name__ == "__main__":
    plot_random_point()
