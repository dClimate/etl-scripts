import random
from pathlib import Path

import click
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore, IPFSZarr3

from etl_scripts.grabbag import eprint


# Find random integer range sized big enough to cover the width
# If width is bigger than the da's length, this just returns the entire span
def rand_islice(da: xr.DataArray, width: int) -> slice:
    if len(da) <= width:
        return slice(0, len(da))

    # inclusive values
    start = random.randrange(0, len(da) - width + 1)
    end = start + width - 1

    # since the end of a slice is exclusive for xarray
    return slice(start, end + 1)


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
    hamt = HAMT(store=IPFSStore(), root_node_id=CID.decode(cid))
    ipfszarr3 = IPFSZarr3(hamt, read_only=True)
    ds = xr.open_zarr(store=ipfszarr3)
    eprint(ds)

    # rand i slice widths are based on the dClimate chunking scheme
    subset = ds.isel(
        latitude=rand_islice(ds.latitude, 25),
        longitude=rand_islice(ds.longitude, 25),
    )
    random_time = np.random.choice(ds.time)
    subset = subset.sel(time=random_time)

    for data_var in ds.data_vars:
        subset[data_var].plot()  # type: ignore
        plt.savefig(
            out_dir / f"plot-{cid}-{data_var}.png", dpi=300, bbox_inches="tight"
        )


if __name__ == "__main__":
    plot_random_point()
