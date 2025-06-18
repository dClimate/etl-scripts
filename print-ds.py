import code
import sys
from pathlib import Path

import asyncclick as click
import xarray as xr
from multiformats import CID

from etl_scripts.grabbag import eprint
from etl_scripts.hamt_store_contextmanager import ipfs_hamt_store


@click.command()
@click.argument("cid")
@click.option("-g", "--gateway-base-url", help="Pass through to KuboCAS")
@click.option("-r", "--rpc-base-url", help="Pass through to KuboCAS")
@click.option(
    "--print-latest-timestamps",
    default=0,
    show_default=True,
    type=click.IntRange(min=0),
    help="Print the latest time coordinate values. If 0 then just print the Dataset. Prints in order from the latest to earliest time coordinate value, assuming time coordinate ascending order. No guarantee on formatting in ISO8601, it just prints whatever xarray presents as the string value.",
)
@click.option(
    "--repl",
    is_flag=True,
    show_default=True,
    default=False,
    help="Drop into python repl after regular operation. You will have access to the xarray Dataset in a variable `ds`.",
)
async def ipfs(
    cid: str,
    gateway_base_url: str | None,
    rpc_base_url: str | None,
    print_latest_timestamps: int,
    repl: bool,
):
    """
    Set CID to the root of a HAMT from py-hamt, load the zarr into xarray, and print the Dataset.
    """

    async with ipfs_hamt_store(
        gateway_base_url, rpc_base_url, root_cid=CID.decode(cid)
    ) as (store, _):
        ds = xr.open_zarr(store=store)

    if print_latest_timestamps == 0:
        print(ds)
    else:
        if "time" not in ds:
            eprint("Error: Time coordinate does not exist in dataset")
            eprint(ds)
            sys.exit(1)
        l = len(ds.time)
        if l < print_latest_timestamps:
            eprint(
                f"Error: Time coordinate has {l} values in dataset, but {print_latest_timestamps} were requested"
            )
            eprint(ds)
            sys.exit(1)
        for i in range(0, print_latest_timestamps):
            print(ds["time"][l - 1 - i].values)

    if repl:
        code.interact(local=locals())


@click.command
@click.argument(
    "path",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
@click.option(
    "--repl",
    is_flag=True,
    show_default=True,
    default=False,
    help="Drop into python repl after regular operation. You will have access to the xarray Dataset in a variable ds.",
)
@click.option(
    "--grib-expver",
    is_flag=True,
    show_default=True,
    default=False,
    help="Load and supply xarray open_dataset with backend_kwargs={'read_keys': ['expver']}, and also print the value for each data variable. This is a variable set in ERA5 GRIB variables, but xarray has to be told to explicitly open it.",
)
def disk(path: Path, repl: bool, grib_expver: bool):
    """Check on a dataset xarray can read from disk."""
    if not grib_expver:
        ds = xr.open_dataset(path)
    else:
        ds = xr.open_dataset(path, backend_kwargs={"read_keys": ["expver"]})
    print(ds)

    if repl:
        code.interact(local=locals())


@click.group
def cli():
    """Load a dataset from disk or IPFS and print it. This tool is mostly for verifying that something is properly readable and usable by xarray."""
    pass


cli.add_command(ipfs)
cli.add_command(disk)

if __name__ == "__main__":
    cli()
