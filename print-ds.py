import code
import sys
from pathlib import Path

import click
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore

from etl_scripts.grabbag import eprint


@click.command()
@click.argument("cid")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
@click.option(
    "--print-latest-timestamps",
    default=0,
    show_default=True,
    type=click.IntRange(min=0),
    help="Print the latest time coordinate values. If 0 then just print the Dataset. Prints in order from the latest time coordinate value to the most recent, assuming time coordinate is in ascending order. No guarantee on formatting in ISO8601, it just prints whatever xarray presents as the string value.",
)
@click.option(
    "--repl",
    is_flag=True,
    show_default=True,
    default=False,
    help="Drop into python repl after regular operation. You will have access to the xarray Dataset in a variable `ds`.",
)
def ipfs(
    cid: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
    print_latest_timestamps: int,
    repl: bool,
):
    """
    Set CID to the root of a HAMT from py-hamt, load the zarr into xarray, and print the Dataset.
    """
    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem

    hamt = HAMT(store=ipfs_store, root_node_id=CID.decode(cid), read_only=True)
    ds = xr.open_zarr(store=hamt)
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
def disk(path: Path, repl: bool):
    """Check on a dataset xarray can read from disk."""
    ds = xr.open_dataset(path)
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
