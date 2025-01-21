from pathlib import Path

import click
import xarray as xr
from py_hamt import HAMT, IPFSStore
from multiformats import CID


@click.command()
@click.argument("cid")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
def ipfs(cid: str, gateway_uri_stem: str, rpc_uri_stem: str):
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
    print(ds)


@click.command
@click.argument(
    "path",
    type=click.Path(exists=True, readable=True, resolve_path=True, path_type=Path),
)
def disk(path: Path):
    """Check on a dataset xarray can read from disk."""
    ds = xr.open_dataset(path)
    print(ds)


@click.group
def cli():
    """Load a dataset from disk or IPFS and print it. This tool is mostly for verifying that something is properly readable and usable by xarray."""
    pass


cli.add_command(ipfs)
cli.add_command(disk)

if __name__ == "__main__":
    cli()
