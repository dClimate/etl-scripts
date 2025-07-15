# stac.py
import json
import pprint
import sys
from pathlib import Path
from typing import Literal
import click
from multiformats import CID
from py_hamt import KuboCAS
from era5.stac import generate_era5_stac
from etl_scripts.grabbag import eprint
import asyncio

def epp(d: dict):
    pprint.pp(d, stream=sys.stderr)

async def gen_async(rpc_uri_stem: str | None, gateway_uri_stem: str | None):
    """
    Creates a STAC catalog with ERA5 and placeholder collections for other datasets.

    Args:
        rpc_uri_stem: URI stem for Kubo RPC API.
        gateway_uri_stem: Optional URI stem for IPFS gateway.

    Returns:
        CID of the root catalog.
    """
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
        async def save_to_ipfs(d: dict) -> CID:
            """Save a dictionary to IPFS using KuboCAS.save."""
            data = json.dumps(d).encode()
            return await kubo_cas.save(data, codec="dag-json")

        # Generate ERA5 collection
        era5_cid = await generate_era5_stac(
            Path("era5/cids.json"),
            rpc_uri_stem,
            gateway_uri_stem
        )

        # Placeholder collections (to be implemented)
        # cpc_cid = await generate_cpc_stac(
        #     Path("cpc/cids.json"),
        #     rpc_uri_stem,
        #     gateway_uri_stem
        # )
        # chirps_cid = await generate_chirps_stac(
        #     Path("chirps/cids.json"),
        #     rpc_uri_stem,
        #     gateway_uri_stem
        # )
        # prism_cid = await generate_prism_stac(
        #     Path("prism/cids.json"),
        #     rpc_uri_stem,
        #     gateway_uri_stem
        # )

        # Generate root catalog
        catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "dClimate-data-catalog",
            "description": "This catalog contains dClimate's data.",
            "links": [
                {
                    "rel": "child",
                    "href": {"/": str(era5_cid)},
                    "type": "application/json",
                    "title": "ERA5",
                },
                # {
                #     "rel": "child",
                #     "href": {"/": str(cpc_cid)},
                #     "type": "application/json",
                #     "title": "CPC",
                # },
                # {
                #     "rel": "child",
                #     "href": {"/": str(chirps_cid)},
                #     "type": "application/json",
                #     "title": "CHIRPS",
                # },
                # {
                #     "rel": "child",
                #     "href": {"/": str(prism_cid)},
                #     "type": "application/json",
                #     "title": "PRISM",
                # },
            ],
        }

        catalog_cid = await save_to_ipfs(catalog)

        eprint("=== Catalog")
        epp(catalog)
        eprint("=== Catalog CID")
        print(catalog_cid)
        return catalog_cid

async def collect_async(
    type: str,
    catalog_cid: str,
    plain: bool,
    search: str | None,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
):
    """
    Print a JSON with the CIDs for all STAC collections, sub-collections, item JSONs, or dataset sharded-zarr roots to stdout.
    """
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
        async def read_from_ipfs(cid: str) -> dict:
            data = await kubo_cas.load(CID.decode(cid))
            return json.loads(data)

        def format_and_print(d: dict):
            if search is not None:
                if search in d:
                    print(d[search])
                return
            if plain:
                for id, cid in d.items():
                    print(f"{id} {cid}")
            else:
                print(json.dumps(d, indent=4, sort_keys=True))

        catalog = await read_from_ipfs(catalog_cid)
        catalog_json_out = {catalog["id"]: catalog_cid}

        collections = []
        collections_json_out = {}
        for link in catalog["links"]:
            cid = link["href"]["/"]
            collection = await read_from_ipfs(cid)
            collections_json_out[collection["id"]] = cid
            collections.append(collection)

        if type == "collection":
            format_and_print(collections_json_out)
            return

        sub_collections = []
        sub_collections_json_out = {}
        for collection in collections:
            if collection["id"] == "ERA5":
                for link in collection["links"]:
                    if link["rel"] == "child":
                        cid = link["href"]["/"]
                        sub_collection = await read_from_ipfs(cid)
                        sub_collections_json_out[sub_collection["id"]] = cid
                        sub_collections.append(sub_collection)

        if type == "sub-collection":
            format_and_print(sub_collections_json_out)
            return

        items_json_out = {}
        for collection in collections + sub_collections:
            for link in collection["links"]:
                if link["rel"] == "item":
                    cid = link["href"]["/"]
                    item = await read_from_ipfs(cid)
                    items_json_out[item["id"]] = cid

        if type == "item":
            format_and_print(items_json_out)
            return

        sharded_zarr_roots = {}
        for id, item_json_cid in items_json_out.items():
            item_json = await read_from_ipfs(item_json_cid)
            sharded_zarr_root = item_json["assets"]["sharded-zarr"]["href"][6:]
            sharded_zarr_roots[id] = sharded_zarr_root

        if type == "sharded-zarr-root":
            format_and_print(sharded_zarr_roots)
            return

# Synchronous wrappers for Click commands
@click.command
@click.option("--gateway-uri-stem", help="Pass through to KuboCAS")
@click.option(
    "--rpc-uri-stem",
    help="Stem of url for Kubo RPC API http endpoint. Also used for KuboCAS.",
    default="http://127.0.0.1:5001",
)
def gen(gateway_uri_stem: str | None, rpc_uri_stem: str | None):
    """Creates a STAC catalog with ERA5 and placeholder collections for other datasets."""
    asyncio.run(gen_async(rpc_uri_stem, gateway_uri_stem))

@click.command
@click.argument("type", type=click.Choice(["collection", "sub-collection", "item", "sharded-zarr-root"]))
@click.argument("catalog-cid")
@click.option(
    "--plain",
    is_flag=True,
    show_default=True,
    default=False,
    help="Print just names with CIDs after them, with newlines in between.",
)
@click.option(
    "--search",
    help="Find a specific id and print its value within the normal output. If nothing is found, this will print nothing. This search term is case-sensitive.",
)
@click.option("--gateway-uri-stem", help="Pass through to KuboCAS")
@click.option("--rpc-uri-stem", help="Pass through to KuboCAS", default="http://127.0.0.1:5001")
def collect(
    type: str,
    catalog_cid: str,
    plain: bool,
    search: str | None,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
):
    """Print a JSON with the CIDs for all STAC collections, sub-collections, item JSONs, or dataset sharded-zarr roots to stdout."""
    asyncio.run(collect_async(type, catalog_cid, plain, search, gateway_uri_stem, rpc_uri_stem))

@click.group
def cli():
    """Tools for creating and navigating the dClimate data catalog STAC."""
    pass

cli.add_command(gen)
cli.add_command(collect)

if __name__ == "__main__":
    cli()