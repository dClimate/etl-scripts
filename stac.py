"""
Dev note

This github repository has lots of useful and clear information about what should go in
https://github.com/radiantearth/stac-spec
"""

import json
import pprint
import sys
from pathlib import Path
from typing import Literal

import click
import pandas as pd
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore, IPFSZarr3

from etl_scripts.grabbag import eprint


# Pretty print a dict to stderr
def epp(d: dict):
    pprint.pp(d, stream=sys.stderr)


# Returns a GeoJSON as a dict
def gen_geometry(ds: xr.Dataset) -> dict:
    top = str(ds.latitude.values[-1])
    bottom = str(ds.latitude.values[0])
    left = str(ds.longitude.values[0])
    right = str(ds.longitude.values[-1])
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [right, bottom],
                [right, top],
                [left, top],
                [left, bottom],
                [right, bottom],
            ]
        ],
    }


@click.command
@click.argument(
    "stac-input-path",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
def gen(
    stac_input_path: Path,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
):
    """
    Creates a STAC catalog of the datasets from etl-scripts, using the CID of each dataset.

    These CIDs should be written to a JSON file, see the `stac-gen-input-template.json` file for what this should look like. For manual use, it is advised to create a copy named stac-gen-input.json and fill cids out in there since that is in the gitignore.

    If a CID is 'null', then stac.py will ignore it entirely, and not generate the STAC item entry.
    """
    stac_input: dict[str, str]
    with open(stac_input_path, "r") as f:
        stac_input = json.load(f)

    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem

    def open_ds(cid: str) -> xr.Dataset:
        return xr.open_zarr(
            store=IPFSZarr3(
                HAMT(store=ipfs_store, root_node_id=CID.decode(cid)), read_only=True
            )
        )

    def save_to_ipfs(d: dict) -> CID:
        return ipfs_store.save_raw(json.dumps(d).encode())

    # Generate the items, then collections, and finally the catalog

    item_cids: dict[str, CID] = {}
    for id in stac_input:
        ds_cid = stac_input[id]
        if ds_cid == "null":
            continue
        ds = open_ds(ds_cid)
        # e.g. "2023-09-25T17:47:10Z"
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        item_cids[id] = save_to_ipfs(
            {
                "stac_version": "1.0.0",
                "type": "Feature",
                "id": id,
                # An example correspondence between what a bbox and geometry should look like
                #   "bbox": [-179.06275, -89.27671, 179.99975, 89.27671],
                # "geometry": "{\"type\": \"Polygon\", \"coordinates\": [[[179.99975, -89.27671], [179.99975, 89.27671], [-179.06275, 89.27671], [-179.06275, -89.27671], [179.99975, -89.27671]]]}",
                #
                "geometry": str(gen_geometry(ds)),
                "bbox": [
                    str(ds.longitude.values[0]),
                    str(ds.latitude.values[0]),
                    str(ds.longitude.values[-1]),
                    str(ds.latitude.values[-1]),
                ],
                "properties": {
                    "datetime": "null",  # the spec states that "null is allowed, but requires start_datetime and end_datetime from common metadata to be set."
                    "start_datetime": pd.Timestamp(ds.time.values[0]).strftime(  # type: ignore strftime is definitely a pandas.Timestamp method
                        time_format
                    ),
                    "end_datetime": pd.Timestamp(ds.time.values[-1]).strftime(  # type: ignore strftime is definitely a pandas.Timestamp method
                        time_format
                    ),
                },
                # links is impossible since we cannot know the CID of the parent or this very own item ahead of time
                "links": [],
                "assets": {"hamt-zarr": {
                    "href": f"/ipfs/{ds_cid}"
                }},
            },
        )

    # Collections
    def add_links_collection(collection: dict, item_ids: list[str]):
        for item_id in item_ids:
            # Check since if the script is called with a null CID, that dataset isn't even in the item_cids dict
            if item_id in item_cids:
                cid = item_cids[item_id]
                collection["links"].append(
                    {
                        "rel": "item",
                        "href": f"/ipfs/{cid}",
                        "type": "application/json",
                        "title": item_id,
                    }
                )

    collection_cids: dict[str, CID] = dict()

    cpc_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "CPC",
        "description": "",
        "license": "noassertion",
        "extent": {
            "spatial": {"bbox": [[-180, 90, 180, 90]]},
            "temporal": {"interval": [["1979-01-01T00:00:00Z", "null"]]},
        },
        "links": [],
    }
    add_links_collection(
        cpc_collection,
        ["cpc-precip-conus", "cpc-precip-global", "cpc-tmax", "cpc-tmin"],
    )
    if len(cpc_collection["links"]) > 0:
        collection_cids["CPC"] = save_to_ipfs(cpc_collection)

    chirps_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "CHIRPS",
        "description": "",
        "license": "noassertion",
        "extent": {
            "spatial": {"bbox": [[-180, 90, 180, 90]]},
            "temporal": {"interval": [["1981-01-01T00:00:00Z", "null"]]},
        },
        "links": [],
    }
    add_links_collection(
        chirps_collection, ["chirps-final-p05", "chirps-final-p25", "chirps-prelim-p05"]
    )
    if len(chirps_collection["links"]) > 0:
        collection_cids["CHIRPS"] = save_to_ipfs(chirps_collection)

    era5_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "ERA5",
        "description": "",
        "license": "noassertion",
        "extent": {
            "spatial": {"bbox": [[-180, 90, 180, 90]]},
            "temporal": {"interval": [["1940-01-01T00:00:00Z", "null"]]},
        },
        "links": [],
    }
    add_links_collection(
        era5_collection,
        [
            "era5-2m_temperature",
            "era5-10m_u_component_of_wind",
            "era5-10m_v_component_of_wind",
            "era5-100m_u_component_of_wind",
            "era5-100m_v_component_of_wind",
            "era5-surface_pressure",
            "era5-surface_solar_radiation_downwards",
            "era5-total_precipitation",
        ],
    )
    if len(era5_collection["links"]) > 0:
        collection_cids["ERA5"] = save_to_ipfs(era5_collection)

    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "dClimate-data-catalog",
        "description": "This catalog contains dClimate's data.",
        "links": [],
    }

    def add_links_catalog(catalog: dict, collection_ids: list[str]):
        for collection_id in collection_ids:
            if collection_id in collection_cids:
                cid = collection_cids[collection_id]
                catalog["links"].append(
                    {
                        "rel": "child",
                        "href": f"/ipfs/{cid}",
                        "type": "application/json",
                        "title": collection_id,
                    }
                )

    add_links_catalog(catalog, ["CPC", "CHIRPS", "ERA5"])

    catalog_cid = save_to_ipfs(catalog)

    eprint("=== Catalog")
    epp(catalog)

    eprint("=== Catalog CID")
    print(catalog_cid)


@click.command
@click.argument("type", type=click.Choice(["collection", "item", "all"]))
@click.argument("catalog-cid")
@click.option(
    "--plain",
    is_flag=True,
    show_default=True,
    default=False,
    help="Print just the CIDs stdout, with newlines in between.",
)
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
def collect(
    type: Literal["collection"] | Literal["item"] | Literal["all"],
    catalog_cid: str,
    plain: bool,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
):
    """
    Print a JSON with the CIDs for all STAC collections, or items to stdout. If set to "all", this will also print the catalog CID, the collections, and the items.
    """
    ipfs_store = IPFSStore()
    if gateway_uri_stem is not None:
        ipfs_store.gateway_uri_stem = gateway_uri_stem
    if rpc_uri_stem is not None:
        ipfs_store.rpc_uri_stem = rpc_uri_stem

    def read_from_ipfs(cid: str) -> dict:
        return json.loads(ipfs_store.load(CID.decode(cid)))

    catalog = read_from_ipfs(catalog_cid)
    catalog_json_out = {catalog["id"]: catalog_cid}

    collections = []
    collections_json_out = {}
    for link in catalog["links"]:
        cid = link["href"][6:]  # [6:] removes the /ipfs/ at the beginning of the href
        collection = read_from_ipfs(cid)
        collections_json_out[collection["id"]] = cid
        collections.append(collection)

    items_json_out = {}
    for collection in collections:
        for link in collection["links"]:
            cid = link["href"][6:]
            item = read_from_ipfs(cid)

            # type must be item to reach here so don't do a check
            items_json_out[item["id"]] = cid

    json_out = {}
    if type == "all":
        json_out = catalog_json_out | collections_json_out | items_json_out
    elif type == "collection":
        json_out = collections_json_out
    elif type == "item":
        json_out = items_json_out

    if plain:
        for id in json_out:
            cid = json_out[id]
            print(cid)
    else:
        pprint.pp(json_out)


@click.group
def cli():
    """Tools for creating and navigating the dClimate data catalog STAC."""
    pass


cli.add_command(gen)
cli.add_command(collect)

if __name__ == "__main__":
    cli()
