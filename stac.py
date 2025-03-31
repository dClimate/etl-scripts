"""
Dev note

This github repository has lots of useful and clear information about what should go in
https://github.com/radiantearth/stac-spec
"""

import json

import click
import pandas as pd
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, IPFSStore, IPFSZarr3

from etl_scripts.grabbag import eprint


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
@click.argument("cpc-precip-conus")
@click.argument("cpc-precip-global")
@click.argument("cpc-tmax")
@click.argument("cpc-tmin")
@click.argument("chirps-final-p05")
@click.argument("chirps-final-p25")
@click.argument("chirps-prelim-p05")
@click.argument("era5-2m_temperature")
@click.argument("era5-10m_u_component-of-wind")
@click.argument("era5-10m_v_component_of_wind")
@click.argument("era5-100m_u_component_of_wind")
@click.argument("era5-100m_v_component_of_wind")
@click.argument("era5-surface_pressure")
@click.argument("era5-surface_solar_radiation_downwards")
@click.argument("era5-total_precipitation")
@click.option("--gateway-uri-stem", help="Pass through to IPFSStore")
@click.option("--rpc-uri-stem", help="Pass through to IPFSStore")
def cli(
    cpc_precip_conus: str,
    cpc_precip_global: str,
    cpc_tmax: str,
    cpc_tmin: str,
    chirps_final_p05: str,
    chirps_final_p25: str,
    chirps_prelim_p05: str,
    era5_2m_temperature: str,
    era5_10m_u_component_of_wind: str,
    era5_10m_v_component_of_wind: str,
    era5_100m_u_component_of_wind: str,
    era5_100m_v_component_of_wind: str,
    era5_surface_pressure: str,
    era5_surface_solar_radiation_downwards: str,
    era5_total_precipitation: str,
    gateway_uri_stem: str | None,
    rpc_uri_stem: str | None,
):
    """
    Used for creating a STAC catalog of the datasets from etl-scripts, using the CID of each dataset, and uses that to generate the STAC.

    If a CID is 'null', then stac.py will ignore it entirely, and not generate the STAC item entry.
    """

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

    # Basic strategy is to generate each item, then the collection, then the catalog

    item_cids: dict[str, CID] = {}

    def save_to_ipfs(d: dict) -> CID:
        return ipfs_store.save_raw(json.dumps(d).encode())

    def save_item(id: str, ds_cid: str):
        if ds_cid == "null":
            return
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
                "assets": {},
            },
        )

    # CPC
    save_item("cpc-precip-conus", cpc_precip_conus)
    save_item("cpc-precip-global", cpc_precip_global)
    save_item("cpc-tmax", cpc_tmax)
    save_item("cpc-tmin", cpc_tmin)

    save_item("chirps-final-p05", chirps_final_p05)
    save_item("chirps-final-p25", chirps_final_p25)
    save_item("chirps-prelim-p05", chirps_prelim_p05)

    save_item("era5-2m_temperature", era5_2m_temperature)
    save_item("era5-10m_u_component_of_wind", era5_10m_u_component_of_wind)
    save_item("era5-10m_v_component_of_wind", era5_10m_v_component_of_wind)
    save_item("era5-100m_u_component_of_wind", era5_100m_u_component_of_wind)
    save_item("era5-100m_v_component_of_wind", era5_100m_v_component_of_wind)
    save_item("era5-surface_pressure", era5_surface_pressure)
    save_item(
        "era5-surface_solar_radiation_downwards", era5_surface_solar_radiation_downwards
    )
    save_item("era5-total_precipitation", era5_total_precipitation)

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
        "id": "CHIRPS",
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
    eprint(catalog)

    eprint("=== Catalog CID")
    print(catalog_cid)


if __name__ == "__main__":
    cli()
