# stac.py
import json
from pathlib import Path
import pandas as pd
import xarray as xr
from multiformats import CID
from py_hamt import ShardedZarrStore, KuboCAS
from .utils import eprint, npdt_to_pydt

async def gen_geometry(ds: xr.Dataset) -> dict:
    """Generate GeoJSON geometry for a dataset."""
    top = float(ds.latitude.values[-1])
    bottom = float(ds.latitude.values[0])
    left = float(ds.longitude.values[0])
    right = float(ds.longitude.values[-1])
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

async def generate_era5_stac(
    cids_path: Path,
    rpc_uri_stem: str | None,
    gateway_uri_stem: str | None = None,
) -> CID:
    """
    Generate STAC metadata for ERA5 datasets, with a sub-collection per variable.

    Args:
        cids_path: Path to cids.json file tracking dataset CIDs.
        rpc_uri_stem: URI stem for Kubo RPC API.
        gateway_uri_stem: Optional URI stem for IPFS gateway.

    Returns:
        CID of the ERA5 parent collection.
    """
    async with KuboCAS(rpc_base_url=rpc_uri_stem, gateway_base_url=gateway_uri_stem) as kubo_cas:
        # Read CIDs
        with open(cids_path, "r") as f:
            cids_data = json.load(f).get("era5", {})

        async def save_to_ipfs(d: dict) -> CID:
            """Save a dictionary to IPFS using KuboCAS.save."""
            data = json.dumps(d).encode()
            return await kubo_cas.save(data, codec="dag-json")

        # Generate STAC items
        item_cids = {}
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        variable_cids = {}
        for variable, cids in cids_data.items():
            variable_collection = {
                "type": "Collection",
                "stac_version": "1.0.0",
                "id": f"{variable}",
                "description": f"ERA5 and ERA5T datasets for {variable}",
                "license": "noassertion",
                "extent": {
                    "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                    "temporal": {"interval": [["1940-01-01T00:00:00Z", "null"]]},
                },
                "links": [],
            }

            finalization_date = cids.get("finalization_date", "null")
            for status in ["finalized", "non-finalized"]:
                cid = cids.get(status, "null")
                if cid == "null":
                    continue
                try:
                    store = await ShardedZarrStore.open(cas=kubo_cas, read_only=True, root_cid=CID.decode(cid))
                    ds = xr.open_zarr(store)
                    item_id = f"{status}"
                    item_properties = {
                        "datetime": "null",
                        "start_datetime": pd.Timestamp(ds.time.values[0]).strftime(time_format),
                        "end_datetime": pd.Timestamp(ds.time.values[-1]).strftime(time_format),
                        "finalization_status": status,
                    }
                    if status == "finalized" and finalization_date != "null":
                        item_properties["finalization_date"] = finalization_date
                        item_properties["end_datetime"] = finalization_date
                    elif status == "non-finalized" and finalization_date != "null":
                        item_properties["start_datetime"] = (
                            pd.Timestamp(finalization_date) + pd.Timedelta(hours=1)
                        ).strftime(time_format)

                    item_cids[item_id] = await save_to_ipfs({
                        "stac_version": "1.0.0",
                        "type": "Feature",
                        "id": item_id,
                        "geometry": await gen_geometry(ds),
                        "bbox": [
                            float(ds.longitude.values[0]),
                            float(ds.latitude.values[0]),
                            float(ds.longitude.values[-1]),
                            float(ds.latitude.values[-1]),
                        ],
                        "properties": item_properties,
                        "links": [],
                        "assets": {"sharded-zarr": {"href": f"{cid}"}},
                    })
                    variable_collection["links"].append({
                        "rel": "item",
                        "href": {"/": str(item_cids[item_id])},
                        "type": "application/json",
                        "title": item_id,
                    })
                    ds.close()
                except Exception as e:
                    eprint(f"Warning: Could not process dataset {item_id} with CID {cid}: {e}")

            # Save variable sub-collection if it has items
            if variable_collection["links"]:
                variable_cids[f"{variable}"] = await save_to_ipfs(variable_collection)

        # Generate ERA5 parent collection
        era5_collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "ERA5",
            "description": "ERA5 and ERA5T reanalysis datasets",
            "license": "noassertion",
            "extent": {
                "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                "temporal": {"interval": [["1940-01-01T00:00:00Z", "null"]]},
            },
            "links": [],
        }

        # Add variable sub-collections to ERA5 collection
        for variable_id in variable_cids:
            era5_collection["links"].append({
                "rel": "child",
                "href": {"/": str(variable_cids[variable_id])},
                "type": "application/json",
                "title": variable_id,
            })

        # Save ERA5 parent collection to IPFS
        era5_cid = await save_to_ipfs(era5_collection)
        return era5_cid