from pathlib import Path
import sys

from dask.distributed import Client, LocalCluster
from msgspec import json
import xarray as xr
import numcodecs


def normalize_longitudes(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180))

    # After converting, the longitudes may still start at zero. This reorders the longitude coordinates from -180
    # to 180 if necessary.
    return ds.sortby(["latitude", "longitude"])


def compress(ds: xr.Dataset, variables: list[str]) -> xr.Dataset:
    for var in variables:
        ds[var].encoding["compressor"] = numcodecs.Blosc()

    return ds


def rename_dimensions(ds: xr.Dataset, names: dict[str, str]) -> xr.Dataset:
    return ds.rename(names)


def perform_transformations(ds: xr.Dataset) -> xr.Dataset:
    if "lat" in ds.dims:
        ds = rename_dimensions(ds, {"lat": "latitude"})
    if "lon" in ds.dims:
        ds = rename_dimensions(ds, {"lon": "longitude"})

    ds = normalize_longitudes(ds)
    # Apply compression to all data variables
    data_vars = list(ds.data_vars.keys())
    ds = compress(ds, data_vars)
    # Set chunk sizes to be determined automatically
    ds = ds.chunk({"time": "auto", "latitude": "auto", "longitude": "auto"})

    return ds


def open_multizarr(multizarr_json_path: Path) -> xr.Dataset:
    with multizarr_json_path.open("rb") as f:
        multizarr_json = json.decode(f.read())

    # Open using kerchunk reference
    ds = xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": {
                "fo": multizarr_json,
                "remote_protocol": "file",
            },
        },
    )

    return ds


def print_usage():
    script_call_path = sys.argv[0]
    print(f"Usage: python {script_call_path} <path to multizarr json>")
    print(f"Example: python {script_call_path} cpc/precip-conus/precip-conus.json")


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print("Error: Script received more than one argument")
        print_usage()
        sys.exit(1)

    multizarr_json_path = Path(sys.argv[1])
    # Ensure the multizarr json exists
    if not multizarr_json_path.exists():
        print(f"Error: Multizarr at {multizarr_json_path} does not exist!")
        sys.exit(1)

    # Set up a dask cluster with memory limits
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="6GB")
    dask_client = Client(cluster)

    try:
        print(
            f"Applying data transformations to Zarr Using kerchunk multizarr from {multizarr_json_path}"
        )
        ds = open_multizarr(multizarr_json_path)
        ds = perform_transformations(ds)

        zarr_path = multizarr_json_path.with_suffix(".zarr")
        print(f"Writing zarr to {zarr_path}")
        with dask_client:
            ds.to_zarr(zarr_path, mode="w", consolidated=True)

    finally:
        dask_client.close()
        cluster.close()


if __name__ == "__main__":
    main()
