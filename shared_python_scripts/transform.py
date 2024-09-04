from pathlib import Path

from dask.distributed import Client, LocalCluster
from msgspec import json
import xarray as xr
import numcodecs


def normalize_longitudes(dataset: xr.Dataset) -> xr.Dataset:
    dataset = dataset.assign_coords(longitude=(((dataset.longitude + 180) % 360) - 180))

    # After converting, the longitudes may still start at zero. This reorders the longitude coordinates from -180
    # to 180 if necessary.
    return dataset.sortby(["latitude", "longitude"])


def compress(dataset: xr.Dataset, variables: list[str]) -> xr.Dataset:
    for var in variables:
        dataset[var].encoding["compressor"] = numcodecs.Blosc()

    return dataset


def rename_dimensions(dataset: xr.Dataset, names: dict[str, str]) -> xr.Dataset:
    return dataset.rename(names)


def transform_and_write_zarr(
    dask_client, multizarr_json_path: Path, destination_dir: Path
):
    print(
        f"Applying data transformations to Zarr Using kerchunk multizarr from {multizarr_json_path}"
    )

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

    # Apply transformations
    ds = rename_dimensions(ds, {"lat": "latitude", "lon": "longitude"})
    ds = normalize_longitudes(ds)
    # Apply compression to all data variables
    data_vars = list(ds.data_vars.keys())
    ds = compress(ds, data_vars)
    # Set chunk sizes to be determined automatically
    ds = ds.chunk({"time": "auto"})

    # Write the final Zarr
    zarr_path = multizarr_json_path.with_suffix(".zarr")
    print(f"Writing zarr to {zarr_path}")
    with dask_client:
        ds.to_zarr(zarr_path, mode="w", consolidated=True)


if __name__ == "__main__":
    import sys

    def print_usage():
        script_call_path = sys.argv[0]
        print(f"Usage: python {script_call_path} <provider> <dataset>")
        print(f"Example: python {script_call_path} cpc precip-conus")

    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 2:
        print(
            f"Error: Script did not receive only two arguments, was provided {num_arguments} arguments"
        )
        print_usage()
        sys.exit(1)

    # Verify that the first argument is one of the valid dataset providers
    data_provider = sys.argv[1]
    match data_provider:
        case "cpc":
            pass
        case "chirps" | "prism":
            print(f"Data provider {data_provider} not supported yet")
            sys.exit(1)
        case _:
            print(f"Invalid data provider argument {data_provider}")
            print_usage()
            sys.exit(1)

    # Verify that the second argument is one of the valid datasets in that provider
    dataset = sys.argv[2]
    match data_provider:
        case "cpc":
            match dataset:
                case "precip-conus" | "precip-global" | "tmax" | "tmin":
                    pass
                case _:
                    print(f"Invalid dataset {dataset} for provider {data_provider}")
                    print_usage()
                    sys.exit(1)
        case "chirps" | "prism":
            print(f"Data provider {data_provider} not supported yet")
            sys.exit(1)

    # Get the multizarr jsons we need
    current_file_dir = Path(__file__).parent
    # The .resolve() removes the ".." from the final path
    dataset_dir = (current_file_dir / ".." / data_provider / dataset).resolve()
    multizarr_json_path = dataset_dir / f"{dataset}.json"

    # Verify that the multizarr file exists
    if not multizarr_json_path.exists():
        print(f"Multizarr file does not exist! Located at {multizarr_json_path}")
        print("Quitting transforming")
        sys.exit(1)

    # Set up a dask cluster with memory limits
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="6GB")
    dask_client = Client(cluster)

    try:
        transform_and_write_zarr(dask_client, multizarr_json_path, dataset_dir)
    finally:
        dask_client.close()
        cluster.close()
