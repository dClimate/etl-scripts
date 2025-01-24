import multiprocessing
import sys
from pathlib import Path

import xarray as xr
from dask.distributed import Client, LocalCluster
from transform_nc import perform_transformations, should_zarr_be_regenerated


def print_usage():
    script_call_path = sys.argv[0]
    print(f"Usage: python {script_call_path} precip-4km|tmax-4km|tmin-4km")
    print(f"Example: python {script_call_path} prism/precip-4km/")


def is_dataset_valid(dataset: str):
    match dataset:
        case "precip-4km" | "tmax-4km" | "tmin-4km":
            return True
        case _:
            return False


def main():
    # Verify the number of arguments
    num_arguments = len(sys.argv) - 1
    if num_arguments != 1:
        print("Error: Script did not receive only one argument")
        print_usage()
        sys.exit(1)

    dataset = sys.argv[1]
    if not is_dataset_valid(dataset):
        print("Error: Invalid dataset specified")
        print_usage()
        sys.exit(1)

    dataset_dir = (Path(__file__).parent.parent / "prism" / dataset).resolve()
    year_nc_files = list(dataset_dir.glob("????.nc"))
    if not year_nc_files:
        print(f"Error: No YYYY.nc files found in {dataset_dir}")
        sys.exit(1)

    zarr_path = dataset_dir / f"{dataset}.zarr"
    if not should_zarr_be_regenerated(zarr_path, year_nc_files):
        print(
            f"Skipping generation of Zarr {zarr_path}, it is newer than YYYY.nc files"
        )
        return

    # Set up a dask cluster with memory limits before computing
    cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit="16GB")
    dask_client = Client(cluster)

    try:
        ds = xr.open_mfdataset(
            year_nc_files,
            combine="by_coords",
            parallel=True,
        )
        ds, encoding = perform_transformations(ds)
        print(f"Writing zarr to {zarr_path}")
        with dask_client:
            ds.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)

    finally:
        dask_client.close()
        cluster.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
