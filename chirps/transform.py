import multiprocessing
import sys

from dask.distributed import LocalCluster
import xarray as xr
from etl_scripts.transform import (
    check_only_one_argument,
    validate_nc_dir_path,
    eprint,
    exit_if_zarr_uptodate,
    compress_all_vars,
    fix_fill_missing_value,
)


def main():
    check_only_one_argument()
    nc_dir_path = validate_nc_dir_path(sys.argv[1])
    nc_files = list(nc_dir_path.glob("*.nc"))
    if not nc_files:
        eprint(f"Error: No .nc files found in {nc_dir_path}")
        sys.exit(1)

    dataset_name = nc_dir_path.stem
    zarr_path = nc_dir_path / f"{dataset_name}.zarr"
    exit_if_zarr_uptodate(zarr_path, nc_files)

    # Set up a dask cluster with memory limits before computing
    cluster = LocalCluster(n_workers=4, threads_per_worker=1, memory_limit="20GB")
    dask_client = cluster.get_client()

    try:
        ds = xr.open_mfdataset(
            nc_files, combine="by_coords", parallel=True, chunks=None
        )

        compress_all_vars(ds)
        ds, encoding = fix_fill_missing_value(ds)
        match dataset_name:
            case "final-p05":
                ds = ds.chunk({"time": 60, "latitude": 354, "longitude": 1272})
            case "final-p25":
                ds = ds.chunk({"time": 60, "latitude": 354, "longitude": 1272})
            case "prelim-p05":
                ds = ds.chunk({"time": 60, "latitude": 354, "longitude": 1272})
        eprint(f"Writing zarr to {zarr_path}")
        with dask_client:
            ds.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)

    finally:
        dask_client.close()
        cluster.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
