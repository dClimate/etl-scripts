import multiprocessing
import sys

from dask.distributed import Client, LocalCluster
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

    cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit="80GB")
    dask_client: Client = cluster.get_client()

    try:
        ds = xr.open_mfdataset(
            nc_files, combine="by_coords", parallel=True, chunks=None
        )
        print(ds)

        compress_all_vars(ds)
        ds, encoding = fix_fill_missing_value(ds)
        ds = ds.chunk({"time": 1769, "lat": 24, "lon": 24})
        eprint(f"Writing zarr to {zarr_path}")
        ds.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)
    finally:
        dask_client.close()
        cluster.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
