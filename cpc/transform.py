import multiprocessing
import sys

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

    ds = xr.open_mfdataset(nc_files, combine="by_coords", parallel=True, chunks=None)

    compress_all_vars(ds)
    ds, encoding = fix_fill_missing_value(ds)
    eprint(f"Writing zarr to {zarr_path}")
    ds.to_zarr(zarr_path, mode="w", consolidated=True, encoding=encoding)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
