import click
import numpy as np
import xarray as xr
from click import Context
from multiformats import CID
from py_hamt import HAMT, IPFSStore


@click.command()
@click.argument("variable-to-check")
@click.argument("cid")
@click.pass_context
def check_identical(ctx: Context, variable_to_check: str, cid: str):
    """
    Check that a random subset of the Google ERA5 ETLed VARIABLE_TO_CHECK data and the data on IPFS at CID are equivalent.
    """
    google_ds = xr.open_zarr(
        "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
        chunks=None,  # type: ignore
        storage_options=dict(token="anon"),
    )
    google_ds = google_ds.sel(
        time=slice(
            google_ds.attrs["valid_time_start"], google_ds.attrs["valid_time_stop"]
        )
    )
    if variable_to_check not in google_ds:
        print(f"Variable {variable_to_check} is not in the Google Dataset")
        ctx.exit(1)

    google_da = google_ds[variable_to_check]
    print("====== Full Google DataArray ======")
    print(google_da)

    hamt = HAMT(store=IPFSStore(), root_node_id=CID.decode(cid), read_only=True)
    ipfs_ds = xr.open_zarr(store=hamt)
    da = ipfs_ds[variable_to_check]
    print("====== Full IPFS DataArray ======")
    print(da)

    # This inherently assumes that the dClimate ETL starts from the same timepoint beginning as the Google one
    n_samples = int(len(da.time) * 0.0001)  # 0.01%
    random_time_indices = np.random.choice(len(da), size=n_samples, replace=False)

    google_subset = google_da.isel(time=random_time_indices)
    da_subset = da.isel(time=random_time_indices)
    print("====== Google subset ======")
    print(google_subset)
    print("====== IPFS subset ======")
    print(da_subset)
    try:
        xr.testing.assert_identical(google_subset, da_subset)
    except AssertionError:
        ctx.exit(1)


if __name__ == "__main__":
    check_identical()
