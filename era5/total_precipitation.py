import xarray as xr
import numcodecs
from py_hamt import IPFSStore, HAMT
import multiprocessing
from pathlib import Path

def main():
    ds = xr.open_zarr(
        'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
        chunks=None, # type: ignore
        storage_options=dict(token='anon'),
    )

    # Only ETL precipitation
    ds = ds.drop_vars([var for var in ds.data_vars if var != 'total_precipitation'])
    ds = ds.drop_vars("level")
    ds["total_precipitation"].encoding["compressor"] = numcodecs.Blosc()
    ds["total_precipitation"].encoding.pop("chunks", None)

    # latitude: 721, longitude: 1440, time: 1323648
    # Original chunks are {'time': 1, 'hybrid': 18, 'latitude': 721, 'longitude': 1440}
    ds = ds.chunk({"time": 1, "latitude": 721, "longitude": 1440})

    # Only get a subset of for our ETL, so that we can do it piece by piece
    # ds = ds.sel(time=slice('1940-01-01', '1969-12-31'))
    time_chunk_size = len(ds.time) // 24 # chosen since len(ds.time) = 1323648 is divisible by 24
    ds_parts = [ds.isel(time=slice(i, i + time_chunk_size))
              for i in range(0, len(ds.time), time_chunk_size)]

    ds = ds_parts[0]

    print("Writing to IPFS")
    print(ds)
    hamt = HAMT(store=IPFSStore())
    ds.to_zarr(store=hamt, mode="w")

    root_cid = str(hamt.root_node_id)
    print(root_cid)
    cid_path = Path("/srv/ipfs/etl-scripts/era5/root.cid")
    with cid_path.open("w") as f:
        f.write(str(root_cid))
        f.write("\n")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
