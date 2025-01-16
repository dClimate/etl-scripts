import xarray as xr
import numcodecs
from py_hamt import IPFSStore, HAMT
from multiformats import CID
from pathlib import Path

import sys

if len(sys.argv) != 4:
    script_name = sys.argv[0]
    print(f"Usage: python {script_name} <variable> <ds part num: 0-11> <rpc port>")
    sys.exit(1)

data_var = sys.argv[1]
num_ds = int(sys.argv[2])
rpc_port = int(sys.argv[3])

ds = xr.open_zarr(
    "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    chunks=None,  # type: ignore
    storage_options=dict(token="anon"),
)
ds = ds.sel(time=slice(ds.attrs["valid_time_start"], ds.attrs["valid_time_stop"]))

# Preserve only the data variable we want
ds = ds.drop_vars([var for var in ds.data_vars if var != data_var])
ds = ds.drop_vars("level")
ds[data_var].encoding["compressor"] = numcodecs.Blosc()
ds[data_var].encoding.pop("chunks", None)

# latitude: 721, longitude: 1440, time: 1323648
# Original chunks are {'time': 1, 'hybrid': 18, 'latitude': 721, 'longitude': 1440}
ds = ds.chunk({"time": 1, "latitude": 721, "longitude": 1440})

# Only get a subset of for our ETL, so that we can do it piece by piece
# ds = ds.sel(time=slice('1940-01-01', '1969-12-31'))
time_chunk_size = len(ds.time) // 12  # chosen since len(ds.time) is divisible by 12
ds_parts = [
    ds.isel(time=slice(i, i + time_chunk_size))
    for i in range(0, len(ds.time), time_chunk_size)
]

ds = ds_parts[num_ds]

# Either
CACHE_SIZE = 100_000_000  # 100 MB
hamt: HAMT
if num_ds == 0:
    hamt = HAMT(
        store=IPFSStore(rpc_uri_stem=f"http://127.0.0.1:{rpc_port}"),
        max_cache_size_bytes=CACHE_SIZE,
    )
    print(f"Writing part {num_ds} to IPFS")
    print(ds)
    ds.to_zarr(store=hamt)
else:
    with Path(f"/srv/ipfs/etl-scripts/era5/{data_var}-part{num_ds - 1}.cid").open(
        "r"
    ) as f:
        original_ds_cid_str = f.read().rstrip()
    original_ds_cid = CID.decode(original_ds_cid_str)
    hamt = HAMT(
        store=IPFSStore(rpc_uri_stem=f"http://127.0.0.1:{rpc_port}"),
        max_cache_size_bytes=CACHE_SIZE,
        root_node_id=original_ds_cid,
    )
    print(f"Writing part {num_ds} to IPFS")
    print(ds)
    ds.to_zarr(store=hamt, mode="a", append_dim="time")

root_cid = str(hamt.root_node_id)
print(root_cid)
cid_path = Path(f"/srv/ipfs/etl-scripts/era5/{data_var}-part{num_ds}.cid")
with cid_path.open("w") as f:
    f.write(str(root_cid))
    f.write("\n")
