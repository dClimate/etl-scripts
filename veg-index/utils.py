from __future__ import annotations

import os
import random
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Generator

import numpy as np
import rasterio
import requests
import xarray as xr
from zarr.codecs import BloscCodec, BloscShuffle

from etl_scripts.grabbag import eprint

# -----------------------------------------------------------------------------#
# Configuration constants
# -----------------------------------------------------------------------------#

#: Path to a local scratch directory where GeoTIFFs are cached on disk. Keeping a
#: persistent on-disk cache greatly speeds up subsequent runs and protects
#: against intermittent connectivity compared to re-downloading from the remote
#: object store every time.
scratchspace: Path = (
    Path(__file__).parent.parent / "scratchspace" / "vegindex"
).absolute()
os.makedirs(scratchspace, exist_ok=True)

SCALE_FACTOR: int = 4  # ↓ downsample original 500 m pixels to ≈2 km for lighter storage

#: Chunk sizes chosen to keep each compressed chunk ≲ ~500 KiB, balancing random
#: access performance against object sharding overhead on IPFS.
CHUNKING: dict[str, int] = {"time": 32, "latitude": 128, "longitude": 128}
COMPRESSOR = BloscCodec(cname="zstd", clevel=7, shuffle=BloscShuffle.bitshuffle)
TIME_COORD_CHUNK = 500_000  # -- covers ~13 000 yrs of dekads; “effectively one chunk”

#: Day-of-month markers that define the 3 dekads inside every calendar month.
DEKADAL_DAYS: tuple[int, int, int] = (1, 11, 21)

#: String template that maps any dekad timestamp to its source GeoTIFF URL.
TIFF_URL_TEMPLATE: str = (
    "https://agricultural-production-hotspots.ec.europa.eu/data/"
    "indicators_fpar/fpar/fpar_{:%Y%m%d}.tif"
)

# -----------------------------------------------------------------------------#
# Helper functions
# -----------------------------------------------------------------------------#


def tiff_url(ts: datetime) -> str:
    """Return the download URL for the dekad corresponding to *ts*."""
    return TIFF_URL_TEMPLATE.format(ts)


def tiff_filename(ts: datetime) -> str:
    """Return the on-disk filename (no directory) for the given dekad."""
    return f"fpar_{ts:%Y%m%d}.tif"


def yield_dekad_dates(
    start: datetime, end: datetime
) -> Generator[datetime, None, None]:
    """Yield **UTC-normalised** dekad start dates inside the ``[start, end]`` interval.

    Parameters
    ----------
    start, end
        Inclusive bounds for the search range; *hour/minute/second* components
        are zero-ed and a UTC timezone is injected to guarantee consistent
        comparisons.

    Yields
    ------
    datetime
        Midnight timestamps for every 1st, 11th and 21st day occurring within
        the interval.
    """

    cur = start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

    # Fast-forward to the first valid dekad marker
    while cur.day not in DEKADAL_DAYS:
        cur += timedelta(days=1)

    while cur <= end:
        yield cur
        # Jump to the next dekad efficiently without *n*×10-day loops.
        if cur.day == 1:
            cur = cur.replace(day=11)
        elif cur.day == 11:
            cur = cur.replace(day=21)
        else:  # cur.day == 21 – roll over to 1st of next month
            year, month = (
                (cur.year + 1, 1) if cur.month == 12 else (cur.year, cur.month + 1)
            )
            cur = cur.replace(year=year, month=month, day=1)

def check_for_tiff_existence(ts: datetime) -> bool:
    """Return **True** if the GeoTIFF for *ts* exists locally or remotely."""

    exists_locally = (scratchspace / tiff_filename(ts)).exists()
    if exists_locally:
        return True

    exists_remotely = requests.head(tiff_url(ts)).status_code == 200
    return exists_remotely


def download_tiff(ts: datetime, *, force: bool = False) -> Path:
    """Download the GeoTIFF for *ts* to *scratchspace* (with resume/cache).

    The function streams the remote file directly to disk in 1 MiB chunks to
    keep memory footprint constant regardless of raster size.

    Parameters
    ----------
    ts
        Dekad timestamp (*UTC midnight*) whose raster is requested.
    force
        If **True**, ignore any file already present on disk and re-download
        (useful when the upstream provider silently republishes updated data).

    Returns
    -------
    pathlib.Path
        Absolute path to the cached TIFF. Guaranteed to exist on return.

    Raises
    ------
    RuntimeError
        When the download fails (non-200 status, timeout, interrupted…). The
        partially written ``*.tmp`` file is deleted to avoid corrupt cache
        entries.
    """

    path = scratchspace / tiff_filename(ts)
    if path.exists() and not force:
        eprint(f"✓ {path.name} already exists")
        return path

    url = tiff_url(ts)
    eprint(f"⇣ Downloading {url}")

    try:
        with requests.get(url, stream=True, timeout=180) as resp:
            resp.raise_for_status()
            tmp = path.with_suffix(".tmp")
            with open(tmp, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=2**20):
                    fh.write(chunk)
            tmp.rename(path)
    except (requests.RequestException, KeyboardInterrupt) as exc:
        with suppress(FileNotFoundError, UnboundLocalError):
            tmp.unlink(missing_ok=True)  # purge incomplete fragment
        raise RuntimeError(f"Could not fetch {url!s}") from exc

    return path


def tiff_to_dataarray(path: Path) -> xr.DataArray:
    """Convert a GeoTIFF to a down-sampled, memory-friendly DataArray.

    The raster is resampled to ≈1 km (``SCALE_FACTOR``) using *average*
    aggregation to balance storage footprint with information content.  The
    function keeps coordinate calculation in pure NumPy to avoid costly calls
    into *pyproj*.

    Notes
    -----
    * The 2-D array is returned with dimensions ``(latitude, longitude)``.
    * Any NODATA value declared in the TIFF is translated to ``np.nan`` so that
      downstream arithmetic can ignore masked cells naturally.
    """

    with rasterio.open(path) as src:
        # Determine the down-sampled grid size
        out_height = src.height // SCALE_FACTOR
        out_width = src.width // SCALE_FACTOR

        data = src.read(
            1,
            out_shape=(out_height, out_width),
            resampling=rasterio.enums.Resampling.average,
        ).astype("float32")

        # Map the provider's NODATA marker to NaN
        nodata_val = src.nodata
        if nodata_val is not None and not np.isnan(nodata_val):
            data[data == nodata_val] = np.nan

        # Adapt the affine transform for the new grid
        transform = src.transform * src.transform.scale(
            src.width / out_width, src.height / out_height
        )
        rows = np.arange(out_height)
        cols = np.arange(out_width)
        _, lat = rasterio.transform.xy(transform, rows, 0)
        lon, _ = rasterio.transform.xy(transform, 0, cols)

    # Ensure ascending latitude (south-to-north) order expected by xarray
    lat = np.array(lat)
    if lat[0] > lat[-1]:
        lat = lat[::-1]
        data = data[::-1, :]

    lon = np.array(lon)

    return xr.DataArray(
        data,
        dims=("latitude", "longitude"),
        coords={"latitude": lat, "longitude": lon},
        name="FPAR",
    )


def standardise(arrays: list[xr.DataArray], dataset_name: str) -> xr.Dataset:
    """Apply *common* chunking & compression conventions before writing.

    The helper ensures every dataset written by this script is chunked and
    encoded consistently so that appended data remain compatible and
    downstream tooling (e.g. *fsspec*, *dask*, *kerchunk*) can rely on a
    predictable layout.

    Notes
    -----
    * **Chunking** – Chosen to balance read performance and object size on
      IPFS; 10 dekads (~3 months) per chunk in *time* and gentle tiling in the
      spatial dimensions.
    * **Compression** – Uses Blosc (default codec for many Zarr stores) with
      the default *zstd* parameters. A single codec is attached to every data
      variable; metadata arrays are left untouched.
    * **Missing data** – ``NaN`` is written as the canonical fill value so
      that consumers do not need to guess nodata markers.
    """
    eprint(f"Standardising {dataset_name} dataset…")
    start = time.time()

    # 1) Concatenate the arrays into a single dataset & chunk it
    ds = xr.concat(arrays, dim="time").to_dataset(name=dataset_name).chunk(CHUNKING)

    # 2) Data-variable encodings
    for var in ds.data_vars:
        ds[var].encoding.update(
            {
                "chunks": tuple(CHUNKING[dim] for dim in ds[var].dims),  # type: ignore
                "compressors": [COMPRESSOR],
                "_FillValue": np.nan,
            }
        )

    # 3) Coordinate-variable encodings  (lat/lon/time)
    for coord in ds.coords:
        if coord == "time":
            # one big slab so xarray needs a single GET to read the axis
            ds[coord].encoding["chunks"] = (TIME_COORD_CHUNK,)
        elif coord in CHUNKING:
            # write the whole axis as one chunk
            ds[coord].encoding["chunks"] = (ds.sizes[coord],)

    eprint(f"✓ Standardised {dataset_name} dataset in {time.time() - start:.2f}s")
    return ds


def quality_check_dataset(
    ds: xr.Dataset,
    raw_arrays: dict[datetime, xr.DataArray],
    dataset_name: str,
    num_checks: int = 150,
) -> None:
    """Perform some quality checks on the dataset before writing."""

    eprint("Performing dataset quality checks…")

    # 1) Check dimension names
    expected_dims = {"time", "latitude", "longitude"}
    actual_dims = set(ds.dims)
    if actual_dims != expected_dims:
        raise RuntimeError(
            f"Dimension names mismatch: expected {expected_dims}, got {actual_dims}"
        )

    # 2) Check dimension lengths
    for dim in expected_dims:
        expected_length = len(raw_arrays) if dim == "time" else ds.sizes[dim]
        actual_length = ds.sizes.get(dim, 0)
        if actual_length != expected_length:
            raise RuntimeError(
                f"Dimension '{dim}' length mismatch: "
                f"expected {expected_length}, got {actual_length}"
            )

    # 3) Perform a random quality‐check on the data without ND fancy indexing
    data_array = ds[dataset_name]
    times = list(raw_arrays)

    for _ in range(num_checks):
        # pick a random dekad & its raw array
        ts = random.choice(times)
        raw = raw_arrays[ts].squeeze("time")  # dims now ("latitude","longitude")
        t64 = np.datetime64(ts, "ns")

        # pick random spatial indices
        lat_idx = random.randrange(raw.shape[0])
        lon_idx = random.randrange(raw.shape[1])

        # get raw value
        v_raw = raw.values[lat_idx, lon_idx]

        # stepwise select from the Zarr-backed DataArray
        v_ds = (
            data_array.sel(time=t64)
            .isel(latitude=lat_idx)
            .isel(longitude=lon_idx)
            .compute()
            .data.tolist()
        )

        if not np.isclose(v_raw, v_ds, equal_nan=True):
            raise ValueError(
                f"Random QC failure at {ts.date()} idx ({lat_idx},{lon_idx}): "
                f"raw={v_raw!r} vs zarr={v_ds!r}"
            )
        else:
            eprint(
                f"✓ Random QC check passed for {ts.date()} at idx ({lat_idx},{lon_idx}): "
                f"raw={v_raw!r} vs zarr={v_ds!r}"
            )

    eprint("✓ Dataset quality check passed: all dimensions and lengths are correct.")
