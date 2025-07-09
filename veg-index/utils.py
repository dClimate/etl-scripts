from __future__ import annotations

import os
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import AsyncIterator, Generator, Optional, Tuple

import numpy as np
import rasterio
import requests
import xarray as xr
from multiformats import CID
from py_hamt import HAMT, KuboCAS, ZarrHAMTStore
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

SCALE_FACTOR: int = 2  # ↓ downsample original 500 m pixels to ≈1 km for lighter storage

#: Chunk sizes chosen to keep each compressed chunk ≲ ~500 KiB, balancing random
#: access performance against object sharding overhead on IPFS.
CHUNKING: dict[str, int] = {"time": 10, "latitude": 512, "longitude": 256}
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
        with suppress(FileNotFoundError):
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


def standardise(ds: xr.Dataset) -> xr.Dataset:
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

    # 1) Dask chunking for computation
    ds = ds.chunk(CHUNKING)

    # 2) Data-variable encodings  (FPAR)
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

    return ds
