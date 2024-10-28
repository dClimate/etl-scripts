
from __future__ import annotations

import os
import datetime
import pathlib
import glob
import nest_asyncio
import natsort
from typing import Generator

import xarray as xr
import numpy as np
import pandas as pd
import re
import time  # noqa: F401

import copernicusmarine as cm_client
from dotenv import load_dotenv


from dc_etl.fetch import Fetcher, Timespan
from dc_etl.filespec import FileSpec
from dataset_manager.utils.logging import Logging
from dataset_manager.utils.converter import NCtoNetCDF4Converter

from .base_values import (
    CopernicusOceanSeaSurfaceHeightValues,
    CopernicusOceanTemp0p5DepthValues,
    CopernicusOceanTemp1p5DepthValues,
    CopernicusOceanTemp6p5DepthValues,
    CopernicusOceanSalinity0p5DepthValues,
    CopernicusOceanSalinity1p5DepthValues,
    CopernicusOceanSalinity2p6DepthValues,
    CopernicusOceanSalinity25DepthValues,
    CopernicusOceanSalinity109DepthValues
)

# "sea_surface_height", "global_physics", "ocean_temp", "ocean_global_physics", "ocean_salinity"

load_dotenv()

_DATA_FILE = re.compile(r".+\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}\.nc")

_DAY_FILE = re.compile(r".+\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}000\d{3}\.nc4")


def _year(path: str) -> int:
    """Given a file path for a CPC data file, return the year from the filename."""
    match = re.search(r'(\d{4})-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}\.nc', path)
    if match:
        return int(match.group(1))
    else:
        raise ValueError(f"Year not found in path: {path}")

def _year_with_days(path: str) -> int:
    """Given a file path for a CPC data file, return the year from the filename."""
    match = re.search(r'(\d{4})-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}000\d{3}\.nc4', path)
    if match:
        return int(match.group(1))
    else:
        raise ValueError(f"Year not found in path: {path}")

def _get_time_from_nc(file_path: str):
    """Extract the exact time range from the NetCDF file using xarray."""
    with xr.open_dataset(file_path) as ds:
        time_var = ds['time']
        start_time = time_var.values.min()
        end_time = time_var.values.max()
        return start_time, end_time

def _extract_dates(path: str):
    """Extract the start and end dates from the filename."""
    match = re.search(r'(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.nc', path)
    if match:
        start_date = match.group(1)
        end_date = match.group(2)
        return start_date, end_date
    else:
        raise ValueError(f"Dates not found in path: {path}")

class CopernicusOcean(Fetcher, Logging):
    """
    Copernicus's Ocean Physics datasets present temperatuers and other variables
     at various depth profiles. See the website for more information
    https://resources.marine.copernicus.eu/products
    """

    def __init__(
        self,
        *args,
        cache: FileSpec | None = None,
        dataset_name: str = None,
        **kwargs,
    ):
        """
        Initialize a new Copernicus Ocean object
        """
        super().__init__(
            dataset_name=dataset_name
        )
         # Initialize properties directly
        self._cache = cache
        self.dataset_name = dataset_name
        self.converter = NCtoNetCDF4Converter(self.dataset_name)

    # ATTRIBUTES
    rebuild_requested = True

    def relative_path(self):
        return pathlib.Path("copernicus_ocean")


    def get_remote_timespan(self) -> Timespan:
        # Moved to assessor
        earliest_time = np.datetime64("1993-01-01")
        latest_time = np.datetime64("1993-01-01")
        return Timespan(start=earliest_time, end=latest_time)

    def prefetch(self):
        # Implementation of the method
        return "prefetch data"

    def cleanup_files(self):
        # Check if the path exists and is a directory
        file_list = self._cache.fs.ls(self.local_input_path(), detail=False)
        for file_path in file_list:
            # Check if the item is a file (not a directory)
            if self._cache.fs.isfile(file_path):
                self._cache.fs.rm(file_path)

    # EXTRACTION

    def extract(self, date_range: tuple[datetime.datetime, datetime.datetime] = None, *args, **kwargs):
        """
        Connect to Copernicus servers and download NetCDF files of daily ocean temperature data.
        Download reanalysis where it has been prepared and analysis where not.
        Do not download 10 days of forecasts (including today) included in the analysis dataset.

        If a download fails, log if this is due to server unavailability (a known issue) or another unanticipated issue.
        Retry up to 5 times before exiting the script.

        Once files have been downloaded, open them, insert/remove needed parameters, and save them out again
        in order to align inconsistently prepared reanalysis and analysis datasets.

        Return `True` if new files were downloaded, `False` otherwise.
        """
        # prevent errors w/ nested asyncio loops w/in containerized environnments
        nest_asyncio.apply()
        # Identify beginning and end of time series to download
        current_datetime, latest_measurement = date_range
        # Download both datasets up to their latest measurement update period, batching requests for all valid dates
        job_args = self.batch_requests(current_datetime, latest_measurement)
        found_any_files = self.make_requests(job_args)  # Request all files asynchronously using separate processes
        return found_any_files

    def fetch(self, span: Timespan, pipeline_info: dict) -> Generator[FileSpec, None, None]:
        """Implementation of :meth:`Fetcher.fetch`"""
        current_datetime = pd.to_datetime(span.start).to_pydatetime()
        limit_datetime = pd.to_datetime(span.end).to_pydatetime()
        # It should only download the data for the datetime given on the dataset
        # reanalysis, interim-reanalysis, or analysis are the only 3 options
        self.dataset_to_download = pipeline_info["dataset_to_download"]
        self.reanalysis_start_date = pipeline_info["reanalysis_start_date"]
        self.reanalysis_end_date = pipeline_info["reanalysis_end_date"]
        self.interim_reanalysis_start_date = pipeline_info["interim_reanalysis_start_date"]
        self.interim_reanalysis_end_date = pipeline_info["interim_reanalysis_end_date"]

        # Download the files
        # Cleanup everything in case of prior issue
        self.cleanup_files()
        self.extract(date_range=(current_datetime, limit_datetime))
        self.prepare_input_files()
        self.postprocess_extract()  # Do post processing of the downloaded files

        # Extracting the start and end years from the timespan
        start_year = current_datetime.year
        end_year = limit_datetime.year
        for year in range(start_year, end_year + 1):
            for day in range(1, 366):  # Loop through all possible days (1 to 365)
                try:
                    yield self._get_file_by_day(year, day)
                except FileNotFoundError:
                    # If a file for this day doesn't exist, skip to the next day
                    continue
    
    def _get_file_by_year(self, year):
        """Get a FileSpec for the year, using the cache if configured."""
        for path in self._cache.fs.ls(self.local_input_path()):
            if _DAY_FILE.match(path) and _year_with_days(path) == year:
                return self._cache_path(path)

    def _get_file_by_day(self, year, day):
        """Get a FileSpec for the year and day, using the cache if configured."""
        day_str = f"{day:03}"
        for path in self._cache.fs.ls(self.local_input_path()):
            if _DAY_FILE.match(path) and _year_with_days(path) == year and f"000{day_str}" in path:
                return self._cache_path(path)
    
    def _get_remote_files(self):
        files = []

        for local_file_path in self.input_files():
            string_path = str(local_file_path)
            if not local_file_path.suffix == ".nc":
                continue
            if not _DATA_FILE.match(string_path):
                continue

            files.append(string_path)

        # Sort files by start date extracted from the filename
        files = sorted(files, key=lambda x: _extract_dates(x)[0])

        if not files:
            raise ValueError("No valid files found.")

        # Get the first and last file after sorting
        first_file = files[0]
        last_file = files[-1]

        # Extract the exact times from the first and last files
        earliest_time, _ = _get_time_from_nc(first_file)
        _, latest_time = _get_time_from_nc(last_file)

        # Convert to seconds resolution
        earliest_time = np.datetime64(earliest_time, "s")
        latest_time = np.datetime64(latest_time, "s")

        return files, earliest_time, latest_time


    def _cache_path(self, path):
        """Compute a file's path in the cache."""
        filename = path.split("/")[-1]
        # SPlit where self._cache.path stops and add everything after
        return self._cache / filename

    def _year_to_path(self, year):
        for path in self._get_remote_files():
            if _year(path) == year:
                return path

        raise KeyError(year)


    def local_input_path(self) -> pathlib.Path:
        """
        The path to local data is built recursively by appending each derivative's relative path to the previous
        derivative's path. If a custom input path is set, force return the custom path.
        """
        if self._cache is not None:
            cache_path = pathlib.Path(self._cache.path)
        path = cache_path
        # Create directory if necessary
        path.mkdir(parents=True, mode=0o755, exist_ok=True)
        return path

    def input_files(self) -> list[pathlib.Path]:
        """
        Iterator for iterating through the list of local input files

        Returns
        -------
        list
            List of input files from `self.local_input_path()`

        """
        root = pathlib.Path(self.local_input_path())
        for entry in natsort.natsorted(pathlib.Path(root).iterdir()):
            if not entry.name.startswith(".") and not entry.name.endswith(".idx") and entry.is_file():
                yield pathlib.Path(root / entry.name)



    def batch_requests(
        self, current_datetime: datetime.datetime, latest_measurement: datetime.datetime
    ) -> list[tuple]:
        """
        Create a batch of job_args for requests to submit to the Copernicus Marine API
         for the desired (or total) date range.

        Parameters
        ----------
        current_datetime : datetime.datetime
            The current datetime to download

        latest_measurement : datetime.datetime
            The last (latest) datetime to download

        Returns
        -------
        list[tuple]
            A list of tuples containing job arguments for each request
        """
        job_args = []
        already_downloaded_files = [
            fil.stem for fil in self.input_files() if self._is_valid_xarray_dataset(fil) and not self.rebuild_requested
        ]
        while current_datetime <= latest_measurement and current_datetime.year <= latest_measurement.year:
            if current_datetime.strftime("_%Y-%m-%d-") in already_downloaded_files:
                self.info(
                    f"Complete file for {current_datetime.strftime('%Y%m%d')} already exists in download directory"
                )
            else:
                job_args.append(
                    self.create_request(
                        current_datetime=current_datetime, limit_datetime=latest_measurement, analysis_type=self.dataset_to_download
                    )
                )
                current_datetime = datetime.datetime(current_datetime.year + 1, 1, 1)

        return job_args

    def _is_valid_xarray_dataset(self, file_path: pathlib.Path) -> bool:
        """
        Quickly check whether a local dataset is a valid xarray datset
        by attempting to lazily load it

        Parameters
        ----------
        file_path
            A pathlib.Path file path

        Returns
        -------
        bool
            A boolean indicating a successful or failed file opening operation
        """
        try:
            xr.open_dataset(file_path)
            return True
        except Exception as e:
            print(f"Error opening {file_path}: {e}")
            return False

    def create_request(
        self, current_datetime: datetime.datetime, limit_datetime: datetime.datetime, analysis_type: str = "reanalysis"
    ) -> dict:
        """
        Build parameters for a subset request to the CDS API

        Parameters
        ----------
        current_datetime : datetime.datetime
            The current datetime to download

        Returns
        -------
        dict
            A dictionary of request parameters
        """
        
        dataset_id, _, _ = self._dataset_parameters(analysis_type)
        # Copernicus starts from the day after a request...
        request_params = {
            "username": os.getenv("COP_MARINE_USERNAME"),
            "password": os.getenv("COP_MARINE_PASSWORD"),
            "dataset_id": dataset_id,
            "dataset_part": "default",
            "service": "arco-geo-series",
            "variables": [self.data_var],
            "output_directory": self.local_input_path(),
            "output_filename": f"{self.dataset_name}_{current_datetime.strftime('%Y-%m-%d')}_to_{limit_datetime.strftime('%Y-%m-%d')}.nc",  # noqa: E501
            "start_datetime": current_datetime,
            "end_datetime": limit_datetime,
            "force_download": True,
        }
        if hasattr(self, "depth"):
            request_params = {**request_params, "minimum_depth": self.depth, "maximum_depth": self.depth}
        return request_params

    def make_requests(self, job_args: list) -> bool:
        """
        Submit API requests to the Copernicus Marine API

        Parameters
        ----------
        job_args : dict
            A list of dictionaries of request parameters to pass to the download function

        Returns
        -------
        found_any_files : bool
            Returns an indication of whether any files were successfully downloaded
        """
        found_any_files = False
        if job_args:
            self.info(
                f"Submitting batched requests for {len(job_args)} datasets between "
                f"{job_args[0]['start_datetime'].date()} and {job_args[-1]['end_datetime'].date()} "
            )
            for job in job_args:
                found_any_files = bool(cm_client.subset(**job))
        if not found_any_files:
            self.info("Couldn't find any new files to download")
        return found_any_files

    def postprocess_extract(self):
        """
        Copernicus Marine datasets come with different dtypes + scale_factor / add_offset
        for analysis and reanalysis datasets. Combining them is impossible because
        Kerchunk will apply the adjustment factors from the first dataset
        it opens to all datasets it opens, and int16 (reanalysis) can't be merged
        with float32 arrays anyways.

        If you save the files out then the adjustments are applied to all data values
        saved and the adjustments themselves can be safely removed.

        Therefore the only way to correct this open, modify, and resave each dataset
          -- no preprocessing step can effect these changes
        """
        # Only touch .nc files
        for local_file_path in self.input_files():
            if not local_file_path.suffix == ".nc4":
                continue
            raw_ds = xr.open_dataset(local_file_path)
            # Correct data type, save amended file to disk and remove old file
            raw_ds[self.data_var].encoding["dtype"] = np.dtype(self.data_var_dtype)
            raw_ds.to_netcdf(
                local_file_path.with_suffix(""), format="NETCDF4"
            )  # NETCDF4 works as well as as NETCDF4_CLASSIC and is more space efficient
            local_file_path.unlink()
            # Now reopen the file, and save it out without scale_factor and add_offset,
            # if they exist
            new_ds = xr.open_dataset(local_file_path.with_suffix(""))
            # This lets the time dimension properly be chunked
            unlimited_dims = {'time': True}
            new_ds[self.data_var].encoding.pop("scale_factor", None)
            new_ds[self.data_var].encoding.pop("add_offset", None)
            new_ds = self.set_zarr_metadata(new_ds)
            new_ds.to_netcdf(local_file_path, format="NETCDF4", unlimited_dims=unlimited_dims)
            local_file_path.with_suffix("").unlink()


    # TRANSFORMATION

    def prepare_input_files(self):
        """
        Convert all NetCDFs to NetCDF 4 Classics compatible with Kerchunk
        """
        input_dir = pathlib.Path(self.local_input_path())
        yearlies = [pathlib.Path(file) for file in glob.glob(str(input_dir / "*.nc"))]
        # Convert input files to hourly NetCDFs
        self.info(f"Converting {(len(list(yearlies)))} yearly NetCDF files to daily NetCDFs")
        self.converter.convert_to_lowest_common_time_denom(raw_files=yearlies)

    def set_zarr_metadata(self, dataset: xr.Dataset):
        """
        Function to append to or update key metadata information to the attributes and encoding of the output Zarr.
        Extends existing class method to create attributes or encoding specific to ERA5.

        :param xarray.Dataset dataset: The dataset being prepared for parsing to IPLD
        """
        dataset[self.data_var].encoding["units"] = self.unit_of_measurement
        # Mark the final date of reanalysis dataset and beginning of
        # Near Real Time ("analysis") data in the combined dataset
        if not hasattr(self, "reanalysis_end_date"):
            self.find_reanalysis_end_dates()
        dataset.attrs["reanalysis_end_date"] = self.reanalysis_end_date
        dataset.attrs["interim_reanalysis_end_date"] = self.interim_reanalysis_end_date

        return dataset


class CopernicusOceanSeaSurfaceHeight(CopernicusOcean,CopernicusOceanSeaSurfaceHeightValues):
    """Child class for Sea Surface Height datasets"""

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanSeaSurfaceHeightValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)


class CopernicusOceanTemp0p5Depth(CopernicusOcean, CopernicusOceanTemp0p5DepthValues):
    """Child class for Sea Surface Height datasets"""

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanTemp0p5DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanTemp1p5Depth(CopernicusOcean, CopernicusOceanTemp1p5DepthValues):
    """Child class for Sea Surface Height datasets"""

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanTemp1p5DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanTemp6p5Depth(CopernicusOcean, CopernicusOceanTemp6p5DepthValues):
    """Child class for Sea Surface Height datasets"""

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanTemp6p5DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanSalinity0p5Depth(CopernicusOcean, CopernicusOceanSalinity0p5DepthValues):
    """
    Salinity data at 0.5 meters depth
    """

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanSalinity0p5DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanSalinity1p5Depth(CopernicusOcean, CopernicusOceanSalinity1p5DepthValues):
    """
    Salinity data at 1.5 meters depth
    """

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanSalinity1p5DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanSalinity2p6Depth(CopernicusOcean, CopernicusOceanSalinity2p6DepthValues):
    """
    Salinity data at 2.6 meters depth
    """

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanSalinity2p6DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanSalinity25Depth(CopernicusOcean, CopernicusOceanSalinity25DepthValues):
    """
    Salinity data at 25 meters depth
    """

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanSalinity25DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)

class CopernicusOceanSalinity109Depth(CopernicusOcean, CopernicusOceanSalinity109DepthValues):
    """
    Salinity data at 109 meters depth
    """

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        CopernicusOceanSalinity109DepthValues.__init__(self)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, dataset_name=self.dataset_name, **kwargs)
