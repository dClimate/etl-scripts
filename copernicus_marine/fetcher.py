
from __future__ import annotations

import os
import datetime
import dask
import pathlib
import glob
import base64
import json
import nest_asyncio
import natsort
from typing import Generator

import xarray as xr
import numpy as np
import pandas as pd
import re

import copernicusmarine as cm_client
from dotenv import load_dotenv

from abc import abstractmethod, ABC

from dc_etl.fetch import Fetcher, Timespan
from dc_etl.filespec import FileSpec
from utils.helper_functions import numpydate_to_py

# "sea_surface_height", "global_physics", "ocean_temp", "ocean_global_physics", "ocean_salinity"

load_dotenv()

_DATA_FILE = re.compile(r".+\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}\.nc")

def _year(path: str) -> int:
    """Given a file path for a CPC data file, return the year from the filename."""
    match = re.search(r'(\d{4})-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}\.nc', path)
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

class CopernicusOcean(Fetcher):
    """
    Copernicus's Ocean Physics datasets present temperatuers and other variables
     at various depth profiles. See the website for more information
    https://resources.marine.copernicus.eu/products
    """

    def __init__(
        self,
        *args,
        # Chunks are populated in child classes
        requested_dask_chunks={},
        requested_zarr_chunks={},
        requested_ipfs_chunker="",
        cache: FileSpec | None = None,
        **kwargs,
    ):
        """
        Initialize a new Copernicus Ocean object
        """
        super().__init__()
         # Initialize properties directly
        self.requested_dask_chunks = requested_dask_chunks
        self.requested_zarr_chunks = requested_zarr_chunks
        self.requested_ipfs_chunker = requested_ipfs_chunker
        self._cache = cache

    # ATTRIBUTES

    dataset_name = "copernicus_ocean"

    time_resolution = "daily"

    collection_name = "Copernicus_Marine"
    """
    Overall collection of data. Used for filling STAC Catalogue.
    """

    protocol = "file"
    """
    Remote protocol string for MultiZarrToZarr and Xarray to use when opening input files.
    'File' for local, 's3' for S3, etc.
    See fsspec docs for more details.
    """

    identical_dimensions = ["longitude"]
    """
    List of dimension(s) whose values are identical in all input datasets.
    This saves Kerchunk time by having it read these dimensions only one time, from the first input file
    """

    concat_dimensions = ["time"]
    """
    List of dimension(s) by which to concatenate input files' data variable(s)
        -- usually time, possibly with some other relevant dimension
    """

    data_var_dtype = "<f8"

    has_nans: bool = True
    """If True, disable quality checks for NaN values to prevent wrongful flags"""

    reanalysis_start_date = datetime.datetime(1993, 1, 1)
    """
    Actual dataset start date of the dataset in 1993
    """

    interim_reanalysis_start_date = datetime.datetime(2021, 7, 1)
    """
    Start of interim reanalysis data not covered by finalized reanalysis data
    """

    preliminary_lag_in_days = 2

    rebuild_requested = True

    def relative_path(self):
        return pathlib.Path("copernicus_ocean")


    def get_remote_timespan(self) -> Timespan:
        files, earliest_time, latest_time = self._get_remote_files()
        # TODO: REMOVE, Just a limit for now
        # earliest_time = np.datetime64("2022-01-01")
        # latest_time = np.datetime64('2022-03-15')
        return Timespan(start=earliest_time, end=latest_time)

    def prefetch(self):
        # Implementation of the method
        return "prefetch data"

    @property
    def file_type(cls):
        """
        File type of raw data.
        Used to trigger file format-appropriate functions and methods for Kerchunking and Xarray operations.
        """
        return "NetCDF"

    @property
    def dataset_start_date(self):
        """
        Official start date as recorded in the the dataset's attributes under the 'julian_day_unit' field.
        This field is included to comply with Climate and Forecasting Convention specs
        Note that this differs from the actual dataset start date of the dataset in 1993
        """
        return datetime.datetime(1950, 1, 1)


    # DOWNLOAD METHODS

    @abstractmethod
    def _dataset_parameters(self, analysis_type: str) -> tuple[str, str, str]:
        """
        Convenience method to return the correct dataset_id, title, info_url for requests to the CDS API

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """

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
        # super().extract()
        # prevent errors w/ nested asyncio loops w/in containerized environnments
        nest_asyncio.apply()
        # Identify beginning and end of time series to download
        current_datetime, latest_measurement = self.define_request_dates(date_range)
        # Download both datasets up to their latest measurement update period, batching requests for all valid dates
        job_args = self.batch_requests(current_datetime, latest_measurement)
        found_any_files = self.make_requests(job_args)  # Request all files asynchronously using separate processes
        self.postprocess_extract()  # Align reanalysis and analysis datasets
        return found_any_files
    
    @classmethod
    def info(cls, message: str, **kwargs):
        """
        Log a message at `logging.INFO` level.

        Parameters
        ----------
        message : str
            Text to write to log.
        **kwargs : dict
            Keywords arguments passed to `logging.Logger.log`.

        """
        print(message)
        # cls.log(message, logging.INFO, **kwargs)
    
    def store(self):
        """
        Return a store object for the dataset
        """
        # SHould check and open zarr
        return None
        # return xr.open_zarr(self.local_input_path())

    # @property
    # def local_input_root(self):
    #     return pathlib.Path.cwd() / "copernicus_marine"/ "datasets"

    def fetch(self, span: Timespan) -> Generator[FileSpec, None, None]:
        """Implementation of :meth:`Fetcher.fetch`"""
        current_datetime = pd.to_datetime(span.start).to_pydatetime()
        limit_datetime = pd.to_datetime(span.end).to_pydatetime()
        # self.extract((current_datetime, limit_datetime))
        # Extracting the start and end years from the timespan
        start_year = current_datetime.year
        end_year = limit_datetime.year
        for year in range(start_year, end_year + 1):
            yield self._get_file_by_year(year)
    
    # TODO: IMPLEMENT FOR EXTERNAL DATA
    def _get_file_by_year(self, year):
        """Get a FileSpec for the year, using the cache if configured."""
        # Not using cache
        # if not self._cache:
        #     print("NOT HERE")
        #     return FileSpec(self._fs, self._year_to_path(year))

        # Check cache
        if self._cache.exists():
            for path in self._cache.fs.ls(self.local_input_path()):
                if _DATA_FILE.match(path) and _year(path) == year:
                    return self._cache_path(path)

        # Download it to the cache
        # path = self._year_to_path(year)
        # cache_path = self._cache_path(path)
        # self._fs.get_file(path, cache_path.open("wb"))

        # return cache_path
    
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
        return self._cache / self.relative_path() / filename

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
        path = cache_path / pathlib.Path(self.relative_path())
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

    def define_request_dates(
        self, date_range: tuple[datetime.datetime, datetime.datetime] = None
    ) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Identify the beginning and end of the time series of desired data to download

        Parameters
        ----------
        date_range : tuple(datetime.datetime, datetime.datetime), optional
            A specific start and end date
        """
        # DEFINE THE REQUEST DATE RANGE
        # Assign dates from provided date range if it exists
        if date_range:
            current_datetime, latest_measurement = date_range[0], date_range[1]
        # Set start and end times by metadata + update schedule if no date range
        else:
            # start time is a day after the most recent metadata end date, or the dataset start date
            try:
                self.info("Calculating start date from STAC metadata")
                current_datetime = self.get_metadata_date_range()["end"] + datetime.timedelta(days=1)
            except (KeyError, ValueError):
                self.info(
                    f"Script failed to find a date range in STAC (or possibly STAC at all), "
                    f"starting file search from {self.reanalysis_start_date}"
                )
                # Start date of reanalysis dataset is Jan 1, 1993 as of Mar 21, 2022
                current_datetime = self.reanalysis_start_date
            # Measurements for the previous day are returned reliably starting at 12:01 PM today
            # -- and only unreliably before then.
            # Therefore we extract up through the latest 12:01 PM period,
            # e.g. if the script runs at 11:59 AM today it should not extract yesterday but the day before it
            now = datetime.datetime.now()
            noon = datetime.datetime(now.year, now.month, now.day, 12)
            if now >= noon:
                latest_measurement = noon - datetime.timedelta(days=1)
            else:
                latest_measurement = noon - datetime.timedelta(days=2)
        # FIND THE REANALYSIS FINALIZATION DATE
        # Find the precise date for the last finalized reanalysis data provided over the Copernicus Data Store
        self.find_reanalysis_end_dates()
        current_datetime = self.trigger_reanalysis_download(current_datetime)
        return current_datetime, latest_measurement

    def find_reanalysis_end_dates(self):
        """
        Find the finalization date of finalized and interim daily reanalysis data.

        Finalized reanalysis data is usually updated once per year.
        Interim reanalysis data is published on a ~3.5-5 month lag.

        Returns
        -------
        datetime.datetime
            The last date through which reanalysis data is prepared (data is finalized)
        """
        reanalysis_ds = cm_client.open_dataset(
            dataset_id=self._dataset_parameters("reanalysis")[0],
            variables=[self.data_var],
            minimum_longitude=0,
            maximum_longitude=0,
            minimum_latitude=0,
            maximum_latitude=0,
            minimum_depth=0.5,
            maximum_depth=0.5,
            service="arco-geo-series",
            username=os.getenv("COP_MARINE_USERNAME"),
            password=os.getenv("COP_MARINE_PASSWORD"),
        )
        self.reanalysis_start_date = numpydate_to_py(reanalysis_ds.time[0].values)
        self.reanalysis_end_date = numpydate_to_py(reanalysis_ds.time[-1].values)
        self.info(f"determined reanalysis data to be final through {self.reanalysis_end_date}")
        # repeat for interim reanalysis
        reanalysis_ds = cm_client.open_dataset(
            dataset_id=self._dataset_parameters("interim-reanalysis")[0],
            variables=[self.data_var],
            minimum_longitude=0,
            maximum_longitude=0,
            minimum_latitude=0,
            maximum_latitude=0,
            minimum_depth=0.5,
            maximum_depth=0.5,
            service="arco-geo-series",
            username=os.getenv("COP_MARINE_USERNAME"),
            password=os.getenv("COP_MARINE_PASSWORD"),
        )
        self.interim_reanalysis_start_date = numpydate_to_py(reanalysis_ds.time[0].values)
        self.interim_reanalysis_end_date = numpydate_to_py(reanalysis_ds.time[-1].values)
        self.info(f"determined interim reanalysis data to be final through {self.interim_reanalysis_end_date}")

    def trigger_reanalysis_download(self, current_datetime: datetime.datetime) -> datetime.datetime:
        """
        Check if the reanalysis finalization date has changed. If it has, set the first day to download
          from the first day of new reanalysis data and enable data overwrites

        Parameters
        ----------
        current_datetime
            The start date for downloads

        Returns
        -------
        current_datetime
            The start date for downloads, adjusted to the first day of new reanalysis data
            if newly finalized or interim reanalysis data is found.
        """
        # If new finalized data is found, allow overwriting of data to insert new data
        # To insert new finalized data, `overwrite` must be set to allowed.
        current_dataset = self.store()
        if current_dataset:
            previous_reanalysis_finalization_date = (
                numpydate_to_py(
                    datetime.datetime.strptime(current_dataset.attrs["reanalysis_end_date"], "%Y%m%d%H")
                )
                if "reanalysis_end_date" in current_dataset.attrs
                else self.reanalysis_start_date
            )
            previous_interim_end_date = (
                numpydate_to_py(
                    datetime.datetime.strptime(current_dataset.attrs["interim_reanalysis_end_date"], "%Y%m%d%H")
                )
                if "interim_reanalysis_end_date" in current_dataset.attrs
                else self.interim_reanalysis_start_date
            )
            # Rewind to start downloading newest reanalysis data,
            # finalized preferentially, interim if only it is available new
            if previous_reanalysis_finalization_date < self.reanalysis_end_date:
                current_datetime = self.reanalysis_start_date
                self.info(
                    f"Reanalysis end date changed from {previous_reanalysis_finalization_date.date().isoformat()} "
                    f"to {self.reanalysis_end_date.date().isoformat()}, setting downloads "
                    f"to start from {current_datetime.date().isoformat()}"
                )
                self.allow_overwrite = True
            elif self.reanalysis_end_date <= previous_interim_end_date < self.interim_reanalysis_end_date:
                current_datetime = self.interim_reanalysis_start_date
                self.info(
                    f"Reanalysis end date changed from {previous_interim_end_date.date().isoformat()} "
                    f"to {self.interim_reanalysis_end_date.date().isoformat()}, setting downloads "
                    f"to start from {current_datetime.date().isoformat()}"
                )
                self.allow_overwrite = True

        return current_datetime

    def batch_requests(
        self, current_datetime: datetime.datetime, latest_measurement: datetime.datetime
    ) -> list[tuple]:
        """
        Create a batch of job_args for requests to submit to the Copernicus Marine API
         for the desired (or total) date range.
        Differentiate between reanalysis and analysis requests by end date, preferring reanalysis

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
        # Download files one year at a time, up through the latest permissible date for each category
        # Preferentially download the most up-to-date reanalysis, interim reanalysis, and analysis in that order
        while current_datetime <= latest_measurement and current_datetime.year <= latest_measurement.year:
            if current_datetime.strftime("_%Y-%m-%d-") in already_downloaded_files:
                self.info(
                    f"Complete file for {current_datetime.strftime('%Y%m%d')} already exists in download directory"
                )
                current_datetime += datetime.timedelta(days=1)
            # Download reanalysis for dates it's available
            elif current_datetime <= self.reanalysis_end_date:
                while current_datetime <= self.reanalysis_end_date and current_datetime <= latest_measurement:
                    # Advance limit by one year, or to the precise end date if it's the same year
                    limit_datetime = (
                        self.reanalysis_end_date
                        if current_datetime.year == self.reanalysis_end_date.year
                        else datetime.datetime(current_datetime.year, 12, 31)
                    )
                    job_args.append(
                        self.create_request(
                            current_datetime=current_datetime,
                            limit_datetime=limit_datetime,
                            analysis_type="reanalysis",
                        )
                    )
                    # Advance DL start one day if at the limit, otherwise advance one year
                    current_datetime = datetime.datetime(current_datetime.year + 1, 1, 1)
                    if limit_datetime == self.reanalysis_end_date:
                        current_datetime = self.reanalysis_end_date + datetime.timedelta(days=1)
            # Download interim reanalysis for dates it's available and final reanalysis is not
            elif self.reanalysis_end_date <= current_datetime <= self.interim_reanalysis_end_date:
                while current_datetime <= self.interim_reanalysis_end_date and current_datetime <= latest_measurement:
                    # Advance limit by one year, or to the precise end date if it's the same year
                    limit_datetime = (
                        self.interim_reanalysis_end_date
                        if current_datetime.year == self.interim_reanalysis_end_date.year
                        else datetime.datetime(current_datetime.year, 12, 31)
                    )
                    job_args.append(
                        self.create_request(
                            current_datetime=current_datetime,
                            limit_datetime=limit_datetime,
                            analysis_type="interim-reanalysis",
                        )
                    )
                    # Advance DL start one day if at the limit, otherwise advance one year
                    current_datetime = datetime.datetime(current_datetime.year + 1, 1, 1)
                    if limit_datetime == self.interim_reanalysis_end_date:
                        current_datetime = self.interim_reanalysis_end_date + datetime.timedelta(days=1)
            # Download analysis for any remaining dates it's available
            else:
                while current_datetime <= latest_measurement:
                    # Advance limit by one year, or to the limit if the same year
                    limit_datetime = (
                        latest_measurement
                        if current_datetime.year == latest_measurement.year
                        else datetime.datetime(current_datetime.year, 12, 31)
                    )
                    job_args.append(
                        self.create_request(
                            current_datetime=current_datetime, limit_datetime=limit_datetime, analysis_type="analysis"
                        )
                    )
                    # Advance one year
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
            if not local_file_path.suffix == ".nc":
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

    def prepare_input_files(self, keep_originals: bool = False):
        """
        Convert all NetCDFs to NetCDF 4 Classics compatible with Kerchunk
        """
        input_dir = pathlib.Path(self.local_input_path())
        yearlies = [pathlib.Path(file) for file in glob.glob(str(input_dir / "*.nc"))]
        # Convert input files to hourly NetCDFs
        self.info(f"Converting {(len(list(yearlies)))} yearly NetCDF files to daily NetCDFs")
        self.convert_to_lowest_common_time_denom(yearlies, keep_originals)

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
        dataset.attrs["reanalysis_end_date"] = datetime.datetime.strftime(self.reanalysis_end_date, "%Y%m%d%H")
        dataset.attrs["interim_reanalysis_end_date"] = datetime.datetime.strftime(
            self.interim_reanalysis_end_date, "%Y%m%d%H"
        )

        return dataset

    def update_zarr(self, publish_dataset: xr.Dataset, *args, **kwargs):
        """
        Override parent method to first move the date range of the update dataset
         up to the end of the original dataset if the original is later.
        This prevents inserts of data into the middle of existing data
         from being finalized with the wrong date range.

        @see Publish.update_zarr
        """
        original_dataset = self.store.dataset()
        if (
            self.allow_overwrite
            and self.get_date_range_from_dataset(original_dataset)[1]
            > self.get_date_range_from_dataset(publish_dataset)[1]
        ):
            publish_dataset.attrs["date_range"] = original_dataset.attrs["date_range"]
        super().update_zarr(publish_dataset, *args, **kwargs)


class CopernicusOceanSeaSurfaceHeight(CopernicusOcean):
    """Child class for Sea Surface Height datasets"""

    def __init__(self, skip_pre_parse_nan_check=True, *args, **kwargs):  # too variable b/w time steps
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        # Sea Surface Height dataset size is time: 10585, latitude: 668, longitude: 1440
        chunks = dict(
            requested_dask_chunks={"time": 200, "latitude": 167, "longitude": -1},  # 192.3 MB
            requested_zarr_chunks={"time": 200, "latitude": 167, "longitude": 16},  # 2.14 MB
            requested_ipfs_chunker="size-10688",
        )
        kwargs.update(chunks)
        super().__init__(*args, skip_pre_parse_nan_check=skip_pre_parse_nan_check, **kwargs)

    dataset_name = f"{CopernicusOcean.dataset_name}_sea_level"

    def relative_path(self):
        return super().relative_path() / "sea_level"

    tags = ["Sea level anomaly, Sea surface height"]

    data_var = "sla"

    update_cadence = "irregular (approximately 5 months)"

    spatial_resolution = 0.25

    spatial_precision = 0.0001

    missing_value = -2147483647

    unit_of_measurement = "m"

    standard_name = "sea_surface_height_above_geoid"

    long_name = "Sea Surface Height Above Geoid"

    final_lag_in_days = 150

    preliminary_lag_in_days = None

    expected_nan_frequency = 0.4226138117283951


    def _dataset_parameters(self, analysis_type: str) -> tuple[str, str, str]:
        """
        Convenience method to return the correct dataset_id, title, and URL for querying the CDS API

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description"  # noqa: E501
            )
        elif analysis_type == "interim-reanalysis":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_myint_allsat-l4-duacs-0.25deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description"  # noqa: E501
            )
        elif analysis_type == "analysis":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.25deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES NRT"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_NRT_008_046/description"  # noqa: E501
            )
        return dataset_id, title, info_url

    def _info_url(self, analysis_type: str) -> str:
        """
        Convenience method to specify the appropriate URL string for finding information about a dataset

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            return "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_MY_008_047/INFORMATION"
        elif analysis_type == "analysis":
            return "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046/INFORMATION"  # noqa: E501


class CopernicusOceanGlobalPhysics(CopernicusOcean, ABC):
    """
    Parent class for all datasets using Copernicus Ocean Global Physics datasets
    Current inheriting datasets include Temperature and Sea Water Salinity
    """

    final_lag_in_days = 1095

    @classmethod
    def preprocess_kerchunk(cls, refs: dict) -> dict:
        """
        Class method to populate with the specific preprocessing routine of each child class (if relevant),
        whilst the file is being read by Kerchunk.

        Note this function works by manipulating Kerchunk's internal "refs"
         -- the dictionary representing a Zarr generated by Kerchunk.

        Parameters
        ----------
        refs: dict
            A dictionary of DataSet attributes and information automatically supplied by Kerchunk

        Returns
        -------
        dict
            A dictionary of DataSet attributes and information automatically supplied by Kerchunk,
            reformatted as needed
        """
        refs = super().preprocess_kerchunk(refs)
        # set consistent coordinates
        for ref, new_arr_coords, index in [
            ("latitude", (-80, 90, 2041), slice(None)),
            ("longitude", (-180, 180, 4321), slice(0, -1)),
        ]:
            coords_arr = np.linspace(*new_arr_coords, dtype=np.float32)[index]  # need to remove last longitude value
            coords_bytes = coords_arr.tobytes()
            refs[f"{ref}/0"] = "base64:" + base64.b64encode(coords_bytes).decode()

        # fix dtype
        dtype_arr = json.loads(refs[f"{cls.data_var}/.zarray"])
        dtype_arr["dtype"] = "<f4"
        refs[f"{cls.data_var}/.zarray"] = json.dumps(dtype_arr)

        # remove adjustment params
        attrs_arr = json.loads(refs[f"{cls.data_var}/.zarray"])
        for prop in ["scale_factor", "add_offset"]:
            attrs_arr.pop(prop, None)
        refs[f"{cls.data_var}/.zarray"] = json.dumps(attrs_arr)

        return refs

    @classmethod
    def postprocess_zarr(cls, dataset):
        """
        Global Ocean Physics datasets are provided in 4D format with a 'depth' dimension we remove here.
        We separate each depth into a separate Zarr and work exclusively with 3D data,
        so this fourth dimension causes parent class code in DatasetManager to break.

        The depth dimension doesn't show up locally but oddly does over AWS, so we make its removal conditional
        to allow for both cases
        """
        # Necessary pre-processing steps specific to Global Physics datasets
        if "depth" in dataset.coords:
            dataset = dataset.drop_vars("depth")
        dataset = dataset.squeeze()
        if "time" not in dataset.dims:
            dataset = dataset.expand_dims("time")
        return dataset

    dataset_name = CopernicusOcean.dataset_name

    data_var_dtype = "<f4"

    update_cadence = "daily"

    spatial_resolution = 1 / 12

    spatial_precision = 0.000001

    missing_value = 9.969209968386869e36

    expected_nan_frequency = 0.302941550075308
    """=Valid for many shallow-depth datasets, must be re-specified for child classes of lower-depth classes"""

    @abstractmethod
    def scale_factor(self):
        """
        Copernicus Marine specifies a different scale factor for analysis and reanalysis Global Physics datasets,
        resulting in marked discrepancies between the two when combined. Harmonizing them fixes this issue.
        """
        pass

    @abstractmethod
    def add_offset(self):
        """
        Copernicus Marine specifies a different add_offset for analysis and reanalysis Global Physics datasets,
        resulting in marked discrepancies between the two when combined. Harmonizing them fixes this issue.
        """
        pass

    def _dataset_parameters(self, analysis_type: str) -> tuple[str, str, str]:
        """
        Convenience method to return the correct server and dataset_id for requests to the OpenDAP API

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
            title = "Global Ocean Physics Reanalysis"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        elif analysis_type == "interim-reanalysis":
            dataset_id = "cmems_mod_glo_phy_myint_0.083deg_P1D-m"
            title = "Global Ocean Physics Reanalysis (Interim)"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        elif analysis_type == "analysis":
            dataset_id = f"cmems_mod_glo_phy-{self.data_var}_anfc_0.083deg_P1D-m"
            title = "Global Ocean Physics Analysis and Forecast"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_ANALYSIS_FORECAST_PHY_001_024/INFORMATION"  # noqa: E501
        return dataset_id, title, info_url

    def _info_url(self, analysis_type: str) -> str:
        """
        Convenience method to specify the appropriate URL string for finding information about a dataset

        Parameters
        ----------
        analysis_type : str
            A string of 'analysis' or 'reanalysis'
        """
        if analysis_type == "reanalysis":
            return "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        elif analysis_type == "analysis":
            return "https://resources.marine.copernicus.eu/product-detail/GLOBAL_ANALYSIS_FORECAST_PHY_001_024/INFORMATION"  # noqa: E501

    title = "Global Ocean Physics Reanalysis"


class CopernicusOceanTemp(CopernicusOceanGlobalPhysics, ABC):  # pragma: nocover
    """Child class for Ocean Temperatures datasets"""

    def __init__(self, *args, **kwargs):
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        # OceanTemp dataset size is time: 10,000+, latitude: 2041, longitude: 4320
        chunks = dict(
            requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1},  # 1.09 MB
            requested_zarr_chunks={"time": 400, "latitude": 157, "longitude": 30},  # 7.53 MB
            requested_ipfs_chunker="size-18840",
        )
        kwargs.update(chunks)
        super().__init__(*args, **kwargs)

    def dask_configuration(self):
        """
        Dask performs much, much better with a threads scheduler for Temperature datasets, so we enable it here
        """
        super().dask_configuration()
        dask.config.set(
            {"scheduler": "threads"}
        )  # default distributed scheduler does not allocate memory correctly for some parses

    dataset_name = f"{CopernicusOceanGlobalPhysics.dataset_name}_temp"

    def relative_path(self):
        return super().relative_path() / "temp"

    data_var = "thetao"

    standard_name = "sea_water_temperature"

    long_name = "Sea Water Temperature"

    tags = ["Temperature"]

    unit_of_measurement = "deg_C"

    scale_factor = 0.0007324442267417908

    add_offset = 21.0

    final_lag_in_days = 1095


class CopernicusOceanTemp0p5Depth(CopernicusOceanTemp):  # pragma: nocover
    """
    Copernicus Daily Ocean Temps at 0.5 meters of depth
    """

    depth = 0.494025

    dataset_name = f"{CopernicusOceanTemp.dataset_name}_0p5_meters"

    def relative_path(self):
        return super().relative_path() / "0p5_meters"


class CopernicusOceanTemp1p5Depth(CopernicusOceanTemp):  # pragma: nocover
    """
    Copernicus Daily Ocean Temps at 1.5 meters of depth
    """

    depth = 1.541375

    dataset_name = f"{CopernicusOceanTemp.dataset_name}_1p5_meters"

    def relative_path(self):
        return super().relative_path() / "1p5_meters"


class CopernicusOceanTemp6p5Depth(CopernicusOceanTemp):  # pragma: nocover
    """
    Copernicus Daily Ocean Temps at 6.5 meters of depth
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a new Copernicus Ocean Temp object with appropriate chunking parameters.
        """
        # OceanTemp dataset size is time: 10,000+, latitude: 2041, longitude: 4320
        chunks = dict(
            requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1},  # 2 GB
            requested_zarr_chunks={
                "time": 400,
                "latitude": 157,
                "longitude": 15,
            },  # 7 MB -- for some reason 6p5 works better w/ 15
            requested_ipfs_chunker="size-9420",
        )
        kwargs.update(chunks)
        super().__init__(*args, **kwargs)

    depth = 6.440614

    dataset_name = f"{CopernicusOceanTemp.dataset_name}_6p5_meters"

    expected_nan_frequency = 0.30298374072259426

    def relative_path(self):
        return super().relative_path() / "6p5_meters"


class CopernicusOceanSalinity(CopernicusOceanGlobalPhysics, ABC):  # pragma: nocover
    """
    Base class for Sea Water Salinity data
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a new Copernicus Ocean Salinity object with appropriate chunking parameters.
        """
        # Salinity dataset sizes are time: ~10,000+, latitude: 2041, longitude: 4320
        chunks = dict(
            requested_dask_chunks={"time": 400, "latitude": 157, "longitude": -1},  # 2 GB
            requested_zarr_chunks={"time": 400, "latitude": 157, "longitude": 30},  # 14 MB
            requested_ipfs_chunker="size-18840",
        )
        kwargs.update(chunks)
        super().__init__(*args, **kwargs)

    dataset_name = f"{CopernicusOceanGlobalPhysics.dataset_name}_salinity"

    def relative_path(self):
        return super().relative_path() / "salinity"

    data_var = "so"

    update_cadence = "daily"

    spatial_resolution = 1 / 12

    spatial_precision = 0.000001

    standard_name = "sea_water_salinity"

    long_name = "Sea Water Salinity"

    tags = ["Salinity"]

    unit_of_measurement = ""
    """
    Copernicus specifies a Practical Salinity Unit (psu) unit
        that is specifically disrecommended by the scientific community.
    Astropy doesn't support it and this causes downstream issues with the API,
        therefore we specify an empty dimension
    """

    scale_factor = 0.0015259254723787308

    add_offset = -0.0015259254723787308

    final_lag_in_days = 1095


class CopernicusOceanSalinity0p5Depth(CopernicusOceanSalinity):  # pragma: nocover
    """
    Salinity data at 0.5 meters depth
    """

    depth = 0.494025

    dataset_name = f"{CopernicusOceanSalinity.dataset_name}_0p5_meters"

    def relative_path(self):
        return super().relative_path() / "0p5_meters"


class CopernicusOceanSalinity1p5Depth(CopernicusOceanSalinity):  # pragma: nocover
    """
    Salinity data at 1.5 meters depth
    """

    depth = 1.541375

    dataset_name = f"{CopernicusOceanSalinity.dataset_name}_1p5_meters"

    def relative_path(self):
        return super().relative_path() / "1p5_meters"


class CopernicusOceanSalinity2p6Depth(CopernicusOceanSalinity):  # pragma: nocover
    """
    Salinity data at 2.6 meters depth
    """

    depth = 2.645669

    dataset_name = f"{CopernicusOceanSalinity.dataset_name}_2p6_meters"

    def relative_path(self):
        return super().relative_path() / "2p6_meters"


class CopernicusOceanSalinity25Depth(CopernicusOceanSalinity):  # pragma: nocover
    """
    Salinity data at 25 meters depth
    """

    depth = 25.21141

    dataset_name = f"{CopernicusOceanSalinity.dataset_name}_25_meters"

    expected_nan_frequency = 0.31756208376431305

    def relative_path(self):
        return super().relative_path() / "25_meters"


class CopernicusOceanSalinity109Depth(CopernicusOceanSalinity):  # pragma: nocover
    """
    Salinity data at 109 meters depth
    """

    depth = 109.7293

    dataset_name = f"{CopernicusOceanSalinity.dataset_name}_109_meters"

    expected_nan_frequency = 0.35055596385214216

    def relative_path(self):
        return super().relative_path() / "109_meters"

    # NOTE consulting the catalog is no longer necessary as the new find_reanalysis_end_date method
    # works well without it. However, it's conceivable it's needed again and the logic is quite convoluted
    # Therefore I'm preserving this function in amber
    # def find_variable_dict(self, ds_type: str = "reanalysis") -> dict[str, Any]:
    #     """
    #     Find the dictionary for a given dataset + variable in the Copernicus Marine Toolbox Catalog
    #     The Catalog returns a really convoluted set of nested dictionaries and lists,
    #     this function pulls out the important info.

    #     Return
    #     ------
    #     dict[str, Any]
    #     """
    #     dataset_id, dataset_title, info_url = self._dataset_parameters(ds_type)
    #     filter_str = re.search(r"([0-9]{3}_[0-9]{3})", info_url)[0]
    #     catalog = cm_client.describe(contains=[filter_str], include_datasets=True)
    #     datasets = [ds["datasets"] for ds in catalog["products"] if dataset_title in ds["title"]][0]
    #     # find desired dataset and variable from the datasets found
    #     for ds in datasets:
    #         if dataset_id in ds["dataset_id"]:
    #             ds_variables = ds["versions"][0]["parts"][0]["services"][0]["variables"]
    #             for var_dict in ds_variables:
    #                 if self.data_var in var_dict["short_name"]:
    #                     return var_dict
    #     raise ValueError(f"Dataset id {dataset_id} not found in catalog")