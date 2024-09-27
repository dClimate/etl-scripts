import pathlib
import datetime

import pandas as pd
import numpy as np
import glob
import multiprocessing
import calendar
import dateutil
import cdsapi
import random
import natsort
import time  # noqa: F401
from datetime import timedelta
from multiprocess.pool import ThreadPool



from dc_etl.fetch import Fetcher, Timespan
from typing import Generator
from dc_etl.filespec import FileSpec
import os
from dataset_manager.utils.logging import Logging
from abc import abstractmethod
from dataset_manager.utils.converter import NCtoNetCDF4Converter


class ERA5Family(Fetcher, Logging):
    """
    The base class for any ERA5 set using the new dClimate data architecture.
    It uses the term 'Family' to indicate it is a superclass of both ERA5 (the regular/vanilla 0.25 resolution dataset)
    and ERA5-Land (the higher resolution, land only 0.1 resolution dataset).
    """
    def __init__(
        self,
        *args,
        cache: FileSpec | None = None,
        dataset_name: str = "default",
        skip_prepare_input_files: bool = False,
        cdsapi_key: str = None,
        **kwargs,
    ):
        """
        Initialize a new ERA5 object with appropriate chunking parameters.

        Parameters
        ----------
        skip_prepare_input_files : bool
            A trigger to skip transformation steps on downloaded data. Useful if data has already been prepared
            and a parse was then interrupted for some reason.
        cdsapi_key : str
            A custom API key string for ECMWF's CDS API. If unfilled the orchestration system will use a default key.
        """
        super().__init__(
            *args,
            **kwargs,
            dataset_name=dataset_name,
        )
        self.standard_dims = ["latitude", "longitude", "valid_time"]
        self.skip_prepare_input_files = skip_prepare_input_files
        self.era5_latest_possible_date = datetime.datetime.utcnow() - datetime.timedelta(days=6)
        self.cdsapi_key = cdsapi_key if cdsapi_key else os.environ.get("CDSAPI_KEY")
        self._cache = cache
        self.dataset_name = dataset_name
        self.converter = NCtoNetCDF4Converter(dataset_name)

    dataset_name = ""
    collection_name = "ERA5"
    """
    Overall collection of data. Used for filling STAC Catalogue.
    """
    time_resolution = "hourly"

    missing_value = -9999

    has_nans: bool = True
    """If True, disable quality checks for NaN values to prevent wrongful flags"""

    @property
    @abstractmethod
    def era5_dataset(self):
        pass

    @property
    @abstractmethod
    def era5_request_name(self):
        pass

    def prefetch(self):
    # Implementation of the method
        return "prefetch data"

    file_type = "NetCDF"
    """
    File type of raw data. Used to trigger file format-appropriate functions
        and methods for Kerchunking and Xarray operations.
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

    dataset_start_date = datetime.datetime(1950, 1, 1, 1)

    preliminary_lag_in_days = 6

    def relative_path(self):
        return pathlib.Path("era5")


    def get_remote_timespan(self) -> Timespan:
        # Get current time in np.datetime64 format
        earliest_time = np.datetime64(self.dataset_start_date)
        latest_time = np.datetime64(self.era5_latest_possible_date)
        # Make it an even day without any hours
        latest_time = np.datetime64(latest_time, "D")
        # minus one day for latest_time
        latest_time -= np.timedelta64(1, "D")
        return Timespan(start=earliest_time, end=latest_time)

    def fetch(self, span: Timespan, pipeline_info: dict) -> Generator[FileSpec, None, None]:
        """Implementation of :meth:`Fetcher.fetch`"""
        self.info(f"Fetching data for the timespan from {span.start} to {span.end}")
        current_datetime = pd.to_datetime(span.start).to_pydatetime()
        limit_datetime = pd.to_datetime(span.end).to_pydatetime()
        # self.extract(date_range=[current_datetime, limit_datetime], enable_caching=True)
        # self.prepare_input_files(keep_originals=False)
        cumulative_hour = 0
        while current_datetime <= limit_datetime:
            # Generate file spec for the current hour
            year = current_datetime.year
            month = current_datetime.month
            cumulative_hour += 1
            file_name = f"{self.dataset_name}_{year:04}{month:02}{cumulative_hour:06}.nc4"
            yield self._get_file_by_name(file_name)
            # Increment the current_datetime by one hour
            current_datetime += timedelta(hours=1)

    def _get_file_by_name(self, file_name: str) -> FileSpec:
        """Get a FileSpec for the file, using the cache if configured."""
        # Check if the cache exists
        if self._cache.exists():
            # Iterate through files in the cache
            for path in self._cache.fs.ls(self.local_input_path()):
                # Only consider files with .nc4 extension
                if path.endswith(".nc4"):
                    # Check if the file name matches the expected file name
                    if file_name in path:
                        return self._cache_path(path)
        
        # If the file isn't found in the cache, handle the case (e.g., raise an exception or log)
        raise FileNotFoundError(f"File {file_name} not found in cache")

    def prepare_input_files(self, keep_originals: bool = False):
        """
        Command line tools converting from GRIB1 to NetCDF4 classic whilst splitting by hour.

        Parameters
        ----------
        keep_originals : bool
            A flag to preserve the original files for debugging purposes. Defaults to False.
        """
        input_dir = pathlib.Path(self.local_input_path())
        monthlies = [pathlib.Path(file) for file in glob.glob(str(input_dir / "*.grib"))]
        # Convert input files to hourly NetCDFs
        self.info(f"Converting {(len(list(monthlies)))} monthly ERA5 GRIB files to hourly NetCDFs")
        self.converter.convert_to_lowest_common_time_denom(raw_files=monthlies, keep_originals=keep_originals)

    def _cache_path(self, path):
        """Compute a file's path in the cache."""
        filename = path.split("/")[-1]
        # Split where self._cache.path stops and add everything after
        return self._cache / filename

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

    def extract(
        self,
        previous_end_date: datetime.datetime = None,
        enable_caching: bool = False,
        date_range: list[datetime.datetime, datetime.datetime] = None,
        *args,
        **kwargs,
    ) -> bool:
        """
        Submit a request to the Copernicus API for every month since the previous end month
        (or since the previous finalization date if there is a new finalization date).
        Download the retrieved files to a temporary location. Compare the retrieved files to our existing files.
        If there are any new files, store them and trigger parse.
        If `previous_end_date` is set, override this behavior and start from that date
        """
        # Original dataset will be None if there is no existing data on the store
        current_date, end = date_range
        # Create request objects for all desired months
        job_args, paths = [], []
        client = cdsapi.Client(verify=True, key=self.cdsapi_key)
        self.info(f"CDS API Client key is {client.key}")
        while current_date <= end:  # will get files for all months if rebuild requested
            self.create_request(client, current_date, end, paths, job_args, enable_caching)
            current_date += dateutil.relativedelta.relativedelta(months=1)
        # request all files asynchronously using separate processes
        found_any_files = False
        with ThreadPool(processes=max(6, multiprocessing.cpu_count() - 1)) as pool:
            for result in pool.starmap(self.request_file, job_args):
                if not result[0]:
                    self.info(f"did not retrieve any data for {result[1]}")
                else:
                    found_any_files = True
                    self.info(f"successfully retrieved {result[1]}")
            pool.close()
            pool.join()
        if not found_any_files:
            raise ValueError("No finalization files successfully retrieved from the API, " "please try again later.")

        return True

    def create_request(
        self,
        client: cdsapi.Client,
        current_date: datetime.datetime,
        end: datetime.datetime,
        paths: list,
        job_args: dict,
        enable_caching: bool = False,
    ):
        """
        Create a request object formatted to the specifications of the Copernicus API (JSON format)
         based on input date ranges.
        Append this request object and a corresponding file pathinto the job args submitted to starmap. in `extract`.
        """
        file_name = f"{self.dataset_name}_{current_date.strftime('%Y%m')}.grib"
        path = self.local_input_path() / file_name
        if current_date.month == end.month and current_date.year == end.year:
            start_date = end.replace(day=1).date()
            end_date = end.date()
            day_range = f"{start_date}/{end_date}"
        else:
            last_day_of_month = calendar.monthrange(current_date.year, current_date.month)[1]
            start_date = current_date.replace(day=1).date()
            end_date = start_date.replace(day=last_day_of_month)
            day_range = f"{start_date}/{end_date}"

        request = {"date": day_range, "time": self.get_list_of_times(), "format": "grib"}

        self.info(f"queuing request to ERA5 Copernicus for {current_date}, days {day_range}")
        paths.append(path)
        job_args.append((client, request, path, enable_caching))

    def request_file(
        self, client: cdsapi.Client, request: dict, output_path: str, enable_caching=False
    ) -> tuple[bool, str]:
        """
        Send a request for data to Copernicus API by passing a dict. The format for the dict is available at
        https://cds-beta.climate.copernicus.eu/how-to-api.

        If the request fails, the function returns false. Any exception raised by the API call is suppressed, and a
        warning with information about the exception is printed to the log.

        If `enable_caching` is `False`, force new data to be served even if there is cached data available on the API
        for the request. Using `enable_caching` allows for downloading previously downloaded files from the API.

        Parameters
        ----------
        client
            A Copernicus API `cdsapi.Client` object
        request
            A dict of Copernicus API request parameters
        output_path
            The file will be written to this path, including the name
        climate_component_full_name
            The Copernicus API's full name for the dataset
        enable_caching
            `True` to allow downloading files from previous requests even if Copernicus has newer data
        dataset
            Copernicus API's name for the dataset category, for example 'reanalysis-era5-single-levels'

        Returns
        -------
        tuple[bool, str]
            A tuple of a bool indicating whether the file was downloaded and the same output path submitted
        """
        # Add these parameters to the passed request
        request.update({"product_type": "reanalysis", "variable": self.era5_request_name})

        # Include a random string as the 'nocache' parameter in the request to avoid requesting a cached file.
        if not enable_caching:
            request["nocache"] = str(random.random())[2:]
            self.info("caching is disabled for current request")
        else:
            self.info("caching is enabled for current request")

        # The request will run in the background. If an exception occurs, a warning will be logged with the exception
        # name and message, and the request will return false. The exception will not be re-raised.
        self.info(f"Submitting request {request}")
        try:
            client.retrieve(self.era5_dataset, request, output_path)
        except Exception as e:
            self.warn(f"{output_path} not retrieved. Exception occured during CDS API call: {type(e).__name__} - {e}")
            return False, output_path

        # If the function hasn't returned false or exited already, the file must have been retrieved
        self.info(f"File {output_path} retrieved")
        return True, output_path

    def get_list_of_times(self, time_range=range(0, 24)) -> str:
        return [f"{time_index:02}:00" for time_index in time_range]

class ERA5(ERA5Family):
    """
    Base class for ERA5 climate data. ERA5 is a global dataset with 0.25 degree resolution data.
    """

    dataset_name = ERA5Family.dataset_name

    def relative_path(self) -> pathlib.Path:
        return pathlib.Path("era5")

    era5_dataset = "reanalysis-era5-single-levels"

    dataset_start_date = datetime.datetime(1950, 1, 1, 0)

    spatial_resolution = 0.25

    final_lag_in_days = 90

    expected_nan_frequency = 0

class ERA5Land(ERA5Family):
    """
    Base class for ERA5-Land climate data. ERA5-Land is a land-only dataset with global, 0.1 degree resolution data.
    """

    dataset_name = ERA5Family.dataset_name + "_land"

    collection_name = "ERA5_Land"
    """
    Overall collection of data. Used for filling STAC Catalogue.
    """

    def relative_path(self) -> pathlib.Path:
        return pathlib.Path("era5_land")

    era5_dataset = "reanalysis-era5-land"

    spatial_resolution = 0.1

    dataset_start_date = datetime.datetime(1950, 1, 1, 0)

    final_lag_in_days = 90

    expected_nan_frequency = 0.6586984082916898
