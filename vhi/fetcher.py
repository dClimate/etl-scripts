# VHI.py
import pathlib
import re
import datetime

import pandas as pd
import numpy as np
import xarray as xr
import glob

import requests
from requests.adapters import HTTPAdapter, Retry

from dc_etl.fetch import Fetcher, Timespan
from typing import Generator
from dc_etl.filespec import FileSpec
from dataset_manager.utils.extractor import HTTPExtractor
import os
import tarfile
import shutil
from dataset_manager.utils.logging import Logging
from dataset_manager.utils.converter import NCtoNetCDF4Converter



class VHI(Fetcher, Logging):
    """
    VHI is a gridded dataset of vegetation health published in netCDF format that's updated every week.
    """

    MIN_LAT = -55.152

    MAX_LAT = 75.024

    MIN_LON = -180

    MAX_LON = 180

    RESOLUTION = 360 / 10000  # this says there are 10000 points in 360 latitude/longitude

    def __init__(
        self,
        *args,
        # dataset size is 2100+, 3616, 10000
        requested_dask_chunks={"time": 200, "latitude": 226, "longitude": -1},  # 1.8 GB
        requested_zarr_chunks={
            "time": 200,
            "latitude": 113,
            "longitude": 250,
        },  # 5.65 MB
        requested_ipfs_chunker="size-113000",
        skip_pre_parse_nan_check=True,
        skip_post_parse_qc=True,
        cache: FileSpec | None = None,
        dataset_name: str = "vhi",
        **kwargs,
    ):
        """
        Initialize a new VHI object with appropriate chunking parameters.
        """
        super().__init__(dataset_name)
         # Initialize properties directly
        self.requested_dask_chunks = requested_dask_chunks
        self.requested_zarr_chunks = requested_zarr_chunks
        self.requested_ipfs_chunker = requested_ipfs_chunker
        self._cache = cache
        self.converter = NCtoNetCDF4Converter(dataset_name)

    data_download_url = "https://www.star.nesdis.noaa.gov/data/pub0018/VHPdata4users/data/Blended_VH_4km/VH"

    dataset_start_date = datetime.datetime(1981, 8, 27, 0)

    standard_name = "vegetation"

    long_name = "Vegetation Health Index"

    unit_of_measurement = "vegetative health score"

    tags = ["index", "vegetation"]

    collection_name = "VHI"
    """
    Overall collection of data. Used for filling STAC Catalogue.
    """

    dataset_name = "vhi"

    file_type = "NetCDF"

    protocol = "file"
    """
    Remote protocol string for MultiZarrToZarr and Xarray to use when opening input files.
    'File' for local, 's3' for S3, etc.
    See fsspec docs for more details.
    """

    identical_dimensions = ["HEIGHT", "WIDTH"]
    """
    List of dimension(s) whose values are identical in all input datasets.
    This saves Kerchunk time by having it read these dimensions only one time, from the first input file
    """

    concat_dimensions = ["time", "sat"]
    """
    List of dimension(s) by which to concatenate input files' data variable(s)
        -- usually time, possibly with some other relevant dimension
    """

    data_var = "VHI"

    bbox_rounding_value = 3
    """Value to round bbox values by"""

    time_resolution = "weekly"

    update_cadence_bounds = [np.timedelta64(7, "D"), np.timedelta64(9, "D")]
    """
    VHI's odd update schedule means that 8 or 9 day gaps can appear around
     the turn of the year (the latter for leap years)
    Therefore we have to enforce contiguousness within a range instead of a set expected timedelta
    """

    missing_value = -999

    has_nans: bool = True
    """Since VHI has variable amounts of NaNs, disable quality checks for NaN values to prevent false negatives"""

    final_lag_in_days = 21

    expected_nan_frequency = 1  # VHI NaNs vary by the # and overlap of satellites, so are impossible to test


    def relative_path(self):
        return pathlib.Path("vhi")


    def get_remote_timespan(self) -> Timespan:
        # files, earliest_time, latest_time = self._get_remote_files()
        # TODO: REMOVE, Just a limit for now
        earliest_time = np.datetime64("2020-01-01")
        # Get current time in np.datetime64 format
        latest_time = np.datetime64(datetime.datetime.now())
        # latest_time = np.datetime64('2022-03-15')
        return Timespan(start=earliest_time, end=latest_time)

    def prefetch(self):
    # Implementation of the method
        return "prefetch data"

    def fetch(self, span: Timespan) -> Generator[FileSpec, None, None]:
        """Implementation of :meth:`Fetcher.fetch`"""
        current_datetime = pd.to_datetime(span.start).to_pydatetime()
        limit_datetime = pd.to_datetime(span.end).to_pydatetime()
        # self.extract((current_datetime, limit_datetime))
        self.prepare_input_files(keep_originals=False)
        # start_date = span.start.astype(datetime.datetime)
        # end_date = span.end.astype(datetime.datetime)
        # Extracting the start and end years from the timespan
        self.info(f"Fetching data for the timespan from {span.start} to {span.end}")
        start_year = current_datetime.year
        end_year = limit_datetime.year
        for year in range(start_year, end_year + 1):
            vhi_weeks = self.vhi_weeks_per_year((current_datetime, limit_datetime))
            for idx in range(len(vhi_weeks[0])):
                yield self._get_file_by_year_week(year, idx + 1)

    
    def _get_file_by_year_week(self, year, week):
        """Get a FileSpec for the year, using the cache if configured."""
        # Check cache
        if self._cache.exists():
            for path in self._cache.fs.ls(self.local_input_path()):
                # Only consider files with .nc extension
                if path.endswith(".nc4"):
                    file_year, file_week = self.return_year_week_from_path(path)
                    if file_year == year and file_week == week:
                        print(f"Found file in cache: {path}")
                        return self._cache_path(path)
    
    def _cache_path(self, path):
        """Compute a file's path in the cache."""
        filename = path.split("/")[-1]
        # Split where self._cache.path stops and add everything after
        return self._cache / self.relative_path() / filename

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
    

    # EXTRACT

    def extract(self, date_range: tuple[str, str] = None, *args, **kwargs) -> bool:
        """
        Check VHRR ftp server for files from the end year of or after our data's end date.
        Download necessary files. Check newest file and return `True` if it has newer data than us
         or `False` otherwise.

        Parameters
        ----------
        date_range : list[str, str], optional
            Date range to download and process data for

        Returns
        -------
        bool
            True if new data found, false otherwise
        """
        # super().extract()  # Initialize an empty list of new files
        # define request dates (must be in weekly format)
        start_date, start_week, end_date = self.define_request_dates(date_range)
        # Prepare a session
        self.get_session()
        # Create requests for all relevant files within the specified time range
        requests = self.batch_requests(start_date, start_week, end_date)
        # Download files
        found_any_files = HTTPExtractor(self).pool(requests)
        # return a list of new files downloaded. If none this equates to False and terminates the ETL
        return found_any_files

    def define_request_dates(self, date_range: list = None) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Define start and end dates to be used when requesting files

        Parameters
        ----------
        date_range : list, optional
            A list of start and end datetimes for retrieval. Defaults to None.

        Returns
        -------
        tuple[datetime.datetime, datetime.datetime]
            A tuple of start and end datetimes for retrival
        """
        if date_range:
            self.info("Calculating new start and end dates based on the provided date range.")
            start_date, start_week = self.get_start_date_and_week(date_range[0])
            end_date = date_range[1]
        else:
            try:
                current_datetime = self.get_metadata_date_range()["end"] + datetime.timedelta(days=7)
                self.info("Calculating new start date based on end date in metadata")
            except (KeyError, ValueError):
                self.info(
                    f"No existing metadata or file found; "
                    "starting download from Arbol's specified start date of "
                    f"{self.dataset_start_date.date().isoformat()}"
                )
                current_datetime = self.dataset_start_date
            start_date, start_week = self.get_start_date_and_week(current_datetime)
            end_date = datetime.datetime.now()

        return start_date, start_week, end_date

    def get_start_date_and_week(self, new_start_date: datetime.datetime) -> tuple[datetime.datetime, int]:
        """
        Derive a start year and week in line with VHI's update schedule from a specified start date

        Parameters
        ----------
        new_start_date : datetime.datetime
            The newly calculated start date for a given year

        Returns
        -------
        tuple[datetime.datetime, int]
            A tuple containing datetime and integer representations of the start date and week, respectively
        """
        year_end = datetime.datetime(new_start_date.year, 12, 30)
        if new_start_date >= year_end:
            return datetime.datetime(new_start_date.year + 1, 1, 1), 1

        day_iterator = datetime.datetime(new_start_date.year, 1, 1)
        week = 1

        while day_iterator < new_start_date:
            day_iterator += datetime.timedelta(days=7)
            week += 1

        if day_iterator != new_start_date:
            raise Exception("invalid first day")

        return new_start_date, week

    def get_session(self):
        """
        Create a STAR session with appropriate retry logic and assign it
        to an instance variable
        """
        retry_strategy = Retry(total=10, status_forcelist=[500, 502, 503, 504], backoff_factor=10)
        self.session = requests.Session()
        self.session.mount(prefix="https://", adapter=HTTPAdapter(max_retries=retry_strategy))
        self.info("Created STAR session")

    def batch_requests(
        self,
        start_date: datetime.datetime,
        start_week: int,
        end_date: datetime.datetime,
    ) -> tuple[str]:
        """
        Prepare a dictionary of request parameters for each file on the VHI HTTPS
        falling within the requested time range

        Parameters
        ----------
        start_date : datetime.datetime
            The desired starting date for data extraction
        start_week : int
            The desired starting week, expressed as an integer, for data extraction
        end_date : datetime.datetime
            The desired end date for data extraction

        Returns
        -------
        tuple[dict[str, str]]
            A list of request parameters formatted as dictionaries of "remote_file_path"
            and "local_file_path" strings.
        """
        # Extract all links on the page hosting 4KM VHI data and filter them down to
        # links representing data within the year(s) of interest
        all_urls = HTTPExtractor(self).get_hrefs(self.data_download_url)
        valid_year_urls = [link for link in all_urls if re.match(r"VHP\.G04\.C07\..*\.P[0-9]{7}\.VH\.nc", link)]
        # retrieve any files created between the desired start/end dates
        requests = []
        for file_url in valid_year_urls:
            file_year, file_week = self.return_year_week_from_path(file_url)
            if (
                (file_year > start_date.year or (file_year == start_date.year and file_week >= start_week))
                and (file_year < end_date.year or (file_year == end_date.year and file_week <= end_date.isocalendar()[1]))
            ):
                requests.append(
                    {
                        "remote_file_path": self.data_download_url + "/" + file_url,
                        "local_file_path": self.local_input_path() / file_url,
                    }
                )
        return requests

    def return_year_week_from_path(self, str_path: str) -> tuple[int, int]:
        """
        Derive the year and week from a filename

        Parameters
        ----------
        str_path : str
            The path of the requested file

        Returns
        -------
        tuple[int, int]
            A tuple containing integer representations of the year and week, e.g. (2023, 23)
        """
        if ".nc4" in str_path:
            year, week = int(str_path[-14:-10]), int(str_path[-10:-7])

        elif ".nc" in str_path:
            year, week = int(str_path[-13:-9]), int(str_path[-9:-6])
        return year, week

    # TRANSFORM STEPS

    def prepare_input_files(self, keep_originals: bool = False):
        """
        Command line tools converting raw downloaded data to daily / hourly data
        Note that `nccopy` will fail when reading incompletely downloaded files

        Parameters
        ----------
        keep_originals : bool, optional
            An optional flag to preserve the original files for debugging purposes. Defaults to False.
        """
        input_dir = pathlib.Path(self.local_input_path())
        originals_dir = input_dir.parent / (input_dir.stem + "_originals")
        for fil in input_dir.glob("*.tar"):
            if tarfile.is_tarfile(fil):
                out_file = input_dir / fil.stem
                with tarfile.TarFile(fil) as item:
                    item.extractall(out_file)
                if keep_originals:
                    os.makedirs(originals_dir, 0o755, True)
                    shutil.move(fil, (originals_dir / (fil.stem + fil.suffix)))
                else:
                    os.unlink(fil)
            else:
                raise TypeError(
                    f"Fil {fil} has a tar extension but is not a valid tar\
                        and hence cannot be safely unpacked. Please investigate."
                )
        for fil in input_dir.glob("*.nc"):
            if ".nc1." in fil.suffix:
                fil.replace(fil.with_suffix(".nc"))
        # move untarred files up one level out of enclosing folders, then delete those folders
        for fil in input_dir.rglob("*.*"):
            fil.rename(input_dir / fil.name)
        for unzipped_dir in list(input_dir.glob("**"))[
            1:
        ]:  # the 1st entry is the current directory, don't want to delete that!
            shutil.rmtree(unzipped_dir)
        # Convert all NCs to NC4s
        raw_files = sorted([pathlib.Path(file) for file in glob.glob(str(self.local_input_path() / "*.nc"))])
        self.converter.ncs_to_nc4s(raw_files, keep_originals)


    def vhi_weeks_per_year(self, date_range: tuple[datetime.datetime, datetime.datetime]) -> list[datetime.datetime]:
        """
        Calculate the weeks covered by VHI in a given time range, according to the logic of its update schedule

        Parameters
        ----------
        date_range : tuple[datetime.datetime, datetime.datetime]
            A range of dates formatted to match VHI's update schedule

        Returns
        -------
        vhi_weeks : list[datetime.datetime]
            A list of weeks to request expressed as datetimes
        """
        vhi_weeks = []
        for year in range(date_range[0].year, date_range[1].year + 1):
            if datetime.datetime(year, 1, 1) > date_range[0]:
                if datetime.datetime(year, 12, 31) > date_range[1]:
                    vhi_weeks.append(
                        self.vhi_daterange(
                            datetime.datetime(year, 1, 1).isoformat(),
                            date_range[1].isoformat(),
                        )
                    )
                else:
                    vhi_weeks.append(
                        self.vhi_daterange(
                            datetime.datetime(year, 1, 1).isoformat(),
                            datetime.datetime(year, 12, 31).isoformat(),
                        )
                    )
            else:
                if datetime.datetime(year, 12, 31) > date_range[1]:
                    vhi_weeks.append(self.vhi_daterange(date_range[0].isoformat(), date_range[1].isoformat()))
                else:
                    vhi_weeks.append(
                        self.vhi_daterange(
                            date_range[0].isoformat(),
                            datetime.datetime(year, 12, 31).isoformat(),
                        )
                    )
        return vhi_weeks

    def vhi_daterange(self, start: str, end: str) -> tuple[datetime.datetime, datetime.datetime]:
        """
        VHI updates on a slightly odd schedule that shifts around every year.
        To ensure we request the precise days we want, we have to do some gymnastics.

        Parameters
        ----------
        start : str (isoformatted)
            First date to include
        end : str (isformatted)
            Last date to include

        Returns
        -------
        date_range : tuple[datetime.datetime, datetime.datetime]
            A range of dates formatted to match VHI's update schedule
        """
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
        date_range = pd.DatetimeIndex([])
        for year in range(start.year, end.year + 1):
            year_end = datetime.datetime(year, 12, 29)
            if year_end > end:
                year_end = end
            year_start = datetime.datetime(year, 1, 1)
            if year_start < start:
                # we want the first valid day after the start
                # this returns the first valid day in the range, or None
                year_start = next(
                    (day for day in pd.date_range(datetime.datetime(year, 1, 1), year_end, freq="7D") if day >= start),
                    None,
                )
            # There's never a file for the last day (or two days in a leap year)
            # don't make a entry for it
            date_range = date_range.union(pd.date_range(year_start, year_end, freq="7D"))
        return date_range

    def get_date_range_from_file(self, path: str) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Open file and return the start and end date of the data.
        The dimension name used to store dates should be passed as `dimension`

        Parameters
        ----------
        path : str
            The location of the file

        Returns
        -------
        tuple
            tuple of datetime.datetime.date, datetime.datetime.date
            the first and last days (inclusive) covered by the netcdf
        """
        year, week = self.return_year_week_from_path(path)
        # There are 52 weeks in a year, but 365 is not dividible by 7,
        # we will end up missing 1-2 days of data a year at the tail end.
        start_date = datetime.datetime(year, 1, 1) + datetime.timedelta(days=(7 * (week - 1)))
        end_date = start_date + datetime.timedelta(days=6)
        return start_date, end_date

    def input_date_range(self) -> tuple[datetime.datetime, datetime.datetime]:
        """
        Get the beginning and end of the range of dates in the list of files in `self.input_files()`

        Returns
        -------
        tuple
            tuple of datetime.datetimes containing (start date, end date)
        """
        input_files = list(self.input_files())
        return (
            self.get_date_range_from_file(input_files[0])[0],
            self.get_date_range_from_file(input_files[-1])[1],
        )

    def set_zarr_metadata(self, dataset: xr.Dataset) -> xr.Dataset:
        """
        Function to append to or update key metadata information to the attributes and encoding of the output Zarr.
        Extends existing class method to create attributes or encoding specific to ERA5.

        Parameters
        ----------
        dataset: xr.Dataset
            The dataset prepared for parsing

        Returns
        -------
        dataset : xr.Dataset
            The dataset with metadata formatted correctly for parsing
        """
        dataset = super().set_zarr_metadata(dataset)
        # Newer satellites have different attributes we must accommodate to prevent update ETL failure
        try:
            dataset.attrs["preferred citation"] = dataset.attrs["CITATION_TO_DOCUMENTS"]
        except KeyError:
            dataset.attrs["preferred citation"] = dataset.attrs["summary"] + " from " + dataset.attrs["creator_name"]
        # Delete problematic or extraneous holdover attributes from the input files
        keys_to_remove = [
            "scale_factor",
            "add_offset",
            "cdm_data_type",
            "ANCILLARY_FILES",
            "CITATION_TO_DOCUMENTS",
            "CONFIGURE_FILE_CONTENT",
            "CONTACT",
            "DAYS_PER_PERIOD",
            "FILENAME",
            "INPUT_FILENAMES",
            "INPUT_FILES",
            "INSTRUMENT",
            "DATE_BEGIN",
            "DATE_END",
            "DAYS PER PERIOD",
            "Metadata_Conventions",
            "PERIOD_OF_YEAR",
            "PRODUCT_NAME",
            "PROJECTION",
            "SATELLITE",
            "START_LATITUDE_RANGE",
            "START_LONGITUDE_RANGE",
            "END_LATITUDE_RANGE",
            "END_LONGITUDE_RANGE",
            "TIME_BEGIN",
            "TIME_END",
            "VERSION",
            "YEAR",
            "time_coverage_start",
            "time_coverage_end",
            "satellite_name",
            "standard_name_vocabulary",
            "version",
            "created",
            "creator_name",
            "creator_email",
            "creator_url",
            "date_created",
            "geospatial_lat_units",
            "geospatial_lon_units",
            "geospatial_lat_min",
            "geospatial_lon_min",
            "geospatial_lat_max",
            "geospatial_lon_max",
            "geospatial_lat_resolution",
            "geospatial_lon_resolution",
            "history",
            "id",
            "institution",
            "instrument_name",
            "naming_authority",
            "process",
            "processing_level",
            "project",
            "publisher_name",
            "publisher_email",
            "publisher_url",
            "references",
            "source",
            "summary",
        ]
        for key in keys_to_remove:
            dataset.attrs.pop(key, None)
            dataset[self.data_var].attrs.pop(key, None)
            if key != "dtype":
                dataset[self.data_var].encoding.pop(key, None)
        return dataset