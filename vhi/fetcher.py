# VHI.py
import pathlib
import re
import datetime

import pandas as pd
import numpy as np
import glob
from typing import List

import requests
from requests.adapters import HTTPAdapter, Retry

from dc_etl.fetch import Fetcher, Timespan
from typing import Generator
from dc_etl.filespec import FileSpec
from dataset_manager.utils.extractor import HTTPExtractor
from dataset_manager.utils.logging import Logging
from dataset_manager.utils.converter import NCtoNetCDF4Converter



class VHI(Fetcher, Logging):
    """
    VHI is a gridded dataset of vegetation health published in netCDF format that's updated every week.
    """
    def __init__(
        self,
        *args,
        cache: FileSpec | None = None,
        dataset_name: str = "vhi",
        **kwargs,
    ):
        """
        Initialize a new VHI object with appropriate chunking parameters.
        """
        super().__init__(dataset_name)
         # Initialize properties directly
        self._cache = cache
        self.converter = NCtoNetCDF4Converter(dataset_name)

    data_download_url = "https://www.star.nesdis.noaa.gov/data/pub0018/VHPdata4users/data/Blended_VH_4km/VH"

    # dataset_start_date = datetime.datetime(1981, 8, 27, 0)
    dataset_start_date = datetime.datetime(2019, 1, 1, 0)

    dataset_name = "vhi"
    data_var = "VHI"

    def relative_path(self):
        return pathlib.Path("vhi")


    def get_remote_timespan(self) -> Timespan:
        all_urls = HTTPExtractor(self).get_hrefs(self.data_download_url)
        valid_year_urls = [link for link in all_urls if re.match(r"VHP\.G04\.C07\..*\.P[0-9]{7}\.VH\.nc", link)]
        # Filter out for the current year and get the latest week
        valid_year_urls = sorted(valid_year_urls, reverse=True)
        latest_available_file = valid_year_urls[0]
        # Get the year and week from the latest file
        year, week = self.return_year_week_from_path(latest_available_file)
        # Using the year and week, calculate the latest date
        first_day_of_year = datetime.date(year, 1, 1)
        week_date = first_day_of_year + datetime.timedelta(weeks=week-1)
        # Convert to np.datetime64 format
        latest_time = np.datetime64(week_date)
        earliest_time = np.datetime64(self.dataset_start_date)
        return Timespan(start=earliest_time, end=latest_time)

    def prefetch(self):
    # Implementation of the method
        return "prefetch data"

    def fetch(self, span: Timespan, pipeline_info: dict) -> Generator[FileSpec, None, None]:
        """Implementation of :meth:`Fetcher.fetch`"""
        current_datetime = pd.to_datetime(span.start).to_pydatetime()
        limit_datetime = pd.to_datetime(span.end).to_pydatetime()
        self.info(f"Fetching data for the timespan from {span.start} to {span.end}")
        self.extract((current_datetime, limit_datetime))
        self.convert_downloaded_files()
        # Extracting the start and end years from the timespan
        start_year = current_datetime.year
        end_year = limit_datetime.year
        for year in range(start_year, end_year + 1):
            vhi_weeks = self.get_vhi_weeks((current_datetime, limit_datetime))
            for week_date in vhi_weeks[0]:
                week_date = pd.to_datetime(week_date).to_pydatetime()
                # Only process dates in the current year
                if week_date.year != year:
                    continue
                # Calculate simple week number (1-based)
                day_of_year = week_date.timetuple().tm_yday
                week_number = ((day_of_year - 1) // 7) + 1
                week_number = min(week_number, 52)  # Cap at 52 weeks
                yield self._get_file_by_year_week(year, week_number)

    
    def _get_file_by_year_week(self, year, week):
        """Get a FileSpec for the year, using the cache if configured."""
        # Check cache
        if self._cache.exists():
            for path in self._cache.fs.ls(self.local_input_path()):
                # Only consider files with .nc extension
                if path.endswith(".nc4"):
                    file_year, file_week = self.return_year_week_from_path(path)
                    if file_year == year and file_week == week:
                        # print(f"Found file in cache: {path}")
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
        # define request dates (must be in weekly format)
        start_date, start_week, end_date = self.define_request_dates(date_range)
        # Prepare a session
        self.get_session()
        # Create requests for all relevant files within the specified time range
        requests = self.batch_requests(start_date, start_week, end_date)
        # Download files
        found_any_files = HTTPExtractor(self).pool(requests)
        # return a list of new files downloaded. If none this equates to False and terminates the ETL
        # Remove "_downloading" from all successfully downloaded files
        # for request in requests:
        #     downloading_file_path = request["local_file_path"]
        #     final_file_path = downloading_file_path.with_name(downloading_file_path.stem.replace("_downloading", "") + downloading_file_path.suffix)
            
        #     # Check if the "_downloading" file exists and rename it
        #     if downloading_file_path.exists():
        #         os.rename(downloading_file_path, final_file_path)
        #         print(f"Renamed {downloading_file_path} to {final_file_path}")

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

    def get_week_number(self, input_date):
        """Get week number (1-52) from a date."""
        year_start = datetime.datetime(input_date.year, 1, 1)
        week = (input_date - year_start).days // 7 + 1
        return min(week, 52)  # Cap at 52 weeks

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
        if new_start_date >= datetime.datetime(new_start_date.year, 12, 30):
            return datetime.datetime(new_start_date.year + 1, 1, 1), 1

        week = self.get_week_number(new_start_date)

        if (new_start_date - datetime.datetime(new_start_date.year, 1, 1)).days % 7 != 0:
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
            # Calculate end week number  
            end_week = self.get_week_number(end_date)
            if (
                (file_year > start_date.year or (file_year == start_date.year and file_week >= start_week))
                and (file_year < end_date.year or (file_year == end_date.year and file_week <= end_week))
            ):
                local_file_path = self.local_input_path() / file_url
                if local_file_path.exists():
                    print(f"File {local_file_path} already exists, skipping download.")
                    continue

                requests.append(
                    {
                        "remote_file_path": self.data_download_url + "/" + file_url,
                        "local_file_path": local_file_path,
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
    def convert_downloaded_files(self):
        """
        Command line tools converting raw downloaded data to daily / hourly data
        Note that `nccopy` will fail when reading incompletely downloaded files

        Parameters
        ----------
        """
        # Convert all NCs to NC4s
        raw_files = sorted([pathlib.Path(file) for file in glob.glob(str(self.local_input_path() / "*.nc"))])
        self.converter.ncs_to_nc4s(raw_files)


    def get_vhi_weeks(self, date_range: tuple[datetime, datetime]) -> List[pd.DatetimeIndex]:
        """
        Calculate the weeks covered by VHI in a given time range, accounting for VHI's update schedule.
        VHI updates every 7 days, but skips the last 1-2 days of each year.

        Parameters
        ----------
        date_range : tuple[datetime, datetime]
            Start and end dates for the period of interest

        Returns
        -------
        List[pd.DatetimeIndex]
            List of DatetimeIndex objects containing valid VHI dates for each year
        """
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
        result = []
        
        for year in range(start_date.year, end_date.year + 1):
            # Define year boundaries
            year_start = max(start_date, pd.Timestamp(f"{year}-01-01"))
            year_end = min(end_date, pd.Timestamp(f"{year}-12-29"))
            
            # Find first valid week start after year_start
            valid_weeks = pd.date_range(
                pd.Timestamp(f"{year}-01-01"),
                year_end,
                freq="7D"
            )
            year_start = valid_weeks[valid_weeks >= year_start][0] if len(valid_weeks[valid_weeks >= year_start]) > 0 else None
            
            if year_start is not None and year_start <= year_end:
                result.append(pd.date_range(year_start, year_end, freq="7D"))
        return result
