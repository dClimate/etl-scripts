import datetime
import os
import xarray as xr

import pathlib


from dataset_manager.utils.ipfs import IPFS


from dataset_manager.utils.logging import Logging


from dc_etl import filespec
import abc


class Assessor(abc.ABC):
    """A component responsible for fetching data from a data source and providing it to an Extractor."""

    @abc.abstractmethod
    def start(self):
        """Start the analysis

        """


HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)

class VHIAssessor(Assessor, Logging, IPFS):

    rebuild_requested: bool = False
    allow_overwrite: bool = False

    def __init__(
        self,
        *args,
        dataset_name: str = None,
        time_resolution: str = None,
        dataset_start_date: datetime.datetime = None,
        skip_finalization: bool = False,
        cdsapi_key: str = None,
        **kwargs,
    ):
        """
        Initialize a new ERA5 object with appropriate chunking parameters.

        Parameters
        ----------
        skip_finalization : bool
            A trigger to ignore the finalization check and proceed directly with the start/end dates
        cdsapi_key : str
            A custom API key string for ECMWF's CDS API. If unfilled the orchestration system will use a default key.
        """
        IPFS.__init__(self, host="http://127.0.0.1:5001")
        super().__init__(
            *args,
            **kwargs,
            dataset_name=dataset_name,
        )
        self.dataset_name = dataset_name
        self.time_resolution = time_resolution
        self.dataset_start_date = datetime.datetime(2019, 1, 1, 0)
        self.skip_finalization = skip_finalization
        self.vhi_latest_possible_date = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        self.cdsapi_key = cdsapi_key if cdsapi_key else os.environ.get("CDSAPI_KEY")


    def start(self, args={}) -> tuple[tuple[datetime.datetime, datetime.datetime], dict]:
        self.allow_overwrite = args["--overwrite"]
        self.rebuild_requested = args["init"] 
        # Load the previous dataset and extract date range
        self.existing_dataset = self.get_existing_stac_metadata()

        # Initialize pipeline metadata
        pipeline_metadata = {
            "existing_dataset": self.existing_dataset,
        }
        # Check if new data should be fetched
        if self.check_if_new_data():
            existing_date_range = self.existing_dataset["properties"]["date_range"]
            if existing_date_range is not None:
                # Calculate the start date as the day after the existing end date
                existing_end_date = datetime.datetime.strptime(existing_date_range[1], "%Y%m%d%H")
                self.info(f"Existing data ends at {existing_end_date}")

                # Start date is the next day after the existing end date
                start_date = existing_end_date + datetime.timedelta(days=7)

                # End date is the latest possible date, minus 7 days to ensure full week increments
                end_date = self.vhi_latest_possible_date.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Return the calculated start and end dates
                self.info(f"New data will be fetched from {start_date} to {end_date}")
                defined_dates = (start_date, end_date)
                return defined_dates, pipeline_metadata
        self.info("No new data detected, skipping fetch")
        return None, pipeline_metadata

    def key(self) -> str:
        """
        Returns the key value that can identify this set in a JSON file. JSON key takes the form of either
        name-measurement_span or name-today. If `append_date` is True, add today's date to the end of the string

        Parameters
        ----------
        append_date : bool, optional
            Whether to add today's date to the end of the key string

        Returns
        -------
        str
            The formatted JSON key

        """
        key = f"{self.dataset_name}-{self.time_resolution}"
        return key

    # Load existing dataset via ipfs 
    def get_existing_stac_metadata(self) -> xr.Dataset:
        """
        Load the existing dataset from IPFS

        Returns
        -------
        xr.Dataset
            The existing dataset
        """
        # Get the latest hash of the dataset
        ipns_name = self.key()
        try:
            ipfs_hash = self.ipns_resolve(ipns_name)
            json_obj = self.ipfs_get(ipfs_hash)
            return json_obj
        except Exception as e:
            print(e)
            return None

    def check_if_new_data(self) -> bool:
        """
        Check if there is new data available in downloaded data
         by comparing its end date to the end date of existing data.
        If there is no existing data, or rebuild was requested, consider downloaded data to be new and return True.

        If there is no downloaded data, issue a warning and return False.

        @return   True if there is new data in downloaded data, False otherwise
        """
        if self.rebuild_requested or not bool(self.existing_dataset):
            self.info("All local data will be used because the incoming dataset is starting from scratch")
            return True
        if self.allow_overwrite:
            self.info("All local data will be used because the allow overwrite flag triggered")
            return True
        existing_date_range = self.existing_dataset["properties"]["date_range"]
        if existing_date_range is not None:
            existing_end_date = datetime.datetime.strptime(existing_date_range[1], "%Y%m%d%H")
            self.info(f"Existing data ends at {existing_end_date}")
            end = self.vhi_latest_possible_date.replace(hour=0, minute=0, second=0, microsecond=0)
            return end > existing_end_date
        return False

    
    def define_dates(self, date_range: list = None) -> tuple[datetime.datetime, datetime.datetime]:
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
