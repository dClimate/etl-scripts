import datetime
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
        **kwargs,
    ):
        """
        Initialize a new VHI assessor.
        """
        IPFS.__init__(self, host="http://127.0.0.1:5001")
        super().__init__(
            *args,
            **kwargs,
            dataset_name=dataset_name,
        )
        self.dataset_name = dataset_name
        self.time_resolution = time_resolution
        self.latest_possible_date = None
        self.dataset_start_date = datetime.datetime(2019, 1, 1, 0)

    def start(self, latest_possible_date, args={}) -> tuple[tuple[datetime.datetime, datetime.datetime], dict]:
        latest_possible_date_dt = latest_possible_date.astype('M8[ms]').astype(datetime.datetime)
        # Reset the time components
        end = latest_possible_date_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        self.latest_possible_date = end
        self.allow_overwrite = args["--overwrite"]
        self.rebuild_requested = args["init"] 
        # Load the previous dataset and extract date range
        self.existing_dataset = self.get_existing_stac_metadata()
        # Initialize pipeline metadata
        pipeline_metadata = {
            "existing_dataset": self.existing_dataset,
        }
        # Skip everything since latest_possible_date is None
        if self.latest_possible_date is None:
            return None, pipeline_metadata
        # Check if new data should be fetched
        if self.check_if_new_data():
            existing_date_range = self.existing_dataset["properties"]["date_range"]
            if existing_date_range is not None:
                # Calculate the start date as the day after the existing end date
                existing_end_date = datetime.datetime.strptime(existing_date_range[1], "%Y%m%d%H")
                self.info(f"Existing data ends at {existing_end_date}")
                # Start date is the next day after the existing end date
                start_date = existing_end_date + datetime.timedelta(days=7)
                # Return the calculated start and end dates
                self.info(f"New data will be fetched from {start_date} to {self.latest_possible_date}")
                defined_dates = (start_date, self.latest_possible_date)
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
            return self.latest_possible_date > existing_end_date
        return False
