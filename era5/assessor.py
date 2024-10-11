import datetime
import os
import xarray as xr
from abc import abstractmethod
from utils.helper_functions import numpydate_to_py
import random

import multiprocessing
import cdsapi
import tempfile
import dateutil
import pathlib
from multiprocess.pool import ThreadPool
from xarray.core.variable import MissingDimensionsError
from typing import Optional
from .base_values import ERA5PrecipValues, ERA52mTempValues, ERA5SurfaceSolarRadiationDownwardsValues, ERA5VolumetricSoilWaterLayer1Values, ERA5VolumetricSoilWaterLayer2Values, ERA5VolumetricSoilWaterLayer3Values, ERA5VolumetricSoilWaterLayer4Values, ERA5InstantaneousWindGust10mValues, ERA5WindU10mValues, ERA5WindV10mValues, ERA5WindU100mValues, ERA5WindV100mValues, ERA5SeaSurfaceTemperatureValues, ERA5SeaSurfaceTemperatureDailyValues, ERA5SeaLevelPressureValues, ERA5LandPrecipValues, ERA5LandDewpointTemperatureValues, ERA5LandSnowfallValues, ERA5Land2mTempValues, ERA5LandSurfaceSolarRadiationDownwardsValues, ERA5LandSurfacePressureValues, ERA5LandWindUValues, ERA5LandWindVValues


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

class ERA5FamilyAssessor(Assessor, Logging, IPFS):

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
        self.dataset_start_date = dataset_start_date
        self.skip_finalization = skip_finalization
        self.era5_latest_possible_date = datetime.datetime.utcnow() - datetime.timedelta(days=6)
        self.cdsapi_key = cdsapi_key if cdsapi_key else os.environ.get("CDSAPI_KEY")


    def start(self, args={}) -> tuple[tuple[datetime.datetime, datetime.datetime], dict]:
        self.allow_overwrite = args["--overwrite"]
        self.rebuild_requested = args["init"] 
        # Load the previous dataset and extract date range
        self.existing_dataset = self.get_existing_stac_metadata()

        # Initialize pipeline metadata
        pipeline_metadata = {
            "existing_dataset": self.existing_dataset,
            "finalization_date": getattr(self, "finalization_date", None),
        }
        # Check if new data should be fetched
        if self.check_if_new_data():
            defined_dates = self.define_dates(original_dataset=self.existing_dataset)
            pipeline_metadata["finalization_date"] = getattr(self, "finalization_date", None)
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
            end = self.era5_latest_possible_date.replace(hour=23, minute=0, second=0, microsecond=0)
            return end > existing_end_date
        return False

    def define_dates(
        self,
        date_range: list[datetime.datetime, datetime.datetime] = None,
        original_dataset: dict = None,
    ) -> tuple[datetime.datetime, datetime.datetime, Optional[datetime.datetime], Optional[datetime.datetime]]:
        """
        Generate key dates for the extract function based on previously generated datasets (if available) and
        the latest published finalization dates from ECMWF.

        Parameters
        ----------
        date_range : list[datetime.datetime, datetime.datetime]
            A tuple of start and end datetime objects specified by the user.
        original_dataset : dict
            The existing Zarr dataset, if it exists.

        Returns
        -------
        tuple[datetime.datetime, datetime.datetime]
            A tuple containing the start and end dates for the request.
        """
        new_finalization_date_start: Optional[datetime.datetime] = None
        new_finalization_date_end: Optional[datetime.datetime] = None
        # Check for conflicts between date_range and existing datasets
        if not self.rebuild_requested and date_range and original_dataset and not self.allow_overwrite:
            self.warn("Date range provided but existing data exists and overwrite is not allowed. Parse will fail.")

        # Define the end date latest possible date
        end = self.era5_latest_possible_date.replace(hour=23, minute=0, second=0, microsecond=0)

        # Rebuild requested or no existing dataset
        if self.rebuild_requested or not original_dataset:
            self.info("Starting new dataset")
            self.load_finalization_date()
            # # TODO: TEMPORARY FIX
            # self.finalization_date = "2024091723"
            current_date = self.dataset_start_date
            return current_date, end, new_finalization_date_start, new_finalization_date_end

        # Use the specified date range if no rebuild is requested
        if date_range:
            self.info("Using provided date range.")
            start, end = date_range
            return start, end, new_finalization_date_start, new_finalization_date_end

        # Derive start date from the existing dataset if available for appends
        if original_dataset:
            props = original_dataset.get("properties", {})
            if "date_range" in props:
                self.info("Using existing dataset's date range.")
                existing_end_date = datetime.datetime.strptime(props["date_range"][1], "%Y%m%d%H")
                start = existing_end_date + datetime.timedelta(hours=1)
            else:
                # Throw error because this should not happen
                raise ValueError("Existing dataset has no date range.")

        # Check if finalization is needed
        if original_dataset and "last_finalization_date_change" in props:
            last_fin_date_change = datetime.datetime.fromisoformat(props["last_finalization_date_change"])
            if (datetime.datetime.now().year == last_fin_date_change.year and
                datetime.datetime.now().month == last_fin_date_change.month):
                self.info(f"Finalization already checked this month on {last_fin_date_change.date().isoformat()}.")
                self.skip_finalization = True

        # If finalization is needed, set the start date to the finalization date and just replace everything from that date
        if not self.skip_finalization:
            self.load_finalization_date()
            # TODO: TEMPORARY FIX
            # self.finalization_date = "2024091723"
            finalization_date_obj = datetime.datetime.strptime(self.finalization_date, "%Y%m%d%H")
            if "finalization_date" in props:
                previous_finalization_date = datetime.datetime.strptime(props["finalization_date"], "%Y%m%d%H")
            print(f"Previous finalization date: {previous_finalization_date}")
            if previous_finalization_date and previous_finalization_date < finalization_date_obj:
                new_finalization_date_start = previous_finalization_date + datetime.timedelta(hours=1)
                new_finalization_date_end = finalization_date_obj
                self.allow_overwrite = True

        # The first portion is the append date range and the second portion is the finalization date range which will be replaced
        return start, end, new_finalization_date_start, new_finalization_date_end

    def get_list_of_times(self, time_range=range(0, 24)) -> str:
        return [f"{time_index:02}:00" for time_index in time_range]

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

    @abstractmethod
    def load_finalization_date(self, force: bool = False):
        """
        Set `ERA5Family.finalization_date` to a datetime representing the date up to which
         data is marked finalized by Copernicus.
        This is determined by examining the `expver` attribute in the source data headers.
        A few months of data needs to be downloaded, so that it can be determined
         at which date `expver` changes from 1 to 5.
        ERA5 and ERA5Land use slightly different methods of examining `expver`,
         so this method is implemented separately in each child class.

        Parameters
        ----------
        force
            A bool representing whether an existing `ERA5Family.finalization_date` value should be overwritten
        """
        pass

class ERA5Assessor(ERA5FamilyAssessor):

    # Sample coordinates to use to check for finalization date
    FINALIZATION_LAT = 40.75
    FINALIZATION_LON = -74.25


    def load_finalization_date(self, force: bool = False):
        """
        Download the six most recent months of data for a small region (NYC)
         in netCDF format from ECMWF and iterate through the time dimension,
        checking for the date when expver changes from 1 to 5.
        This will indicate when ERA5 (finalized) changes to ERA5T (preliminary).
        If `ERA5Family.finalization_date` is already set, this will be skipped unless `force` is set.

        Parameters
        ----------
        end
            A datetime representing the final value of the date range requested
        force
            A bool representing whether an existing `ERA5Family.finalization_date` value should be overwritten
        """
        if force or not hasattr(self, "finalization_date"):
            self.info("requesting history of NYC lat/lon to check for ERA5 finalization date")

            # Get the most recent six months of data as a single netcdf
            temp_path = tempfile.TemporaryDirectory()
            path = pathlib.Path(temp_path.name) / "era5_cutoff_date_test.nc"

            now = datetime.datetime.now()
            current_date = datetime.datetime(now.year, now.month, 1) - dateutil.relativedelta.relativedelta(months=6)

            request = {
                "date": f"{current_date.date()}/{now.date()}",
                "time": self.get_list_of_times(),
                "area": [
                    self.FINALIZATION_LAT,
                    self.FINALIZATION_LON,
                    self.FINALIZATION_LAT - 0.25,
                    self.FINALIZATION_LON + 0.25,
                ],
                "format": "netcdf",
            }
            client = cdsapi.Client(verify=True, key=self.cdsapi_key)
            self.info(f"CDS API Client key is {client.key}")
            self.request_file(client, request, path)
            try:
                test_set = xr.load_dataset(path)
            except FileNotFoundError:
                raise ValueError(
                    f"Unable to open a dataset at {path}. This likely indicates that ERA5's CDS API "
                    "failed to deliver the requested finalization data. Please try again later when "
                    "the CDS API is less stressed."
                )
            # if expver dimension is not present,something unexpected is wrong
            # since there should be a mix of finalized and unfinalized data in the set
            if "expver" not in test_set:
                raise MissingDimensionsError(f"No expver dimension found in {path}")
            else:
                # find the position of the first nan in the first
                # (final, expver = 1) column of the component key values.
                # Move back one for the previous time index.
                # argmax finds the max value -- the first nan
                previous_time_idx = test_set.expver.values.argmax() - 1
                previous_time = test_set.valid_time[previous_time_idx].values
                if previous_time is None:
                    raise ValueError(
                        f"No finalized data detected in {path}, even though *expver* dimension is present"
                    )
                else:
                    self.finalization_date = datetime.datetime.strftime(numpydate_to_py(previous_time), "%Y%m%d%H")
                    self.info(f"determined data to be final through {self.finalization_date}")


class ERA5LandAssessor(ERA5FamilyAssessor):

    # Sample coordinates to use to check for finalization date
    FINALIZATION_LAT = 40.75
    FINALIZATION_LON = -74.25

    def load_finalization_date(self, force: bool = False):
        """
        Copernicus sets a header variable named `expver` to 1 if GRIB data contains finalized data.
        Otherwise, it sets `expver` to 5. Copernicus always finalizes a full month's worth of data at a time.
        Therefore, to get the finalization date, download the first day of the month for the previous
        six months of GRIB data for the same region of data as used in the test suite, one file per day.
        Iterate through the files, incrementing `ERA5Land.finalization_date` while the `expver` is 1.
        When `expver` equals 5, or all the files have been checked, end the function.

        Parameters
        ----------
        force
            A bool representing whether an existing `ERA5Family.finalization_date` value should be overwritten
        """
        if force or not hasattr(self, "finalization_date"):
            self.info(
                "requesting the previous three first days of the month "
                "for a small region to check for ERA5Land finalization date"
            )

            # Start on the first day of the month six months before today
            now = datetime.datetime.now()
            current_date = datetime.datetime(now.year, now.month, 1) - dateutil.relativedelta.relativedelta(months=6)

            # Args to collect to pass to ERA5Land.request_file via multiprocessing.Pool.starmap
            job_args = []

            # Get a directory for storing the temporary GRIB data
            temp_dir = tempfile.TemporaryDirectory()
            temp_dir_path = pathlib.Path(temp_dir.name)
            self.info(f"Got a temporary directory at {temp_dir_path}")

            # Build requests for each first day of the month from six months before today
            client = cdsapi.Client(verify=True, key=self.cdsapi_key)
            self.info(f"CDS API Client key is {client.key}")
            while current_date <= now:
                request = {
                    "date": str(current_date.date()),
                    "time": self.get_list_of_times(),
                    "area": [
                        self.FINALIZATION_LAT,
                        self.FINALIZATION_LON,
                        self.FINALIZATION_LAT - 0.25,
                        self.FINALIZATION_LON + 0.25,
                    ],
                    "format": "grib",
                }
                path = temp_dir_path.joinpath(
                    f"{self.dataset_name}_finalization_test_{current_date.strftime('%Y%m%d')}.grib"
                )
                self.debug(f"Queuing request for {path}")
                job_args.append((client, request, path))
                current_date += dateutil.relativedelta.relativedelta(months=1)

            # Request all files asynchronously using separate processes
            found_any_files = False
            with ThreadPool(processes=max(6, multiprocessing.cpu_count() - 1)) as pool:
                for result in pool.starmap(self.request_file, job_args):
                    if not result[0]:
                        self.info(f"did not retrieve any data for {result[1]}")
                    else:
                        found_any_files = True
                        self.info(f"successfully retrieved {result[1]}")
            if not found_any_files:
                raise ValueError(
                    "No finalization files successfully retrieved from the API, " "please try again later."
                )

            # Start looking at every file just downloaded,sorted by date from the earliest day.
            # Break if prelim data is detected.
            for path in sorted(temp_dir_path.iterdir()):
                # The expver header needs to be explictly requested
                dataset = xr.load_dataset(path, backend_kwargs={"read_keys": ["expver"]})

                # Any value besides 1 should be treated as non-finalized data.
                # If non-finalized data is detected, the finalization date has
                # incremented as far as it should go.
                if int(dataset[self.data_var].attrs["GRIB_expver"]) == 1:
                    self.info(f"{path} contains finalized data, setting finalization date")
                    # Use the first timestamp because expver == 1 is only guaranteed to be true for the first value
                    self.finalization_date = datetime.datetime.strftime(numpydate_to_py(dataset.time.isel(time=0).values), "%Y%m%d%H")
                else:
                    self.info(f"{path} contains preliminary data, ending search for finalization date")
                    break


# SEA NEEDS TO HAVE THE LON MOVED
class ERA5SeaAssessor(ERA5Assessor):
    # need to shift the longitude west so it's over the ocean (pacific)
    FINALIZATION_LON = 130



# init the class, and run start
class ERA5SurfaceSolarRadiationDownwardsValuesAssessor(ERA5Assessor, ERA5SurfaceSolarRadiationDownwardsValues):

    # Init the class
    def __init__(self, *args, **kwargs):
        ERA5SurfaceSolarRadiationDownwardsValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)
        
class ERA5PrecipValuesAssessor(ERA5Assessor, ERA5PrecipValues):

    def __init__(self, *args, **kwargs):
        ERA5PrecipValues.__init__(self)
        ERA5Assessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA52mTempValuesAssessor(ERA5Assessor, ERA52mTempValues):
    def __init__(self, *args, **kwargs):
        ERA52mTempValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5VolumetricSoilWaterLayer1ValuesAssessor(ERA5Assessor, ERA5VolumetricSoilWaterLayer1Values):
    def __init__(self, *args, **kwargs):
        ERA5VolumetricSoilWaterLayer1Values.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5VolumetricSoilWaterLayer2ValuesAssessor(ERA5Assessor, ERA5VolumetricSoilWaterLayer2Values):
    def __init__(self, *args, **kwargs):
        ERA5VolumetricSoilWaterLayer2Values.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5VolumetricSoilWaterLayer3ValuesAssessor(ERA5Assessor, ERA5VolumetricSoilWaterLayer3Values):
    def __init__(self, *args, **kwargs):
        ERA5VolumetricSoilWaterLayer3Values.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5VolumetricSoilWaterLayer4ValuesAssessor(ERA5Assessor, ERA5VolumetricSoilWaterLayer4Values):
    def __init__(self, *args, **kwargs):
        ERA5VolumetricSoilWaterLayer4Values.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5InstantaneousWindGust10mValuesAssessor(ERA5Assessor, ERA5InstantaneousWindGust10mValues):
    def __init__(self, *args, **kwargs):
        ERA5InstantaneousWindGust10mValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5WindU10mValuesAssessor(ERA5Assessor, ERA5WindU10mValues):
    def __init__(self, *args, **kwargs):
        ERA5WindU10mValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5WindV10mValuesAssessor(ERA5Assessor, ERA5WindV10mValues):
    def __init__(self, *args, **kwargs):
        ERA5WindV10mValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5WindU100mValuesAssessor(ERA5Assessor, ERA5WindU100mValues):
    def __init__(self, *args, **kwargs):
        ERA5WindU100mValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5WindV100mValuesAssessor(ERA5Assessor, ERA5WindV100mValues):
    def __init__(self, *args, **kwargs):
        ERA5WindV100mValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5SeaSurfaceTemperatureValuesAssessor(ERA5SeaAssessor, ERA5SeaSurfaceTemperatureValues):
    def __init__(self, *args, **kwargs):
        ERA5SeaSurfaceTemperatureValues.__init__(self)
        ERA5SeaAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5SeaSurfaceTemperatureDailyValuesAssessor(ERA5SeaAssessor, ERA5SeaSurfaceTemperatureDailyValues):
    def __init__(self, *args, **kwargs):
        ERA5SeaSurfaceTemperatureDailyValues.__init__(self)
        ERA5SeaAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5SeaLevelPressureValuesAssessor(ERA5SeaAssessor, ERA5SeaLevelPressureValues):
    def __init__(self, *args, **kwargs):
        ERA5SeaLevelPressureValues.__init__(self)
        ERA5SeaAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandPrecipValuesAssessor(ERA5LandAssessor, ERA5LandPrecipValues):
    def __init__(self, *args, **kwargs):
        ERA5LandPrecipValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandDewpointTemperatureValuesAssessor(ERA5LandAssessor, ERA5LandDewpointTemperatureValues):
    def __init__(self, *args, **kwargs):
        ERA5LandDewpointTemperatureValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandSnowfallValuesAssessor(ERA5LandAssessor, ERA5LandSnowfallValues):
    def __init__(self, *args, **kwargs):
        ERA5LandSnowfallValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5Land2mTempValuesAssessor(ERA5LandAssessor, ERA5Land2mTempValues):
    def __init__(self, *args, **kwargs):
        ERA5Land2mTempValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandSurfaceSolarRadiationDownwardsValuesAssessor(ERA5LandAssessor, ERA5LandSurfaceSolarRadiationDownwardsValues):
    def __init__(self, *args, **kwargs):
        ERA5LandSurfaceSolarRadiationDownwardsValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandSurfacePressureValuesAssessor(ERA5LandAssessor, ERA5LandSurfacePressureValues):
    def __init__(self, *args, **kwargs):
        ERA5LandSurfacePressureValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandWindUValuesAssessor(ERA5LandAssessor, ERA5LandWindUValues):
    def __init__(self, *args, **kwargs):
        ERA5LandWindUValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class ERA5LandWindVValuesAssessor(ERA5LandAssessor, ERA5LandWindVValues):
    def __init__(self, *args, **kwargs):
        ERA5LandWindVValues.__init__(self)
        ERA5LandAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

