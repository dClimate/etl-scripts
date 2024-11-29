import datetime
import xarray as xr

import pathlib

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

from utils.helper_functions import numpydate_to_py

import copernicusmarine as cm_client
import os
from dc_etl.fetch import Timespan
from dc_etl.assessor import Assessor
import numpy as np


from dataset_manager.utils.ipfs import IPFS


from dataset_manager.utils.logging import Logging

from dc_etl import filespec


HERE = filespec.file(pathlib.Path(__file__).parent, auto_mkdir=True)

class CopernicusAssessor(Assessor, Logging, IPFS):

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
        Initialize a new Copernicus assessor.
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
        self.dataset_start_date = datetime.datetime(1993, 1, 1)

    def get_remote_timespan(self) -> Timespan:
        # Get the end and start dates of the reanalysis data
        self.find_reanalysis_end_dates()
        earliest_time = np.datetime64(self.reanalysis_start_date)
        latest_time = np.datetime64(self.analysis_end_date)
        return Timespan(start=earliest_time, end=latest_time)

    def get_all_timespans(self) -> list[Timespan]:
        # self.find_reanalysis_end_dates()
        reanalysis_start_date = np.datetime64(self.reanalysis_start_date)
        reanalysis_end_date = np.datetime64(self.reanalysis_end_date)
        interim_reanalysis_start_date = np.datetime64(self.interim_reanalysis_start_date)
        interim_reanalysis_end_date = np.datetime64(self.interim_reanalysis_end_date)
        analysis_end_date = np.datetime64(self.analysis_end_date)
        return [
            Timespan(start=reanalysis_start_date, end=reanalysis_end_date),
            Timespan(start=interim_reanalysis_start_date, end=interim_reanalysis_end_date),
            Timespan(start=analysis_end_date, end=analysis_end_date),
        ]

    def get_end_date(self) -> datetime.datetime:
        metadata = self.get_existing_stac_metadata()
        if metadata is not None:
            date_str = metadata["properties"]["date_range"][1]
            try:
                # Try parsing with just the date
                dt = datetime.datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                # If parsing fails, try with the date and hour
                dt = datetime.datetime.strptime(date_str, "%Y%m%d%H")

            # Convert to numpy.datetime64 and return
            return np.datetime64(dt)

    def get_analysis_end_dates(self) -> tuple[np.datetime64, np.datetime64]:
        metadata = self.get_existing_stac_metadata()
        if metadata is not None:
            # Retrieve the datetime strings from the metadata
            reanalysis_date_str = metadata["properties"]["reanalysis_end_date"]
            interim_date_str = metadata["properties"]["interim_reanalysis_end_date"]
            
            if reanalysis_date_str is not None and interim_date_str is not None:
                try:
                    # Parse the reanalysis end date
                    reanalysis_dt = datetime.datetime.strptime(reanalysis_date_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                    # Parse the interim analysis end date, removing 'Z' and setting UTC timezone
                    interim_dt = datetime.datetime.strptime(interim_date_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                    # Convert both to numpy.datetime64 and return as a tuple
                    return np.datetime64(reanalysis_dt), np.datetime64(interim_dt)
                except ValueError as e:
                    raise ValueError(f"Error parsing dates: {e}")
        
        # If metadata is None or the dates are missing, raise an appropriate error
        raise ValueError(f"Invalid or missing end dates in metadata: {metadata}")

        

    def start(self, args={}) -> tuple[tuple[datetime.datetime, datetime.datetime], dict]:
        self.allow_overwrite = args["--overwrite"]
        self.rebuild_requested = args["init"] 
        # Load the previous dataset and extract date range
        self.existing_dataset = self.get_existing_stac_metadata()
        # Initialize pipeline metadata
        pipeline_metadata = {
            "existing_dataset": self.existing_dataset,
            "reanalysis_start_date": self.reanalysis_start_date.replace(tzinfo=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "reanalysis_end_date": self.reanalysis_end_date.replace(tzinfo=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "interim_reanalysis_start_date": self.interim_reanalysis_start_date.replace(tzinfo=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "interim_reanalysis_end_date": self.interim_reanalysis_end_date.replace(tzinfo=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        }
        return pipeline_metadata

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
            print(ipfs_hash)
            json_obj = self.ipfs_get(ipfs_hash)
            return json_obj
        except Exception as e:
            print(e)
            return None

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
        interim_reanalysis_ds = cm_client.open_dataset(
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
        self.interim_reanalysis_start_date = numpydate_to_py(interim_reanalysis_ds.time[0].values)
        self.interim_reanalysis_end_date = numpydate_to_py(interim_reanalysis_ds.time[-1].values)
        self.info(f"determined interim reanalysis data to be final through {self.interim_reanalysis_end_date}")
        # Get Analysis End Date
        now = datetime.datetime.now()
        noon = datetime.datetime(now.year, now.month, now.day, 12)
        if now >= noon:
            latest_measurement = noon - datetime.timedelta(days=1)
        else:
            latest_measurement = noon - datetime.timedelta(days=2)
        self.analysis_end_date = numpydate_to_py(latest_measurement)
        self.info(f"determined analysis data to be final through {self.analysis_end_date}")


class CopernicusOceanSeaSurfaceHeightAssessor(CopernicusAssessor, CopernicusOceanSeaSurfaceHeightValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSeaSurfaceHeightValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanTemp0p5DepthAssessor(CopernicusAssessor, CopernicusOceanTemp0p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanTemp0p5DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanTemp1p5DepthAssessor(CopernicusAssessor, CopernicusOceanTemp1p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanTemp1p5DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanTemp6p5DepthAssessor(CopernicusAssessor, CopernicusOceanTemp6p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanTemp6p5DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanSalinity0p5DepthAssessor(CopernicusAssessor, CopernicusOceanSalinity0p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity0p5DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanSalinity1p5DepthAssessor(CopernicusAssessor, CopernicusOceanSalinity1p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity1p5DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanSalinity2p6DepthAssessor(CopernicusAssessor, CopernicusOceanSalinity2p6DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity2p6DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanSalinity25DepthAssessor(CopernicusAssessor, CopernicusOceanSalinity25DepthValues):
    
    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity25DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)

class CopernicusOceanSalinity109DepthAssessor(CopernicusAssessor, CopernicusOceanSalinity109DepthValues):
    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity109DepthValues.__init__(self)
        CopernicusAssessor.__init__(self, dataset_name=self.dataset_name, time_resolution=self.time_resolution, dataset_start_date=self.dataset_start_date)