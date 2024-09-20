import datetime

import xarray as xr

from .fetcher import ERA5, ERA5Land
from .base_values import ERA5LandWindValues, ERA5PrecipValues, ERA52mTempValues, ERA5SurfaceSolarRadiationDownwardsValues, ERA5VolumetricSoilWaterValues, ERA5VolumetricSoilWaterLayer1Values, ERA5VolumetricSoilWaterLayer2Values, ERA5VolumetricSoilWaterLayer3Values, ERA5VolumetricSoilWaterLayer4Values, ERA5Wind10mValues, ERA5InstantaneousWindGust10mValues, ERA5WindU10mValues, ERA5WindV10mValues, ERA5Wind100mValues, ERA5WindU100mValues, ERA5WindV100mValues, ERA5SeaSurfaceTemperatureValues, ERA5SeaSurfaceTemperatureDailyValues, ERA5SeaLevelPressureValues, ERA5LandPrecipValues, ERA5LandDewpointTemperatureValues, ERA5LandSnowfallValues, ERA5Land2mTempValues, ERA5LandSurfaceSolarRadiationDownwardsValues, ERA5LandSurfacePressureValues, ERA5LandWindUValues, ERA5LandWindVValues

class ERA5Sea(ERA5):  # pragma: nocover
    """Abstract base class for ERA5 sea datasets"""
    

class ERA5LandWind(ERA5Land, ERA5LandWindValues):  # pragma: nocover
    """
    Base class for ERA5Land wind datasets
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandWindValues.__init__(self)

    # Use ERA5LandWindValues properties directly
    # era5_dataset = ERA5LandWindValues.era5_dataset
    # era5_request_name = ERA5LandWindValues.era5_request_name

# Regular ERA5
class ERA5Precip(ERA5, ERA5PrecipValues):  # pragma: nocover
    """
    Total precipitation data on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5PrecipValues.__init__(self)
    
    # Use ERA5PrecipValues properties directly
    era5_dataset = ERA5PrecipValues.era5_dataset
    era5_request_name = ERA5PrecipValues.era5_request_name

class ERA52mTemp(ERA5, ERA52mTempValues):  # pragma: nocover
    """
    Data for air temperature at 2m above the surface on ERA5. From ECMWF:

    This parameter is the temperature of air at 2m above the surface of land, sea or inland waters.
    2m temperature is calculated by interpolating between the lowest model level and the Earth's surface,
     taking account of the atmospheric conditions. This parameter has units of kelvin (K).
    Temperature measured in kelvin can be converted to degrees Celsius (°C) by subtracting 273.15.
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA52mTempValues.__init__(self)
    
    # Use ERA52mTempValues properties directly
    era5_dataset = ERA52mTempValues.era5_dataset
    era5_request_name = ERA52mTempValues.era5_request_name


class ERA5SurfaceSolarRadiationDownwards(ERA5, ERA5SurfaceSolarRadiationDownwardsValues):  # pragma: nocover
    """
    Data for solar radiation that reaches the Earth's surface on ERA5. From ECMWF:

    This parameter is the amount of solar radiation (also known as shortwave radiation)
     that reaches a horizontal plane at the surface of the Earth.
    This parameter comprises both direct and diffuse solar radiation.
    Radiation from the Sun (solar, or shortwave, radiation) is partly reflected back to space
     by clouds and particles in the atmosphere (aerosols) and some of it is absorbed.
    The rest is incident on the Earth's surface (represented by this parameter).
    To a reasonably good approximation, this parameter is the model equivalent of what would be measured
     by a pyranometer (an instrument used for measuring solar radiation) at the surface.
    However, care should be taken when comparing model parameters with observations,
     because observations are often local to a particular point in space and time,
     rather than representing averages over a model grid box.
    This parameter is accumulated over a particular time period which depends on the data extracted.
    The units are joules per square metre (J m-2).
    To convert to watts per square metre (W m-2), the accumulated values should be divided
     by the accumulation period expressed in seconds. The ECMWF convention for vertical fluxes is positive downwards.
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5SurfaceSolarRadiationDownwardsValues.__init__(self)

    # Use ERA5SurfaceSolarRadiationDownwardsValues properties directly
    era5_dataset = ERA5SurfaceSolarRadiationDownwardsValues.era5_dataset
    era5_request_name = ERA5SurfaceSolarRadiationDownwardsValues.era5_request_name

class ERA5VolumetricSoilWater(ERA5, ERA5VolumetricSoilWaterValues):  # pragma: nocover
    """
    Data for volumetric soil moisture from ECMWF. From ECMWF:

    The ECMWF Integrated Forecasting System (IFS) has a four-layer representation of soil:
    Layer 1: 0 - 7cm, Layer 2: 7 - 28cm, Layer 3: 28 - 100cm, Layer 4: 100 - 289cm.
    Soil water is defined over the whole globe, even over ocean.
    Regions with a water surface can be masked out by only considering grid points
     where the land-sea mask has a value greater than 0.5.
    The volumetric soil water is associated with the soil texture (or classification),
     soil depth, and the underlying groundwater level
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5VolumetricSoilWaterValues.__init__(self)
    
    # Use ERA5VolumetricSoilWaterValues properties directly
    era5_dataset = ERA5VolumetricSoilWaterValues.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterValues.era5_request_name

class ERA5VolumetricSoilWaterLayer1(ERA5VolumetricSoilWater, ERA5VolumetricSoilWaterLayer1Values):  # pragma: nocover
    """
    Data for volumetric soil moisture layer 1 from ECMWF. From ECMWF:

    The ECMWF Integrated Forecasting System (IFS) has a four-layer representation of soil:
    Layer 1: 0 - 7cm, Layer 2: 7 - 28cm, Layer 3: 28 - 100cm, Layer 4: 100 - 289cm.
    Soil water is defined over the whole globe, even over ocean.
    Regions with a water surface can be masked out by only considering grid points
     where the land-sea mask has a value greater than 0.5.
    The volumetric soil water is associated with the soil texture (or classification),
     soil depth, and the underlying groundwater level
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5VolumetricSoilWaterLayer1Values.__init__(self)
    
    # Use ERA5VolumetricSoilWaterLayer1Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer1Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer1Values.era5_request_name

class ERA5VolumetricSoilWaterLayer2(ERA5VolumetricSoilWater, ERA5VolumetricSoilWaterLayer2Values):  # pragma: nocover
    """
    Data for volumetric soil moisture layer 2 from ECMWF. From ECMWF:

    The ECMWF Integrated Forecasting System (IFS) has a four-layer representation of soil:
    Layer 1: 0 - 7cm, Layer 2: 7 - 28cm, Layer 3: 28 - 100cm, Layer 4: 100 - 289cm.
    Soil water is defined over the whole globe, even over ocean.
    Regions with a water surface can be masked out by only considering grid points
     where the land-sea mask has a value greater than 0.5.
    The volumetric soil water is associated with the soil texture (or classification),
     soil depth, and the underlying groundwater level
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5VolumetricSoilWaterLayer2Values.__init__(self)
    
    # Use ERA5VolumetricSoilWaterLayer2Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer2Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer2Values.era5_request_name

class ERA5VolumetricSoilWaterLayer3(ERA5VolumetricSoilWater, ERA5VolumetricSoilWaterLayer3Values):  # pragma: nocover
    """
    Data for volumetric soil moisture layer 3 from ECMWF. From ECMWF:

    The ECMWF Integrated Forecasting System (IFS) has a four-layer representation of soil:
    Layer 1: 0 - 7cm, Layer 2: 7 - 28cm, Layer 3: 28 - 100cm, Layer 4: 100 - 289cm.
    Soil water is defined over the whole globe, even over ocean.
    Regions with a water surface can be masked out by only considering grid points
     where the land-sea mask has a value greater than 0.5.
    The volumetric soil water is associated with the soil texture (or classification),
     soil depth, and the underlying groundwater level
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5VolumetricSoilWaterLayer3Values.__init__(self)
    
    # Use ERA5VolumetricSoilWaterLayer3Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer3Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer3Values.era5_request_name

class ERA5VolumetricSoilWaterLayer4(ERA5VolumetricSoilWater, ERA5VolumetricSoilWaterLayer4Values):  # pragma: nocover
    """
    Data for volumetric soil moisture layer 4 from ECMWF. From ECMWF:

    The ECMWF Integrated Forecasting System (IFS) has a four-layer representation of soil:
    Layer 1: 0 - 7cm, Layer 2: 7 - 28cm, Layer 3: 28 - 100cm, Layer 4: 100 - 289cm.
    Soil water is defined over the whole globe, even over ocean.
    Regions with a water surface can be masked out by only considering grid points
     where the land-sea mask has a value greater than 0.5.
    The volumetric soil water is associated with the soil texture (or classification),
     soil depth, and the underlying groundwater level
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5VolumetricSoilWaterLayer4Values.__init__(self)
    
    # Use ERA5VolumetricSoilWaterLayer4Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer4Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer4Values.era5_request_name

class ERA5Wind10m(ERA5, ERA5Wind10mValues):  # pragma: nocover
    """
    Base class for wind U and V components at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5Wind10mValues.__init__(self)
    
    # Use ERA5Wind10mValues properties directly
    # era5_dataset = ERA5Wind10mValues.era5_dataset
    # era5_request_name = ERA5Wind10mValues.era5_request_name

class ERA5InstantaneousWindGust10m(ERA5Wind10m, ERA5InstantaneousWindGust10mValues):  # pragma: nocover
    """
    Base class for wind U and V components at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5InstantaneousWindGust10mValues.__init__(self)
    
    # Use ERA5InstantaneousWindGust10mValues properties directly
    era5_dataset = ERA5InstantaneousWindGust10mValues.era5_dataset
    era5_request_name = ERA5InstantaneousWindGust10mValues.era5_request_name

class ERA5WindU10m(ERA5Wind10m, ERA5WindU10mValues):  # pragma: nocover
    """
    U-component of wind at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5WindU10mValues.__init__(self)
    
    # Use ERA5WindU10mValues properties directly
    era5_dataset = ERA5WindU10mValues.era5_dataset
    era5_request_name = ERA5WindU10mValues.era5_request_name

class ERA5WindV10m(ERA5Wind10m, ERA5WindV10mValues):  # pragma: nocover
    """
    V-component of wind at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5WindV10mValues.__init__(self)
    
    # Use ERA5WindV10mValues properties directly
    era5_dataset = ERA5WindV10mValues.era5_dataset
    era5_request_name = ERA5WindV10mValues.era5_request_name

class ERA5Wind100m(ERA5, ERA5Wind100mValues):  # pragma: nocover
    """
    Abstract base class for 100m wind component parameters in ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5Wind100mValues.__init__(self)
    
    # Use ERA5Wind100mValues properties directly
    # era5_dataset = ERA5Wind100mValues.era5_dataset
    # era5_request_name = ERA5Wind100mValues.era5_request_name

class ERA5WindU100m(ERA5Wind100m, ERA5WindU100mValues):  # pragma: nocover
    """
    U-component of 100m height wind data on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5WindU100mValues.__init__(self)
    
    # Use ERA5WindU100mValues properties directly
    era5_dataset = ERA5WindU100mValues.era5_dataset
    era5_request_name = ERA5WindU100mValues.era5_request_name

class ERA5WindV100m(ERA5Wind100m, ERA5WindV100mValues):  # pragma: nocover
    """
    V-component of 100m height wind data on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5WindV100mValues.__init__(self)
    
    # Use ERA5WindV100mValues properties directly
    era5_dataset = ERA5WindV100mValues.era5_dataset
    era5_request_name = ERA5WindV100mValues.era5_request_name

# SEA datasets
class ERA5SeaSurfaceTemperature(ERA5Sea, ERA5SeaSurfaceTemperatureValues):
    """
    Class for Mean Sea Surface (0-10m) Temperature dataset
    """
    # need to shift the longitude west so it's over the ocean (pacific)
    FINALIZATION_LON = -130
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5SeaSurfaceTemperatureValues.__init__(self)
    

# TODO: Implement the ERA5SeaSurfaceTemperatureDaily class
class ERA5SeaSurfaceTemperatureDaily(ERA5SeaSurfaceTemperature, ERA5SeaSurfaceTemperatureDailyValues):
    """
    Class for resampling ERA5 Sea Surface Temperature hourly data to daily data for ENSO calculations
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a new ERA5 object with appropriate chunking parameters.
        """
        super().__init__(
            *args,
            skip_post_parse_qc=True,
            skip_post_parse_api_check=True,
            dataset_name=self.dataset_name,
            **kwargs,
        )
        self.standard_dims = ["latitude", "longitude", "valid_time"]
        self.era5_latest_possible_date = datetime.datetime.utcnow() - datetime.timedelta(days=6)
        self.dask_use_process_scheduler = True
        self.dask_scheduler_protocol = "tcp://"

    @property
    def file_type(cls) -> str:
        """
        File type of raw data.
        Used to trigger file format-appropriate functions and methods for Kerchunking and Xarray operations.
        """
        return "Zarr"

    @property
    def reference_dataset_url(self) -> str:
        return "s3://arbol-gridded-prod/datasets/era5_sea_surface_temperature-hourly.zarr/"

    #####################
    # OPERATIONAL METHODS
    #####################

    def extract(self, date_range: list[datetime.datetime, datetime.datetime] = None, *args, **kwargs) -> bool:
        """
        Override parent extraction method to instead extract from Arbol's internal copy of the ERA5 SST dataset
        """
        if date_range and date_range[0] < self.dataset_start_date:
            raise ValueError(
                f"First datetime requested {date_range[0]} is before "
                "the start of the dataset in question. Please request a valid datetime."
            )
        start_date, end_date = self.define_request_dates(date_range)
        self.reference_dataset_extract = self.extract_from_reference_dataset(start_date, end_date)
        if len(self.reference_dataset_extract.time):
            return True
        else:
            self.info("No new days' data found to resample, exiting ETL script")
            return False

    def reference_dataset(self, **kwargs: dict) -> xr.Dataset:
        """
        Pull a reference dataset from an S3 bucket so that it can be manipulated in Xarray

        Returns
        -------
        reference_dataset : xr.Dataset
            An Xarray Dataset for the reference dataset in Arbol's gridded prod bucket
        """
        if hasattr(self, "reference_dataset_url"):
            if self.store.fs().exists(self.reference_dataset_url):
                mapper = s3fs.S3Map(root=self.reference_dataset_url, s3=self.store.fs(), **kwargs)
                return xr.open_zarr(mapper)
            else:
                raise FileNotFoundError(f"Reference dataset not found at {self.reference_dataset_url}")
        else:
            raise ValueError(
                "reference_dataset_url member variable undefined. "
                "Please define a reference dataset before continuing."
            )

    def extract_from_reference_dataset(self, start_date: datetime.datetime, end_date: datetime.datetime) -> xr.Dataset:
        """
        Extract a partial dataset from the reference dataset using a date range as the filter

        Parameters
        ----------
        start_date : datetime.datetime
            The desired start date for data from the reference dataset

        end_date : datetime.datetime
            The desired end date for data from the reference dataset

        Returns
        -------
        xr.Dataset
            A selection of the reference dataset between start_date and end_date
        """
        if self.dataset_category == "forecast":
            return self.reference_dataset().sel(forecast_reference_time=slice(start_date, end_date))
        else:
            return self.reference_dataset().sel(time=slice(start_date, end_date))

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
        # Use the start/end dates specified, if provided
        if date_range:
            request_start_date, request_end_date = date_range
        # Otherwise start from the last update in the metadata.
        # Fall back to the dataset_start_date property if no metadata available.
        else:
            try:
                self.info("Calculating new start date based on end date in metadata or existing file")
                request_start_date = self.get_metadata_date_range()["end"] + datetime.timedelta(days=1)
            except (KeyError, ValueError):
                self.info(
                    "No existing metadata or file found; "
                    f"extracting from Arbol's specified start date of {self.dataset_start_date.date().isoformat()}"
                )
                request_start_date = self.dataset_start_date
            request_end_date = datetime.datetime.today()

        return request_start_date, request_end_date

    def transform_data_on_disk(self):
        """
        Resample the hourly dataset to a daily dataset
        """
        self.populate_metadata()

    def load_dataset_from_disk(self):
        """
        Override parent method to pass the resampled dataset into normal parsing operations
        """
        resampled_dataset = self.reference_dataset_extract.resample(time="1D").mean(skipna=True)
        return resampled_dataset

    def postprocess_zarr(self):
        """
        Override postprocessing routine for load_dataset_from_disk
        intended for data read from NetCDFs, not processed Zarrs
        """
        pass


class ERA5SeaLevelPressure(ERA5Sea, ERA5SeaLevelPressureValues):  # pragma: nocover
    """
    Class for Mean Sea Level Pressure dataset
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5SeaLevelPressureValues.__init__(self)
    
    # Use ERA5SeaLevelPressureValues properties directly
    era5_dataset = ERA5SeaLevelPressureValues.era5_dataset
    era5_request_name = ERA5SeaLevelPressureValues.era5_request_name

# LAND Datasets
class ERA5LandPrecip(ERA5Land, ERA5LandPrecipValues):  # pragma: nocover
    """
    Total precipitation data on ERA5 Land
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandPrecipValues.__init__(self)
    
    # Use ERA5LandPrecipValues properties directly
    era5_dataset = ERA5LandPrecipValues.era5_dataset
    era5_request_name = ERA5LandPrecipValues.era5_request_name

class ERA5LandDewpointTemperature(ERA5Land, ERA5LandDewpointTemperatureValues):  # pragma: nocover
    """
    Dewpoint temperature data on ERA5 Land. From ECMWF:

    This parameter is the temperature to which the air, at 2 metres above the surface of the Earth,
    would have to be cooled for saturation to occur. It is a measure of the humidity of the air.
    Combined with temperature and pressure, it can be used to calculate the relative humidity.
    2m dew point temperature is calculated by interpolating between the lowest model level
    and the Earth's surface, taking account of the atmospheric conditions.

    This parameter has units of kelvin (K).
    Temperature measured in kelvin can be converted to degrees Celsius (°C) by subtracting 273.15.
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandDewpointTemperatureValues.__init__(self)
    
    # Use ERA5LandDewpointTemperatureValues properties directly
    era5_dataset = ERA5LandDewpointTemperatureValues.era5_dataset
    era5_request_name = ERA5LandDewpointTemperatureValues.era5_request_name

class ERA5LandSnowfall(ERA5Land, ERA5LandSnowfallValues):  # pragma: nocover
    """
    Snowfall data on ERA5 Land. From ECMWF:

    This parameter is the accumulated snow that falls to the Earth's surface.
    It is the sum of large-scale snowfall and convective snowfall.
    Large-scale snowfall is generated by the cloud scheme in the ECMWF Integrated Forecasting System (IFS).
    The cloud scheme represents the formation and dissipation of clouds and large-scale precipitation
    due to changes in atmospheric quantities (such as pressure, temperature and moisture)
    predicted directly by the IFS at spatial scales of the grid box or larger.
    Convective snowfall is generated by the convection scheme in the IFS,
    which represents convection at spatial scales smaller than the grid box.

    This parameter is the total amount of water accumulated over
     a particular time period which depends on the data extracted.
    The units of this parameter are depth in metres of water equivalent.
    It is the depth the water would have if it were spread evenly over the grid box.

    Care should be taken when comparing model parameters with observations, because observations are often local
    to a particular point in space and time, rather than representing averages over a model grid box.
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandSnowfallValues.__init__(self)
    
    # Use ERA5LandSnowfallValues properties directly
    era5_dataset = ERA5LandSnowfallValues.era5_dataset
    era5_request_name = ERA5LandSnowfallValues.era5_request_name

class ERA5Land2mTemp(ERA5Land, ERA5Land2mTempValues):  # pragma: nocover
    """
    2m Temperature data on ERA5 Land
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5Land2mTempValues.__init__(self)
    
    # Use ERA5Land2mTempValues properties directly
    era5_dataset = ERA5Land2mTempValues.era5_dataset
    era5_request_name = ERA5Land2mTempValues.era5_request_name

class ERA5LandSurfaceSolarRadiationDownwards(ERA5Land, ERA5LandSurfaceSolarRadiationDownwardsValues):  # pragma: nocover
    """
    Data for solar radiation that reaches the Earth's surface on ERA5. From ECMWF:

    This parameter is the amount of solar radiation (also known as shortwave radiation
      that reaches a horizontal plane at the surface of the Earth.
    This parameter comprises both direct and diffuse solar radiation.
    Radiation from the Sun (solar, or shortwave, radiation) is partly reflected back to space by
     clouds and particles in the atmosphere (aerosols) and some of it is absorbed.
    The rest is incident on the Earth's surface (represented by this parameter).
    To a reasonably good approximation, this parameter is the model equivalent of what would be measured
     by a pyranometer (an instrument used for measuring solar radiation) at the surface.
    However, care should be taken when comparing model parameters with observations, because observations
     are often local to a particular point in space and time, rather than representing averages over a model grid box.
    This parameter is accumulated over a particular time period which depends on the data extracted.
    The units are joules per square metre (J m-2).
    To convert to watts per square metre (W m-2), the accumulated values
     should be divided by the accumulation period expressed in seconds.
    The ECMWF convention for vertical fluxes is positive downwards.
    """  # noqa: E501
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandSurfaceSolarRadiationDownwardsValues.__init__(self)
    
    # Use ERA5LandSurfaceSolarRadiationDownwardsValues properties directly
    era5_dataset = ERA5LandSurfaceSolarRadiationDownwardsValues.era5_dataset
    era5_request_name = ERA5LandSurfaceSolarRadiationDownwardsValues.era5_request_name

class ERA5LandSurfacePressure(ERA5Land, ERA5LandSurfacePressureValues):  # pragma: nocover
    """
    Copernicus's ERA5-Land dataset for the pressure of the atmosphere on the surface of land. From Copernicus:

    Pressure (force per unit area) of the atmosphere on the surface of land, sea and in-land water. It is a measure of the weight of all the
    air in a column vertically above the area of the Earth's surface represented at a fixed point. Surface pressure is often used in combination
    with temperature to calculate air density. The strong variation of pressure with altitude makes it difficult to see the low and high pressure
    systems over mountainous areas, so mean sea level pressure, rather than surface pressure, is normally used for this purpose. The units of this
    variable are Pascals (Pa). Surface pressure is often measured in hPa and sometimes is presented in the old units of millibars,
    mb (1 hPa = 1 mb = 100 Pa).
    """  # noqa: E501
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandSurfacePressureValues.__init__(self)
    
    # Use ERA5LandSurfacePressureValues properties directly
    era5_dataset = ERA5LandSurfacePressureValues.era5_dataset
    era5_request_name = ERA5LandSurfacePressureValues.era5_request_name

class ERA5LandWindU(ERA5LandWind, ERA5LandWindUValues):  # pragma: nocover
    """
    U-component of 10m height wind data on ERA5Land
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandWindUValues.__init__(self)

class ERA5LandWindV(ERA5LandWind, ERA5LandWindVValues):  # pragma: nocover
    """
    V-component of 10m height wind data on ERA5Land
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        ERA5LandWindVValues.__init__(self)
    
    # Use ERA5LandWindVValues properties directly
    era5_dataset = ERA5LandWindVValues.era5_dataset
    era5_request_name = ERA5LandWindVValues.era5_request_name