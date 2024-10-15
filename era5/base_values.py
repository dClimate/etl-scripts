import pathlib
import datetime

class ERA5Family():
    dataset_name = "era5"
    collection_name = "ERA5"
    """
    Overall collection of data. Used for filling STAC Catalogue.
    """
    time_resolution = "hourly"
    missing_value = -9999
    has_nans: bool = True
    """If True, disable quality checks for NaN values to prevent wrongful flags"""
    preliminary_lag_in_days = 6

    @property
    def static_metadata(self) -> dict:
        """
        dict containing static fields in the metadata
        """
        static_metadata = {
            "coordinate_reference_system": "Reduced Gaussian Grid",
            "update_cadence": "daily",
            "temporal_resolution": self.time_resolution,
            "spatial_resolution": self.spatial_resolution,
            "spatial_precision": 0.01,
            "provider_url": "https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era5",
            "data_download_url": "https://cds.climate.copernicus.eu/#!/search?text=ERA5&type=dataset",
            "publisher": "Copernicus Climate Change Service (C3S)",
            "title": "ECMWF Reanalysis 5th Generation (ERA5)",
            "provider_description": "ECMWF is the European Centre for Medium-Range Weather Forecasts."  # noqa: E501
            " It is both a research institute and a 24/7 operational service, producing global numerical"  # noqa: E501
            " weather predictions and other data for its Member and Co-operating States and the broader community."  # noqa: E501
            " The Centre has one of the largest supercomputer facilities and meteorological data archives"  # noqa: E501
            " in the world. Other strategic activities include delivering advanced training and assisting the WM"  # noqa: E501
            " in implementing its programmes.",
            "dataset_description": (
                "ERA5 provides hourly estimates of a large number of atmospheric, land and oceanic climate variables."  # noqa: E501
                " ERA5 combines vast amounts of historical observations into global estimates using advanced modelling"  # noqa: E501
                " and data assimilation systems. The data cover the Earth on a 30km grid and resolve the atmosphere"  # noqa: E501
                " using 137 levels from the surface up to a height of 80km. ERA5 includes information about uncertainties"  # noqa: E501
                " for all variables at reduced spatial and temporal resolutions.\n"  # noqa: E501
                "Quality-assured monthly updates of ERA5 (1959 to present) are published within 3 months of real time."  # noqa: E501
                "Preliminary daily updates of the dataset are available to users within 5 days of real time.\n"  # noqa: E501
                "More dataset information at https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation\n"  # noqa: E501
                f"More information about this dataset at {self.dataset_info_url}\n"
                "More information about reduced Gaussian Grids at "
                " https://confluence.ecmwf.int/display/CKB/ERA5%3A+What+is+the+spatial+reference"
            ),
            "license": "Apache License 2.0",
            "terms_of_service": "https://www.ecmwf.int/en/terms-use",
            "name": self.dataset_name,
            "updated": str(datetime.datetime.now()),
            "missing_value": self.missing_value,
            "tags": self.tags,
            "standard_name": self.standard_name,
            "long_name": self.long_name,
            "unit_of_measurement": self.unit_of_measurement,
            "final_lag_in_days": self.final_lag_in_days,
            "preliminary_lag_in_days": self.preliminary_lag_in_days,
            "expected_nan_frequency": self.expected_nan_frequency,
        }

        return static_metadata

class ERA5Values(ERA5Family):

    dataset_name = ERA5Family.dataset_name
    def relative_path(self) -> pathlib.Path:
        return pathlib.Path("era5")

    era5_dataset = "reanalysis-era5-single-levels"
    dataset_start_date = datetime.datetime(1950, 1, 1, 0)
    spatial_resolution = 0.25
    final_lag_in_days = 90
    expected_nan_frequency = 0


class ERA5LandValues(ERA5Family):  # pragma: nocover


    requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
    requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
    requested_ipfs_chunker="size-57600"
    dask_scheduler_worker_saturation = 1.0

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


    @property
    def static_metadata(self) -> dict:
        """
        Get the super metadata, replace some fields with ERA5-Land specific information, and return.

        Returns
        -------
        static_metadata
            A populated dictionary of metadata
        """
        static_metadata = super().static_metadata
        static_metadata.update(
            {
                "update_cadence": "monthly",
                "provider_url": "https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=overview",
                "data_download_url": "https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land?tab=form",
                "title": "ECMWF ERA5-Land Reanalysis",
                "dataset description": (
                    "ERA5-Land is a reanalysis dataset providing a consistent view of the evolution of land variables over several"  # noqa: E501
                    " decades at an enhanced resolution compared to ERA5. ERA5-Land has been produced by replaying the land component"  # noqa: E501
                    " of the ECMWF ERA5 climate reanalysis. Reanalysis combines model data with observations from across the world into"  # noqa: E501
                    " a globally complete and consistent dataset using the laws of physics. Reanalysis produces data that goes several"  # noqa: E501
                    " decades back in time, providing an accurate description of the climate of the past."  # noqa: E501
                    f"More information about this dataset at {self.dataset_info_url}\n"  # noqa: E501
                ),  # noqa: E501
            }
        )
        return static_metadata

class ERA5LandWindValues(ERA5LandValues):  # pragma: nocover
    """
    Base class for ERA5Land wind datasets
    """

    dataset_name = f"{ERA5LandValues.dataset_name}_wind"

    def relative_path(self):
        return super().relative_path() / "wind"

class ERA5PrecipValues(ERA5Values):  # pragma: nocover
    """
    Total precipitation data on ERA5
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name =  f"{ERA5Values.dataset_name}_precip"
        self.data_var = "tp"
        self.standard_name = "precipitation_amount"
        self.long_name = "Total Precipitation"
        self.tags = ["Precipitation"]
        self.unit_of_measurement = "m"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=228"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "precip"

    @property
    def era5_request_name(self) -> str:
        return "total_precipitation"


class ERA52mTempValues(ERA5Values):  # pragma: nocover
    """
    Data for air temperature at 2m above the surface on ERA5. From ECMWF:

    This parameter is the temperature of air at 2m above the surface of land, sea or inland waters.
    2m temperature is calculated by interpolating between the lowest model level and the Earth's surface,
     taking account of the atmospheric conditions. This parameter has units of kelvin (K).
    Temperature measured in kelvin can be converted to degrees Celsius (°C) by subtracting 273.15.
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Values.dataset_name}_2m_temp"
        self.data_var = "t2m"
        self.standard_name = "air_temperature"
        self.long_name = "Hourly Near-Surface Air Temperature"
        self.tags = ["Temperature"]
        self.unit_of_measurement = "K"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=500011"
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "2m_temp"

    @property
    def era5_request_name(self) -> str:
        return "2m_temperature"


class ERA5SurfaceSolarRadiationDownwardsValues(ERA5Values):  # pragma: nocover
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

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name =  f"{ERA5Values.dataset_name}_surface_solar_radiation_downwards"
        self.data_var = "ssrd"
        self.standard_name = "surface_downwelling_shortwave_flux_in_air"
        self.long_name = "Surface Downwelling Shortwave Solar Radiation"
        self.tags = ["Solar", "Radiation"]
        self.unit_of_measurement = "J / m**2"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=169"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "surface_solar_radiation_downwards"

    @property
    def era5_request_name(self) -> str:
        return "surface_solar_radiation_downwards"



class ERA5VolumetricSoilWaterValues(ERA5Values):  # pragma: nocover
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

    def __init__(self, layer: str):
        # Ensure dataset_name is an instance attribute
        self.layer = layer
        self.dataset_name = f"{ERA5Values.dataset_name}_volumetric_soil_water_{self.layer}"
        self.data_var = f"swvl{self.layer[-1]}"  # Extract layer number from the string
        self.standard_name = f"volumetric_soil_moisture_{self.layer}"
        self.long_name = f"Volumetric Soil Moisture {self.layer}"
        self.tags = ["Soil", "Moisture"]
        self.unit_of_measurement = "m**3 m**-3"
        self.requested_zarr_chunks = {"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks = {"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.dataset_info_url = "https://data.cci.ceda.ac.uk/thredds/fileServer/esacci/soil_moisture/docs/v06.1/ESA_CCI_SM_RD_D2.1_v2_ATBD_v06.1_issue_1.1.pdf"
        self.missing_value = -9999
        self.time_resolution = "hourly"

    dataset_name = f"{ERA5Values.dataset_name}_volumetric_soil_water"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "volumetric_soil_water" / self.layer

    @property
    def era5_request_name(self) -> str:
        return "volumetric_soil_water_" + self.layer

class ERA5VolumetricSoilWaterLayer1Values(ERA5VolumetricSoilWaterValues):  # pragma: nocover
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

    def __init__(self):
        # Initialize the parent class with layer 1
        super().__init__(layer="layer_1")


class ERA5VolumetricSoilWaterLayer2Values(ERA5VolumetricSoilWaterValues):  # pragma: nocover
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

    def __init__(self):
        # Initialize the parent class with layer 2
        super().__init__(layer="layer_2")


class ERA5VolumetricSoilWaterLayer3Values(ERA5VolumetricSoilWaterValues):  # pragma: nocover
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

    def __init__(self):
        # Initialize the parent class with layer 3
        super().__init__(layer="layer_3")

class ERA5VolumetricSoilWaterLayer4Values(ERA5VolumetricSoilWaterValues):  # pragma: nocover
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

    def __init__(self):
        # Initialize the parent class with layer 4
        super().__init__(layer="layer_4")


class ERA5Wind10mValues(ERA5Values):  # pragma: nocover
    """
    Base class for wind U and V components at 10m height on ERA5
    """

    dataset_name = f"{ERA5Values.dataset_name}_wind_10m"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "wind_10m"

    tags = ["Wind"]

    unit_of_measurement = "m / s"


class ERA5InstantaneousWindGust10mValues(ERA5Wind10mValues):  # pragma: nocover
    """
    Base class for wind U and V components at 10m height on ERA5
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Wind10mValues.dataset_name}_instantaneous_gust"
        self.data_var = "i10fg"
        self.standard_name = "instantaneous_10m_wind_gust"
        self.long_name = "Maximum 10m Wind Gust In the Last Hour"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=228029"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "inst_wind_gust_10m"

    @property
    def era5_request_name(self) -> str:
        return "instantaneous_10m_wind_gust"

class ERA5WindU10mValues(ERA5Wind10mValues):  # pragma: nocover
    """
    U-component of wind at 10m height on ERA5
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Wind10mValues.dataset_name}_u"
        self.data_var = "u10"
        self.standard_name = "eastward_wind"
        self.long_name = "Eastward Near-Surface 10m Wind Velocity"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=165"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self):
        return super().relative_path() / "wind_10m_u"

    @property
    def era5_request_name(self) -> str:
        return "10m_u_component_of_wind"


class ERA5WindV10mValues(ERA5Wind10mValues):  # pragma: nocover
    """
    V-component of wind at 10m height on ERA5
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Wind10mValues.dataset_name}_v"
        self.data_var = "v10"
        self.standard_name = "northward_wind"
        self.long_name = "Northward Near-Surface 10m Wind Velocity"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=166"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "wind_10m_v"
    @property
    def era5_request_name(self) -> str:
        return "10m_v_component_of_wind"


class ERA5Wind100mValues(ERA5Values):  # pragma: nocover
    """
    Abstract base class for 100m wind component parameters in ERA5
    """

    dataset_name = f"{ERA5Values.dataset_name}_wind_100m"

    def relative_path(self):
        return super().relative_path() / "wind_100m"

    tags = ["Wind"]

    unit_of_measurement = "m / s"

    final_lag_in_days = 90


class ERA5WindU100mValues(ERA5Wind100mValues):  # pragma: nocover
    """
    U-component of 100m height wind data on ERA5
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Wind100mValues.dataset_name}_u"
        self.data_var = "u100"
        self.standard_name = "eastward_wind"
        self.long_name = "Eastward Near-Surface 100m Wind Velocity"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=228246"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"
        self.final_lag_in_days = 90
    
    def relative_path(self):
        return super().relative_path() / "wind_100m_u"

    @property
    def era5_request_name(self) -> str:
        return "100m_u_component_of_wind"


class ERA5WindV100mValues(ERA5Wind100mValues):  # pragma: nocover
    """
    V-component of 100m height wind data on ERA5
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Wind100mValues.dataset_name}_v"
        self.data_var = "v100"
        self.standard_name = "northward_wind"
        self.long_name = "Northward Near-Surface 100m Wind Velocity"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=228247"
        self.requested_zarr_chunks={"time": 5000, "latitude": 16, "longitude": 16}
        self.requested_dask_chunks={"time": 5000, "latitude": 16, "longitude": -1}
        self.requested_ipfs_chunker = "size-24576"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"
        self.final_lag_in_days = 90

    def relative_path(self):
        return super().relative_path() / "wind_100m_v"

    @property
    def era5_request_name(self) -> str:
        return "100m_v_component_of_wind"

class ERA5SeaSurfaceTemperatureValues(ERA5Values):  # pragma: nocover
    """
    Class for Mean Sea Surface (0-10m) Temperature dataset
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Values.dataset_name}_sea_surface_temperature"
        self.data_var = "sst"
        self.standard_name = "sea_surface_temperature"
        self.long_name = "Sea Surface Temperature"
        self.tags = ["Sea", "Temperature"]
        self.unit_of_measurement = "K"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=34"
        self.requested_zarr_chunks={"time": 5000, "latitude": 32, "longitude": 32}
        self.requested_dask_chunks={"time": 5000, "latitude": 32, "longitude": -1}
        self.requested_ipfs_chunker = "size-4096"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"
        self.expected_nan_frequency = 0.338915857605178
        self.final_lag_in_days = 90

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "sea_surface_temperature"

    @property
    def era5_request_name(self) -> str:
        return "sea_surface_temperature"

class ERA5SeaSurfaceTemperatureDailyValues(ERA5SeaSurfaceTemperatureValues):
    """
    Class for resampling ERA5 Sea Surface Temperature hourly data to daily data for ENSO calculations
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = "era5_sea_surface_temperature_daily_resample"
        self.data_var = "sst"
        self.standard_name = "sea_surface_temperature"
        self.long_name = "Sea Surface Temperature"
        self.tags = ["Sea", "Temperature"]
        self.unit_of_measurement = "K"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=34"
        self.requested_zarr_chunks={"time": 5000, "latitude": 32, "longitude": 32}
        self.requested_dask_chunks={"time": 5000, "latitude": 32, "longitude": -1}
        self.requested_ipfs_chunker = "size-4096"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "daily"
        self.expected_nan_frequency = 0.338915857605178
        self.standard_dims = ["latitude", "longitude", "valid_time"]

    """
    Overall collection of data. Used for filling STAC Catalogue.
    """
    @property
    def static_metadata(self) -> dict:
        """
        dict containing static fields in the metadata
        """
        static_metadata = super().static_metadata
        static_metadata["dataset_description"] = (
            "Internal resampling of hourly ERA5 SST data into daily data for analytical purposes. \n"
            + static_metadata["dataset_description"]
        )
        return static_metadata

    def relative_path(self) -> pathlib.Path:
        return super().relative_path().parent / "sea_surface_temperature_daily_resample"

class ERA5SeaLevelPressureValues(ERA5Values):  # pragma: nocover
    """
    Class for Mean Sea Level Pressure dataset
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5Values.dataset_name}_mean_sea_level_pressure"
        self.data_var = "msl"
        self.standard_name = "air_pressure_at_mean_sea_level"
        self.long_name = "Mean Sea Level Pressure"
        self.tags = ["Sea", "Pressure"]
        self.unit_of_measurement = "Pa"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=151"
        self.requested_zarr_chunks={"time": 5000, "latitude": 32, "longitude": 32}
        self.requested_dask_chunks={"time": 5000, "latitude": 32, "longitude": -1}
        self.requested_ipfs_chunker = "size-4096"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"
        self.final_lag_in_days = 90

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "mean_sea_level_pressure"

    @property
    def era5_request_name(self) -> str:
        return "mean_sea_level_pressure"


# LAND Datasets
class ERA5LandPrecipValues(ERA5LandValues):  # pragma: nocover
    """
    Total precipitation data on ERA5 Land
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandValues.dataset_name}_precip"
        self.data_var = "tp"
        self.standard_name = "precipitation_amount"
        self.long_name = "Precipitation"
        self.tags = ["Precipitation"]
        self.unit_of_measurement = "m"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=228"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 1)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self):
        return super().relative_path() / "precip"
    
    @property
    def era5_request_name(self) -> str:
        return "total_precipitation"


class ERA5LandDewpointTemperatureValues(ERA5LandValues):  # pragma: nocover
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
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandValues.dataset_name}_dewpoint_temperature"
        self.data_var = "d2m"
        self.standard_name = "dew_point_temperature"
        self.long_name = "2 Metre Dewpoint Temperature"
        self.tags = ["Dewpoint", "Humidity"]
        self.unit_of_measurement = "K"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=168"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 1)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    
    def relative_path(self):
        return super().relative_path() / "dewpoint_temperature"

    @property
    def era5_request_name(self) -> str:
        return "2m_dewpoint_temperature"


class ERA5LandSnowfallValues(ERA5LandValues):  # pragma: nocover
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

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandValues.dataset_name}_snowfall"
        self.data_var = "sf"
        self.standard_name = "precipitation_amount"
        self.long_name = "Snowfall"
        self.tags = ["Snowfall"]
        self.unit_of_measurement = "m of water equivalent"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=144"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 1)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "snowfall"

    @property
    def era5_request_name(self) -> str:
        return "snowfall"

class ERA5Land2mTempValues(ERA5LandValues):  # pragma: nocover
    """
    2m Temperature data on ERA5 Land
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandValues.dataset_name}_2m_temp"
        self.data_var = "t2m"
        self.standard_name = "air_temperature"
        self.long_name = "Hourly Near-Surface Air Temperature"
        self.tags = ["Temperature"]
        self.unit_of_measurement = "K"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=500011"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "2m_temp"

    @property
    def era5_request_name(self) -> str:
        return "2m_temperature"


class ERA5LandSurfaceSolarRadiationDownwardsValues(ERA5LandValues):  # pragma: nocover
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

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandValues.dataset_name}_surface_solar_radiation_downwards"
        self.data_var = "ssrd"
        self.standard_name = "surface_downwelling_shortwave_flux_in_air"
        self.long_name = "Surface Downwelling Shortwave Solar Radiation"
        self.tags = ["Solar", "Radiation"]
        self.unit_of_measurement = "J / m**2"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=169"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 1)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> pathlib.Path:
        return super().relative_path() / "surface_solar_radiation_downwards"

    @property
    def era5_request_name(self) -> str:
        return "surface_solar_radiation_downwards"

class ERA5LandSurfacePressureValues(ERA5LandValues):  # pragma: nocover
    """
    Copernicus's ERA5-Land dataset for the pressure of the atmosphere on the surface of land. From Copernicus:

    Pressure (force per unit area) of the atmosphere on the surface of land, sea and in-land water. It is a measure of the weight of all the
    air in a column vertically above the area of the Earth's surface represented at a fixed point. Surface pressure is often used in combination
    with temperature to calculate air density. The strong variation of pressure with altitude makes it difficult to see the low and high pressure
    systems over mountainous areas, so mean sea level pressure, rather than surface pressure, is normally used for this purpose. The units of this
    variable are Pascals (Pa). Surface pressure is often measured in hPa and sometimes is presented in the old units of millibars,
    mb (1 hPa = 1 mb = 100 Pa).
    """  # noqa: E501

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandValues.dataset_name}_surface_pressure"
        self.data_var = "sp"
        self.standard_name = "surface_air_pressure"
        self.long_name = "Surface Air Pressure"
        self.tags = ["Pressure"]
        self.unit_of_measurement = "Pa"
        self.dataset_info_url = "https://apps.ecmwf.int/codes/grib/param-db?id=134"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 0)
        self.missing_value = -9999
        self.time_resolution = "hourly"

    def relative_path(self) -> str:
        return super().relative_path() / "surface_pressure"

    @property
    def era5_request_name(self) -> str:
        return "surface_pressure"

class ERA5LandWindUValues(ERA5LandWindValues):  # pragma: nocover
    """
    U-component of 10m height wind data on ERA5Land
    """

    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandWindValues.dataset_name}_u"
        self.data_var = "u10"
        self.standard_name = "eastward_wind"
        self.long_name = "10 metre U wind component"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=165"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 1)
        self.missing_value = -9999
        self.time_resolution = "hourly"
        self.final_lag_in_days = 90

    def relative_path(self):
        return super().relative_path() / "wind_u"
    
    @property
    def era5_request_name(self) -> str:
        return "10m_u_component_of_wind"

class ERA5LandWindVValues(ERA5LandWindValues):  # pragma: nocover
    """
    V-component of 10m height wind data on ERA5Land
    """
    def __init__(self):
        # If you want to ensure dataset_name is an instance attribute
        self.dataset_name = f"{ERA5LandWindValues.dataset_name}_v"
        self.data_var = "v10"
        self.standard_name = "northward_wind"
        self.long_name = "10 metre V wind component"
        self.tags = ["Wind"]
        self.unit_of_measurement = "m / s"
        self.dataset_info_url = "https://codes.ecmwf.int/grib/param-db/?id=166"
        self.requested_zarr_chunks={"time": 1000, "latitude": 15, "longitude": 40}
        self.requested_dask_chunks={"time": 1000, "latitude": 15, "longitude": -1}
        self.requested_ipfs_chunker = "size-57600"
        self.dataset_start_date = datetime.datetime(1950, 1, 1, 1)
        self.missing_value = -9999
        self.time_resolution = "hourly"
        self.final_lag_in_days = 90


    def relative_path(self):
        return super().relative_path() / "wind_v"


    @property
    def era5_request_name(self) -> str:
        return "10m_v_component_of_wind"
