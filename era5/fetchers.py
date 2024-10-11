

from .fetcher import ERA5, ERA5Land
from .base_values import ERA5PrecipValues, ERA52mTempValues, ERA5SurfaceSolarRadiationDownwardsValues, ERA5VolumetricSoilWaterLayer1Values, ERA5VolumetricSoilWaterLayer2Values, ERA5VolumetricSoilWaterLayer3Values, ERA5VolumetricSoilWaterLayer4Values, ERA5InstantaneousWindGust10mValues, ERA5WindU10mValues, ERA5WindV10mValues, ERA5WindU100mValues, ERA5WindV100mValues, ERA5SeaSurfaceTemperatureValues, ERA5SeaSurfaceTemperatureDailyValues, ERA5SeaLevelPressureValues, ERA5LandPrecipValues, ERA5LandDewpointTemperatureValues, ERA5LandSnowfallValues, ERA5Land2mTempValues, ERA5LandSurfaceSolarRadiationDownwardsValues, ERA5LandSurfacePressureValues, ERA5LandWindUValues, ERA5LandWindVValues

# Regular ERA5
class ERA5Precip(ERA5, ERA5PrecipValues):  # pragma: nocover
    """
    Total precipitation data on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5PrecipValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        
    
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
        ERA52mTempValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
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
        ERA5SurfaceSolarRadiationDownwardsValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    # Use ERA5SurfaceSolarRadiationDownwardsValues properties directly
    era5_dataset = ERA5SurfaceSolarRadiationDownwardsValues.era5_dataset
    era5_request_name = ERA5SurfaceSolarRadiationDownwardsValues.era5_request_name



class ERA5VolumetricSoilWaterLayer1(ERA5, ERA5VolumetricSoilWaterLayer1Values):  # pragma: nocover
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
        ERA5VolumetricSoilWaterLayer1Values.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5VolumetricSoilWaterLayer1Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer1Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer1Values.era5_request_name

class ERA5VolumetricSoilWaterLayer2(ERA5, ERA5VolumetricSoilWaterLayer2Values):  # pragma: nocover
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
        ERA5VolumetricSoilWaterLayer2Values.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5VolumetricSoilWaterLayer2Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer2Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer2Values.era5_request_name

class ERA5VolumetricSoilWaterLayer3(ERA5, ERA5VolumetricSoilWaterLayer3Values):  # pragma: nocover
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
        ERA5VolumetricSoilWaterLayer3Values.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5VolumetricSoilWaterLayer3Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer3Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer3Values.era5_request_name

class ERA5VolumetricSoilWaterLayer4(ERA5, ERA5VolumetricSoilWaterLayer4Values):  # pragma: nocover
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
        ERA5VolumetricSoilWaterLayer4Values.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        
    
    # Use ERA5VolumetricSoilWaterLayer4Values properties directly
    era5_dataset = ERA5VolumetricSoilWaterLayer4Values.era5_dataset
    era5_request_name = ERA5VolumetricSoilWaterLayer4Values.era5_request_name

class ERA5InstantaneousWindGust10m(ERA5, ERA5InstantaneousWindGust10mValues):  # pragma: nocover
    """
    Base class for wind U and V components at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5InstantaneousWindGust10mValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5InstantaneousWindGust10mValues properties directly
    era5_dataset = ERA5InstantaneousWindGust10mValues.era5_dataset
    era5_request_name = ERA5InstantaneousWindGust10mValues.era5_request_name

class ERA5WindU10m(ERA5, ERA5WindU10mValues):  # pragma: nocover
    """
    U-component of wind at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5WindU10mValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5WindU10mValues properties directly
    era5_dataset = ERA5WindU10mValues.era5_dataset
    era5_request_name = ERA5WindU10mValues.era5_request_name

class ERA5WindV10m(ERA5, ERA5WindV10mValues):  # pragma: nocover
    """
    V-component of wind at 10m height on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5WindV10mValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5WindV10mValues properties directly
    era5_dataset = ERA5WindV10mValues.era5_dataset
    era5_request_name = ERA5WindV10mValues.era5_request_name

class ERA5WindU100m(ERA5, ERA5WindU100mValues):  # pragma: nocover
    """
    U-component of 100m height wind data on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5WindU100mValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5WindU100mValues properties directly
    era5_dataset = ERA5WindU100mValues.era5_dataset
    era5_request_name = ERA5WindU100mValues.era5_request_name

class ERA5WindV100m(ERA5, ERA5WindV100mValues):  # pragma: nocover
    """
    V-component of 100m height wind data on ERA5
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5WindV100mValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5WindV100mValues properties directly
    era5_dataset = ERA5WindV100mValues.era5_dataset
    era5_request_name = ERA5WindV100mValues.era5_request_name

# SEA datasets
class ERA5SeaSurfaceTemperature(ERA5, ERA5SeaSurfaceTemperatureValues):
    """
    Class for Mean Sea Surface (0-10m) Temperature dataset
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5SeaSurfaceTemperatureValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    era5_dataset = ERA5SeaSurfaceTemperatureValues.era5_dataset
    era5_request_name = ERA5SeaSurfaceTemperatureValues.era5_request_name


class ERA5SeaSurfaceTemperatureDaily(ERA5, ERA5SeaSurfaceTemperatureValues):
    """
    Class for Mean Sea Surface (0-10m) Temperature dataset
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5SeaSurfaceTemperatureDailyValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    era5_dataset = ERA5SeaSurfaceTemperatureDailyValues.era5_dataset
    era5_request_name = ERA5SeaSurfaceTemperatureDailyValues.era5_request_name
    
class ERA5SeaLevelPressure(ERA5, ERA5SeaLevelPressureValues):  # pragma: nocover
    """
    Class for Mean Sea Level Pressure dataset
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5SeaLevelPressureValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    
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
        ERA5LandPrecipValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    
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
        ERA5LandDewpointTemperatureValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    
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
        ERA5Land2mTempValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)

    
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
        ERA5LandSurfaceSolarRadiationDownwardsValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
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
        ERA5LandSurfacePressureValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        
    
    # Use ERA5LandSurfacePressureValues properties directly
    era5_dataset = ERA5LandSurfacePressureValues.era5_dataset
    era5_request_name = ERA5LandSurfacePressureValues.era5_request_name

class ERA5LandWindU(ERA5Land, ERA5LandWindUValues):  # pragma: nocover
    """
    U-component of 10m height wind data on ERA5Land
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5LandWindUValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
        
    
    # Use ERA5LandWindUValues properties directly
    era5_dataset = ERA5LandWindUValues.era5_dataset
    era5_request_name = ERA5LandWindUValues.era5_request_name

class ERA5LandWindV(ERA5Land, ERA5LandWindVValues):  # pragma: nocover
    """
    V-component of 10m height wind data on ERA5Land
    """
    def __init__(self, *args, **kwargs):
        # Ensure the dataset_name is passed to constructor
        ERA5LandWindVValues.__init__(self)
        super().__init__(*args, dataset_name=self.dataset_name, **kwargs)
    
    # Use ERA5LandWindVValues properties directly
    era5_dataset = ERA5LandWindVValues.era5_dataset
    era5_request_name = ERA5LandWindVValues.era5_request_name