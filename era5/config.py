
from .transformer import ERA5FamilyDatasetTransformer, ERA5VolumetricSoilWaterTransformer, ERA5SeaDatasetTransformer
from .ipld_stac_loader import (
    ERA5PrecipStacLoader,
    ERA5SurfaceSolarRadiationDownwardsStacLoader,
    ERA52mTempStacLoader,
    ERA5VolumetricSoilWaterLayer1StacLoader,
    ERA5VolumetricSoilWaterLayer2StacLoader,
    ERA5VolumetricSoilWaterLayer3StacLoader,
    ERA5VolumetricSoilWaterLayer4StacLoader,
    ERA5InstantaneousWindGust10mStacLoader,
    ERA5WindU10mStacLoader,
    ERA5WindV10mStacLoader,
    ERA5WindU100mStacLoader,
    ERA5WindV100mStacLoader,
    ERA5SeaSurfaceTemperatureStacLoader,
    ERA5SeaSurfaceTemperatureDailyStacLoader,
    ERA5SeaLevelPressureStacLoader,
    ERA5LandPrecipStacLoader,
    ERA5LandDewpointTemperatureStacLoader,
    ERA5LandSnowfallStacLoader,
    ERA5LandSurfaceSolarRadiationDownwardsStacLoader,
    ERA5LandSurfacePressureStacLoader,
    ERA5LandWindUStacLoader,
    ERA5LandWindVStacLoader
    )

from .metadata import (
    ERA5PrecipValuesMetadataTransformer,
    ERA52mTempValuesMetadataTransformer,
    ERA5SurfaceSolarRadiationDownwardsValuesMetadataTransformer,
    ERA5VolumetricSoilWaterLayer1ValuesMetadataTransformer,
    ERA5VolumetricSoilWaterLayer2ValuesMetadataTransformer,
    ERA5VolumetricSoilWaterLayer3ValuesMetadataTransformer,
    ERA5VolumetricSoilWaterLayer4ValuesMetadataTransformer,
    ERA5InstantaneousWindGust10mValuesMetadataTransformer,
    ERA5WindU10mValuesMetadataTransformer,
    ERA5WindV10mValuesMetadataTransformer,
    ERA5WindU100mValuesMetadataTransformer,
    ERA5WindV100mValuesMetadataTransformer,
    ERA5SeaSurfaceTemperatureValuesMetadataTransformer,
    ERA5SeaSurfaceTemperatureDailyValuesMetadataTransformer,
    ERA5SeaLevelPressureValuesMetadataTransformer,
    ERA5LandPrecipValuesMetadataTransformer,
    ERA5LandDewpointTemperatureValuesMetadataTransformer,
    ERA5LandSnowfallValuesMetadataTransformer,
    ERA5Land2mTempValuesMetadataTransformer,
    ERA5LandSurfaceSolarRadiationDownwardsValuesMetadataTransformer,
    ERA5LandSurfacePressureValuesMetadataTransformer,
    ERA5LandWindUValuesMetadataTransformer,
    ERA5LandWindVValuesMetadataTransformer
    )

from .assessor import (
    ERA5PrecipValuesAssessor, 
    ERA5SurfaceSolarRadiationDownwardsValuesAssessor,
    ERA52mTempValuesAssessor,
    ERA5VolumetricSoilWaterLayer1ValuesAssessor,
    ERA5VolumetricSoilWaterLayer2ValuesAssessor,
    ERA5VolumetricSoilWaterLayer3ValuesAssessor,
    ERA5VolumetricSoilWaterLayer4ValuesAssessor,
    ERA5InstantaneousWindGust10mValuesAssessor,
    ERA5WindU10mValuesAssessor,
    ERA5WindV10mValuesAssessor,
    ERA5WindU100mValuesAssessor,
    ERA5WindV100mValuesAssessor,
    ERA5SeaSurfaceTemperatureValuesAssessor,
    ERA5SeaSurfaceTemperatureDailyValuesAssessor,
    ERA5SeaLevelPressureValuesAssessor,
    ERA5LandPrecipValuesAssessor,
    ERA5LandDewpointTemperatureValuesAssessor,
    ERA5LandSnowfallValuesAssessor,
    ERA5Land2mTempValuesAssessor,
    ERA5LandSurfaceSolarRadiationDownwardsValuesAssessor,
    ERA5LandSurfacePressureValuesAssessor,
    ERA5LandWindUValuesAssessor,
    ERA5LandWindVValuesAssessor
    )


from .fetchers import ERA5Precip, ERA5SurfaceSolarRadiationDownwards, ERA52mTemp, ERA5WindU10m, ERA5WindV10m, ERA5WindU100m, ERA5WindV100m, ERA5SeaSurfaceTemperature, ERA5SeaSurfaceTemperatureDaily, ERA5SeaLevelPressure, ERA5LandPrecip, ERA5LandDewpointTemperature, ERA5LandSnowfall, ERA5Land2mTemp, ERA5LandSurfaceSolarRadiationDownwards, ERA5LandSurfacePressure, ERA5LandWindU, ERA5LandWindV, ERA5VolumetricSoilWaterLayer1, ERA5VolumetricSoilWaterLayer2, ERA5VolumetricSoilWaterLayer3, ERA5VolumetricSoilWaterLayer4, ERA5InstantaneousWindGust10m
from .base_values import ERA5PrecipValues, ERA52mTempValues, ERA5SurfaceSolarRadiationDownwardsValues, ERA5VolumetricSoilWaterLayer1Values, ERA5VolumetricSoilWaterLayer2Values, ERA5VolumetricSoilWaterLayer3Values, ERA5VolumetricSoilWaterLayer4Values, ERA5InstantaneousWindGust10mValues, ERA5WindU10mValues, ERA5WindV10mValues, ERA5WindU100mValues, ERA5WindV100mValues, ERA5SeaSurfaceTemperatureValues, ERA5SeaSurfaceTemperatureDailyValues, ERA5SeaLevelPressureValues, ERA5LandPrecipValues, ERA5LandDewpointTemperatureValues, ERA5LandSnowfallValues, ERA5Land2mTempValues, ERA5LandSurfaceSolarRadiationDownwardsValues, ERA5LandSurfacePressureValues, ERA5LandWindUValues, ERA5LandWindVValues

# Centralized config for each dataset
DATASET_CONFIG = {
    "precip": {
        "dataset_values_class": ERA5PrecipValues,
        "assessor_class": ERA5PrecipValuesAssessor,
        "fetcher_class": ERA5Precip,
        "stac_loader_class": ERA5PrecipStacLoader,
        "metadata_transformer_class": ERA5PrecipValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "solar-surface-downwards": {
        "dataset_values_class": ERA5SurfaceSolarRadiationDownwardsValues,
        "assessor_class": ERA5SurfaceSolarRadiationDownwardsValuesAssessor,
        "fetcher_class": ERA5SurfaceSolarRadiationDownwards,
        "stac_loader_class": ERA5SurfaceSolarRadiationDownwardsStacLoader,
        "metadata_transformer_class": ERA5SurfaceSolarRadiationDownwardsValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "2m-temperature": {
        "dataset_values_class": ERA52mTempValues,
        "assessor_class": ERA52mTempValuesAssessor,
        "fetcher_class": ERA52mTemp,
        "stac_loader_class": ERA5PrecipStacLoader,
        "metadata_transformer_class": ERA52mTempValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "volumetric-soil-water-layer-1": {
        "dataset_values_class": ERA5VolumetricSoilWaterLayer1Values,
        "assessor_class": ERA5VolumetricSoilWaterLayer1ValuesAssessor,
        "fetcher_class": ERA5VolumetricSoilWaterLayer1,
        "stac_loader_class": ERA5VolumetricSoilWaterLayer1StacLoader,
        "metadata_transformer_class": ERA5VolumetricSoilWaterLayer1ValuesMetadataTransformer,
        "transformer_class": ERA5VolumetricSoilWaterTransformer,
    },
    "volumetric-soil-water-layer-2": {
        "dataset_values_class": ERA5VolumetricSoilWaterLayer2Values,
        "assessor_class": ERA5VolumetricSoilWaterLayer2ValuesAssessor,
        "fetcher_class": ERA5VolumetricSoilWaterLayer2,
        "stac_loader_class": ERA5VolumetricSoilWaterLayer2StacLoader,
        "metadata_transformer_class": ERA5VolumetricSoilWaterLayer2ValuesMetadataTransformer,
        "transformer_class": ERA5VolumetricSoilWaterTransformer,
    },
    "volumetric-soil-water-layer-3": {
        "dataset_values_class": ERA5VolumetricSoilWaterLayer3Values,
        "assessor_class": ERA5VolumetricSoilWaterLayer3ValuesAssessor,
        "fetcher_class": ERA5VolumetricSoilWaterLayer3,
        "stac_loader_class": ERA5VolumetricSoilWaterLayer3StacLoader,
        "metadata_transformer_class": ERA5VolumetricSoilWaterLayer3ValuesMetadataTransformer,
        "transformer_class": ERA5VolumetricSoilWaterTransformer,
    },
    "volumetric-soil-water-layer-4": {
        "dataset_values_class": ERA5VolumetricSoilWaterLayer4Values,
        "assessor_class": ERA5VolumetricSoilWaterLayer4ValuesAssessor,
        "fetcher_class": ERA5VolumetricSoilWaterLayer4,
        "stac_loader_class": ERA5VolumetricSoilWaterLayer4StacLoader,
        "metadata_transformer_class": ERA5VolumetricSoilWaterLayer4ValuesMetadataTransformer,
        "transformer_class": ERA5VolumetricSoilWaterTransformer,
    },
    "instantaneous-wind-gust-10m": {
        "dataset_values_class": ERA5InstantaneousWindGust10mValues,
        "assessor_class": ERA5InstantaneousWindGust10mValuesAssessor,
        "fetcher_class": ERA5InstantaneousWindGust10m,
        "stac_loader_class": ERA5InstantaneousWindGust10mStacLoader,
        "metadata_transformer_class": ERA5InstantaneousWindGust10mValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "wind-u-10m": {
        "dataset_values_class": ERA5WindU10mValues,
        "assessor_class": ERA5WindU10mValuesAssessor,
        "fetcher_class": ERA5WindU10m,
        "stac_loader_class": ERA5WindU10mStacLoader,
        "metadata_transformer_class": ERA5WindU10mValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "wind-v-10m": {
        "dataset_values_class": ERA5WindV10mValues,
        "assessor_class": ERA5WindV10mValuesAssessor,
        "fetcher_class": ERA5WindV10m,
        "stac_loader_class": ERA5WindV10mStacLoader,
        "metadata_transformer_class": ERA5WindV10mValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "wind-u-100m": {
        "dataset_values_class": ERA5WindU100mValues,
        "assessor_class": ERA5WindU100mValuesAssessor,
        "fetcher_class": ERA5WindU100m,
        "stac_loader_class": ERA5WindU100mStacLoader,
        "metadata_transformer_class": ERA5WindU100mValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer
    },
    "wind-v-100m": {
        "dataset_values_class": ERA5WindV100mValues,
        "assessor_class": ERA5WindV100mValuesAssessor,
        "fetcher_class": ERA5WindV100m,
        "stac_loader_class": ERA5WindV100mStacLoader,
        "metadata_transformer_class": ERA5WindV100mValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "sea-surface-temperature": {
        "dataset_values_class": ERA5SeaSurfaceTemperatureValues,
        "assessor_class": ERA5SeaSurfaceTemperatureValuesAssessor,
        "fetcher_class": ERA5SeaSurfaceTemperature,
        "stac_loader_class": ERA5SeaSurfaceTemperatureStacLoader,
        "metadata_transformer_class": ERA5SeaSurfaceTemperatureValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "sea-surface-temperature-daily": {
        "dataset_values_class": ERA5SeaSurfaceTemperatureDailyValues,
        "assessor_class": ERA5SeaSurfaceTemperatureDailyValuesAssessor,
        "fetcher_class": ERA5SeaSurfaceTemperatureDaily,
        "stac_loader_class": ERA5SeaSurfaceTemperatureDailyStacLoader,
        "metadata_transformer_class": ERA5SeaSurfaceTemperatureDailyValuesMetadataTransformer,
        "transformer_class": ERA5SeaDatasetTransformer,
    },
    "sea-level-pressure": {
        "dataset_values_class": ERA5SeaLevelPressureValues,
        "assessor_class": ERA5SeaLevelPressureValuesAssessor,
        "fetcher_class": ERA5SeaLevelPressure,
        "stac_loader_class": ERA5SeaLevelPressureStacLoader,
        "metadata_transformer_class": ERA5SeaLevelPressureValuesMetadataTransformer,
        "transformer_class": ERA5SeaDatasetTransformer,
    },
    "land-precip": {
        "dataset_values_class": ERA5LandPrecipValues,
        "assessor_class": ERA5LandPrecipValuesAssessor,
        "fetcher_class": ERA5LandPrecip,
        "stac_loader_class": ERA5LandPrecipStacLoader,
        "metadata_transformer_class": ERA5LandPrecipValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-dewpoint-temperature": {
        "dataset_values_class": ERA5LandDewpointTemperatureValues,
        "assessor_class": ERA5LandDewpointTemperatureValuesAssessor,
        "fetcher_class": ERA5LandDewpointTemperature,
        "stac_loader_class": ERA5LandDewpointTemperatureStacLoader,
        "metadata_transformer_class": ERA5LandDewpointTemperatureValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-snowfall": {
        "dataset_values_class": ERA5LandSnowfallValues,
        "assessor_class": ERA5LandSnowfallValuesAssessor,
        "fetcher_class": ERA5LandSnowfall,
        "stac_loader_class": ERA5LandSnowfallStacLoader,
        "metadata_transformer_class": ERA5LandSnowfallValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-2m-temp": {
        "dataset_values_class": ERA5Land2mTempValues,
        "assessor_class": ERA5Land2mTempValuesAssessor,
        "fetcher_class": ERA5Land2mTemp,
        "stac_loader_class": ERA52mTempStacLoader,
        "metadata_transformer_class": ERA5Land2mTempValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-surface-solar-radiation-downwards": {
        "dataset_values_class": ERA5LandSurfaceSolarRadiationDownwardsValues,
        "assessor_class": ERA5LandSurfaceSolarRadiationDownwardsValuesAssessor,
        "fetcher_class": ERA5LandSurfaceSolarRadiationDownwards,
        "stac_loader_class": ERA5LandSurfaceSolarRadiationDownwardsStacLoader,
        "metadata_transformer_class": ERA5LandSurfaceSolarRadiationDownwardsValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-surface-pressure": {
        "dataset_values_class": ERA5LandSurfacePressureValues,
        "assessor_class": ERA5LandSurfacePressureValuesAssessor,
        "fetcher_class": ERA5LandSurfacePressure,
        "stac_loader_class": ERA5LandSurfacePressureStacLoader,
        "metadata_transformer_class": ERA5LandSurfacePressureValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-wind-u": {
        "dataset_values_class": ERA5LandWindUValues,
        "assessor_class": ERA5LandWindUValuesAssessor,
        "fetcher_class": ERA5LandWindU,
        "stac_loader_class": ERA5LandWindUStacLoader,
        "metadata_transformer_class": ERA5LandWindUValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },
    "land-wind-v": {
        "dataset_values_class": ERA5LandWindVValues,
        "assessor_class": ERA5LandWindVValuesAssessor,
        "fetcher_class": ERA5LandWindV,
        "stac_loader_class": ERA5LandWindVStacLoader,
        "metadata_transformer_class": ERA5LandWindVValuesMetadataTransformer,
        "transformer_class": ERA5FamilyDatasetTransformer,
    },

}