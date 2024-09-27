import xarray

from .base_values import (
    ERA5Values, 
    ERA5Family, 
    ERA5LandValues, 
    ERA5SeaValues, 
    ERA5LandWindValues, 
    ERA5PrecipValues,
    ERA52mTempValues, 
    ERA5SurfaceSolarRadiationDownwardsValues, 
    ERA5VolumetricSoilWaterValues, 
    ERA5VolumetricSoilWaterLayer1Values, 
    ERA5VolumetricSoilWaterLayer2Values, 
    ERA5VolumetricSoilWaterLayer3Values, 
    ERA5VolumetricSoilWaterLayer4Values, 
    ERA5Wind10mValues, 
    ERA5InstantaneousWindGust10mValues, 
    ERA5WindU10mValues, 
    ERA5WindV10mValues, 
    ERA5Wind100mValues, 
    ERA5WindU100mValues, 
    ERA5WindV100mValues, 
    ERA5SeaSurfaceTemperatureValues, 
    ERA5SeaSurfaceTemperatureDailyValues, 
    ERA5SeaLevelPressureValues, 
    ERA5LandPrecipValues, 
    ERA5LandDewpointTemperatureValues, 
    ERA5LandSnowfallValues, 
    ERA5Land2mTempValues, 
    ERA5LandSurfaceSolarRadiationDownwardsValues, 
    ERA5LandSurfacePressureValues, 
    ERA5LandWindUValues, 
    ERA5LandWindVValues
)
import datetime


class MetadataTransformer():

    rebuild_requested: bool = False

    def store(self):
        pass


    def set_metadata(self, dataset: xarray.Dataset, pipeline_info: dict) -> xarray.Dataset:
        """
        Function to append to or update key metadata information to the attributes and encoding of the output Zarr.
        Extends existing class method to create attributes or encoding specific to ERA5.

        Parameters
        ----------
        dataset
            The dataset prepared for parsing to IPLD
        """
        # dataset = super().set_zarr_metadata(dataset)
        # GRIB filters carried over from the original ERA5 datasets will result
        # in the dataset being unwriteable b/c "ValueError: codec not available: 'grib"
        for coord in ["latitude", "longitude"]:
            dataset[coord].encoding.pop("_FillValue", None)
            dataset[coord].encoding.pop("missing_value", None)
        # Remove extraneous data from the data variable's attributes
        keys_to_remove = ["coordinates", "GRIB_paramId", "history", "CDO", "CDI"]
        for key in keys_to_remove:
            dataset.attrs.pop(key, None)
            dataset[self.data_var].attrs.pop(key, None)

        # Add a finalization date attribute to the Zarr metadata.
        # Set the value to the object's finalization date if it is present in this object.
        # If not, try to carry over the finalization date from an existing dataset.
        # Finally, if there is no existing data, set the date attribute to an empty string.
        # If a new finalization date exists, format it to %Y%m%d%H.
        # Record the date the new fin date was discovered to prevent wasteful checking
        finalization_date = pipeline_info["finalization_date"]
        existing_dataset = pipeline_info["existing_dataset"]
        existing_properties = existing_dataset.get("properties", {}) if existing_dataset else {}
        existing_finalizating_date = existing_properties.get("finalization_date", None)
        existing_last_finalization_date_change = existing_properties.get("last_finalization_date_change", None)
        if finalization_date is not None:
            dataset.attrs["finalization_date"] = finalization_date
            dataset.attrs["last_finalization_date_change"] = datetime.datetime.today().date().isoformat()
        else:
            if (
                existing_finalizating_date
                and not self.rebuild_requested
            ):
                dataset.attrs["finalization_date"] = existing_finalizating_date
                print(
                    "Finalization date not set previously, setting to existing finalization date: "
                    f"{dataset.attrs['finalization_date']}"
                )
                if existing_last_finalization_date_change:
                    dataset.attrs["last_finalization_date_change"] = existing_last_finalization_date_change
            else:
                dataset.attrs["finalization_date"] = ""
                print("Finalization date not set previously, setting to empty string")
        dataset.attrs.update(self.static_metadata)
        return dataset


    def metadata_transformer(self, dataset: xarray.Dataset, pipeline_info: dict) -> tuple[xarray.Dataset, dict]:
        return self.set_metadata(dataset=dataset, pipeline_info=pipeline_info), pipeline_info


class ERA5FamilyMetadataTransformer(MetadataTransformer, ERA5Family):
    pass

class ERA5MetadataTransformer(ERA5FamilyMetadataTransformer, ERA5Values):
    pass

class ERA5LandValuesMetadataTransformer(ERA5FamilyMetadataTransformer, ERA5LandValues):
    pass

class ERA5SeaValuesMetadataTransformer(ERA5MetadataTransformer, ERA5SeaValues):
    pass

class ERA5LandWindValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandWindValues):
    pass

class ERA5PrecipValuesMetadataTransformer(ERA5MetadataTransformer, ERA5PrecipValues):
    pass

class ERA52mTempValuesMetadataTransformer(ERA5MetadataTransformer, ERA52mTempValues):
    pass

class ERA5SurfaceSolarRadiationDownwardsValuesMetadataTransformer(ERA5MetadataTransformer, ERA5SurfaceSolarRadiationDownwardsValues):
    pass

class ERA5VolumetricSoilWaterValuesMetadataTransformer(ERA5MetadataTransformer, ERA5VolumetricSoilWaterValues):
    pass

class ERA5VolumetricSoilWaterLayer1ValuesMetadataTransformer(ERA5VolumetricSoilWaterValuesMetadataTransformer, ERA5VolumetricSoilWaterLayer1Values):
    pass

class ERA5VolumetricSoilWaterLayer2ValuesMetadataTransformer(ERA5VolumetricSoilWaterValuesMetadataTransformer, ERA5VolumetricSoilWaterLayer2Values):
    pass

class ERA5VolumetricSoilWaterLayer3ValuesMetadataTransformer(ERA5VolumetricSoilWaterValuesMetadataTransformer, ERA5VolumetricSoilWaterLayer3Values):
    pass

class ERA5VolumetricSoilWaterLayer4ValuesMetadataTransformer(ERA5VolumetricSoilWaterValuesMetadataTransformer, ERA5VolumetricSoilWaterLayer4Values):
    pass

class ERA5Wind10mValuesMetadataTransformer(ERA5MetadataTransformer, ERA5Wind10mValues):
    pass

class ERA5InstantaneousWindGust10mValuesMetadataTransformer(ERA5Wind10mValuesMetadataTransformer, ERA5InstantaneousWindGust10mValues):
    pass

class ERA5WindU10mValuesMetadataTransformer(ERA5Wind10mValuesMetadataTransformer, ERA5WindU10mValues):
    pass

class ERA5WindV10mValuesMetadataTransformer(ERA5Wind10mValuesMetadataTransformer, ERA5WindV10mValues):
    pass

class ERA5Wind100mValuesMetadataTransformer(ERA5MetadataTransformer, ERA5Wind100mValues):
    pass

class ERA5WindU100mValuesMetadataTransformer(ERA5Wind100mValuesMetadataTransformer, ERA5WindU100mValues):
    pass

class ERA5WindV100mValuesMetadataTransformer(ERA5Wind100mValuesMetadataTransformer, ERA5WindV100mValues):
    pass

# SEA TRANSFORMERS
class ERA5SeaSurfaceTemperatureValuesMetadataTransformer(ERA5SeaValuesMetadataTransformer, ERA5SeaSurfaceTemperatureValues):
    pass

class ERA5SeaSurfaceTemperatureDailyValuesMetadataTransformer(ERA5SeaValuesMetadataTransformer, ERA5SeaSurfaceTemperatureDailyValues):
    pass

class ERA5SeaLevelPressureValuesMetadataTransformer(ERA5SeaValuesMetadataTransformer, ERA5SeaLevelPressureValues):
    pass

# LAND TRANSFORMERS
class ERA5LandPrecipValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandPrecipValues):
    pass

class ERA5LandDewpointTemperatureValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandDewpointTemperatureValues):
    pass

class ERA5LandSnowfallValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandSnowfallValues):
    pass

class ERA5Land2mTempValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5Land2mTempValues):
    pass

class ERA5LandSurfaceSolarRadiationDownwardsValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandSurfaceSolarRadiationDownwardsValues):
    pass

class ERA5LandSurfacePressureValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandSurfacePressureValues):
    pass

class ERA5LandWindUValuesMetadataTransformer(ERA5LandValuesMetadataTransformer, ERA5LandWindUValues):
    pass

class ERA5LandWindVValuesMetadataTransformer(ERA5LandWindUValuesMetadataTransformer, ERA5LandWindVValues):
    pass

