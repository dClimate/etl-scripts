import xarray
import datetime
from .metadata import dataset_urls, ocean_physics_description, sea_level_description, salinity_description
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

class DatasetTransformer():

    def __init__(self, data_var: str, dataset_name: str):
        self.data_var = data_var
        self.dataset_name = dataset_name

    def static_metadata(self) -> dict:
        """
        dict containing static fields in the metadata
        """
        static_metadata_dict = {
            "coordinate_reference_system": "EPSG:4326",
            "update_cadence": self.update_cadence,
            "spatial_resolution": self.spatial_resolution,
            "spatial_precision": self.spatial_precision,
            "provider_url": "https://resources.marine.copernicus.eu/product-detail/",
            "reanalysis_data_download_url": self.reanalysis_data_download_url,
            "analysis_data_download_url": self.analysis_data_download_url,
            "publisher": "Copernicus Marine Service",
            "title": "Copernicus Marine Anaylsis and Reanalysis",
            "provider_description": (
                "Based on satellite and in situ observations, the"
                " Copernicus services deliver near-real-time data on a global level which can"  # noqa: E501
                " also be used for local and regional needs, to help us better understand our planet"  # noqa: E501
                " and sustainably manage the environment we live in. "  # noqa: E501
                "Copernicus is served by a set of dedicated satellites (the Sentinel families) and"  # noqa: E501
                " contributing missions (existing commercial and public satellites). The Sentinel satellites"  # noqa: E501
                " are specifically designed to meet the needs of the Copernicus services and their users. Since"  # noqa: E501
                " the launch of Sentinel-1A in 2014, the European Union set in motion a process to place a"  # noqa: E501
                " constellation of almost 20 more satellites in orbit before 2030. "  # noqa: E501
                "Copernicus also collects information from in situ systems such as ground stations, which"  # noqa: E501
                " deliver data acquired by a multitude of sensors on the ground, at sea or in the air. "  # noqa: E501
                "The Copernicus services transform this wealth of satellite and in situ data into"  # noqa: E501
                " value-added information by processing and analysing the data. Datasets stretching"  # noqa: E501
                " back for years and decades are made comparable and searchable, thus ensuring the"  # noqa: E501
                " monitoring of changes; patterns are examined and used to create better forecasts,"  # noqa: E501
                " for example, of the ocean and the atmosphere. Maps are created from imagery, features"  # noqa: E501
                " and anomalies are identified and statistical information is extracted."  # noqa: E501
            ),
            "dataset_description": self.dataset_description,
            "license": "Reuse allowed with attribution (custom license)",
            "terms_of_service": "https://marine.copernicus.eu/user-corner/service-commitments-and-licence",
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
            "reanalysis_end_date": self.reanalysis_end_date,
            "reanalysis_start_date": self.reanalysis_start_date,
            "interim_reanalysis_end_date": self.interim_reanalysis_end_date,
            "interim_reanalysis_start_date": self.interim_reanalysis_start_date,

        }
        return static_metadata_dict
        
    # Delete problematic or extraneous holdover attributes from the input files
    # Because each Copernicus Marine dataset names fields differently
    # ('latitude' vs 'lat') this list is long and duplicative
    def set_dataset_metadata(self, dataset: xarray.Dataset) -> xarray.Dataset:
        keys_to_remove = [
            "processing_level",
            "source",
            "_CoordSysBuilder",
            "FROM_ORIGINAL_FILE__platform",
            "ancillary_variables",
            "time_coverage_resolution",
            "contact",
            "keywords_vocabulary",
            "ssalto_duacs_comment",
            "cdm_data_type",
            "institution",
            "geospatial_vertical_max",
            "date_created",
            "summary",
            "FROM_ORIGINAL_FILE__product_version",
            "FROM_ORIGINAL_FILE__geospatial_lat_units",
            "keywords",
            "time_coverage_end",
            "geospatial_vertical_positive",
            "geospatial_vertical_units",
            "provider url",
            "FROM_ORIGINAL_FILE__Metadata_Conventions",
            "FROM_ORIGINAL_FILE__geospatial_lon_min",
            "FROM_ORIGINAL_FILE__geospatial_lon_max",
            "FROM_ORIGINAL_FILE__geospatial_lon_units",
            "FROM_ORIGINAL_FILE__geospatial_lat_min",
            "FROM_ORIGINAL_FILE__latitude_min",
            "FROM_ORIGINAL_FILE__geospatial_lat_max",
            "FROM_ORIGINAL_FILE__field_type",
            "FROM_ORIGINAL_FILE__longitude_min",
            "FROM_ORIGINAL_FILE__longitude_max",
            "FROM_ORIGINAL_FILE__latitude_max",
            "FROM_ORIGINAL_FILE__geospatial_lon_resolution",
            "FROM_ORIGINAL_FILE__geospatial_lat_resolution",
            "FROM_ORIGINAL_FILE__software_version",
            "date_issued",
            "date_modified",
            "geospatial_vertical_min",
            "history",
            "time_coverage_start",
            "creator_name",
            "time_coverage_duration",
            "creator_url",
            "comment",
            "creator_email",
            "project",
            "standard_name_vocabulary",
            "z_min",
            "z_max",
            "easting",
            "northing",
            "domain_name",
            "bulletin_date",
            "bulletin_type",
            "forecast_type",
            "forecast_range",
            "field_date",
            "field_type",
            "julian_day_unit",
            "field_julian_date",
            "geospatial_vertical_resolution",
            "references",
            "compute_hosts",
            "n_workers",
            "sshcluster_timeout",
            "product_user_manual",
            "quality_information_document",
            "_ChunkSizes",
            "copernicus_marine_client_version",
            "CDI",
            "CDO",
            "chunks",
            "original_shape",
            "chunksizes",
            "add_offset",
            "scale_factor",
            "latitude_max",
            "latitude_min",
            "longitude_max",
            "longitude_min",
            "geospatial_lat_max",
            "geospatial_lat_min",
            "geospatial_lon_max",
            "geospatial_lon_min",
            "copernicusmarine_version",
        ]

        all_keys = (
            list(dataset.attrs.keys())
            + list(dataset[self.data_var].attrs.keys())
            + list(dataset[self.data_var].encoding.keys())
        )
        for key in all_keys:
            if key in keys_to_remove:
                dataset.attrs.pop(key, None)
                dataset["latitude"].attrs.pop(key, None)
                dataset["longitude"].attrs.pop(key, None)
                dataset[self.data_var].attrs.pop(key, None)
                dataset[self.data_var].encoding.pop(key, None)

        dataset.attrs.update(self.static_metadata())
        return dataset

    def dataset_transformer(self, dataset: xarray.Dataset, metadata_info: dict) -> xarray.Dataset:
        self.reanalysis_end_date = metadata_info["reanalysis_end_date"]
        self.reanalysis_start_date = metadata_info["reanalysis_start_date"]
        self.interim_reanalysis_end_date = metadata_info["interim_reanalysis_end_date"]
        self.interim_reanalysis_start_date = metadata_info["interim_reanalysis_start_date"]
        return self.set_dataset_metadata(dataset=dataset), metadata_info


class GlobalPhysicsDatasetTransformer(DatasetTransformer):

    @classmethod
    def postprocess_zarr(cls, dataset):
        """
        Global Ocean Physics datasets are provided in 4D format with a 'depth' dimension we remove here.
        We separate each depth into a separate Zarr and work exclusively with 3D data,
        so this fourth dimension causes parent class code in DatasetManager to break.

        The depth dimension doesn't show up locally but oddly does over AWS, so we make its removal conditional
        to allow for both cases
        """
        # Necessary pre-processing steps specific to Global Physics datasets
        if "depth" in dataset.coords:
            dataset = dataset.drop_vars("depth")
        dataset = dataset.squeeze()
        if "time" not in dataset.dims:
            dataset = dataset.expand_dims("time")
        return dataset

    def dataset_transformer(self, dataset: xarray.Dataset, metadata_info: dict) -> xarray.Dataset:
        dataset = self.postprocess_zarr(dataset)
        self.reanalysis_end_date = metadata_info["reanalysis_end_date"]
        self.reanalysis_start_date = metadata_info["reanalysis_start_date"]
        self.interim_reanalysis_end_date = metadata_info["interim_reanalysis_end_date"]
        self.interim_reanalysis_start_date = metadata_info["interim_reanalysis_start_date"]
        return self.set_dataset_metadata(dataset=dataset), metadata_info


class CopernicusOceanSeaSurfaceHeightTransformer(DatasetTransformer, CopernicusOceanSeaSurfaceHeightValues):
    def __init__(self):
        CopernicusOceanSeaSurfaceHeightValues.__init__(self)
        DatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = sea_level_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["sea_level"]
        self.analysis_data_download_url = dataset_urls["analysis"]["sea_level"]

class CopernicusOceanTemp0p5DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanTemp0p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanTemp0p5DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = ocean_physics_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]

class CopernicusOceanTemp1p5DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanTemp1p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanTemp1p5DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = ocean_physics_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]

class CopernicusOceanTemp6p5DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanTemp6p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanTemp6p5DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = ocean_physics_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]

class CopernicusOceanSalinity0p5DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanSalinity0p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity0p5DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = salinity_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]

class CopernicusOceanSalinity1p5DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanSalinity1p5DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity1p5DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = salinity_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]

class CopernicusOceanSalinity2p6DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanSalinity2p6DepthValues):

    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity2p6DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = salinity_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]
        
class CopernicusOceanSalinity25DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanSalinity25DepthValues):
    
    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity25DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = salinity_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]
        
class CopernicusOceanSalinity109DepthTransformer(GlobalPhysicsDatasetTransformer, CopernicusOceanSalinity109DepthValues):
    def __init__(self, *args, **kwargs):
        CopernicusOceanSalinity109DepthValues.__init__(self)
        GlobalPhysicsDatasetTransformer.__init__(self, self.data_var, self.dataset_name)
        self.dataset_description = salinity_description
        self.reanalysis_data_download_url = dataset_urls["reanalysis"]["ocean_physics"]
        self.analysis_data_download_url = dataset_urls["analysis"]["ocean_physics"]