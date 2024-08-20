from dc_etl.transform import Transformer
import xarray
from .metadata import static_metadata

def set_dataset_metadata(variable: str, dataset_name: str) -> Transformer:
    """Transformer to update the zarr metadata.

    Returns
    -------
    Transformer :
        The transformer.
    """

    # Delete problematic or extraneous holdover attributes from the input files
    # Because each Copernicus Marine dataset names fields differently
    # ('latitude' vs 'lat') this list is long and duplicative
    def set_dataset_metadata(dataset: xarray.Dataset) -> xarray.Dataset:
        keys_to_remove = [
            "processing_level",
            "source",
            "_CoordSysBuilder",
            "FROM_ORIGINAL_FILE__platform",
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
            "original_shape",
            "chunksizes",
            "add_offset",
            "scale_factor",
            "latitude_max",
            "latitude_min",
            "longitude_max",
            "longitude_min",
            "copernicusmarine_version",
        ]

        all_keys = (
            list(dataset.attrs.keys())
            + list(dataset[variable].attrs.keys())
            + list(dataset[variable].encoding.keys())
        )
        for key in all_keys:
            if key in keys_to_remove:
                dataset.attrs.pop(key, None)
                dataset["latitude"].attrs.pop(key, None)
                dataset["longitude"].attrs.pop(key, None)
                dataset[variable].attrs.pop(key, None)
                dataset[variable].encoding.pop(key, None)

        dataset.attrs.update(static_metadata(dataset_name))
        return dataset

    return set_dataset_metadata




