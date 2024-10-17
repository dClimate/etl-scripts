import xarray
import datetime

class MetadataTransformer():

    # Init with variable and dataset_name

    def __init__(self, variable: str, dataset_name: str):
        self.variable = variable
        self.dataset_name = dataset_name

    def set_dataset_metadata(self, dataset: xarray.Dataset) -> xarray.Dataset:
        # Newer satellites have different attributes we must accommodate to prevent update ETL failure
        try:
            dataset.attrs["preferred_citation"] = dataset.attrs["CITATION_TO_DOCUMENTS"]
        except KeyError:
            dataset.attrs["preferred_citation"] = dataset.attrs["summary"] + " from " + dataset.attrs["creator_name"]
        # Delete problematic or extraneous holdover attributes from the input files
        keys_to_remove = [
            "scale_factor",
            "add_offset",
            "cdm_data_type",
            "ANCILLARY_FILES",
            "CITATION_TO_DOCUMENTS",
            "CONFIGURE_FILE_CONTENT",
            "CONTACT",
            "DAYS_PER_PERIOD",
            "FILENAME",
            "INPUT_FILENAMES",
            "INPUT_FILES",
            "INSTRUMENT",
            "DATE_BEGIN",
            "DATE_END",
            "DAYS PER PERIOD",
            "Metadata_Conventions",
            "PERIOD_OF_YEAR",
            "PRODUCT_NAME",
            "PROJECTION",
            "SATELLITE",
            "START_LATITUDE_RANGE",
            "START_LONGITUDE_RANGE",
            "END_LATITUDE_RANGE",
            "END_LONGITUDE_RANGE",
            "TIME_BEGIN",
            "TIME_END",
            "VERSION",
            "YEAR",
            "time_coverage_start",
            "time_coverage_end",
            "satellite_name",
            "standard_name_vocabulary",
            "version",
            "created",
            "creator_name",
            "creator_email",
            "creator_url",
            "date_created",
            "geospatial_lat_units",
            "geospatial_lon_units",
            "geospatial_lat_min",
            "geospatial_lon_min",
            "geospatial_lat_max",
            "geospatial_lon_max",
            "geospatial_lat_resolution",
            "geospatial_lon_resolution",
            "history",
            "id",
            "institution",
            "instrument_name",
            "naming_authority",
            "process",
            "processing_level",
            "project",
            "publisher_name",
            "publisher_email",
            "publisher_url",
            "references",
            "source",
            "summary",
        ]

        all_keys = (
            list(dataset.attrs.keys())
            + list(dataset[self.variable].attrs.keys())
            + list(dataset[self.variable].encoding.keys())
        )

        for key in all_keys:
            if key in keys_to_remove:
                dataset.attrs.pop(key, None)
                dataset["latitude"].attrs.pop(key, None)
                dataset["longitude"].attrs.pop(key, None)
                dataset[self.variable].attrs.pop(key, None)
                dataset[self.variable].encoding.pop(key, None)

        dataset.attrs.update(static_metadata)
        return dataset

    def metadata_transformer(self, dataset: xarray.Dataset, pipeline_info: dict) -> tuple[xarray.Dataset, dict]:
        return self.set_dataset_metadata(dataset=dataset), pipeline_info


static_metadata = {
    "coordinate_reference_system": "EPSG:32662",
    "spatial_resolution": 360 / 10000,
    "spatial_precision": 0.01,
    "temporal_resolution": "weekly",
    "update_cadence": "weekly",
    "bbox": [-179.982, -55.134, 179.982, 75.006], 
    "provider_url": "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/index.php",
    "data_download_url": "https://www.star.nesdis.noaa.gov/data/pub0018/VHPdata4users/data/Blended_VH_4km/VH",
    "publisher": "DOC/NOAA/NESDIS/STAR > VHP Team, Center for Satellite Applications and Research (STAR)"
    " National Environmental Satellite, Data, and Information Service (NOAA-NESDIS)"
    " National Oceanic and Atmospheric Administration (NOAA)"
    " Department of Commerce",
    "title": "Global and Regional Vegetation Health Index (VHI)",
    "provider_description": "The National Ocean and Atmospheric Administration's (NOAA) Center for Satellite Applications"  # noqa: E501
    "and Research (STAR) uses innovative science and applications to transform satellite observations of the earth into"  # noqa: E501
    " meaninguful information essential to society's evolving environmental, security, and economic decision-making.",  # noqa: E501
    "dataset_description": (
        "This dataset contains Vegetation Health Indices (VHI) derived from the radiance observed by the"  # noqa: E501
        " Advanced Very High Resolution Radiometer (AVHRR) onboard afternoon polar-orbiting satellites."  # noqa: E501
        " The VH products from AVHRR were produced from the NOAA/NESDIS Global Area Coverage (GAC) data set for the period"  # noqa: E501
        " 1981 to the present. The data and images have 4 km spatial and 7-day composite temporal resolution."  # noqa: E501
        " VH products from VIIRS were also processed from 2012 to present (1km resolution, 7 day composite)."  # noqa: E501
        " The VH indices range from 0 to 100 characterizing changes in vegetation conditions from extremely poor (0) to excellent (100)."  # noqa: E501
        " The VH reflects indirectly a combination of chlorophyll and moisture content in the vegetationhealth and also changes in thermal"  # noqa: E501
        " conditions at the surface.Monitoring vegetation health (condition), including drought detection and watch, is based on radiance"  # noqa: E501
        " measurements in the visible (VIS), near infrared (NIR), and 10.3-11.3 micrometers thermal (T) bands (channels) of the AVHRR."  # noqa: E501
        " The VH products can be used as proxy data for monitoring vegetation health, drought, soil saturation, moisture and"  # noqa: E501
        " thermal conditions, fire risk, greenness of vegetation cover, vegetation fraction, leave area index, start/end of the"  # noqa: E501
        " growing season, crop and pasture productivity, teleconnection with ENSO, desertification, mosquito-borne diseases,"  # noqa: E501
        " invasive species, ecological resources, land degradation, etc."  # noqa: E501
        " More dataset information at https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH_doc/VHP_uguide_v2.0_2018_0727.pdf"  # noqa: E501
    ),
    "license": "Public",
    "terms_of_service": "https://www.ngdc.noaa.gov/ngdcinfo/privacy.html",
    "name": "vhi",
    "updated": str(datetime.datetime.now()),
    "missing_value": -999,
    "tags": ["index", "vegetation"],
    "standard_name": "vegetation",
    "long_name": "Vegetation Health Index",
    "unit_of_measurement": "vegetative health score",
    "final_lag_in_days": 21,
    "expected_nan_frequency": 1,
}
