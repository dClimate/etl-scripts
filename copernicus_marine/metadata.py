import datetime



def _dataset_parameters(analysis_type: str, dataset_type: str, data_var: str = "") -> tuple[str, str, str]:
    """
    Convenience method to return the correct dataset_id, title, and URL for querying the CDS API

    Parameters
    ----------
    analysis_type : str
        A string of 'analysis' or 'reanalysis'
    """
    if analysis_type == "reanalysis":
        if dataset_type == "sea_level":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description"  # noqa: E501
            )
        if dataset_type == "ocean_physics":
            dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
            title = "Global Ocean Physics Reanalysis"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
        
    elif analysis_type == "interim-reanalysis":
        if dataset_type == "sea_level":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_myint_allsat-l4-duacs-0.25deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES REPROCESSED (1993-ONGOING)"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_MY_008_047/description"  # noqa: E501
            )
        if dataset_type == "ocean_physics":
            dataset_id = "cmems_mod_glo_phy_myint_0.083deg_P1D-m"
            title = "Global Ocean Physics Reanalysis (Interim)"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030/INFORMATION"
    elif analysis_type == "analysis":
        if dataset_type == "sea_level":
            dataset_id = "cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.25deg_P1D"
            title = "GLOBAL OCEAN GRIDDED L4 SEA SURFACE HEIGHTS AND DERIVED VARIABLES NRT"
            info_url = (
                "https://data.marine.copernicus.eu/product/SEALEVEL_GLO_PHY_L4_NRT_008_046/description"  # noqa: E501
            )
        if dataset_type == "ocean_physics":
            dataset_id = f"cmems_mod_glo_phy-{data_var}_anfc_0.083deg_P1D-m"
            title = "Global Ocean Physics Analysis and Forecast"
            info_url = "https://resources.marine.copernicus.eu/product-detail/GLOBAL_ANALYSIS_FORECAST_PHY_001_024/INFORMATION" 
    return dataset_id, title, info_url


dataset_description = {
    "sea_level": {
        "Altimeter satellite gridded Sea Level Anomalies (SLA) computed with respect to a twenty-year 2012 mean. " +
        "The sea level anomaly (SLA) is the current height of the sea (in meters) above the mean sea surface height. "  + # noqa: E501
        "The SLA is estimated by Optimal Interpolation, merging the L3 along-track measurement from the different altimeter missions available. "  # noqa: E501
        "Part of the processing is fitted to the Global ocean. (see QUID document or 1 [http://duacs.cls.fr] pages for processing details)."  # noqa: E501
        "The product gives additional variables (i.e. Absolute Dynamic Topography and geostrophic currents (absolute and anomalies))."  # noqa: E501
        "It serves in delayed-time applications. This product is processed by the DUACS multimission altimeter data processing system. "  # noqa: E501
        f"More information at {_dataset_parameters(analysis_type='analysis', dataset_type='sea_level')[2]} and {_dataset_parameters(analysis_type='reanalysis', dataset_type='sea_level')[2]}"
    },
    # TODO FIX THE ANALYSIS AND REANALYSIS LINK
    "ocean_physics": {
         "The Operational Mercator global ocean analysis and forecast system at 1/12 degree includes daily mean ",
        "files of temperature from the top to the bottom over the global ocean. ",
            "The global ocean output files are displayed with a 1/12 degree horizontal ",
            "resolution with regular longitude/latitude equirectangular projection. ",
            "50 vertical levels are provided, ranging from 0 to 5500 meters. ",
            "Data is updated on a 24 hour lag at 12:01 PM every day. ",
            "Prior to January 1, 2020 data from the GLORYS12V1 reanalysis is provided. ",
            "The GLORYS12V1 product is the CMEMS global ocean eddy-resolving (1/12° horizontal resolution, ",
            "50 vertical levels) reanalysis covering the altimetry (1993 onward). ",
            "It is based largely on the current real-time global forecasting CMEMS system. ",
            "The model component is the NEMO platform driven at surface by ECMWF ERA-Interim ",
            "then ERA5 reanalyses for recent years. Observations are assimilated by means of a reduced-order ",
            "Kalman filter. Along track altimeter data (Sea Level Anomaly), Satellite Sea Surface Temperature, ",
            "Sea Ice Concentration and In situ Temperature and Salinity vertical Profiles are jointly assimilated. ",
            "Moreover, a 3D-VAR scheme provides a correction for the slowly-evolving large-scale biases ",
            "in temperature and salinity. ",
            f"More information at {_dataset_parameters(analysis_type='analysis', dataset_type='ocean_physics')[2]} and {_dataset_parameters(analysis_type='reanalysis', dataset_type='ocean_physics')[2]}"
    },
    "salinity": {
            "The Operational Mercator global ocean analysis and forecast system at 1/12 degree includes daily mean ",
            "files of temperature from the top to the bottom over the global ocean. ",
            "The global ocean output files are displayed with a 1/12 degree horizontal ",
            "resolution with regular longitude/latitude equirectangular projection. ",
            "50 vertical levels are provided, ranging from 0 to 5500 meters. ",
            "Data is updated on a 24 hour lag at 12:01 PM every day. ",
            "Prior to January 1, 2020 data from the GLORYS12V1 reanalysis is provided. ",
            "The GLORYS12V1 product is the CMEMS global ocean eddy-resolving (1/12° horizontal resolution, ",
            "50 vertical levels) reanalysis covering the altimetry (1993 onward). ",
            "It is based largely on the current real-time global forecasting CMEMS system. ",
            "The model component is the NEMO platform driven at surface by ECMWF ERA-Interim ",
            "then ERA5 reanalyses for recent years. Observations are assimilated by means of ",
            "a reduced-order Kalman filter. Along track altimeter data (Sea Level Anomaly), ",
            "Satellite Sea Surface Temperature, Sea Ice Concentration and In situ Temperature ",
            "and Salinity vertical Profiles are jointly assimilated.  Moreover, a 3D-VAR scheme ",
            "provides a correction for  the slowly-evolving large-scale biases in temperature and salinity. ",
            "Note that salinity data values are returned in Practical Salinity Units (PSUs), ",
            "which are explicitly discouraged within the scientific community. ",
            "Specifying PSU under `unit_of_measurement` therefore breaks dClimate's API ",
            "due to incongruencies with the supporting libraries for unit conversion. ",
            "For this reason we leave the `unit_of_measurement` field blank, ",
            "although the dataset values are in fact measured in PSUs.",
            f"More information at {_dataset_parameters(analysis_type='analysis', dataset_type='ocean_physics')[2]} and {_dataset_parameters(analysis_type='reanalysis',  dataset_type='ocean_physics')[2]}"
    }
}


# dict containing static fields in the metadata
dataset_metadata = {
    "sea_level": {
        "update_cadence": "daily",
        "temporal_resolution": "daily",
        "spatial_resolution": "0.25 degrees",
        "spatial_precision": 0.01,
        "dataset_description": str(dataset_description["sea_level"]),
        "name": "copernicus_ocean_sea_level",
        "updated": str(datetime.datetime.now()),
        "missing_value": -2147483647,
        "tags": ["Sea level anomaly, Sea surface height"],
        "standard_name": "sea_surface_height_above_geoid",
        "long_name": "Sea Surface Height Above Geoid",
        "unit_of_measurement": "m",
        "final_lag_in_days": 150,
        "preliminary_lag_in_days": None,
        "expected_nan_frequency": 0.4226138117283951,
    },
    "ocean_temp": {
        "update_cadence": "daily",
        "temporal_resolution": "daily",
        "spatial_resolution": "0.25 degrees",
        "spatial_precision": 0.01,
        "dataset_description": str(dataset_description["ocean_physics"]),
        "name": "copernicus_ocean_physics",
        "updated": str(datetime.datetime.now()),
        "missing_value": -2147483647,
        "tags": ["Temperature, Salinity, Ocean Physics"],
        "standard_name": "sea_water_temperature",
        "long_name": "Sea Water Temperature",
        "unit_of_measurement": "°C",
        "final_lag_in_days": 150,
        "preliminary_lag_in_days": None,
        "expected_nan_frequency": 0.4226138117283951,
    },
}

def static_metadata(dataset_type, variable) -> dict:
    """
    dict containing static fields in the metadata
    """
    # Convert ocean_temp to ocean_physics
    if dataset_type == "ocean_temp":
        dataset_type_parameters = "ocean_physics"
    else:
        dataset_type_parameters = dataset_type

    static_metadata = {
        "coordinate_reference_system": "EPSG:4326",
        "update_cadence": dataset_metadata[dataset_type]["update_cadence"],
        "temporal_resolution": dataset_metadata[dataset_type]["temporal_resolution"],
        "spatial_resolution": dataset_metadata[dataset_type]["spatial_resolution"],
        "spatial_precision": 0.01,
        "provider_url": "https://resources.marine.copernicus.eu/product-detail/",
        "reanalysis_data_download_url": _dataset_parameters(analysis_type="reanalysis", dataset_type=dataset_type_parameters, data_var=variable )[2],
        "analysis_data_download_url": _dataset_parameters(analysis_type="analysis", dataset_type=dataset_type_parameters, data_var=variable)[2],
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
        "dataset_description": dataset_metadata[dataset_type]["dataset_description"],
        "license": "Reuse allowed with attribution (custom license)",
        "terms_of_service": "https://marine.copernicus.eu/user-corner/service-commitments-and-licence",
        "name": dataset_metadata[dataset_type]["name"],
        "updated": str(datetime.datetime.now()),
        "missing_value": dataset_metadata[dataset_type]["missing_value"],
        "tags": dataset_metadata[dataset_type]["tags"],
        "standard_name": dataset_metadata[dataset_type]["standard_name"],
        "long_name": dataset_metadata[dataset_type]["long_name"],
        "unit_of_measurement": dataset_metadata[dataset_type]["unit_of_measurement"],
        "final_lag_in_days": dataset_metadata[dataset_type]["final_lag_in_days"],
        "preliminary_lag_in_days": dataset_metadata[dataset_type]["preliminary_lag_in_days"],
        "expected_nan_frequency": dataset_metadata[dataset_type]["expected_nan_frequency"],
    }
    return static_metadata