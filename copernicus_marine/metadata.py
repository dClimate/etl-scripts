
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

dataset_urls = {
    "reanalysis": {
        "sea_level": "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_MY_008_047",
        "ocean_physics": "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030",
    },
    "analysis": {
        "sea_level": "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_NRT_008_046",
        "ocean_physics": "https://resources.marine.copernicus.eu/product-detail/GLOBAL_ANALYSIS_FORECAST_PHY_001_024",
    },
    "interim-reanalysis": {
        "sea_level": "https://resources.marine.copernicus.eu/product-detail/SEALEVEL_GLO_PHY_L4_MY_008_047",
        "ocean_physics": "https://resources.marine.copernicus.eu/product-detail/GLOBAL_MULTIYEAR_PHY_001_030",
    }
}

ocean_physics_description = (
    "The Operational Mercator global ocean analysis and forecast system at 1/12 degree includes daily mean "
    "files of temperature from the top to the bottom over the global ocean. "
    "The global ocean output files are displayed with a 1/12 degree horizontal "
    "resolution with regular longitude/latitude equirectangular projection. "
    "50 vertical levels are provided, ranging from 0 to 5500 meters. "
    "Data is updated on a 24 hour lag at 12:01 PM every day. "
    "Prior to January 1, 2020 data from the GLORYS12V1 reanalysis is provided. "
    "The GLORYS12V1 product is the CMEMS global ocean eddy-resolving (1/12° horizontal resolution, "
    "50 vertical levels) reanalysis covering the altimetry (1993 onward). "
    "It is based largely on the current real-time global forecasting CMEMS system. "
    "The model component is the NEMO platform driven at surface by ECMWF ERA-Interim "
    "then ERA5 reanalyses for recent years. Observations are assimilated by means of a reduced-order "
    "Kalman filter. Along track altimeter data (Sea Level Anomaly), Satellite Sea Surface Temperature, "
    "Sea Ice Concentration and In situ Temperature and Salinity vertical Profiles are jointly assimilated. "
    "Moreover, a 3D-VAR scheme provides a correction for the slowly-evolving large-scale biases "
    "in temperature and salinity. "
    f"More information at {dataset_urls['analysis']['ocean_physics']} and {dataset_urls['reanalysis']['ocean_physics']}"
)

sea_level_description = (
    "Altimeter satellite gridded Sea Level Anomalies (SLA) computed with respect to a twenty-year 2012 mean. "
    "The sea level anomaly (SLA) is the current height of the sea (in meters) above the mean sea surface height. "
    "The SLA is estimated by Optimal Interpolation, merging the L3 along-track measurement from the different altimeter missions available. "
    "Part of the processing is fitted to the Global ocean. (see QUID document or 1 [http://duacs.cls.fr] pages for processing details)."
    "The product gives additional variables (i.e. Absolute Dynamic Topography and geostrophic currents (absolute and anomalies))."
    "It serves in delayed-time applications. This product is processed by the DUACS multimission altimeter data processing system. "
    f"More information at {dataset_urls['analysis']['sea_level']} and {dataset_urls['reanalysis']['sea_level']}"
)

salinity_description = (
    "The Operational Mercator global ocean analysis and forecast system at 1/12 degree includes daily mean "
    "files of temperature from the top to the bottom over the global ocean. "
    "The global ocean output files are displayed with a 1/12 degree horizontal "
    "resolution with regular longitude/latitude equirectangular projection. "
    "50 vertical levels are provided, ranging from 0 to 5500 meters. "
    "Data is updated on a 24 hour lag at 12:01 PM every day. "
    "Prior to January 1, 2020 data from the GLORYS12V1 reanalysis is provided. "
    "The GLORYS12V1 product is the CMEMS global ocean eddy-resolving (1/12° horizontal resolution, "
    "50 vertical levels) reanalysis covering the altimetry (1993 onward). "
    "It is based largely on the current real-time global forecasting CMEMS system. "
    "The model component is the NEMO platform driven at surface by ECMWF ERA-Interim "
    "then ERA5 reanalyses for recent years. Observations are assimilated by means of "
    "a reduced-order Kalman filter. Along track altimeter data (Sea Level Anomaly), "
    "Satellite Sea Surface Temperature, Sea Ice Concentration and In situ Temperature "
    "and Salinity vertical Profiles are jointly assimilated.  Moreover, a 3D-VAR scheme "
    "provides a correction for  the slowly-evolving large-scale biases in temperature and salinity. "
    "Note that salinity data values are returned in Practical Salinity Units (PSUs), "
    "which are explicitly discouraged within the scientific community. "
    "Specifying PSU under `unit_of_measurement` therefore breaks dClimate's API "
    "due to incongruencies with the supporting libraries for unit conversion. "
    "For this reason we leave the `unit_of_measurement` field blank, "
    "although the dataset values are in fact measured in PSUs."
    f"More information at {dataset_urls['analysis']['ocean_physics']} and {dataset_urls['reanalysis']['ocean_physics']}"
)