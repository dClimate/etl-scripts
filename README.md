This repository contains scripts that compute and deploy ETLs for dClimate.

For:
+ **Dev environment setup, Writing ETLs** See README-dev.md
  + **Provider and dataset specific information** See the README.md inside that provider's directory (e.g. `cpc/README.md`)
+ **Running ETLs and deploying to IPFS** See README-ops.md
+ **Ensuring data access reliaibility and accuracy** See README-monitoring.md

Almost all scripts are **idempotent**. This means rerunning script and it will only change things if any of the underlying data has changed.
Necessary exceptions have been made however, and are noted where possible. Idempotency can be assumed for all the rest.

The rest of this document is meant to contain information about all the files and folders in this project and their purpose.

# Project Organization Overview
## Data Folders
These are the datasets we concerned with, under their larger overall groupings.
```
AGB
agb-quarterly

CHIRPS
chirps_final_05-daily
chirps_final_25-daily
chirps_prelim_05-daily

Copernicus
copernicus_ocean_salinity_0p5_meters-daily
copernicus_ocean_salinity_109_meters-daily
copernicus_ocean_salinity_1p5_meters-daily
copernicus_ocean_salinity_25_meters-daily
copernicus_ocean_salinity_2p6_meters-daily
copernicus_ocean_sea_level-daily
copernicus_ocean_temp_1p5_meters-daily
copernicus_ocean_temp_6p5_meters-daily

CPC
cpc_precip_global-daily
cpc_precip_us-daily
cpc_temp_max-daily
cpc_temp_min-daily

Deforestation
deforestation-quarterly

ERA5
era5_2m_temp-hourly
era5_land_precip-hourly
era5_land_surface_pressure-hourly
era5_precip-hourly
era5_surface_solar_radiation_downwards-hourly
era5_wind_100m_u-hourly
era5_wind_100m_v-hourly
era5_wind_10m_u-hourly
era5_wind_10m_v-hourly

PRISM
prism-precip-daily
prism-tmax-daily
prism-tmin-daily

VHI
vhi-weekly
```
You can see that there is a directory for each of these larger groupings, and every dataset has its own directory under those groupings. CPC is a good example to verify this.

Under each of those directories, there are also various files that correspond to the ETL operations.
