<p align="center">
<a href="https://dclimate.net/" target="_blank" rel="noopener noreferrer">
<img width="50%" src="https://user-images.githubusercontent.com/41392423/173133333-79ef15d0-6671-4be3-ac97-457344e9e958.svg" alt="dClimate logo">
</a>
</p>

This repository contains ETLs of publicly available datasets to IPFS.

# Dataset Monitoring Dashboard
## Dataset Availability
[![cpc-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-availablity-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-availablity-check.yaml)

[![chirps-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-availability-check.yaml)

[![era5-availability-check](https://github.com/dClimate/etl-scripts/actions/workflows/era5-availability-check.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/era5-availability-check.yaml)

[![era5-check-identical-to-source](https://github.com/dClimate/etl-scripts/actions/workflows/era5-check-identical-to-source.yaml/badge.svg?branch=main)](https://github.com/dClimate/etl-scripts/actions/workflows/era5-check-identical-to-source.yaml)

## Datasets are up to date
[![cpc-check-uptodate](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-check-uptodate.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/cpc-check-uptodate.yaml)

[![chirps-check-uptodate](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-check-uptodate.yaml/badge.svg)](https://github.com/dClimate/etl-scripts/actions/workflows/chirps-check-uptodate.yaml)

# Dataset Standardization Practices
+ zarr chunking is set so that each chunk is around 1 megabyte
+ zarr chunks are wide on time, short on latitude and longitude
+ dataset latitude and longitude coordinates are sorted in ascending order
+ longitude is -180 to 180, latitude is -90 to 90

# Datasets

## CPC
+ `precip-conus` Daily precipitation data for the continental United States. https://psl.noaa.gov/mddb2/showDataset.html?datasetID=45
+ `precip-global` Daily precipitation data for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html
+ `tmax` Daily maximum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html
+ `tmin` Daily minimum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html

## CHIRPS
CHIRPS provides land precipitation estimates. For a more detailed overview, see https://data.chc.ucsb.edu/products/CHIRPS-2.0/README-CHIRPS.txt

The "p05"/"p25" suffix refers to the precision/width of the grid cells. Based on the fact that https://www.chc.ucsb.edu/data states "CHIRPS incorporates 0.05° resolution satellite imagery", we assume that the p05 and p25 directory at https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/ means a precision of 0.05 degrees and 0.25 degrees.

Final refers to finalized data, and prelim refers to data that may be changed by CHIRPS, but is closer to realtime values.

+ `final-p05/` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/
+ `final-p25/` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25/
+ `prelim-p05` https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/netcdf/p05/
