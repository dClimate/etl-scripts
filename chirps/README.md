CHIRPS provides land precipitation estimates. For a more detailed overview, see https://data.chc.ucsb.edu/products/CHIRPS-2.0/README-CHIRPS.txt

# Datasets
The "p05"/"p25" suffix refers to the precision/width of the grid cells. Based on the fact that https://www.chc.ucsb.edu/data states "CHIRPS incorporates 0.05° resolution satellite imagery", we assume that the p05 and p25 directory at https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/ means a precision of 0.05 degrees and 0.25 degrees.

Final refers to finalized data, and prelim refers to data that may be changed by CHIRPS, but is closer to realtime values.

+ `final-p05/` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/
+ `final-p25/` https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25/
+ `prelim-p05` https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/netcdf/p05/

# ETL Overview
The ETL for CHIRPS is very similar to CPC, except for the fact that due to final-p05's especially large sizes, zarr transformation step requires using dask to put a cap on RAM usage. However, this is automatically done by `transform_nc.py` for all Zarrs it is called on, so this ends up looking identical to the ETL for CPC. You can see the ETL diagram in CPC for a visualization of the ETL, but in action they both look identical.
