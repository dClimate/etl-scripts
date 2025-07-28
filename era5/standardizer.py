# standardizer.py
import warnings
import numpy as np
import xarray as xr
import zarr.codecs

from etl_scripts.grabbag import eprint

from era5.utils import chunking_settings, time_chunk_size

def standardize(dataset: str, ds: xr.Dataset) -> xr.Dataset:
    # ERA5 normally spits things out with the short form name, use the long form name to make it more readable
    match dataset:
        case "2m_temperature":
            ds = ds.rename({"t2m": dataset})
        case "10m_u_component_of_wind":
            ds = ds.rename({"u10": dataset})
        case "10m_v_component_of_wind":
            ds = ds.rename({"v10": dataset})
        case "100m_u_component_of_wind":
            ds = ds.rename({"u100": dataset})
        case "100m_v_component_of_wind":
            ds = ds.rename({"v100": dataset})
        case "surface_pressure":
            ds = ds.rename({"sp": dataset})
        case "surface_solar_radiation_downwards":
            ds = ds.rename({"ssrd": dataset})
        case "total_precipitation":
            ds = ds.rename({"tp": dataset})
        case _:
            raise ValueError(f"Invalid dataset value {dataset}")

    if "time" in ds.sizes and "step" in ds.sizes:
        ds_stack = ds.stack(throwaway=("time", "step"))
        ds_linear = ds_stack.swap_dims({"throwaway": "valid_time"})
        ds = ds_linear.drop_vars(["throwaway"])
        ds = ds.groupby("valid_time").first(skipna=True)


    # for loop to try and drop number, step, surface, time
    for var in ["number", "step", "surface", "time"]:
        if var in ds:
            ds = ds.drop_vars(var, errors="ignore")
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="rename 'valid_time' to 'time'")
        # UserWarning: rename 'valid_time' to 'time' does not create an index anymore. Try using swap_dims instead or use set_index after rename to create an indexed coordinate.
        # Surpess this warning since we are renaming valid_time to time, which is the standard name in dClimate.
        # We properply set the index after renaming.
        ds = ds.rename({"valid_time": "time"})
        try:
            ds = ds.set_xindex("time")
        except ValueError:
            eprint("Time Index already exists, not setting it again.")

    # ERA5 GRIB files have longitude from 0 to 360, but dClimate standardizes from -180 to 180
    new_longitude = np.where(ds.longitude > 180, ds.longitude - 360, ds.longitude)
    ds = ds.assign_coords(longitude=new_longitude)

    ds = ds.sortby("latitude", ascending=True)
    del ds.latitude.attrs["stored_direction"]  # normally says descending
    ds = ds.sortby("longitude", ascending=True)

    for param in list(ds.attrs.keys()):
        del ds.attrs[param]

    for var in ds.data_vars:
        da = ds[var]

        # Apply compression
        da.encoding["compressors"] = zarr.codecs.BloscCodec()

        for param in list(da.attrs.keys()):
            if param.startswith("GRIB"):
                del da.attrs[param]

        # fill value should be float NaN
        da.encoding["_FillValue"] = np.nan

    # if list(ds.sizes) != ["time", "latitude", "longitude"]:
    ds = ds.transpose('time', 'latitude', 'longitude')

    return ds