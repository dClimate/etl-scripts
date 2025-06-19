import asyncio
import xarray as xr
from py_hamt import ShardedZarrStore, KuboCAS
import numpy as np
from etl_scripts.grabbag import eprint

# The NEW CID from the end of the 'extend' command
final_cid = "bafyr4igd7j2bl47liq75jxkha7bjj7kcqbzre2xt4hx34wujh65l2rkwre" # PASTE cid_B HERE

chunking_settings = {"time": 400, "latitude": 25, "longitude": 25}


async def verify():
    async with KuboCAS() as cas:
        # Use a fresh store instance to open the final dataset
        final_store = await ShardedZarrStore.open(cas=cas, read_only=False, root_cid=final_cid)
        final_ds = xr.open_zarr(final_store) # Use consolidated metadata for speed

        print(final_ds)

        # read the data at time index 749041
        time_index = 749040
        sliced_ds = final_ds.isel(time=time_index)
        print(f"--- Verifying data at time index {time_index} ---")
        # To access a variable starting with a number, use dictionary-style access ['...']
        # The .load() command tells xarray to compute the values and bring them into memory.
        temperature_values = sliced_ds['2m_temperature'].load().values
        print(f"Values for 2m_temperature at time index {time_index}: {temperature_values}")

        # Create a test dataset to append (mimicking the structure of final_ds)
        eprint("Creating test dataset for append...")
        # Example: Create a synthetic dataset with the same variables and structure
        time_coords = np.arange(
            final_ds.time[-1].values + np.timedelta64(1, 'h'),
            final_ds.time[-1].values + np.timedelta64(10, 'h'),  # Append 10 hours
            np.timedelta64(1, 'h'),
            dtype="datetime64[ns]"
        )
        test_data = xr.Dataset(
            {
                "2m_temperature": (["time", "latitude", "longitude"], 
                    np.random.uniform(
                        low=-30,
                        high=40,
                        size=(len(time_coords), final_ds.latitude.size, final_ds.longitude.size)
                    )
                    # np.zeros((len(time_coords), final_ds.latitude.size, final_ds.longitude.size))
                )
            },
            coords={
                "time": time_coords,
                "latitude": final_ds.latitude,
                "longitude": final_ds.longitude
            }
        )
        # Apply chunking to match final_ds
        test_data = test_data.chunk(chunking_settings)


        # Set encoding for the data variable
        encoding_chunks = tuple(chunking_settings.get(dim) for dim in test_data["2m_temperature"].dims)
        test_data["2m_temperature"].encoding['chunks'] = encoding_chunks

        for coord_name, coord_array in test_data.coords.items():
            if coord_name in chunking_settings:
                test_data[coord_name].encoding['chunks'] = (chunking_settings[coord_name],)

        ordered_dims = list(test_data.dims)
        array_shape = tuple(test_data.dims[dim] for dim in ordered_dims)
        chunk_shape = tuple(test_data.chunks[dim][0] for dim in ordered_dims)

        # Reorder to be time, latitude, longitude
        if ordered_dims != ["time", "latitude", "longitude"]:
            ordered_dims = ["time", "latitude", "longitude"]
            test_data = test_data.transpose(*ordered_dims)
            array_shape = tuple(test_data.dims[dim] for dim in ordered_dims)
            chunk_shape = tuple(test_data.chunks[dim][0] for dim in ordered_dims)

        eprint(f"Test dataset: {test_data}")
        print("APPENDING DATASET")
        test_data.to_zarr(store=final_store, append_dim="time")

        updated_ds = xr.open_zarr(final_store)
        eprint(f"Updated dataset after append: {updated_ds}")

        # Flush the store to ensure all changes are written
        root_cid = await final_store.flush()
        eprint(f"Final CID after append: {root_cid}")

        # END HERE

# Run the verification
asyncio.run(verify())