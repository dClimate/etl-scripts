import asyncio
import xarray as xr
from py_hamt import ShardedZarrStore, KuboCAS
import numpy as np

# The NEW CID from the end of the 'extend' command
final_cid = "bafyr4ibswblof25eekiq5t6rod5iueyp6uuaapwzeil4sboikdq3k67qs4" # PASTE cid_B HERE

async def verify():
    async with KuboCAS() as cas:
        # Use a fresh store instance to open the final dataset
        final_store = await ShardedZarrStore.open(cas=cas, read_only=True, root_cid=final_cid)
        final_ds = xr.open_zarr(final_store, consolidated=True) # Use consolidated metadata for speed

        # Select a time in the middle of the dataset
        time_index = 1200
        sliced_ds = final_ds.isel(time=time_index)
        
        print(f"--- Verifying data at time index {time_index} ---")
        
        # To access a variable starting with a number, use dictionary-style access ['...']
        # The .load() command tells xarray to compute the values and bring them into memory.
        temperature_values = sliced_ds['2m_temperature'].load().values

        # Starting at 1200, loop until 2400 and stop when values different than 0 are found
        for i in range(1200, 2400):
            print(f"Checking time index {i}...")
            values = final_ds.isel(time=i)['2m_temperature'].values
            if np.any(values != 0):
                print(f"Found non-zero value at time index {i}: {values}")
                break
        
        print(f"Values for 2m_temperature at this time index:")
        print(temperature_values)

        print("\n--- Sliced Dataset Metadata ---")
        print(sliced_ds)

# Run the verification
asyncio.run(verify())