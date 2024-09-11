PRISM provides
For more info, always see the PRISM website: https://prism.oregonstate.edu/

# Datasets
In terms of grid precision, PRISM creates both 800 meter and 4 km versions. Only the 4 km versions are free however, so we ETL those. Hence also the 4km suffix at the end of the dataset directory names.

For more about getting higher resolution datasets see: https://prism.oregonstate.edu/orders/

As for the climate variables, we ETL precipitation `precip`, maximum temperature `tmax`, and minimum temperature `tmin`. We are only concerned with the daily versions of all of those. These all only cover the continental US.

+ `precip-4km`
+ `tmax-4km`
+ `tmin-4km`

# ETL Diagram
This is an overview of PRISM's ETL, using precip-4km as an example. The transform step is different than CPC and CHIRPS since PRISM provides a .nc file per day, with no time coordinate attached that we must create ourselves in xarray. Thus, there is a `transform_prism.py` file that imports a lot of functionality from `transform_nc.py`.

> [!NOTE]
> GitHub's mermaid diagram viewer elides nodes with long text, so will need to cross reference the mermaid source code in the markdown to see the descriptions for each step.

```mermaid
graph LR

subgraph Machine Processes
ipfs[IPFS Daemon]
cluster[IPFS Cluster Daemon]
end

subgraph shared_python_scripts
transform_nc[transform_nc.py]
transform_prism[transform_prism.py]
zarr_to_ipld[zarr_to_ipld.py]

transform_nc -->|imports functions from| transform_prism
end

subgraph precip-4km/ directory
available_for_download[available-for-download.txt]
daily_nc_files[1981-01-01_8.nc 1981-01-02_8.nc ... 1982-01-01_8.nc 1982-01-02_8.nc ...]
yearly_nc_files[1981.nc 1982.nc ...]
zarr[precip-4km.zarr]
cid[precip-4km.zarr.cid]
ipns_txt[ipns-key-name.txt]
end

subgraph PRISM Web Service
prism_range((PRISM data range query service))
prism_nc((PRISM netCDF converter service))
end


subgraph Prefetch
prefetch_description[[This step queries PRISM's web service to find all available daily data files, and then writes that list of downloadable files.]]
prefetch[bash prefetch.sh precip-4km] --> prism_range --> available_for_download
end

subgraph Fetch
fetch_description[[This step downloads the netCDF .nc files form PRISM's conversion step. This step is idempotent, and will only download files that are either missing or have updateed grid counts.]]
fetch[bash fetch.sh precip-4km]
available_for_download --> fetch --> prism_nc --> daily_nc_files
end

subgraph Transform
transform_description[[This step first combines data from each year, while adding a time dimension. e.g. 1981-01-01_8.nc to 1981-12-31_8.nc are all merged into 1981.nc. This step then combines the yearly files into the Zarr, and writes that to disk. Because of this, this functionality lives in a separate python file that imports shared logic from transform_nc.py. This step is also idempotent, and will not act if the .nc files have not changed.]]
transform[bash transform.sh precip-4km]
daily_nc_files --> transform --> transform_prism --> yearly_nc_files --> transform_prism --> zarr
end

subgraph Load to IPLD
load_to_ipld_description[[This step puts the zarr onto IPFS using ipldstore. Once done, it writes the CID of the HAMT root to the dataset directory, and pins it to the cluster.]]
load_to_ipld[bash load_to_ipld.sh precip-conus]
zarr --> load_to_ipld --> zarr_to_ipld --> cid
load_to_ipld -->|pin cid| cluster
end

subgraph Update IPNS
update_ipns_description[[This step pins publishes the CID to the IPNS record for the specified dataset. That way, consumers of this data only need to store and resolve IPNS records instead of keeping track of changing CIDs.]]
update_ipns[bash update_ipns.sh precip-conus]
ipns_txt --> update_ipns -->|publish cid to ipns| ipfs
cid --> update_ipns
end
```

# bil vs nc, and FTP vs HTTPS
PRISM provides both FTP and HTTPS services for retrieving their data.

The FTP service is unencrypted and provides data in the `bil` format.  You can browse the FTP directories using your browser here: https://ftp.prism.oregonstate.edu/

The HTTPS service allows for downloading in both bil and netCDF. This is why we chose the HTTPS service, as it removes a layer of complexity of converting from the bil format, and allows for encrypted file transfer, as opposed to the unencrypted FTP.
You can see more information about the HTTPS service here: https://prism.oregonstate.edu/documents/PRISM_downloads_web_service.pdf

## PRISM Grid Count
Files downloaded for a dataset are named `YYYY-MM-DD_N.nc`. The YYYY-MM-DD specifies the date the data corresponds to, N is PRISM's grid count. Grid count ranges from 1-8, with a 1 specifying completely new and 8 representing finalized. PRISM assigns a higher numbers when data has been more finalized.

See more about the grid count at https://prism.oregonstate.edu/documents/PRISM_update_schedule.pdf

## Data Bootstrapping
Doing a full download of the PRISM data takes a long time. Their documentation recommends at least a 2 second lapse in between web requests to not overload their servers. Thus, if you are downloading all data for even 1 variable, it will take approximately 13.4 hours. (If each download takes 1 second and you wait 2 seconds in between files).

To solve this issue, we periodically download and store a copy of netCDF files on IPFS. This is what `update-bootstrap.sh` does.

To pull down all the data files from the last saved bootstrap, we run `download-from-bootstrap.sh`. The `fetch.sh` script can then download only the files that have changed, resulting in a much shorter overall time to setup the ETLs.
