# Datasets
+ `precip-conus` Daily precipitation data for the continental United States. https://psl.noaa.gov/mddb2/showDataset.html?datasetID=45
+ `precip-global` Daily precipitation data for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html
+ `tmax` Daily maximum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html
+ `tmin` Daily minimum temperature for Earth. https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html

# ETL Diagram
The following is a an example of how the ETL for precip-conus runs. ETLs for the other 3 datasets run very similarly, so you can substitute `precip-conus/` in the diagram for one of the other datasets.

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
zarr_to_ipld[zarr_to_ipld.py]
end

subgraph precip-conus/ directory
dl_txt["available-downloads.txt"]
nc_files[2007.nc 2008.nc ...]
zarr[precip-conus.zarr]
cid[precip-conus.zarr.cid]
ipns_txt[ipns-key-name.txt]
end

subgraph Prefetch
prefetch_description[[This step generates download links and writes them to a file in the dataset's directory.]]
prefetch[bash prefetch.sh precip-conus] --> dl_txt
end

subgraph Fetch
fetch_description[[This step downloads the netCDF .nc files. This step is idempotent, and won't redownload files if the files haven't been updated on the remote server.]]
fetch[bash fetch.sh precip-conus]
dl_txt --> fetch
fetch --> nc_files
end

subgraph Transform
transform_description[[This step combines the data from all the .nc files and writes it to a Zarr on disk. This step is also idempotent, and will not act if the .nc files have not changed.]]
transform[bash transform.sh precip-conus]
nc_files --> transform --> transform_nc --> zarr
end

subgraph Load to IPLD
load_to_ipld_description[[This step puts the zarr onto IPFS using ipldstore. Once done, it writes the CID of the HAMT root to the dataset directory, and pins it to the cluster.]]
load_to_ipld[bash load_to_ipld.sh precip-conus]
zarr --> load_to_ipld -->zarr_to_ipld --> cid
load_to_ipld -->|pin cid| cluster
end

subgraph Update IPNS
update_ipns_description[[This step pins publishes the CID to the IPNS record for the specified dataset. That way, consumers of this data only need to store and resolve IPNS records instead of keeping track of changing CIDs.]]
update_ipns[bash update_ipns.sh precip-conus]
ipns_txt --> update_ipns -->|publish cid to ipns| ipfs
cid --> update_ipns
end
```

This can all be run automatically by calling `bash pipeline.sh <dataset>`.
