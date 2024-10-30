# How it Works
To manage complexity, our ETL (Extract, Transform, Load) processes are divided into defined stages, standardizing our approach across all ETLs. Below are the primary components involved:

## The Assessor
The assessor is responsible for essential logic, such as fetching prior datasets and making decisions based on them. Its primary role is to determine whether the ETL should proceed automatically or await manual intervention. If it decides to run, it also defines the time span for processing.

## The Fetcher
The fetcher’s focus is solely on downloading data for the specified time span and converting it into a suitable format, such as NetCDF4.

## The Extractor
This stage converts the downloaded NetCDF4 data into a Zarr JSON format, preparing it for storage and subsequent processing.

## The Combiner
The combiner merges multiple files into a single Zarr JSON file, creating a unified dataset for further transformation.

## The Transformer
The transformer modifies the data to the required format, which could include normalizing latitude and longitude values, or adding or removing metadata as needed.

## The Uploader
The uploader handles the data upload to IPFS, with options to initialize, append, or replace data. Once the upload is complete, the uploader also performs a garbage cleanup to free up space.

These steps collectively reduce complexity and enhance debugging efficiency.

## Important Information
### Dask, Zarr, and IPFS Sizes
Dask sizes: Dask handles in-memory data processing and should ideally match or exceed the Zarr chunk size to avoid reloading data from storage, which impacts performance. If a dataset segment exceeds the Dask chunk size, you may encounter ValueError due to chunk overlap, especially during append operations. To prevent this, either append smaller-than-Dask chunks or increase the Dask chunk size to exceed the data segment being appended.

Zarr sizes: When selecting Zarr chunk sizes, consider data access patterns. Zarr data can only be accessed in minimum chunk units. If retrieving small geographical areas over long time periods, use smaller latitude/longitude chunks and larger time chunks. For larger spatial areas over short time frames, set time as a smaller chunk. Be mindful that the chosen Zarr chunk size is fixed post-initialization.

IPFS sizes: The IPFS storage works best with block sizes below 2MB, aligning roughly with Zarr chunks for optimal performance. Smaller blocks can increase retrieval time, so balancing IPFS block and Zarr chunk sizes is crucial.

Understanding IPFS
IPFS (InterPlanetary File System) is a decentralized storage and data transfer system that ensures data integrity through hashing. Each data chunk is hashed, and retrieval relies on these hashes. For larger files, IPFS splits data into 2MB chunks, which are then combined at a higher level, forming a hashed tree structure for seamless retrieval. This bottom-up hashing protects against data tampering, as altering any chunk would change the hash, invalidating the original reference. Storing Zarr data in 2MB chunks aligns well with IPFS’s architecture, ensuring manageable retrieval and security.

Zarr leverages an addressing system where a specific data segment, such as the 30th time chunk, 22nd latitude, and 34th longitude, is accessed via its address (e.g., 30.22.34). This address typically matches the corresponding IPFS hash, enabling efficient data access without loading unrelated chunks. Smaller chunks increase data refinement but also expand the size of the registry (mapping addresses to IPFS hashes). For instance, storing 10MB in 2MB chunks requires five registry entries, while 100KB chunks require 100 entries, each with a unique IPFS hash. This can become inefficient as chunk sizes decrease.

### HAMTs and Zarrs
For extremely large datasets in the petabyte or terabyte range, the Zarr registry can balloon to hundreds of megabytes, posing challenges for in-browser support or smaller dataset retrievals, as users must download significant registry data to locate just a few 2MB blocks.

To solve this, Hash Array Mapped Tries (HAMTs) are employed. HAMTs enable hierarchical organization of the Zarr registry, using hashed references to reduce the size of each node while preserving fast lookup capabilities. This structure effectively "compresses" the registry by storing hashes in a tree-like structure, allowing users to locate data with fewer registry lookups. As a result, even massive datasets remain navigable with a minimal metadata footprint, significantly enhancing efficiency for decentralized, large-scale data storage and retrieval.



# FAQ

WHY DOESN'T MY ZARR PARSE PROPERLY?!?!
- Try setting the time dimension to unlimited in the .nc file