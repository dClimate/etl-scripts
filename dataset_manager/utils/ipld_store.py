# The annotations dict and TYPE_CHECKING var are necessary for referencing types that aren't fully imported yet. See
# https://peps.python.org/pep-0563/
from __future__ import annotations

import xarray as xr
import ipldstore
import collections

from abc import abstractmethod, ABC


class StoreInterface(ABC):
    """
    Base class for an interface that can be used to access a dataset's Zarr.

    Zarrs can be stored in different types of data stores, for example IPLD, S3, and the local filesystem, each of
    which is accessed slightly differently in Python. This class abstracts the access to the underlying data store by
    providing functions that access the Zarr on the store in a uniform way, regardless of which is being used.
    """

    def __init__(self):
        """
        Create a new `StoreInterface`. Pass the dataset manager this store is being associated with, so the interface
        will have access to dataset properties.

        Parameters
        ----------
        dm : dataset_manager.DatasetManager
            The dataset to be read or written.
        """

    @abstractmethod
    def mapper(self, **kwargs) -> collections.abc.MutableMapping:
        """
        Parameters
        ----------
        **kwargs : dict
            Implementation specific keywords. TODO: standardize interface across implementations

        Returns
        -------
        collections.abc.MutableMapping
            A key/value mapping of files to contents
        """

    @property
    @abstractmethod
    def has_existing(self) -> bool:
        """
        Returns
        -------
        bool
            Return `True` if there is existing data for this dataset on the store.
        """

    @abstractmethod
    def metadata_exists(self, title: str, stac_type: str) -> bool:
        """
        Check whether metadata exists at a given local path

        Parameters
        ----------
        title : str
            STAC Entity title
        stac_type : StacType
            Path part corresponding to type of STAC entity
            (empty string for Catalog, 'collections' for Collection or 'datasets' for Item)

        Returns
        -------
        bool
            Whether metadata exists at path
        """

    @abstractmethod
    def push_metadata(self, title: str, stac_content: dict, stac_type: str):
        """
        Publish metadata entity to s3 store. Tracks historical state
        of metadata as well

        Parameters
        ----------
        title : str
            STAC Entity title
        stac_content : dict
            content of the stac entity
        stac_type : StacType
            Path part corresponding to type of STAC entity
            (empty string for Catalog, 'collections' for Collection or 'datasets' for Item)
        """

    @abstractmethod
    def retrieve_metadata(self, title: str, stac_type: str) -> tuple[dict, str]:
        """
        Retrieve metadata entity from local store

        Parameters
        ----------
        title : str
            STAC Entity title
        stac_type : StacType
            Path part corresponding to type of STAC entity
            (empty string for Catalog, 'collections' for Collection or 'datasets' for Item)

        Returns
        -------
        dict
            Tuple of content of stac entity as dict and the local path as a string
        """

    @abstractmethod
    def get_metadata_path(self, title: str, stac_type: str) -> str:
        """
        Get the s3 path for a given STAC title and type

        Parameters
        ----------
        title : str
            STAC Entity title
        stac_type : StacType
            Path part corresponding to type of STAC entity
            (empty string for Catalog, 'collections' for Collection or 'datasets' for Item)

        Returns
        -------
        str
            The s3 path for this entity as a string
        """

    @property
    def path(self) -> str:
        """
        Get the S3-protocol URL to the parent `DatasetManager`'s Zarr .

        Returns
        -------
        str
            A URL string starting with "s3://" followed by the path to the Zarr.
        """

    def dataset(self, **kwargs) -> xr.Dataset | None:
        """
        Parameters
        ----------
        **kwargs
            Implementation specific keyword arguments to forward to `StoreInterface.mapper`. S3 and Local accept
            `refresh`, and IPLD accepts `set_root`.

        Returns
        -------
        xr.Dataset | None
            The dataset opened in xarray or None if there is no dataset currently stored.
        """
        if self.has_existing:
            return xr.open_zarr(self.mapper(**kwargs))
        else:
            return None

    @abstractmethod
    def write_metadata_only(self, attributes: dict):
        """
        Writes the metadata to the stored Zarr. Not available to datasets on the IPLD store. Use DatasetManager.to_zarr
        instead.

        Open the Zarr's `.zmetadata` and `.zattr` files with the JSON library, update the values with the values in the
        given dict, and write the files.

        These changes will be reflected in the attributes dict of subsequent calls to `DatasetManager.store.dataset`
        without needing to call `DatasetManager.to_zarr`.

        Parameters
        ----------
        attributes
            A dict of metadata attributes to add or update to the Zarr

        Raises
        ------
        NotImplementedError
            If the store is IPLD
        """

class IPLDStore(StoreInterface):
    """
    Provides an interface for reading and writing a dataset's Zarr on IPLD.

    If there is existing data for the dataset, it is assumed to be stored at the hash returned by
    `IPLD.dm.latest_hash`, and the mapper will return a hash that can be used to retrieve the data. If there is no
    existing data, or the mapper is called without `set_root`, an unrooted IPFS mapper will be returned that can be
    used to write new data to IPFS and generate a new recursive hash.
    """

    def mapper(self, set_root: bool = True, refresh: bool = False) -> ipldstore.IPLDStore:
        """
        Get an IPLD mapper by delegating to `ipldstore.get_ipfs_mapper`, passing along an IPFS chunker value if the
        associated dataset's `requested_ipfs_chunker` property has been set.

        If `set_root` is `False`, the root will not be set to the latest hash, so the mapper can be used to open a new
        Zarr on the IPLD datastore. Otherwise, `DatasetManager.latest_hash` will be used to get the latest hash (which
        is stored in the STAC at the IPNS key for the dataset).

        Parameters
        ----------
        set_root : bool
            Return a mapper rooted at the dataset's latest hash if `True`, otherwise return a new mapper.
        refresh : bool
            Force getting a new mapper by checking the latest IPNS hash. Without this set, the mapper will only be set
            the first time this function is called.

        Returns
        -------
        ipldstore.IPLDStore
            An IPLD `MutableMapping`, usable, for example, to open a Zarr with `xr.open_zarr`
        """
        print(self.requested_ipfs_chunker)
        if refresh or not hasattr(self, "_mapper"):
            if self.requested_ipfs_chunker:
                self._mapper = ipldstore.get_ipfs_mapper(host=self._host, chunker=self.requested_ipfs_chunker)
            else:
                self._mapper = ipldstore.get_ipfs_mapper(host=self._host)
            self.info(f"IPFS chunker is {self._mapper._store._chunker}")
            if set_root and self.latest_hash():
                self._mapper.set_root(self.latest_hash())
        return self._mapper

    def __str__(self) -> str:
        """
        Returns
        -------
        str
            The path as "/ipfs/[hash]". If the hash has not been determined, return "/ipfs/"
        """
        # TODO: Is anything relying on this? It's not super intuitive behavior. If this is for debugging in a REPL, it
        # is more common to implement __repr__ which generally returns a string that could be code to instantiate the
        # instance.
        if not self.latest_hash():
            return "/ipfs/"
        else:
            return f"/ipfs/{self.latest_hash()}"

    @classmethod
    def info(cls, message: str, **kwargs):
        """
        Log a message at `logging.INFO` level.

        Parameters
        ----------
        message : str
            Text to write to log.
        **kwargs : dict
            Keywords arguments passed to `logging.Logger.log`.

        """
        print(message)
        # cls.log(message, logging.INFO, **kwargs)

    @property
    def path(self) -> str | None:
        """
        Get the IPFS-protocol hash pointing to the latest version of the parent `DatasetManager`'s Zarr,
        or return None if there is none.

        Returns
        -------
        str | None
            A URL string starting with "ipfs" followed by the hash of the latest Zarr, if it exists.
        """
        if not self.latest_hash():
            return None
        else:
            return f"/ipfs/{self.latest_hash()}"

    @property
    def has_existing(self) -> bool:
        """
        Returns
        -------
        bool
            Return `True` if the dataset has a latest hash, or `False` otherwise.
        """
        return bool(self.latest_hash())

    def write_metadata_only(self, attributes: dict):
        raise NotImplementedError("Can't write metadata-only on the IPLD store. Use DatasetManager.to_zarr instead.")

    def metadata_exists(self, title: str, stac_type: str) -> bool:
        raise NotImplementedError

    def push_metadata(self, title: str, stac_content: dict, stac_type: str):
        raise NotImplementedError

    def retrieve_metadata(self, title: str, stac_type: str) -> tuple[dict, str]:
        raise NotImplementedError

    def get_metadata_path(self, title: str, stac_type: str) -> str:
        raise NotImplementedError
