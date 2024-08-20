from multiformats import CID

from dc_etl.filespec import FileSpec
from dc_etl.ipld.loader import IPLDPublisher

# This will probably need to be modified to support custom cid publishing

class LocalFileIPLDPublisher(IPLDPublisher):
    """Publishes CID of dataset to a local file."""

    def __init__(self, path: FileSpec):
        self.path = path

    def publish(self, cid: CID):
        """Implementation of :meth:`IPLDPublisher.publish"""
        
        with self.path.open("w") as f:
            print(cid, file=f)

    def retrieve(self) -> CID:
        """Implementation of :meth:`IPLDPublisher.retrieve"""
        if self.path.exists():
            return CID.decode(self.path.open("r").read().strip())
