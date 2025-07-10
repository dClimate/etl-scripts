from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Tuple

from multiformats import CID
from py_hamt import HAMT, KuboCAS, ZarrHAMTStore


@asynccontextmanager
async def ipfs_hamt_store(
    gateway_base_url: Optional[str] = None,
    rpc_base_url: Optional[str] = None,
    root_cid: CID | None = None,
    read_only: bool = False,
) -> AsyncIterator[Tuple[ZarrHAMTStore, HAMT]]:
    """
    Yield a writable Zarr store backed by a HAMT persisted on IPFS/Kubo.

    Parameters
    ----------
    gateway_base_url
        Optional HTTP(S) gateway to consult on *reads* (e.g.
        ``"https://dweb.link/ipfs/"``).
    rpc_base_url
        Optional Kubo RPC (API) root used for *writes*
        (e.g. ``"http://127.0.0.1:5001/api/v0"``).
    root_cid
        Optional CID of an existing HAMT root node to use as the starting point.
        If not given, a new HAMT is created.
    read_only
        If ``True``, the store is opened in read-only mode, meaning no new data
        can be written to it. This is useful for inspecting existing datasets.

    Yields
    ------
    (ZarrHAMTStore, HAMT)
        * `ZarrHAMTStore` – hand this to ``xarray.Dataset.to_zarr`` or
          ``zarr.save``.
        * `HAMT` – lets you inspect the `.root_node_id` (CID) later.
    """
    async with KuboCAS(
        gateway_base_url=gateway_base_url,
        rpc_base_url=rpc_base_url,
    ) as kubo_cas:
        hamt = await HAMT.build(cas=kubo_cas, values_are_bytes=True)

        if root_cid:
            hamt.root_node_id = root_cid

        if read_only:
            await hamt.make_read_only()

        try:
            yield ZarrHAMTStore(hamt, read_only=read_only), hamt
        finally:
            if not read_only:
                # Seal the DAG so it becomes immutable / gateway-friendly.
                await hamt.make_read_only()
