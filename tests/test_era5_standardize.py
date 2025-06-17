import importlib
import json
import sys
import types
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

# Helper to import era5.standardize with stub dependencies

def load_era5(monkeypatch):
    # Create placeholder env file in repo root
    env_path = Path(__file__).resolve().parents[1] / "era5-env.json"
    env_path.write_text(
        json.dumps(
            {
                "ENDPOINT_URL": "https://example.com",
                "AWS_ACCESS_KEY_ID": "key",
                "AWS_SECRET_ACCESS_KEY": "secret",
                "BUCKET_NAME": "bucket",
            }
        )
    )
    # Stub py_hamt and boto3 before import
    mock_py_hamt = types.ModuleType("py_hamt")
    mock_py_hamt.HAMT = type("HAMT", (), {})
    mock_py_hamt.IPFSStore = type("IPFSStore", (), {})
    mock_py_hamt.IPFSZarr3 = type("IPFSZarr3", (), {})
    monkeypatch.setitem(sys.modules, "py_hamt", mock_py_hamt)

    class DummyClient:
        def __getattr__(self, name):
            def method(*args, **kwargs):
                pass
            return method

    mock_boto3 = types.ModuleType("boto3")
    mock_boto3.client = lambda *args, **kwargs: DummyClient()
    monkeypatch.setitem(sys.modules, "boto3", mock_boto3)

    import era5
    importlib.reload(era5)
    return era5, env_path


def make_ds(short_var):
    data = np.zeros((1, 1, 1), dtype=float)
    ds = xr.Dataset(
        {short_var: (("valid_time", "latitude", "longitude"), data)},
        coords={
            "valid_time": ("valid_time", [np.datetime64("2020-01-01")]),
            "time": ("valid_time", [np.datetime64("2020-01-01")]),
            "step": ("valid_time", [0]),
            "number": ("valid_time", [0]),
            "surface": ("valid_time", [0]),
            "latitude": ("latitude", [0.0]),
            "longitude": ("longitude", [190.0]),
        },
    )
    ds.latitude.attrs["stored_direction"] = "descending"
    ds = ds.reset_index("valid_time")
    return ds


datasets = {
    "2m_temperature": "t2m",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "100m_u_component_of_wind": "v100",
    "100m_v_component_of_wind": "v100",
    "surface_pressure": "sp",
    "surface_solar_radiation_downwards": "ssrd",
    "total_precipitation": "tp",
}


@pytest.mark.parametrize("dataset,short", datasets.items())
def test_standardize_renaming(monkeypatch, dataset, short):
    era5, env_path = load_era5(monkeypatch)
    ds = make_ds(short)
    out = era5.standardize(dataset, ds)

    assert dataset in out.data_vars
    assert short not in out.data_vars
    assert float(out.longitude.values[0]) == -170.0
    assert "valid_time" not in out.coords
    assert "time" in out.coords

    env_path.unlink()
