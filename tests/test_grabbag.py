from datetime import datetime

import numpy as np

from etl_scripts.grabbag import eprint, npdt_to_pydt


def test_eprint(capsys):
    eprint("hello")
    captured = capsys.readouterr()
    assert captured.err.strip() == "hello"


def test_npdt_to_pydt():
    dt64 = np.datetime64("2021-01-02T03:04:05")
    result = npdt_to_pydt(dt64)
    assert result == datetime(2021, 1, 2, 3, 4, 5)
