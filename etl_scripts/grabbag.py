"""
This file houses miscellaneous utilities used by many other scripts. Something should be very universal to be put in here. If only used by a couple of files, then stick to copy paste.
"""

import sys
from datetime import datetime

import numpy as np
import pandas as pd


def eprint(arg):
    """Print one argument to stderr"""
    print(arg, file=sys.stderr)

def npdt_to_pydt(dt64: np.datetime64) -> datetime:
    return pd.Timestamp(dt64).to_pydatetime() # type: ignore
