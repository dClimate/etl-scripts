
import datetime
import pandas as pd

import numpy as np



def numpydate_to_py(numpy_date: np.datetime64) -> datetime.datetime:
    """
    Convert a numpy datetime object to a python standard library datetime object

    Parameters
    ----------
    np.datetime64
        A numpy.datetime64 object to be converted

    Returns
    -------
    datetime.datetime
        A datetime.datetime object

    """
    return pd.Timestamp(numpy_date).to_pydatetime()