"""
This file houses miscellaneous utilities used by many other scripts. Something should be very universal to be put in here. If only used by a couple of files, then stick to copy paste.
"""

import sys


def eprint(arg):
    """Print one argument to stderr"""
    print(arg, file=sys.stderr)
