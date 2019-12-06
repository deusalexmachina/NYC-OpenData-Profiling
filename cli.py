import sys

from typing import Union

"""
functions to grab arguments from the cli
"""

def get_rand_arg(i=3) -> bool:
    """
    rand argument from cli to shuffle files before running (run in random order)
    """
    try:
        rand = bool(sys.argv[i])
    except (IndexError, ValueError):
        rand = False
    return rand

def get_limit_arg(i=2) -> Union[int, None]:
    """
    limit argument to limit the number of datasets run
    """
    try:
        limit = int(sys.argv[i])
    except (IndexError, ValueError):
        limit = None

    return limit


def get_ds_path_arg(i=1) -> Union[str, None]:
    """
    limit argument to limit the number of datasets run
    """
    try:
        path = str(sys.argv[i])
    except (IndexError, ValueError):
        path = None

    if path is not '-':
        return path
    else:
        return None
