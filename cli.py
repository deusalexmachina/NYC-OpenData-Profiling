import sys

from typing import Union

"""
functions to grab arguments from the cli
"""


### RULES ON GETTING ARGS ###


def get_bool_arg(i: int) -> bool:
    try:
        arg = sys.argv[i]
    except Exception:
        arg = False
    if arg == '-' or arg == 'False':
        arg = False
    else:
        try:
            arg = bool(arg)
        except (IndexError, ValueError):
            arg = False
    return arg


def get_int_arg(i: int) -> Union[int, None]:
    try:
        arg = int(sys.argv[i])
    except (IndexError, ValueError):
        arg = None

    return arg


def get_str_arg(i: int) -> Union[str, None]:
    try:
        s = str(sys.argv[i])
    except (IndexError, ValueError):
        s = None

    if s is '-':
        return None
    else:
        return s


### TYPICAL ARGS ###


def get_sort_arg(i=4) -> bool:
    """
    rand argument from cli to shuffle files before running (run in random order)
    """
    return get_bool_arg(i)


def get_rand_arg(i=3) -> bool:
    """
    rand argument from cli to shuffle files before running (run in random order)
    """
    return get_bool_arg(i)


def get_limit_arg(i=2) -> Union[int, None]:
    """
    limit argument to limit the number of datasets run
    """
    return get_int_arg(i)


def get_ds_path_arg(i=1) -> Union[str, None]:
    """
    limit argument to limit the number of datasets run
    """
    return get_str_arg(i)
