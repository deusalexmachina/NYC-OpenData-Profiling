from ds_reader import datasets_to_dataframes, datasets_to_dataframes_select

from pyspark.sql import SparkSession
from pyspark.sql import utils

import time
import sys

from typing import List

import random

from cli import get_rand_arg, get_limit_arg


def timed(fn, files: List[str] = None) -> None:
    """
    a standardized interface for timing some function fn which accepts a df and potentially other args and kwargs. 
    it calculates runtimes and avg runtimes while iterating through the df generator and calling fn, and prints them.

    when this is called by another module, use sys.argv[1] for the limit and sys.arv[2] for selecting random datasets (or what is defaulted by the cli module). 
    to not set an option, pass in '-'.
    example from cli: `./env.sh "basic_metadata.py - True"`
    """
    spark = SparkSession.builder.getOrCreate()  # init spark

    time_start_all = time.time()
    time_end = time_start_all
    total_runtime = 0
    total_runtime_load = 0
    max_runtime = 0
    max_output = None

    len_dfs = 0
    dfs = None

    rand = get_rand_arg()

    if files is None:
        len_dfs, dfs = datasets_to_dataframes(spark, sys.argv[1], rand)
    else:
        # no random order if files are passed (call random.shuffle before passing files)
        len_dfs, dfs = datasets_to_dataframes_select(spark, files)

    limit = get_limit_arg()
    if limit is None:
        limit = len_dfs

    actual_len_dfs = len_dfs

    for i, df in enumerate(dfs):
        print(f"### i: {i}, ds: {df.ds_name} ###")

        actual_len_dfs = i+1

        time_start = time.time()
        # output = fn(df)
        try:
            output = fn(df)
        except utils.AnalysisException as e:
            print(e)
            continue
        time_end = time.time()

        # specific to current ds
        runtime = time_end - time_start
        print("actual runtime:", runtime)  # DEBUG
        total_runtime += runtime

        # general for all ds
        total_runtime_load = time_end - time_start_all
        print('running avg runtime (including load):', total_runtime_load / actual_len_dfs)

        if runtime > max_runtime:
            max_runtime = runtime
            max_output = output

        if actual_len_dfs >= limit:
            break

    total_runtime_load = time_end - time_start_all

    print()
    print(f"### SUMMARY FOR {actual_len_dfs} DATASETS ###")
    print("max runtime:", max_runtime)
    if max_output is not None:
        print("max output:", max_output)

    print('# RUNTIME (ONLY FOR FN) #')
    print('total runtime:', total_runtime)
    print('total avg runtime:', total_runtime / actual_len_dfs)

    print('# RUNTIME (INCLUDING LOADS AND INITIAL DIR PARSING) #')
    print('total runtime:', total_runtime_load)
    print('total avg runtime:', total_runtime_load / actual_len_dfs)
