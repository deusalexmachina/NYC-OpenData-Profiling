from ds_reader import datasets_to_dataframes

from pyspark.sql import SparkSession

import time
import sys


def timed(fn, *args, **kwargs):
    """
    a standardized interface for timing some function fn which accepts a df and potentially other args and kwargs. 
    it calculates runtimes and avg runtimes while iterating through the df generator and calling fn, and prints them
    """
    spark = SparkSession.builder.getOrCreate()  # init spark

    time_start_all = time.time()
    total_runtime = 0
    total_runtime_load = 0
    max_runtime = 0
    max_output = None

    len_dfs, dfs = datasets_to_dataframes(spark, sys.argv[1])

    try:
        limit = int(sys.argv[2])
    except IndexError:
        limit = len_dfs

    for i, df in enumerate(dfs):
        actual_len_dfs = i+1

        time_start = time.time()
        output = fn(df, *args, **kwargs)
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
    print("### SUMMARY ###")
    print("max runtime:", max_runtime)
    if max_output is not None:
        print("max output:", max_output)

    print('# RUNTIME (ONLY FOR FN) #')
    print('total runtime:', total_runtime)
    print('total avg runtime:', total_runtime / actual_len_dfs)

    print('# RUNTIME (INCLUDING LOADS AND INITIAL DIR PARSING) #')
    print('total runtime:', total_runtime_load)
    print('total avg runtime:', total_runtime_load / actual_len_dfs)
