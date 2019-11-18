#!/usr/bin/env python

# Goal: read in the datasets and create rdd's out of them

from typing import List, Generator, Iterable, Union

import sys
from os.path import splitext, basename
import subprocess
import time

from pyspark.sql import DataFrame, SparkSession

# hdfs commands to interact with the hdfs outside of Spark
HDFS_LIST_DIR = 'hdfs dfs -ls -C {ds_path}'


def run_hdfs_cmd(cmd: str) -> bytes:
    """
    allows an arbitrary hdfs command to be run
    """
    cmd: List[str] = cmd.split(' ')
    return subprocess.check_output(cmd)


def get_ds_file_names(ds_path: str) -> List[str]:
    """
    get the file names of compressed (.gz) datasets in the directory
    """
    # cmd to get list of files in hdfs dir
    files = run_hdfs_cmd(HDFS_LIST_DIR.format(ds_path=ds_path)).decode(
        'ASCII').strip().split('\n')  # sanitize output into list of strings

    # filter files
    files: List[str] = [
        ds_path + '/' + basename(rel_path) for rel_path in files
        if splitext(rel_path)[-1] == ".gz" and basename(rel_path)
        != "datasets.tsv.gz"]

    return files


def generate_dataframes(spark: SparkSession, files: Iterable[str]) -> Generator[DataFrame, None, None]:
    """
    takes a list of .gz dataset filenames on the hdfs and reads them into a Spark DataFrame (Spark does decompression).
    because it returns a generator, the above process occurs lazily per dataset
    """
    for path in files:
        # read compressed tsv file
        df = spark.read.csv(path, sep="\t", header=True, inferSchema=True)
        df.ds_name = basename(path)
        yield df


def datasets_to_dataframes(spark: SparkSession, ds_path: str) -> Union[int, Generator[DataFrame, None, None]]:
    """
    datasets_to_dataframes takes the path to the hdfs directory that holds all of the datasets, reads them into dataframes (except the meta file "datasets.tsv"). 
    it outputs the dataframes in a generator. a generator is a lazily evaluated iterator, which allows the dataset to be read (and stored in memory) only when requested (i.e. iterated in a for loop).
    using this pattern, n datasets can be read into mem at a time and operated on
    """
    files: List[str] = get_ds_file_names(ds_path)
    df_generator = generate_dataframes(spark, files)

    return len(files), df_generator


if __name__ == '__main__':
    # use cli as "ds_reader.py <hdfs dir> <limit> <print>" where limit is an optional int and print is the optional string "print"
    spark = SparkSession.builder.getOrCreate()

    time_start = time.time()
    
    len_dfs, dfs = datasets_to_dataframes(spark, sys.argv[1])

    # get limit
    limit = len_dfs
    try:
        limit = int(sys.argv[2])
    except Exception:
        pass

    # get print
    print_schema = ''
    try:
        print_schema = sys.argv[2]
        print_schema = sys.argv[3]
    except Exception:
        pass
    
    for i, df in enumerate(dfs):
        if print_schema == 'print':
            df.printSchema()
        if i >= limit - 1:
            len_dfs = limit
            break
    
    time_end = time.time()
    timed = time_end - time_start
    print(f'time to load {len_dfs} dfs (secs): {timed}')
