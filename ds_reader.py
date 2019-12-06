#!/usr/bin/env python

# Goal: read in the datasets and create rdd's out of them

from typing import List, Generator, Iterable, Union

import sys
from os.path import splitext, basename
import subprocess
import time
import random

from pyspark.sql import DataFrame, SparkSession

# hdfs commands to interact with the hdfs outside of Spark
HDFS_LIST_DIR = 'hdfs dfs -ls -C {ds_path}'
HDFS_SIZE_F = 'hdfs dfs -stat %b {ds_path}'


def run_hdfs_cmd(cmd: str) -> bytes:
    """
    allows an arbitrary hdfs command to be run
    """
    cmd: List[str] = cmd.split(' ')
    return subprocess.check_output(cmd).decode('utf_8').strip()


def get_ds_file_names(ds_path: str) -> List[str]:
    """
    get the file names of compressed (.gz) datasets in the directory
    """
    # cmd to get list of files in hdfs dir
    files = run_hdfs_cmd(HDFS_LIST_DIR.format(ds_path=ds_path)).split('\n')  # sanitize output into list of strings

    # filter files
    files: List[str] = [
        ds_path + '/' + basename(rel_path) for rel_path in files
        if splitext(rel_path)[-1] == ".gz" and basename(rel_path)
        != "datasets.tsv.gz"]

    return files


def generate_dataframes(spark: SparkSession, files: Iterable[str]) -> Generator[Union[int, DataFrame], None, None]:
    """
    takes a list of .gz dataset filenames on the hdfs and reads them into a Spark DataFrame (Spark does decompression).
    skips files that may be decompressed to larger than 2.5GB.
    because it returns a generator, the above process occurs lazily per dataset
    """
    for i, path in enumerate(files):
        # limit files to a certain filesize to prevent memory issues
        if int(run_hdfs_cmd(HDFS_SIZE_F.format(ds_path=path))) < 6e+8:  # 600MB
            # read compressed tsv file
            df = spark.read.csv(path, sep="\t", header=True, inferSchema=True)
            df.ds_name = basename(path)
            yield i, df
        else:
            try:
                raise MemoryError(f'{path}: large compressed file size that may decompress to +2.5GB')
            except MemoryError as e:
                print(e)
        # # do not limit filesize
        # df = spark.read.csv(path, sep="\t", header=True, inferSchema=True)
        # df.ds_name = basename(path)
        # yield i, df


def datasets_to_dataframes(spark: SparkSession, ds_path: str, rand: bool = False) -> Union[int, Generator[DataFrame, None, None]]:
    """
    datasets_to_dataframes takes the path to the hdfs directory that holds all of the datasets, reads them into dataframes (except the meta file "datasets.tsv"). 
    it outputs the dataframes in a generator. a generator is a lazily evaluated iterator, which allows the dataset to be read (and stored in memory) only when requested (i.e. iterated in a for loop).
    using this pattern, n datasets can be read into mem at a time and operated on
    """
    files: List[str] = get_ds_file_names(ds_path)
    if rand:
        random.shuffle(files)

    return datasets_to_dataframes_select(spark, files)


def datasets_to_dataframes_select(spark: SparkSession, files: List[str]) -> Union[int,
                                                                       Generator[DataFrame, None, None]]:
    """
    datasets_to_dataframes takes a list of files in the hdfs, and reads the files into dataframes.
    it outputs the dataframes in a generator. a generator is a lazily evaluated iterator, which allows the dataset to be read (and stored in memory) only when requested (i.e. iterated in a for loop).
    using this pattern, n datasets can be read into mem at a time and operated on
    """
    df_generator = generate_dataframes(spark, files)

    return len(files), df_generator


def test():
    from timing import timed

    # get print
    print_schema = ''
    try:
        print_schema = sys.argv[4]
    except IndexError:
        pass
    
    def _run(df, i):
        if print_schema == 'print':
            df.printSchema()

    timed(_run)

    
if __name__ == '__main__':
    # use cli as "ds_reader.py <hdfs dir> <limit> <print>" where limit is an optional int and print is the optional string "print"
    test()
