#!/usr/bin/env python

# Goal: read in the datasets and create rdd's out of them

from typing import List, Generator, Iterable, Union, Tuple

import sys
from os.path import splitext, basename
import subprocess
import time
import random

from pyspark.sql import DataFrame, SparkSession

# hdfs commands to interact with the hdfs outside of Spark
HDFS_LIST_DIR_OPTIONS = """hdfs dfs -ls {ds_path} | awk '{{print $8 "\t" $5}}'"""
HDFS_SIZE_F = 'hdfs dfs -stat %b {ds_path}'


def run_hdfs_cmd(cmd: str, shell=True) -> bytes:
    """
    allows an arbitrary hdfs command to be run
    """
    if not shell:
        cmd: List[str] = cmd.split(' ')
    return subprocess.check_output(cmd, shell=shell).decode('utf_8').strip()


def filter_files(files: List[str]) -> List[str]:
    # filter files
    files: List[str] = [
        rel_path for rel_path in files
        if splitext(rel_path)[-1] == ".gz" and basename(rel_path)
        != "datasets.tsv.gz"]
    return files


def get_ds_file_size(f_path: str) -> int:
    """
    return in bytes
    """
    return int(run_hdfs_cmd(HDFS_SIZE_F.format(ds_path=f_path)))


def generalize_paths(files: Union[str, List[str]]) -> str:
    if type(files) == list:
        files: str = ' '.join(files)
    else:
        files: str = files
    return files


def get_ds_file_sizes(files: Union[str, List[str]]) -> List[Tuple[str, int]]:
    files: str = generalize_paths(files)
    files_n_sizes = [f.split('\t') for f in run_hdfs_cmd(HDFS_LIST_DIR_OPTIONS.format(ds_path=files)).split('\n')]
    files_n_sizes = [(name, int(size)) for name, size in files_n_sizes]
    return files_n_sizes

def get_ds_file_names(files: Union[str, List[str]], to_sort = False) -> List[str]:
    """
    reads dir or list of files and returns ds paths in ascending sorted order of file size on hdfs
    """
    files: str = generalize_paths(files)
    files_n_sizes = get_ds_file_sizes(files)
    if to_sort:
        files_n_sizes = sorted(files_n_sizes, key=lambda x: x[1])
    files = [tup[0] for tup in files_n_sizes]
    files = filter_files(files)

    return files


def generate_dataframes(spark: SparkSession, files: Iterable[str]) -> Generator[Union[int, DataFrame], None, None]:
    """
    takes a list of .gz dataset filenames on the hdfs and reads them into a Spark DataFrame (Spark does decompression).
    skips files that may be decompressed to larger than 2.5GB.
    because it returns a generator, the above process occurs lazily per dataset
    """
    for i, path in enumerate(files):
        try:
            is_small_file = get_ds_file_size(path) < 6e+8
        except Exception:
            is_small_file = False
        # limit files to a certain filesize to prevent memory issues
        if is_small_file:  # 600MB
            # read compressed tsv file
            print('path:', path)
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


def datasets_to_dataframes(spark: SparkSession, ds_path: str, rand: bool = False, to_sort: bool = False) -> Union[int, Generator[DataFrame, None, None]]:
    """
    deprecated but kept to call datasets_to_dataframes_select
    """
    return datasets_to_dataframes_select(spark, ds_path, rand, to_sort)


def datasets_to_dataframes_select(
        spark: SparkSession, files: Union[str, List[str]],
        rand: bool = False, to_sort: bool = False) -> Union[int, Generator[DataFrame, None, None]]:
    """
    datasets_to_dataframes takes a list of file paths in the hdfs or the directory path, and reads the files into dataframes.
    it outputs the dataframes in a generator. a generator is a lazily evaluated iterator, which allows the dataset to be read (and stored in memory) only when requested (i.e. iterated in a for loop).
    using this pattern, n datasets can be read into mem at a time and operated on
    """
    files: List[str] = get_ds_file_names(files, to_sort)

    if rand:
        random.shuffle(files)

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
