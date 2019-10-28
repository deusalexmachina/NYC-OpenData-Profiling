# Goal: read in the datasets and create rdd's out of them

from typing import List

import sys
from os import listdir
from os.path import splitext

from pyspark.sql import DataFrame, SparkSession


def datasets_to_dataframes(ds_path: str) -> List[DataFrame]:
    '''
    datasets_to_dataframes takes the path to the directory that holds all of the datasets and converts every .tsv file in that directory to a dataframe (except the meta file "datasets.tsv"). 
    It outputs the dataframes in a list
    '''
    files: List[str] = [
        ds_path + '/' + rel_path for rel_path in listdir(ds_path)
        if splitext(rel_path)[-1] == ".tsv" and rel_path != "datasets.tsv"]

    dfs: List[DataFrame] = []
    for path in files:
        dfs.append(
            spark.read.csv(
                path, sep="\t", header=True, inferSchema=True))

    return dfs


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    dfs: List[DataFrame] = datasets_to_dataframes(sys.argv[1])
    for df in dfs:
        df.printSchema()
