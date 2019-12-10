import sys

from ds_reader import datasets_to_dataframes

from pyspark.sql import DataFrame, Column, Row
from pyspark.sql.functions import countDistinct, create_map, lit, col, explode, count, when, isnan, isnull, sum, collect_list, udf
from pyspark.sql.types import ArrayType, StringType

import time
from typing import Union, Callable, Dict, Iterable

spark_col = col

OUTPUT_KEY = 'columns' # key mapped to a dataset which holds the column output (like column metadata)


def map_cols(df: DataFrame) -> Column:
    """
    maps column names to column values using Spark's MapType. A column of Spark "Maps" is returned where each row represents a Map like {column_name: [column_vals]...}.
    It then explodes (flattens) the output of map_cols so that each row maps one column_name to one column_value, i.e. [col1: val1, col1: val2...]
    Spark "Column" (of dataset columns) is returned 
    """

    df_mapped_cols = df.select(
        create_map(
            *
            [j
             for l
             in
             [[lit(col),
               spark_col(col)] for col in df.columns] for j in
             l]))

    df_exploded_cols = df_mapped_cols.select(
        explode(*df_mapped_cols.columns))

    return df_exploded_cols


def reduce_cols(df_cols: Union[DataFrame, Column],
               agg_name: str, aggFunc: Callable, other_groupBy_keys: Iterable[Column] = []) -> DataFrame:
    """
    accepts map_cols output (flattened columns of columns) and groups by column name using the aggFunc.
    agg_name is later used in append_output as a key to get the outputs and add them to the output dict. 
    other_groupBy_keys is used in cases such as getting the top 5 frequent vals
    """
    return df_cols.groupBy(
        spark_col("key").alias("col_name"), *other_groupBy_keys).agg(
        aggFunc("value").alias(agg_name))


def append_output(ds_dct: Dict, result_key: str, df_output: DataFrame):
    """
    adds result_key (a column_name in df_output) to ds_dct (output dict for a dataset), where it is mapped to the appropriate column name. 
    is used for the output of reduce_cols.
    """
    output_rows = df_output.collect()
    for row in output_rows:
        col_name = row['col_name']
        if not col_name in ds_dct[OUTPUT_KEY]:
            ds_dct[OUTPUT_KEY][col_name] = {'column_name': col_name}
        # if result_key already in dct then change val into list and append value (like for frequent values)
        if result_key in ds_dct[OUTPUT_KEY][col_name]:
            if not isinstance(ds_dct[OUTPUT_KEY][col_name][result_key], list):
                ds_dct[OUTPUT_KEY][col_name][result_key] = [
                    ds_dct[OUTPUT_KEY][col_name][result_key]]
            ds_dct[OUTPUT_KEY][col_name][result_key].append(row[result_key])
        # if result_key not in dct then set result_key = val
        else:
            ds_dct[OUTPUT_KEY][col_name][result_key] = row[result_key]


def get_count_distincts(df_cols: Union[DataFrame, Column], ds_dct: Dict):
    df_output = reduce_cols(df_cols, 'number_distinct_values',
                      countDistinct)
    append_output(ds_dct, 'number_distinct_values', df_output)


def get_top_5(df_cols: Union[DataFrame, Column], ds_dct: Dict):
    df_output = reduce_cols(df_cols, 'count', count, other_groupBy_keys=[
                      spark_col('value').alias("frequent_values")])

    counts = df_output.sort('count', ascending=False)  # sort only on count for speed
    # counts.sort('col_name', 'count', ascending=False).show()  # DEBUG

    df_output = counts.groupBy('col_name').agg(
        collect_list('frequent_values').alias('frequent_values'))
    top_5 = udf(lambda lst: [str(val)
                             for val in lst[:5]], ArrayType(StringType())) # take top 5
    df_output = df_output.select('col_name', top_5(
        'frequent_values').alias('frequent_values'))
    append_output(ds_dct, 'frequent_values', df_output)


def get_non_empty(df_cols: Union[DataFrame, Column], ds_dct: Dict):
    df_output = reduce_cols(df_cols, 'number_non_empty_cells',
                      lambda c: count(when(~(isnan(c) | isnull(c)), c)))
    append_output(ds_dct, 'number_non_empty_cells', df_output)


def get_empty(df_cols: Union[DataFrame, Column], ds_dct: Dict):
    df_output = reduce_cols(df_cols, 'number_empty_cells',
                      lambda c: count(when(isnan(c) | isnull(c), c)))
    append_output(ds_dct, 'number_empty_cells', df_output)


def get_basic_metadata(df_cols: Union[DataFrame, Column], ds_dct: Dict):
    """
    driver function that updates ds_dct with the following:
    {
        "columns": {
            "<column_name>": {
            "column_name": <string>,
            "number_distinct_values": <int>,
            "frequent_values": [
                <string1>,
                <string2>,
                <string3>,
                <string4>,
                <string5>,
            ],
            "number_non_empty_cells": <int>, 
            "number_empty_cells": <int>
            ...
        }
    }
    """
    get_non_empty(df_cols, ds_dct)
    get_empty(df_cols, ds_dct)
    get_count_distincts(df_cols, ds_dct)
    get_top_5(df_cols, ds_dct)
    
    


def get_val_from_single_val_col(df: Column):
    return list(df.collect()[0].asDict().values())[0]


def __test_compare(dfs):
    """
    compare Spark implementation to naive one (iterate over each row with python) for count_distinct
    """
    limit = 20

    for i, df in enumerate(dfs):
        start = time.time()

        ds_dct = {
            'dataset': df.ds_name,
            OUTPUT_KEY: {}
        }
        df_cols = map_cols(df)

        get_count_distincts(df_cols, ds_dct)

        end = time.time()
        print(end-start)
        print(ds_dct)

        ###

        start = time.time()

        ds_dct = {
            'dataset': df.ds_name,
            OUTPUT_KEY: {}
        }
        for col_name in df.columns:
            num_distincts = get_val_from_single_val_col(
                df.agg(countDistinct(col_name)))
            if not col_name in ds_dct[OUTPUT_KEY]:
                ds_dct[OUTPUT_KEY][col_name] = {'column_name': col_name}
            ds_dct[OUTPUT_KEY][col_name]['number_distinct_values'] = num_distincts

        end = time.time()
        print(end-start)
        print(ds_dct)

        if i >= limit - 1:
            exit(0)


def test():
    """
    test_timing with standardized interface in timing module
    """
    from timing import timed

    # master_dct contains all output to be used in json
    master_dct = {}

    def _run(df, i):
        ### MAIN ###
        # dct belongs to a dataset
        dct = {
            'dataset_name': df.ds_name,
            OUTPUT_KEY: {}
        }
        df_cols = map_cols(df)
        get_basic_metadata(df_cols, dct)  # main driver

        ### test num_cells (counts) ###
        # rows = reduce_cols(df_cols, 'number_cells', lambda c: sum(lit(1)))
        # append_output(dct, 'number_cells', rows)

        # for col in dct[OUTPUT_KEY].keys():
        #     if dct[OUTPUT_KEY][col]['number_cells'] != dct[OUTPUT_KEY][col]['number_non_empty_cells'] + dct[OUTPUT_KEY][col]['number_empty_cells']:
        #         raise ValueError('Error in dataset: {}. Column: {}'.format(
        #             dct.keys()[0], str(dct[OUTPUT_KEY][col])))

        # update master_dct with dataset
        master_dct.update({dct['dataset_name']: dct})

        return dct

    timed(_run)

    # DEBUG print all output
    # print()
    # print("### MASTER_DCT###")
    # print(master_dct)


if __name__ == '__main__':
    # use cli as "ds_reader.py <hdfs dir> <limit>" where limit is an optional int
    test()
