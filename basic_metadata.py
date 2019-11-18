import sys

from ds_reader import datasets_to_dataframes

from pyspark.sql import SparkSession, DataFrame, Column, Row
from pyspark.sql.functions import countDistinct, create_map, lit, col, explode, count, when, isnan, isnull, sum, collect_list, udf
from pyspark.sql.types import ArrayType, StringType

import time
from typing import Union, Callable, Dict, Iterable

spark_col = col

OUTPUT_KEY = 'columns'


def map_cols(df: DataFrame) -> Column:
    """
    maps column names to column values using Spark's MapType. A column of Maps is returned where each row represents a map like {column_name: [column_vals]}
    """

    return df.select(
        create_map(
            *
            [j
             for l
             in
             [[lit(col),
               spark_col(col)] for col in df.columns] for j in
             l]))


def reduce_col(df_cols: Union[DataFrame, Column],
               agg_name: str, aggFunc: Callable, other_groupBy_keys: Iterable[Column] = []) -> DataFrame:
    """
    explodes (flattens) the output of map_cols so that each row maps one column_name to one column_value. it then groups by column name using the aggFunc.
    agg_name is later in append_output as a key to get the outputs out. 
    other_groupBy_keys is a list of other groupby keys and is used in cases such as getting the top 5 frequent vals
    """
    return df_cols.select(
        explode(*df_cols.columns)).groupBy(
        spark_col("key").alias("col_name"), *other_groupBy_keys).agg(
        aggFunc("value").alias(agg_name))


def append_output(master_dct: Dict, key: str, df_output: DataFrame):
    """
    adds key (a column_name in df_output) to master_dct, where it is mapped to a column name. is used for the output of reduce_col
    """
    output_rows = df_output.collect()
    for row in output_rows:
        col_name = row['col_name']
        if not col_name in master_dct[OUTPUT_KEY]:
            master_dct[OUTPUT_KEY][col_name] = {'column_name': col_name}
        # if key already in dct then change val into list and append value
        if key in master_dct[OUTPUT_KEY][col_name]:
            if not isinstance(master_dct[OUTPUT_KEY][col_name][key], list):
                master_dct[OUTPUT_KEY][col_name][key] = [
                    master_dct[OUTPUT_KEY][col_name][key]]
            master_dct[OUTPUT_KEY][col_name][key].append(row[key])
        # if key not in dct then set key = val
        else:
            master_dct[OUTPUT_KEY][col_name][key] = row[key]


def get_count_distincts(df_cols: Union[DataFrame, Column], master_dct: Dict):
    rows = reduce_col(df_cols, 'number_distinct_values',
                      countDistinct)
    append_output(master_dct, 'number_distinct_values', rows)


def get_top_5(df_cols: Union[DataFrame, Column], master_dct: Dict):
    rows = reduce_col(df_cols, 'count', count, other_groupBy_keys=[
                      spark_col('value').alias("frequent_values")])

    counts = rows.sort('count', ascending=False)  # speed
    # counts.sort('col_name', 'count', ascending=False).show()  # DEBUG

    rows = counts.groupBy('col_name').agg(
        collect_list('frequent_values').alias('frequent_values'))
    top_5 = udf(lambda lst: [str(val)
                             for val in lst[:5]], ArrayType(StringType()))
    rows = rows.select('col_name', top_5(
        'frequent_values').alias('frequent_values'))
    append_output(master_dct, 'frequent_values', rows)


def get_non_empty(df_cols: Union[DataFrame, Column], master_dct: Dict):
    rows = reduce_col(df_cols, 'number_non_empty_cells',
                      lambda c: count(when(~(isnan(c) | isnull(c)), c)))
    append_output(master_dct, 'number_non_empty_cells', rows)


def get_empty(df_cols: Union[DataFrame, Column], master_dct: Dict):
    rows = reduce_col(df_cols, 'number_empty_cells',
                      lambda c: count(when(isnan(c) | isnull(c), c)))
    append_output(master_dct, 'number_empty_cells', rows)


def get_basic_metadata(df_cols: Union[DataFrame, Column], master_dct: Dict):
    get_count_distincts(df_cols, master_dct)
    get_top_5(df_cols, master_dct)
    get_non_empty(df_cols, master_dct)
    get_empty(df_cols, master_dct)


def get_val_from_single_val_col(df: Column):
    return list(df.collect()[0].asDict().values())[0]


def __test(dfs):
    """
    compare Spark implementation to naive one (iterate over each row with python) for count_distinct
    """
    limit = 20

    for i, df in enumerate(dfs):
        start = time.time()

        master_dct = {
            'dataset': df.ds_name,
            OUTPUT_KEY: {}
        }
        df_cols = map_cols(df)

        get_count_distincts(df_cols, master_dct)

        end = time.time()
        print(end-start)
        print(master_dct)

        ###

        start = time.time()

        master_dct = {
            'dataset': df.ds_name,
            OUTPUT_KEY: {}
        }
        for col_name in df.columns:
            num_distincts = get_val_from_single_val_col(
                df.agg(countDistinct(col_name)))
            if not col_name in master_dct[OUTPUT_KEY]:
                master_dct[OUTPUT_KEY][col_name] = {'column_name': col_name}
            master_dct[OUTPUT_KEY][col_name]['number_distinct_values'] = num_distincts

        end = time.time()
        print(end-start)
        print(master_dct)

        if i >= limit - 1:
            exit(0)


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    len_dfs, dfs = datasets_to_dataframes(spark, sys.argv[1])
    limit = 3

    max_time = 0
    max_dct = None

    start_all_time = time.time()

    master_dct = {}
    for i, df in enumerate(dfs):
        start = time.time()

        ### MAIN ###
        dct = {
            'dataset_name': df.ds_name,
            OUTPUT_KEY: {}
        }
        df_cols = map_cols(df)
        get_basic_metadata(df_cols, dct)
        
        ### test num_cells (counts) ###
        # rows = reduce_col(df_cols, 'number_cells', lambda c: sum(lit(1)))
        # append_output(dct, 'number_cells', rows)

        # for col in dct[OUTPUT_KEY].keys():
        #     if dct[OUTPUT_KEY][col]['number_cells'] != dct[OUTPUT_KEY][col]['number_non_empty_cells'] + dct[OUTPUT_KEY][col]['number_empty_cells']:
        #         raise ValueError('Error in dataset: {}. Column: {}'.format(
        #             dct.keys()[0], str(dct[OUTPUT_KEY][col])))

        master_dct.update({dct['dataset_name']: dct})

        end = time.time()
        timed = end-start
        print("runtime:", timed)  # DEBUG
        # print("dct:", dct)  # DEBUG

        if timed > max_time:
            max_time = timed
            max_dct = dct

        # DEBUG
        if i >= limit - 1:
            break

    
    total_runtime = end-start_all_time

    print()
    print("### MASTER_DCT###")
    print(master_dct)
    print()
    print("### SUMMARY ###")
    print("max runtime:", max_time)
    print("max runtime for dct:", max_dct)

    print('total runtime:', total_runtime)
    print('avg runtime (including load):', total_runtime / (i + 1))
    exit(0)
