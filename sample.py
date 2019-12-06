from timing import timed

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import create_map, count, collect_list, col as spark_col, isnull, udf, struct, lit
from pyspark.sql.types import ArrayType, StringType, MapType, IntegerType

from basic_metadata import map_cols, reduce_cols

from typing import Union, List, Tuple, Dict, Any

import sys
import os.path

from cli import get_rand_arg
import random


def get_counts(df_cols: Union[DataFrame, Column]) -> DataFrame:
    df_output = reduce_cols(df_cols, 'count', count, other_groupBy_keys=[
        spark_col('value').alias("frequent_value")])

    # sort only on count for speed
    df_counts = df_output.sort('count', ascending=False)
    df_counts = df_counts.withColumn(
        'frequent_value', df_counts.frequent_value)

    return df_counts


def get_n_freq_str(df_counts, top_n: Union[DataFrame, Column]) -> DataFrame:
    """
    extract n frequent values from get_counts and collect to a string representation so that each column has 1 row mapped to a string with the 10 frequent values and their counts
    """
    conc = udf(
        lambda lst: "{}->{}".format(str(lst[0]), str(lst[1])),
        StringType())  # take top n
    mapped_counts = df_counts.select(
        'col_name', conc(struct('frequent_value', 'count')).alias(
            'frequent_value->count'))
    # mapped_counts = df_counts.select('col_name', create_map(['frequent_value', 'count']).alias('frequent_value->count'))  # to collect to map

    df_output = mapped_counts.groupBy('col_name').agg(collect_list(
        spark_col('frequent_value->count')).alias('frequent_values->counts'))

    freq_col_name = 'frequent_values->counts_(top_{:d})'.format(top_n)

    conv = udf(
        lambda lst: [str(dct) for dct in lst[:top_n]],
        ArrayType(StringType()))  # take top n
    df_output = df_output.select('col_name', conv(
        'frequent_values->counts').alias(freq_col_name))

    return df_output


def get_task2_dfs(ds_path) -> List[Tuple[str, str]]:
    """
    read file from ta and return a list of tuples of parsed and converted names (to file in hdfs dir) paired with the col name to be extracted
    """
    fs = None
    with open('task_2_names.txt') as f:
        fs = f.read().splitlines()

    gz_paths_cols = []
    for f in fs:
        spl = f.split('.', 1)
        ds_name = spl[0]

        _rest = spl[1]
        col_name = _rest[::-1].split('.', 2)[2][::-1]

        # print('test:', ds_name, '|', col_name)  # DEBUG

        gz_name = ds_name + '.tsv' + '.gz'
        gz_path = ds_path + '/' + gz_name

        gz_paths_cols.append((gz_path, col_name))

    return gz_paths_cols


def comp_letters(s1, s2):
    """
    test two strings for whether they are identical by removing all chars but letters and testing whether either is contained in the other
    """
    remove_chars = string.punctuation + string.whitespace

    s1_translated = s1.translate(
        str.maketrans('', '', remove_chars))
    s2_translated = s2.translate(
        str.maketrans('', '', remove_chars))

    return (s1_translated in s2_translated) or (s2_translated in s1_translated)


def t2_get_n_frequents(gz_paths_cols: List[Tuple[str, str]],
                       top_n: int = 10) -> Union[List[str],
                                                 List[Dict[str, Any]],
                                                 List[Dict[str, str]]]:
    """
    take in a List of Tuple[ds_name in hdfs, column name of interest from ta] and get the top n frequent values from the column.
    it returns the column names of the output df for reconstructing into a df, the output df as a list of dictionaries (converted from rows from df.collect), 
    and the missing columns (weren't matched in the ds and aren't present in output). the reason a df isn't output is because the output dfs at each iteration are converted into python objects.
    concantenating dfs using Union is a bad idea since lazy evaluation means that all the columns will eventually be loaded into memory somewhere and an error is thrown for lack of memory. 

    this function outputs good quality representative values for the column
    """
    rand = get_rand_arg()
    if rand:
        random.shuffle(gz_paths_cols)

    # unzip basically
    gz_paths: List[str] = [gz_paths for gz_paths, _ in gz_paths_cols]
    cols: List[str] = [cols for _, cols in gz_paths_cols]

    records = []
    i = 0
    missing = set()

    columns = None

    def _run(df):
        nonlocal records
        nonlocal i
        nonlocal missing
        nonlocal columns

        print('i: ', i, "ds_path:", gz_paths[i], "col_name:", cols[i])

        # match cols (they removed replaced space in column names when saving them to the file name)
        col = cols[i]
        ds_name = os.path.basename(gz_paths[i])

        for c in df.columns:
            if comp_letters(cols[i], c):
                col = c
                break

        try:
            df = df.select(spark_col(col))  # remove all but col of interest
        except Exception:
            missing.add({'ds_name': ds_name, 'col_name_(ta)': cols[i]})
            i += 1
            raise ValueError(
                'missing:', (ds_name, cols[i]),
                'cols:', df.columns)
        df_cols = map_cols(df)
        df_counts = get_counts(df_cols)
        df_output = get_n_freq_str(df_counts, top_n)
        df_output = df_output.select(lit(ds_name).alias('ds_path'), '*')

        if columns is None:
            columns = df_output.columns

        # concat
        records.append([row.asDict() for row in df_output.collect()][0])

        i += 1

        return df_output

    timed(_run, gz_paths)

    print('missing:', missing)

    return columns, records, List(missing)


if __name__ == '__main__':
    import pandas as pd

    gz_paths_cols: List[Tuple[str, str]] = get_task2_dfs(sys.argv[1])

    try:
        # DEBUG: slice gz_paths and cols
        gz_paths_cols = gz_paths_cols[int(sys.argv[2]):]
    except IndexError:
        pass

    columns, records, missing = t2_get_n_frequents(gz_paths_cols)
    # print(records)

    df_output = pd.DataFrame.from_records(records, columns=columns)
    print(columns)
    print(df_output)
    df_output.to_csv('task2_label.csv', index=False)

    df_missing = pd.DataFrame.from_records(missing)
    print(df_missing)
    df_missing.to_csv('task2_label_missing.csv', index=False)
