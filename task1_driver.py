from timing import timed

from pyspark.sql import SparkSession

from basic_metadata import map_cols, get_basic_metadata, OUTPUT_KEY
from basic_profile import get_dataset_profile
from compile_itemset import combine_itemsets, most_frequent_itemsets

from os import makedirs
from os.path import isdir

import json


def append_data_profile(ds_dct, dct_output):
    for col, dct in dct_output.items():
        if not 'data_types' in ds_dct[OUTPUT_KEY][col]:
            ds_dct[OUTPUT_KEY][col]['data_types'] = []
        for col2, dct2 in dct.items():
            ds_dct[OUTPUT_KEY][col]['data_types'].append(dct2)


def run():
    spark = SparkSession.builder.getOrCreate()

    # master_dct contains all output to be used in json
    master_dct = {}
    # master_itemset contains all itemsets to be used in json
    master_itemset = {}
    def _run(df, i):
        # BASIC METADATA
        # ds_dct belongs to a dataset
        ds_dct = {
            'dataset_name': df.ds_name,
            OUTPUT_KEY: {}
        }
        df_cols = map_cols(df)
        get_basic_metadata(df_cols, ds_dct)  # main driver

        ### BASIC PROFILE ###
        dct_output, dct_itemset = get_dataset_profile(spark, df_cols)
        append_data_profile(ds_dct, dct_output)
        # print(ds_dct)
        ds_dct['itemset'] = dct_itemset

        # add the counts for df_itemset to master
        combine_itemsets(dct_itemset, master_itemset)

        ### DONE: update master_dct with dataset ###
        master_dct.update({ds_dct['dataset_name']: ds_dct})

        # Save the output for the df to json for each run
        with open("results_times/master_dct.json", "w") as json_file:
            json.dump(master_dct, json_file, indent=4)
            json_file.write("\n")

        print("Master Itemset: ", master_itemset)
        with open("master_itemset.json", "w") as json_file:
            json.dump(master_itemset, json_file, indent=4)

        return ds_dct

    timed(_run)

    result_set = most_frequent_itemsets(master_itemset)
    print("Most Frequent Itemsets: ", result_set)


if __name__ == '__main__':
    if not isdir('results_times'):
        makedirs('results_times')

    run()
