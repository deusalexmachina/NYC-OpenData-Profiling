"""
{
    "column_name": the name of the column (type: string)
    "semantic_types": [
        {
            "semantic_type": label of the semantic type choosing from the list provided below (type: string)
            "label": semantic type is other, provide a label of the semantic type (type: string)
            "count": the number of instances in the column that belong to that semantic types (type: integer)
        }
    ]
}

label list:
[person_name, business_name, phone_number, address, street_name, city, 
neighborhood, lat_lon_cord, zip_code, borough, school_name, color, car_make, 
city_agency, area_of_study, subject_in_school, school_level, college_name, website, 
building_classification, vehicle_type, location_type, park_playground, other]

ours:
['address', 'borough', 'building_classification', 'business_name',
'car_make', 'city', 'city_agency', 'city_agency (medical)',
'color', 'interest', 'lat_lon_cord', 'location_type',
'neighborhood', 'park_playground', 'person_name (first_name)',
'person_name (full_name)', 'person_name (last_name)',
'person_name (middle_initial)', 'phone_number', 'school_level',
'school_name', 'school_type', 'street_name', 'street_number',
'subject_in_school', 'vehicle_type', 'website', 'zip_code']
"""

from sample import get_task2_dfs
from cli import get_ds_path_arg, get_rand_arg, get_sort_arg
from similarity import match_preprocess, match_jacc_avg, S_TYPE, ADJ_DIST, DIST, MIN_DIST, AVG_DIST, COL
from timing import timed
from basic_metadata import map_cols
from ds_reader import get_ds_file_sizes

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, count as spark_count
from pyspark.sql.types import StringType

import random

import pandas as pd

from typing import List, Tuple, Dict

import json

import copy
import os


LABEL_LIST_TA = set([
    "person_name", "business_name", "phone_number", "address", "street_name", "city",
    "neighborhood", "lat_lon_cord", "zip_code", "borough", "school_name", "color",
    "car_make", "city_agency", "area_of_study", "subject_in_school", "school_level",
    "college_name", "website", "building_classification", "vehicle_type",
    "location_type", "park_playground", "other"])


def collect(df: pd.DataFrame, col_key: str, col_val: str) -> Dict[str, List[str]]:
    """
    groupby col_key (df column name as str) and collect col_val (df column name as str) into a list. return dict mapping col_key to the list of col_vals
    """
    return df.astype(str).groupby(
        col_key).agg(
        lambda x: list(x))[col_val].to_dict()


def _default(s_type, col_val):
    return (s_type, col_val, 0.1, 0.1)


def run(gz_paths_cols: List[Tuple[str, str]], ref_set_cols, ref_set_vals):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    if not os.path.isdir('results_similarities'):
        os.makedirs('results_similarities')

    rand = get_rand_arg()
    if rand:
        random.shuffle(gz_paths_cols)
    
    # unzip
    gz_paths: List[str] = [gz_paths for gz_paths, _ in gz_paths_cols]

    to_sort = get_sort_arg()
    if to_sort:
        files_n_sizes = get_ds_file_sizes(gz_paths)
        sizes = [size for f, size in files_n_sizes]
        gz_paths_cols = list(zip(
            *sorted(list(zip(gz_paths_cols, sizes)),
                    key=lambda x: x[1])))[0]  # sort on sizes
        gz_paths: List[str] = [gz_paths for gz_paths, _ in gz_paths_cols]

    # unzip
    cols: List[str] = [cols for _, cols in gz_paths_cols]

    cache_col_name = {}  # {col_name: semantic_type} | may help if cols are repeated across dfs
    cache_col_val = {}  # there are many values repeated in a column so this helps

    # def _match_semantic_cols(col_name):
    #     if not col_name in cache_col_name:
    #         cache_col_name[col_name] = str(match_preprocess(col_name, ref_set_cols)[S_TYPE])
    #     return cache_col_name[col_name] 

    def _match_semantic_vals(col_val, s_type_col):
        """
        stage 1:
        run value matcher ('match_preprocess') on only the matched s_type_col

        if the cutoff not passed (avg distance from column is too high):
            stage 2:
            use heuristics (from manually examining frequent data for each col (ref_set)) to limit the amount of s_type_vals in ref_set_vals to compare to.
            I.e. null is automatically assigned the matched s_type_col
            I.e. check for subtrings, like if 'com' is in the val, then check 'website' s_type_vals for similarity. 'co' is implicitly in 'com' so check business_name as well, etc.
            this is to minimize misclassifications
            place them in 'check' to later build another s_type_vals using only those s_types

            stage 3:
            run 'match_preprocess' again on all s_types except the match s_type_col, or only on the heuristic matches in stage 2 (if they exist (if the heuristic check yielded results))
        
            stage 4:
            check whether the stage 3 result is significantly better than the stage 1 result--by checking whether the avg_dist is some percentage ('IMPROVE_RATIO') better than what it was. If not, assign the val to the matched s_type_col as would happen if the value was null

            stage 5 (doesn't work in spark):
            if the min_dist is less than some similarity cutoff: 'MIN_DIST' (meaning it is sufficiently small) and is larger than some similarity cutoff: 'IDENTICAL_CUTOFF' (meaning it isn't nearly identical to something already in the ref_set) add it to the ref_set. if initial matches are correct, later matches should be more accurate. the ref_set tops out at some sufficient size as to prevent slow down and redundant matching

        all {col_val: s_type} combinations are cached so that identical column values aren't recomputed, and so that spark can assign each to the dataframe by using a udf after they are computed outside of Spark. the cache is cleared after each dataset
        """
        col_val = str(col_val)
        s_type_col = str(s_type_col)

        # print(col_val, s_type_col, {s_type_col: [ref_set_vals[s_type_col]]})
        if not col_val in cache_col_val:
            AVG_CUTOFF = 0.9  # similarity measure worse than this triggers second more general run
            MIN_CUTOFF = 0.65
            IDENTICAL_CUTOFF = 0.10
            IMPROVE_RATIO = 0.5  # second run improved by some percent

            str_col_val = str(col_val).lower()
            # print(str_col_val)
            if str_col_val == 'null' or str_col_val == '-' or str_col_val == '0' or str_col_val == 'none' or str_col_val == '' or col_val is None:
                res_final = (s_type_col, col_val, 0.0, 0.0)  # default to s_type_col
            else:
                res0 = match_preprocess(col_val, {s_type_col: ref_set_vals[s_type_col]}, match_jacc_avg)  # compare to values of matched (based on col_name) semantic type
                # print('res0:', res0)
                # res0[MIN_DIST] != 0.0
                if AVG_CUTOFF < res0[AVG_DIST] or res0 is None:  # was the cutoff passed, i.e. was the value present for this semantic type based on the col_name match?
                    # check only these semantic types based on the content of the col_val (more explicit rules after examining data)
                    check = []
                    if len(str_col_val) == 1 and str_col_val.isalpha():
                        possibles = ['person_name (middle_initial)', 'borough']
                        for pos_s_type in possibles:
                            if s_type_col == pos_s_type:  # which of these is the s_type of the col?
                                check.extend([pos_s_type])
                                break
                    if len(str_col_val) == 2 and str_col_val.isalpha():
                        check.extend(['color'])
                    if len(str_col_val) >= 3:  # can have numbers and other chars
                        if 'llc' in str_col_val or 'inc' in str_col_val or 'co' in str_col_val:
                            check.extend(['business_name'])
                        if 'http' in str_col_val or 'www' in str_col_val or 'org' in str_col_val or 'com' in str_col_val:
                            check.extend(['website'])
                    if len(str_col_val) == 5 and str_col_val.isdigit():
                        check.extend(['zip_code'])
                    if len(str_col_val) >= 6 and 'school' in str_col_val:
                        check.extend(['city_agency', 'street_number',
                                    'phone_number', 'building_classification'])
                    if len(str_col_val) >= 3 and str_col_val.isdigit():
                        check.extend(['city_agency', 'street_number',
                                    'phone_number', 'building_classification'])
                    if len(str_col_val) >= 1 and str_col_val.isdigit():
                        check.extend(['street_number'])

                    # if len(check) > 0:
                    #     print('check:', check)

                    check = list(set(check))
                    
                    if len(check) == 0:
                        # compare to every semantic type but already checked
                        ref_set_diff = copy.deepcopy(ref_set_vals)  # clone
                        for key, val in ref_set_cols.items():  # compare to column names as well (for ms_core)
                            ref_set_diff[key].extend(copy.deepcopy(val))
                    else:
                        # compare to only those in check
                        ref_set_diff = {}
                        for s_type in check:
                            ref_set_diff[s_type] = copy.deepcopy(ref_set_vals[s_type])
                    ref_set_diff[s_type_col] = []  # prevent key error and delete all values for already matched

                    res1 = match_preprocess(col_val, ref_set_diff, match_jacc_avg)  # find similarity with other semantic value types
                    
                    if res0 is None and res1 is None:
                        res_final = (s_type_col, col_val, 0.0, 0.0)
                    elif res0 is None:
                        res_final = res1
                    elif res1 is None:
                        res_final = res0
                    else: # neither are None
                        res_final = min([res0, res1], key=lambda x: x[AVG_DIST])

                    # if AVG_CUTOFF < res_final[AVG_DIST]:  # still greater than cutoff and therefore unknown
                    if not (res_final[AVG_DIST] < res0[AVG_DIST] * (1 - IMPROVE_RATIO)):  # dist has not improved
                        res_final = _default(s_type_col, col_val)  # default to s_type_col
                        # ^ should the distance be non-0 to add to ref_set?
                else:
                    # print('FALSE')
                    res_final = res0  # cutoff passed, return initial result
                
            # not an exact match and up to n different values stored
            if res_final[MIN_DIST] <= MIN_CUTOFF and res_final[MIN_DIST] >= IDENTICAL_CUTOFF and len(ref_set_vals[res_final[S_TYPE]]) < 100:
                ref_set_vals[res_final[S_TYPE]].append(col_val)  # append to ref_set
            cache_col_val[col_val] = str(res_final[S_TYPE])
            # print('res_final:', res_final)

        return cache_col_val[col_val]
        

    # match_semantic_cols = udf(_match_semantic_cols, StringType())
    match_semantic_vals = udf(_match_semantic_vals, StringType())

    master_dct = {}
    def _run(df, i):
        print("col_name:", cols[i])

        col = None

        match_col = match_preprocess(cols[i], {'foo': df.columns})  # match the col from ta name to ds cols name
        if match_col is not None:
            col = match_col[COL]
        else:  # shouldn't exec
            raise Exception(f'{cols[i]} not matched in {str(df.columns)}')

        df_cols = map_cols(df.select(col))  # filter single col
        # df_cols = df_cols.sample(0.5, seed=3).limit(500)  # TEST

        if not col in cache_col_name:  # currently uneccessary since cache_col_name is cleared after every ds
            cache_col_name[col] = match_preprocess(col, ref_set_cols)[S_TYPE]  # match col to s_type
        s_type_col = cache_col_name[col]

        print('s_type_col:', s_type_col)
        print('ref_set_vals[s_type_col]:', ref_set_vals[s_type_col])

        df_cols = df_cols.withColumn('s_type_col', lit(s_type_col))  # populate df with col s_type
        
        # if i < 35: # run on small datasets (before it gets slow)
        s_types_all = []
        ### Python method: no spark to add to ref_set_vals
        for row in df_cols.select('value', 's_type_col').collect():
            s_type_i = _match_semantic_vals(row['value'], row['s_type_col'])
            s_types_all.append(s_type_i)
        # get (s_type, count)
        s_types_distinct = sc.parallelize(s_types_all).countByValue().items()
        ###

        # the below udf call just pulls out the s_types from the cache
        df_cols = df_cols.withColumn('s_type_val', match_semantic_vals('value', 's_type_col'))  # match uknown col value to semantic type
        df_test = df_cols.groupby('s_type_col', 'value', 's_type_val').count()
        df_test = df_test.sort('count', ascending=False)
        df_test.filter('s_type_val != s_type_col').show(25)
        df_test.show(25)
        # results = [str(list(row.asDict().values())) + '\n' for row in df_test.collect()]
        # print(results[:10])
        # with open('results_similarities/test.txt', '+a') as f:
        #     for s in results:
        #         f.write(s)

        ds_dict = {
            'column_name': col,
            'semantic_types': []
        }
        for s_type, count in s_types_distinct:
            if s_type in LABEL_LIST_TA:
                ds_dict['semantic_types'].append({
                    'semantic_type': s_type,
                    'count': count
                })
            else:
                ds_dict['semantic_types'].append({
                    'semantic_type': 'other',
                    'label': s_type,
                    'count': count
                })
        if gz_paths[i] not in master_dct:
            master_dct[gz_paths[i]] = {}
        master_dct[gz_paths[i]].update({col: ds_dict})

        print('gz_paths[i]:', {gz_paths[i]: master_dct[gz_paths[i]]})

        with open("results_similarities/master_dct.json", "w") as json_file:
            json.dump(master_dct, json_file, indent=4)

        cache_col_name.clear()
        cache_col_val.clear()


    timed(_run, gz_paths)


if __name__ == '__main__':
    ds_path = get_ds_path_arg()
    if ds_path is None:
        print('No base ds path')
        exit()
    gz_paths_cols: List[Tuple[str, str]] = get_task2_dfs(ds_path)
    # print(gz_paths_cols[:10])

    ref_set_cols = collect(
        pd.read_csv('results_reference_sets/reference_set_columns_done.csv'),
        'true_label', 'col_name_ta')
    # print(list(ref_set_cols.items())[:10])

    ref_set_vals = collect(
        pd.read_csv('results_reference_sets/reference_set_values_done.csv'),
        'true_label', 'frequent_value')
    # print(list(ref_set_vals.items())[:10])

    run(gz_paths_cols, ref_set_cols, ref_set_vals)
