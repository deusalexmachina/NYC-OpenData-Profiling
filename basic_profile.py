import sys
from pyspark.sql import SparkSession, DataFrame, Column, Row
from pyspark.sql.functions import countDistinct, create_map, lit, col, explode, count, when, isnan, isnull, sum, collect_list, udf
from pyspark.sql.types import ArrayType, StringType
import time
import pyspark
import json
from dateutil.parser import parse, parserinfo
from dateutil import parser
import re
from datetime import datetime

# class customParserInfo(parserinfo):
#     pass


def checkReal(element):
    '''
        Check if element is a float ie. an optional sign followed by optional digits, a dot and digist
        Args: Value of the row
        Return:
        Bool -> True if element is a floating point, otherwise False
    '''
    reFloat = "[-+]?[0-9]*\.[0-9]+([eE][-+]?[0-9]+)?"
    try:
        if re.search(reFloat, element):  # if element is real (float)
            return True
    except:
        print(element)
        pass
    return False



def checkColName(col_name):
    col_name = col_name.lower()
    checkList = ["date", "period", "year", "month", "day", "time"]
    for elem in checkList:
        if elem in col_name:
            return True
    return False



def checkDate(element):
    # check if the element is date/time
    dateObj = None
    try: 
        dateObj = parse(element, fuzzy=True)
    except: 
        pass
    try:
        dateObj = datetime.strptime(element, '%Y%m')
    except:
        pass
    if dateObj:
        if dateObj.year > 2019 or dateObj.year < 1850:
            return False
        else:
            return dateObj
    return False



def checkInt(element):
    reInt = "^[-+]?[0-9]*$"
    if re.search(reInt, element):
        return True
    return False



def get_col_array(col):
    col_arr = col.collect()
    arr = [x.value for x in col_arr]
    return arr


def sort_dates(arr, L):
    for i in range(0,L,1):
        for j in range(0, L-i-1, 1):
            if arr[j] > arr[j+1]: 
                arr[j], arr[j+1] = arr[j+1], arr[j]
    return arr


def get_analysis(col_dict):
    stats_dict = {}
    if "INTEGER" in col_dict.keys():
        int_rdd = sc.parallelize(col_dict["INTEGER"])
        temp_dict = {}
        int_stats = int_rdd.stats()
        int_count, int_max, int_min, int_mean, int_std = int_stats.count(), int_stats.max(), int_stats.min(), int_stats.mean(), int_stats.stdev()
        temp_dict["count"] = int_count
        temp_dict["max_value"] = int_max
        temp_dict["min_value"] = int_min
        temp_dict["mean"] = int_mean
        temp_dict["std"] = int_std
        stats_dict["INTEGER"] = temp_dict
    if "REAL" in col_dict.keys():
        float_rdd = sc.parallelize(col_dict["REAL"])
        temp_dict = {}
        real_stats = float_rdd.stats()
        real_count, real_max, real_min, real_mean, real_std = real_stats.count(), real_stats.max(), real_stats.min(), real_stats.mean(), real_stats.stdev()
        temp_dict["count"] = real_count
        temp_dict["max_value"] = real_max
        temp_dict["min_value"] = real_min
        temp_dict["mean"] = real_mean
        temp_dict["std"] = real_std
        stats_dict["REAL"] = temp_dict
    if "DATE/TIME" in col_dict.keys():
        dates = col_dict["DATE/TIME"]
        temp_dict = {}
        count = len(dates)
        dates = sort_dates(dates, count)
        max_date = dates[-1]
        min_date = dates[0]
        str_max_date = max_date.strftime("%m/%d/%Y, %H:%M:%S")
        str_min_date = min_date.strftime("%m/%d/%Y, %H:%M:%S")
        temp_dict["count"] = count
        temp_dict["max_value"] = str_max_date
        temp_dict["min_value"] = str_min_date
        stats_dict["DATE/TIME"] = temp_dict
    if "TEXT" in col_dict.keys():
        # print("Text Found")
        text_rdd = sc.parallelize(col_dict["TEXT"])
        temp_dict = {}
        text_count = text_rdd.count() 
        text_sort = text_rdd.map(lambda x: [x, len(x)]).sortBy(lambda x: x[1])
        text_len = text_sort.collect()
        text_top5, text_low5 = [], []
        for i, j in text_len[-5:]:
            text_top5.append(i)
        for i, j in text_len[:5]:
            text_low5.append(i)
        text_avg_len = text_sort.map(lambda x: x[1]).sum()
        temp_dict["count"] = text_count
        temp_dict["shortest_values"] = text_low5
        temp_dict["longest_values"] = text_top5
        temp_dict["average_length"] = text_avg_len/text_count
        stats_dict["TEXT"] = temp_dict
    return stats_dict

    
  

'''
        Accepts the dataframe with two columns (keys: column name and value: row value)
        Returns a dictionary with the desired statistics
        "data-types":
        {
            "type": data_type, (for real and integers)
            "count": int,
            "max_value": real,
            "min_value": real,
            "mean": real,
            "stddev": real

        },
        {
            "type": "DATE/TIME",
            "count": int,
            "max_value": max Date/time,
            "min_value": the min date/time

        },
        {
            "type": "Text",
            "count": int,
            "shortest_values": [list of top-5 shortest values],
            "longest_values": [list of top-5 longest values],
            "average_length": float

        }
    '''
 # get the column names from the original df
    # NOTE: @vincent This can be skipped if you pass me the df.columns
            # All I need is a way to iterate over the values

def driver(df_cols): 
    start = time.time()
    columns = df_cols.select("key").distinct().collect()
    columns = [x.key for x in columns]
    master_dict = {}
    for column in columns:
        column_info = {}
        name = column
        # print(name)
        temp_df = df_cols.filter(col("key")==name).select("value")
        # temp_df.show()
        col_values = get_col_array(temp_df)
        c_list = ['null', 'None', 'N/A', '--', '-', '---', 'none', 'nan', 'NAN']
        checkList = set(c_list)
        # for every value in the column, see which Dtype fits the best
        for value in col_values: 
            # check if the value is a datetime -> float -> int 
            # otherwise, it's considered text
            # not changing the StructType of the DF, just monitoring
            if str(value) in checkList:
                continue
            if checkReal(value):
                value = float(value)
                if "REAL" in column_info:
                    column_info["REAL"].append(value)
                else: 
                    column_info["REAL"] = [value]
            elif checkColName(column):
                date = checkDate(value)
                if date:
                    if "DATE/TIME" in column_info:
                        column_info["DATE/TIME"].append(date)
                    else:
                        column_info["DATE/TIME"] = [date]
            elif checkInt(value):
                value = int(value)
                if "INTEGER" in column_info:
                    column_info["INTEGER"].append(value)
                else:
                    column_info["INTEGER"] = [value]
            elif str(value):
                print("IN TEXT")
                if "TEXT" in column_info:
                    column_info["TEXT"].append(str(value))
                else: 
                    column_info["TEXT"] = [str(value)]
        master_dict[column] = get_analysis(column_info)
    end = time.time()
    print(end-start)
    print(master_dict)
    return master_dict
