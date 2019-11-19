'''  
Problems:
1. Making use of the space wisely and keeping the time complexity low. Even if it O(n), i wanted to keep it down in
terms of 2n or 3n instead of say 5n
2. Managing additional space. 
    I tried to not use dictionaries add elements to different DFs according to their datatype, but that process was  extremly slow
    I was using union to add new values to an existing DF

    For example: checkReal looked something like

    if checkReal(value):
        if "REAL" in col_types:
            # column_info["real"].append(float(value))
            temp_df = spark.createDataFrame([[float(value)]], ["REAL"])
            real_df = real_df.union(temp_df)
        else:
            real_df = spark.createDataFrame([[float(value)]], ["REAL"])
            col_types.add("REAL")

    this was taking ages, so I went back to the hashmap

'''

''' YET TO FIX: The code breaks at the point where I try to calculate the mean of intRdd
        I have a feeling I'm making a really stupid mistake somewhere or overlooking something. But its 6 AM and I need 
        to get ready for work now :p

        Will fix this tonight!
'''

# !/usr/bin/env python


def checkDate(element):
    # check if the element is date/time
    # convert it into a standard format and save it to the dict
    from dateutil.parser import parse
    print("Check Date")
    dateObj = parse(element, fuzzy=True)

    if dateObj:
        # parse the output as a readible date/time for min max values
        return True

    return False

def checkReal(element):
    #print("Check Real")
    '''
        Check if element is a float ie. an optional sign followed by optional digits, a dot and digist
        Args: Value of the row
        Return:
        Bool -> True if element is a floating point, otherwise False
    '''
    import re

    reFloat = "[-+]?[0-9]*\.[0-9]+([eE][-+]?[0-9]+)?"
    if re.search(reFloat, element):  # if element is real (float)
        return True

    return False


def checkInt(element):
    # print("Check Int")
    import re

    reInt = "[-+]?[0-9]*"
    if re.search(reInt, element):
        return True

    return False


def validate(df, col_name):
    '''
        Args:
            df: DataFrame which is a single column of the Dataset
            col_name: Column Name

        Returns:

            A nested dictionary with key as the col_name and
            value as a as another dictionary.

            Difference between all values and distinct values

        Structure of returned dict:

        {
            "col_name":
            {
                    "number_of_empty_cells": int,
                    "number_of_distinct_values": int,
                    "frequent_values": [list of top-5 frequent values in the col in desc],
                    "data-types": [list of dicts for each type
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
                    ],
                    ...semantic ...
            }
        }

    Logical flow:
        1. Check if value is a Real.
        2. If not float, check Integer
        3. If not int, check Date
        4. Otherwise, Text.

    Selecting primary key: (Extra)
        Look at the difference between total_count and distinct count
    '''

    column_info = {}
    formatted_output = {}
    col_types = set()

    probable_nan_values = ["NAN", "N/A", "n/a", "na", "NaN", "None", "none", "NONE", "", " ", "blankspace", "blank",
                           "BLANKSPACE", "BLANK", "-", "--", "---"]
    checkList = set(probable_nan_values)

    col_name = df.columns[0]

    count_total = df.select("*").count()

    # distinct values
    count_distinct = df.select("*").distinct().count()
    diff_total_distinct = count_total - count_distinct  # the lower this number, the more unique the values in col

    # empty_cells: Clarify this
    count_empty = 0

    # frequent values

    temp = df.groupBy(col_name).count().alias("count").orderBy("count", ascending=False).limit(5).rdd.flatMap(
        lambda x: x).collect()  # MAKE THIS FASTER

    frequent = [temp[i] for i in range(0, len(temp), 2)]  # this takes literally no  extra space or time

    # add everything found so far to the dict formatted_output

    formatted_output["number_of_empty_cells"] = None  # clarify this
    formatted_output["number_of_distinct_values"] = count_distinct
    formatted_output["frequent_values"] = frequent
    formatted_output["data_type"] = {}

    for row in df.rdd.toLocalIterator():
        value = row.__getitem__(col_name)
        if value in checkList:
            count_empty = count_empty + 1

        elif checkReal(value):
            if "real" in column_info:
                column_info["real"].append(float(value))
            else:
                column_info["real"] = [float(value)]
                col_types.add("REAL")

        elif checkInt(value):
            if "int" in column_info:
                column_info["int"].append(int(value))
            else:
                column_info["int"] = [int(value)]
                col_types.add("INTEGER")

        elif checkDate(value):
            if "date" in column_info:
                #column_info["date"].append("...") #format the string to get the max min
                # Challenge: What if there  is not year?
                pass
            else:
                #column_info["date"] = [...]
                col_types.add("DATE/TIME")

        else:
            if "text" in column_info:
                column_info["text"].append(str(value))
            else:
                column_info["text"] = [str(value)]
                col_types.add("TEXT")


    print(column_info.keys(), col_types)

    if "INTEGER" in col_types:
        print("Formulating Int")

        intRDD = spark.createDataFrame(column_info["int"], LongType()).rdd
    
        intLen = intRDD.count()
        print(intLen)

        intMax = intRDD.max()
        intMin = intRDD.min()
        intMean = intRDD.mean() # THIS IS WHERE THE CODE BREAKS. 
        intStdev = intRDD.stdev()

        formatted_output["data_type"]["INTEGER (LONG)"] = {}

        formatted_output["data_type"]["INTEGER (LONG)"]["count"] = intLen
        formatted_output["data_type"]["INTEGER (LONG)"]["max_value"] = intMax # i'm thinking i'll make these inline. Keep a check on the min/max value while i go over the values above
        formatted_output["data_type"]["INTEGER (LONG)"]["min_value"] = intMin
        formatted_output["data_type"]["INTEGER (LONG)"]["mean"] = intMean  
        formatted_output["data_type"]["INTEGER (LONG)"]["stddev"] = intStdev


    print(formatted_output)

    if "REAL" in col_types:
        print("Formulating Real")
        realRDD = spark.createDataFrame(column_info["real"], DecimalType()).rdd

        realLen = realRDD.count()
        realMax = realRDD.max()
        realMin = realRDD.min()
        realMean = realRDD.mean() # TypeError: unsupported operand type(s) for -: 'Row' and 'float'
        realStdev = realRDD.stdev()

        formatted_output["data_type"]["REAL"] = {}

        formatted_output["data_type"]["REAL"]["count"] = realLen
        formatted_output["data_type"]["REAL"]["max_value"] = realMax # same with floating numbers. Saves 4 scans.
        formatted_output["data_type"]["REAL"]["min_value"] = realMin
        formatted_output["data_type"]["REAL"]["mean"] = realMean
        formatted_output["data_type"]["REAL"]["stddev"] = realStdev

    if "DATE/TIME" in col_types:
        pass  # get the count, max and min

    if "TEXT" in col_types:
        print("Formulating TEXT")

        textRDD = spark.createDataFrame(column_info["text"], StringType()).rdd

        textLen = textRDD.count()  # works

        # 5 shortest elems
        shortest = df.sortBy(lambda x: len(x), ascending=True).take(5)

        # 5 longest elems
        longest = df.sortBy(lambda x: len(x), ascending=False).take(5)

        # average length
        textAvgLength = df.map(lambda x: len(x)).sum()
        textAvgLength /= textLen

        formatted_output["data_type"]["TEXT"] = {}

        formatted_output["data_type"]["TEXT"]["count"] = textLen
        formatted_output["data_type"]["TEXT"]["shortest_values"] = shortest
        formatted_output["data_type"]["TEXT"]["longest_values"] = longest
        formatted_output["data_type"]["TEXT"]["average_length"] = textAvgLength

    formatted_output["number_of_empty_cells"] = count_empty

    return formatted_output, col_types


def driver(df, df_name):
    statistics = {df_name: {}}
    frequent_itemsets = []

    columns = df.columns
    num_cols = len(columns)

    if num_cols == 0:
        return {df_name: {}}

    for col in columns:
        print(col, "is being processed")
        temp_df = df.select(col)  # get one col at a time
        col_stats, col_types = validate(temp_df, col)
        print(col_types)

        statistics[df_name][col] = col_stats
        col_types = list(col_types)

        frequent_itemsets.append(col_types)

    return statistics, frequent_itemsets


if __name__ == '__main__':
    import sys
    import pyspark
    import json
    import time

    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql import SQLContext

    from pyspark.sql.functions import *

    from pyspark.sql.types import StructType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, LongType, DecimalType

    # import findspark
    # findspark.init()

    sc = SparkContext()

    spark = SparkSession \
        .builder \
        .appName("DataProfile") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    start = time.time()

    # load the sample df
    df = spark.read.csv("/user/ecc290/HW1data/parking-violations.csv")
    df_name = "parking-violations"
    print(type(df))
    # get the ane
    # pass the sample df
    statistics, frequent_itemsets = driver(df, df_name) # use frequent itemsets for .. well frequent itemsets

    print(time.time() - start)

    # check the output
    print(statistics)

    # output_json = json.dumps(statistics)

    # with open('/usr/hsc367/trial.json', 'w') as json_file:
    #     json.dump(statistics, json_file)

    # return the output
    # return statistics


