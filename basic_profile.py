''' 
    The module works on a dataset, returning the formatted dictionary of 
    data type in the dictionary for each column 

    return: dataset_dict -> Dictionary with the metadata for each column {column_name: {datatypes: {attributes}}}
            frequent_itemsets -> return the itemset for that dictionary, for instance:
                                { "-REAL-DATE-INTEGER-TEXT-": 1, "-REAL-DATE-INTEGER-": 2, "DATE":1 ... }
                                suggesting there was one column with real, date, integer and text values
                                two columns with real, date and integer and 
                                one with just datetime values

    @vincent: I am creating a different function to accept these responses 
    for each dataset. If you could add it to your code "basic_metadate.py" 
    and call it for each dataset we profile i.e. when you get the value of 
    frequent_itemsets back

    For the timing module, you can call the "driver" function with "timed" from your main function. 
    I'm not adding it over here since all this gets run from your "basic_metadata"

'''

import sys
from pyspark.sql import SparkSession, DataFrame, Column, Row
from pyspark import SparkContext
from pyspark.sql.functions import countDistinct, create_map, lit, col, explode, count, when, isnan, isnull, sum, collect_list, udf
from pyspark.sql.types import ArrayType, StringType
import time
import pyspark
import json
from dateutil.parser import parse, parserinfo
from dateutil import parser
import re

from datetime import datetime
from compile_itemset import combine_itemsets, most_frequent_itemsets


def checkReal(element):
    '''
        Check if element is a float ie. an optional sign followed by optional digits, a dot and digits
        Args: Value of the row
        Return:
        Bool -> True if element is a floating point, otherwise False
    '''
    try:
        reFloat = "^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$"
        if re.search(reFloat, element):  # if element is real (float)
            try: 
                value = float(element)
                if value:
                    return True
                else:
                    return False
            except:
                pass
    except:
        # print("Failed to match real element: {}".format(element))
        pass
    return False



def checkText(element):
    try:
        if str(element):
            return True
    except:
        print("Conversion to string failed for value: {}".format(element))
        return False




def checkColName(col_name):
    '''
    Check if the column name has any reference indicating 
    that the value might be a date
    '''
    col_name = col_name.lower()
    checkList = ["date", "period", "year", "month", "day", "time", "interval"] 
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
        dateObj = datetime.strptime(element, '%Y%m') # 201911
    except:
        pass
    if dateObj:
        if dateObj.year > 2019 or dateObj.year < 1850: #188 datetime(0000, 8, 18)
            return False
        else:
            return dateObj
    return False


def checkInt(element):
    '''
        Check if an element is integer
        Regex: start to end, begin with +/- (optional)
        followed by digits 0 to 9 def checkDate(element):
    '''
    try: 
        element = element.replace(",", "")
        element = element.replace("$", "")
        value = int(element)
        if value:
            return True
    except:
        pass
    try:
        element = element.replace(",", "")
        element = element.replace("$", "")
        reInt = "^[-+]?[0-9]*$"
        if re.search(reInt, element):
            return True
    except:
        return False
    return False



def get_col_array(col):
    col_arr = col.collect()
    arr = [x.value for x in col_arr]
    return arr



def sort_dates(arr, L):
    ''' Get max and min date '''
    max_date = arr[0]
    min_date = arr[0]
    for i in range(0,L,1):
        try:
            if arr[i] > max_date:
                max_date = arr[i]
            if min_date > arr[i]:
                min_date = arr[i]
        except:
            pass
    return [min_date, max_date]



def get_analysis(sc, col_dict):
    '''
        Returns a dictionary with the desired statistics 
        for a given column if a particular datatype was seen in the 
        was seen in the column

    '''
    stats_dict = {}
    if "INTEGER" in col_dict.keys():
        int_rdd = sc.parallelize(col_dict["INTEGER"])
        temp_dict = {"type": "INTEGER"}
        int_stats = int_rdd.stats()
        int_count, int_max, int_min, int_mean, int_std = type_counts["INTEGER"], int_stats.max(), int_stats.min(), int_stats.mean(), int_stats.stdev()
        temp_dict["count"] = int_count
        temp_dict["max_value"] = int_max
        temp_dict["min_value"] = int_min
        temp_dict["mean"] = int_mean
        temp_dict["stddev"] = int_std
        stats_dict["INTEGER"] = temp_dict
    if "REAL" in col_dict.keys():
        float_rdd = sc.parallelize(col_dict["REAL"])
        temp_dict = {"type": "REAL"}
        real_stats = float_rdd.stats()
        real_count, real_max, real_min, real_mean, real_std = real_stats.count(), real_stats.max(), real_stats.min(), real_stats.mean(), real_stats.stdev()
        temp_dict["count"] = real_count
        temp_dict["max_value"] = real_max
        temp_dict["min_value"] = real_min
        temp_dict["mean"] = real_mean
        temp_dict["stddev"] = real_std
        stats_dict["REAL"] = temp_dict
    if "DATE/TIME" in col_dict.keys():
        dates = list(col_dict["DATE/TIME"])
        temp_dict = {"type": "DATE/TIME"}
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
        temp_dict = {"type": "TEXT"}
        text_count = text_rdd.count()
        text_sort = text_rdd.map(lambda x: [x, len(x)]).sortBy(lambda x: x[1])
        text_distinct = text_rdd.distinct().map(lambda x: [x, len(x)]).sortBy(lambda x: x[1]).collect()
        text_top5, text_low5 = [], []
        if len(text_distinct) >= 5: 
            for i, j in text_distinct[-5:]:
                text_top5.append(i)
            for i, j in text_distinct[:5]:
                text_low5.append(i)
        else: #  in case the column has less than  5 distinct values
            for i, j in text_distinct:
                text_low.append(i)
            for i, j in text_distinct[::-1]: # to maintain the order even if there are less than 5 elements
                text_top5.append(i)
        text_total_len = text_sort.map(lambda x: x[1]).sum()
        temp_dict["count"] = text_count
        temp_dict["shortest_values"] = text_low5
        temp_dict["longest_values"] = text_top5
        temp_dict["average_length"] = text_total_len/text_count
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


def get_dataset_profile(spark, df_cols): 
    columns = df_cols.select("key").distinct().collect() # returns a list of columns as row objects
    columns = [x.key for x in columns] # get the actual value (column name) from the list of row objects
    dataset_dict = {}
    dataset_itemset = {}
    for column in columns:
        column_info = {}
        total_counts = {}
        name = column
        # print(name)
        temp_df = df_cols.filter(col("key")==name).select("value") # remove these values from the DF # also sorting! 
        # temp_df.show()
        # df_cols = df_cols.subtract(temp_df) # tried this. Gave a time of 50 secs to 140 secs excluding load time
        # temp_df = temp_df.select("value")
        col_values = get_col_array(temp_df)
        #c_list = ['null', 'None', 'N/A', '--', '-', '---', 'none', 'nan', 'NAN', 'No Data']
        #checkList = set(c_list)
        item_set = ""
        flag_date, flag_real, flag_int, flag_text = [False]*4
        # for every value in the column, see which Dtype fits the best
        for value in col_values: 
            # check if the value is a datetime -> float -> int 
            # otherwise, it's considered text
            # not changing the StructType of the DF, just monitoring
            #if str(value) in checkList:
                #continue
            if checkReal(value):
                value = float(value) # add try
                if "REAL" in column_info:
                    column_info["REAL"].append(value)
                else: 
                    column_info["REAL"] = [value]
                    if not flag_real:
                        flag_real= True
            elif checkColName(column): # time period interval date 
                date = checkDate(value)
                if date:
                    if "DATE/TIME" in column_info:
                        column_info["DATE/TIME"].append(date)
                    else:
                        column_info["DATE/TIME"] = [date]
                        if not flag_date:
                            flag_date = True
            elif checkInt(value):
                value = int(value)
                if "INTEGER" in column_info:
                    column_info["INTEGER"].append(value)
                else:
                    column_info["INTEGER"] = [value]
                    if not flag_int:
                        flag_int = True
            elif checkText(value):
                #print("IN TEXT")
                value = str(value)
                if "TEXT" in column_info:
                    column_info["TEXT"].append(value)
                else: 
                    column_info["TEXT"] = [value]
                    if not flag_text:
                        flag_text = True
        dataset_dict[column] = get_analysis(spark.sparkContext, column_info)
        if flag_real:
            item_set += "-REAL-"
        if flag_date:
            item_set += "-DATE-"
        if flag_int: 
            item_set += "-INTEGER-"
        if flag_text:
            item_set += "-TEXT-"
        if item_set in dataset_itemset:
            dataset_itemset[item_set] += 1
        else: 
            dataset_itemset[item_set] = 1
    return dataset_dict, dataset_itemset


# Testing
def test():
    master_itemset = {}
    for i in range(2):
        start = time.time()
        df = spark.read.csv("sampledf{}.csv".format(i)).withColumnRenamed("_c0", "key").withColumnRenamed("_c1", "value")
        dataset_dict, dataset_itemset = get_dataset_profile(df)
        print("Result for df{}: ".format(i), dataset_dict)
        print("Itemset for df{}: ".format(i), dataset_itemset)
        master_itemset = frequent_itemsets(dataset_itemset, master_itemset)
        print("Master itemset: ", master_itemset)
        print("Time taken for dataset: ", time.time() - start)
    common_itemset = most_frequent_itemsets(master_itemset)
    print("Most Frequent Itemset: ", common_itemset)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc  = spark.sparkContext
    # test()


