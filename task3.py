import sys
import time
import json
from pyspark import SparkSession
 
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, col, udf

def checkInt(element):
    '''
        Check if an element is integer
        Regex: start to end, begin with +/- (optional)
        followed by digits 0 to 9 def checkDate(element):
    '''
    try: 
        value = int(element)
        if value:
        	if len(str(value)) == 5:
        		return "True"
    except:
        pass
    try:
        reInt = "^[-+]?[0-9]*$"
        if re.search(reInt, element):
            return "True"
    except:
        pass
    return "False"


def get_df_for_year(df, year):
	df_year = df.filter(col("Created Date").like("%/{}%".format(year)))
	return df_year 

def get_df_for_borough(df, borough):
	df_borough = df.filter(col("Borough")==borough)
	return df_borough

def get_variation_with_years(df_years):
	for year, df_year in df_years.items():
		df = df_year.groupBy(["Complaint Type"]).count().orderBy("count", ascending=False)
		df.show(5)
		df = df.select("Complaint Type", "count")
		df.write.csv("/user/hsc367/task3-year-{}.csv".format(year))
		print("File saved to: /user/hsc367/task3-year-{}.csv".format(year))

def get_variation_with_neighborhood(df_years):
	for year, df_year in df_years.items():
		df = df_years.filer(col("Zip")=="True")
		df = df.groupBy(["Zip Code", "Complaint Type"]).count().orderBy("count", ascending=False)
		df.show(5)
		df.write.csv("/user/hsc367/task3-neighborhoods-{}.csv".format(year))
		print("File saved to: /user/hsc367/task3-neighborhoods-{}.csv".format(year))


def get_variations_with_borough(df_boroughs):
	for borough, df in df_boroughs.items():
		df = df.groupBy(["Complaint Type"]).count().orderBy("count", ascending=False)
		df.show(5)
		df = df.select("Complaint Type", "count")
		df.write.csv("/user/hsc367/task3-year-{}.csv".format(year))
		print("File saved to: /user/hsc367/task3-year-{}.csv".format(year))

if __name__ == "__main__":

	# initiate spark and spark context

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	# start time 
	start_time = time.time()

	# import the dataset
	_311_reports_df = spark.read.csv("/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz", header='True', inferSchema='True', sep='\t')
	df = _311_reports_df 

	df_years = {}
	df_boroughs = {}
	checkZipCode = udf(checkInt, StringType())

	# filter and keep only attributes that we need for our analysis
	# [type of compliant, compliant id, date open, date close, response time*, borough, zipcode, neighborhood, ]
	df = df.select("Unique Key", "Created Date", "Closed Date", "Complaint Type", "Incident Zip", "Status", "Borough", "X Coordinate (State Plane)", "Y Coordinate (State Plane)", "Latitude", "Longitude", "Location")
	df = df.withColumn("Zip", checkZipCode("Incident Zip"))

	# divide the dfs into years and save filter/groupby operations later
	for i in range(2010, 2020, 1):
		df_years[i] = get_df_for_year(df, i)

	# similarly for boroughs 
	borough = df.select("Borough").distinct().collect()
	boroughs = []
	for element in borough:
		boroughs.append(element.borough)

	for element in boroughs:
		df_boroughs[element] = get_df_for_borough(df, borough)

	# variation with years 
	get_variation_with_years(df_years)

	# variation with neighborhoods
	get_variation_with_neighborhood(df_years)

	# variation with boroughs
	get_variation_with_boroughs(df_boroughs)

	# correlation with filimg locations 
		# get zipcodes with most filimg 
		# get response time in those zipcodes

	# correlation with tourist attrations


	# end time
	print("Time taken: ", time.time() - start)

