# Databricks notebook source
# MAGIC %md
# MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 2
# MAGIC
# MAGIC This exercise contains basic tasks of data processing using Spark and DataFrames. The tasks can be done in either Scala or Python. This is the **Python** version, switch to the Scala version if you want to do the tasks in Scala.
# MAGIC
# MAGIC Each task has its own cell for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with example outputs or test code following most of the tasks.
# MAGIC
# MAGIC Don't forget to submit your solutions to Moodle.

# COMMAND ----------

# some imports that might be required in the tasks

from typing import List

from pyspark.sql import functions
from pyspark.sql import DataFrame
from pyspark.sql import Row


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Create DataFrame
# MAGIC
# MAGIC As mentioned in the tutorial notebook, Azure Storage Account and Azure Data Lake Storage Gen2 are used in the course to provide a place to read and write data files.
# MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) in the `exercises/ex2` folder is file `rdu-weather-history.csv` that contains weather data in CSV format.
# MAGIC The direct address for the data file is: `abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex2/rdu-weather-history.csv`
# MAGIC
# MAGIC Read the data from the CSV file into DataFrame called weatherDataFrame. Let Spark infer the schema for the data.
# MAGIC
# MAGIC Print out the schema.
# MAGIC Study the schema and compare it to the data in the CSV file. Do they match?
# MAGIC

# COMMAND ----------

file = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex2/rdu-weather-history.csv"
weatherDataFrame: DataFrame = spark.read.option("header","true").option("inferSchema","true").option("sep",",").csv(file)

# code that prints out the schema for weatherDataFrame
#display(weatherDataFrame)
weatherDataFrame.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 1 (only the first few lines):
# MAGIC
# MAGIC ```text
# MAGIC root
# MAGIC  |-- date: date (nullable = true)
# MAGIC  |-- temperaturemin: double (nullable = true)
# MAGIC  |-- temperaturemax: double (nullable = true)
# MAGIC  |-- precipitation: double (nullable = true)
# MAGIC  |-- snowfall: double (nullable = true)
# MAGIC  |-- snowdepth: double (nullable = true)
# MAGIC  |-- avgwindspeed: double (nullable = true)
# MAGIC  ...
# MAGIC  ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - The first items from DataFrame
# MAGIC
# MAGIC Fetch the first **five** rows of the weather dataframe and print their contents. You can use the DataFrame variable from task 1.

# COMMAND ----------

weatherSample: List[Row] = weatherDataFrame.take(5)

print(*[list(row.asDict().values()) for row in weatherSample], sep="\n")  # prints each Row to its own line


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 2:
# MAGIC
# MAGIC ```text
# MAGIC [datetime.date(2008, 5, 20), 57.9, 82.9, 0.43, 0.0, 0.0, 10.51, 230, 25.05, 220, 31.99, 'Yes', 'No', 'Yes', 'Yes', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'Yes', 'No', 'Yes', 'No', 'No', 'No']
# MAGIC [datetime.date(2008, 5, 22), 48.0, 78.1, 0.0, 0.0, 0.0, 4.03, 230, 16.11, 280, 21.03, 'Yes', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No']
# MAGIC [datetime.date(2008, 5, 23), 52.0, 79.0, 0.0, 0.0, 0.0, 4.7, 70, 10.07, 100, 14.99, 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No']
# MAGIC [datetime.date(2008, 6, 7), 73.9, 100.0, 0.0, 0.0, 0.0, 5.59, 230, 16.11, 220, 21.92, 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No', 'No']
# MAGIC [datetime.date(2008, 6, 22), 64.9, 87.1, 0.93, 0.0, 0.0, 6.93, 200, 23.04, 200, 29.97, 'Yes', 'No', 'No', 'Yes', 'No', 'No', 'No', 'No', 'No', 'No', 'Yes', 'Yes', 'No', 'Yes', 'No', 'Yes', 'No']
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Minimum and maximum
# MAGIC
# MAGIC Find the minimum temperature and the maximum temperature from the whole data.

# COMMAND ----------

weatherDataFrame.createOrReplaceTempView("weather_Data")
minTemp: float = spark.sql("Select min(temperaturemin) from weather_Data").collect()[0][0]
maxTemp: float = spark.sql("Select max(temperaturemax) from weather_Data").collect()[0][0]

print(f"Min temperature is {minTemp}")
print(f"Max temperature is {maxTemp}")


# COMMAND ----------

if 4.05 < minTemp < 4.15:
    print("correct result: minimum temperature is 4.1 °F (-15,5 °C)")
else:
    print(f"wrong result: {minTemp} != 4.1")

if 105.05 < maxTemp < 105.15:
    print("correct result: maximum temperature is 105.1 °F (40.6 °C)")
else:
    print(f"wrong result: {maxTemp} != 105.1")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Adding a column
# MAGIC
# MAGIC Add a new column `year` to the weatherDataFrame and print out the schema for the new DataFrame.
# MAGIC
# MAGIC The type of the new column should be integer and value calculated from column `date`.
# MAGIC You can use function `functions.year` from `pyspark.sql`
# MAGIC
# MAGIC See documentation: [https://spark.apache.org/docs/3.4.1/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html#pyspark.sql.functions.year](https://spark.apache.org/docs/3.4.1/api/python/reference/pyspark.sql/api/pyspark.sql.functions.year.html#pyspark.sql.functions.year)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import year
weatherDataFrameWithYear: DataFrame = weatherDataFrame.withColumn("year",year("date"))

# code that prints out the schema for weatherDataFrameWithYear
weatherDataFrameWithYear.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 4 (only the last few lines):
# MAGIC
# MAGIC ```text
# MAGIC ...
# MAGIC  |-- highwind: string (nullable = true)
# MAGIC  |-- hail: string (nullable = true)
# MAGIC  |-- blowingsnow: string (nullable = true)
# MAGIC  |-- dust: string (nullable = true)
# MAGIC  |-- year: integer (nullable = true)
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Aggregated DataFrame 1
# MAGIC
# MAGIC Find the minimum and the maximum temperature for each year.
# MAGIC
# MAGIC Sort the resulting DataFrame based on year so that the latest year is in the first row and the earliest year is in the last row.
# MAGIC

# COMMAND ----------

weatherDataFrameWithYear.createOrReplaceTempView("weather_Data")
aggregatedDF: DataFrame = spark.sql("select year, min(temperaturemin) as min_temperature, max(temperaturemax) as max_temperature from weather_Data group by(year) order by year desc")
aggregatedDF.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 5:
# MAGIC
# MAGIC ```text
# MAGIC +----+---------------+---------------+
# MAGIC |year|min_temperature|max_temperature|
# MAGIC +----+---------------+---------------+
# MAGIC |2018|            4.1|           98.1|
# MAGIC |2017|            9.1|          102.0|
# MAGIC |2016|           15.3|           99.0|
# MAGIC |2015|            7.2|          100.0|
# MAGIC |2014|            7.2|           98.1|
# MAGIC |2013|           18.0|           96.1|
# MAGIC |2012|           19.0|          105.1|
# MAGIC |2011|           16.0|          104.0|
# MAGIC |2010|           15.1|          102.0|
# MAGIC |2009|           10.9|           99.0|
# MAGIC |2008|           15.1|          100.9|
# MAGIC |2007|           15.1|          105.1|
# MAGIC +----+---------------+---------------+
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Aggregated DataFrame 2
# MAGIC
# MAGIC Expanding from task 5, create a DataFrame that contains the following for each year:
# MAGIC
# MAGIC - the minimum temperature
# MAGIC - the maximum temperature
# MAGIC - the number of entries (as in rows in the original data) there are for that year
# MAGIC - the average wind speed (rounded to 2 decimal precision)
# MAGIC

# COMMAND ----------

task6DF: DataFrame = spark.sql("select year, min(temperaturemin) as min_temperature, max(temperaturemax) as max_temperature , count(*) as entries , round(avg(avgwindspeed),2) as avg_windspeed from weather_Data group by(year)")

task6DF.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 6:
# MAGIC
# MAGIC ```text
# MAGIC +----+---------------+---------------+-------+-------------+
# MAGIC |year|min_temperature|max_temperature|entries|avg_windspeed|
# MAGIC +----+---------------+---------------+-------+-------------+
# MAGIC |2007|           15.1|          105.1|    365|         6.14|
# MAGIC |2018|            4.1|           98.1|    228|         6.55|
# MAGIC |2015|            7.2|          100.0|    365|         5.44|
# MAGIC |2013|           18.0|           96.1|    365|         5.51|
# MAGIC |2014|            7.2|           98.1|    365|         5.56|
# MAGIC |2012|           19.0|          105.1|    366|         5.41|
# MAGIC |2009|           10.9|           99.0|    365|         6.13|
# MAGIC |2016|           15.3|           99.0|    366|         5.78|
# MAGIC |2010|           15.1|          102.0|    365|         5.49|
# MAGIC |2011|           16.0|          104.0|    365|         5.84|
# MAGIC |2008|           15.1|          100.9|    366|         6.49|
# MAGIC |2017|            9.1|          102.0|    365|         6.25|
# MAGIC +----+---------------+---------------+-------+-------------+
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Aggregated DataFrame 3
# MAGIC
# MAGIC Using the DataFrame created in task 6, `task6DF`, find the following values:
# MAGIC
# MAGIC - the minimum temperature for year 2012
# MAGIC - the maximum temperature for year 2016
# MAGIC - the number of entries for year 2018
# MAGIC - the average wind speed for year 2008
# MAGIC

# COMMAND ----------

task6DF.createOrReplaceTempView("task6_DF")
min2012: float = spark.sql("select min_temperature from task6_DF where year = 2012").collect()[0][0]
max2016: float = spark.sql("select max_temperature from task6_DF where year = 2016").collect()[0][0]
entries2018: int = spark.sql("select entries from task6_DF where year = 2018").collect()[0][0]
wind2008: float = spark.sql("select avg_windspeed from task6_DF where year = 2008").collect()[0][0]


# COMMAND ----------

if 18.95 < min2012 < 19.05:
    print("correct result: minimum temperature for year 2012 19.0 °F")
else:
    print(f"wrong result: {min2012} != 19.0")

if 98.95 < max2016 < 99.05:
    print("correct result: maximum temperature for year 2016 is 99.0 °F")
else:
    print(f"wrong result: {max2016} != 99.0")

if entries2018 == 228:
    print("correct result: there are 228 entries for year 2018")
else:
    print(f"wrong result: {entries2018} != 228")

if 6.485 < wind2008 < 6.495:
    print("correct result: average wind speed for year 2008 is 6.49")
else:
    print(f"wrong result: {wind2008} != 6.49")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - One additional aggregated DataFrame
# MAGIC
# MAGIC Find the year that has the highest number of days that had fog.
# MAGIC
# MAGIC Note, days that have been marked as `heavyfog` days but not as `fog` should not be counted.
# MAGIC

# COMMAND ----------

yearWithMostDaysWithFog: int = spark.sql("select year from weather_Data where fog='Yes' group by(year) order by count(*) desc limit 1").collect()[0][0]


# COMMAND ----------

if yearWithMostDaysWithFog == 2015:
    print("correct result: year 2015 had the highest number of days with fog (32)")
else:
    print(f"wrong result: {yearWithMostDaysWithFog} != 2015")

