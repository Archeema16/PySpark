# Databricks notebook source
# MAGIC %md
# MAGIC # COMP.CS.320 Data-Intensive Programming, Exercise 5
# MAGIC
# MAGIC This exercise demonstrates different file formats (CSV, Parquet, Delta) and some of the operations that can be used with them. The tasks can be done in either Scala or Python. This is the **Scala** version, switch to the Python version if you want to do the tasks in Python.
# MAGIC
# MAGIC - Tasks 1-5 concern reading and writing operations with CSV and Parquet
# MAGIC - Tasks 6-8 introduces the Delta format
# MAGIC
# MAGIC Each task has its own cell for the code. Add your solutions to the cells. You are free to add more cells if you feel it is necessary. There are cells with example outputs following most of the tasks.
# MAGIC
# MAGIC Don't forget to submit your solutions to Moodle.

# COMMAND ----------

# some imports that might be required in the tasks
from functools import reduce
from typing import List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions
from pyspark.sql.types import *


# COMMAND ----------

# some helper functions used in this exercise

def sizeInMB(sizeInBytes: int) -> float:
    return round(sizeInBytes/1024.0/1024.0, 2)

def sizeInKB(filePath: str) -> float:
    return round(sum([file_info.size for file_info in dbutils.fs.ls(filePath)])/1024.0, 2)

# print the files and their sizes from the target path
def printStorage(path: str) -> None:
    def getStorageSize(currentPath: str) -> float:
        # using Databricks utilities to get the list of the files in the path
        fileInformation = dbutils.fs.ls(currentPath)
        sizes = []
        for file_info in fileInformation:
            if file_info.isDir():
                sizes.append(getStorageSize(file_info.path))
            else:
                print(f"{sizeInMB(file_info.size)} MB --- {file_info.path}")
                sizes.append(file_info.size)
        return sum(sizes)

    sizeInBytes = getStorageSize(path)
    print(f"Total size: {sizeInMB(sizeInBytes)} MB")

# remove all files and folders from the target path
def cleanTargetFolder(path: str) -> None:
    dbutils.fs.rm(path, True)

# Print column types in a nice format
def printColumnTypes(inputDF: DataFrame) -> None:
    for column_name, column_type in inputDF.dtypes:
        print(f"{column_name}: {column_type}")

# Returns a limited sample of the input data frame
def getTestDF(inputDF: DataFrame, ids: List[str] = ["Z1", "Z2"], limitRows: int = 2) -> DataFrame:
    origSample: DataFrame = inputDF \
        .filter(~functions.col("ID").contains("_")) \
        .limit(limitRows)
    extraSample: DataFrame = reduce(
        lambda df1, df2: df1.union(df2),
        [inputDF.filter(functions.col("ID").endswith(id)).limit(limitRows) for id in ids]
     )

    return origSample.union(extraSample)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Read data in two formats
# MAGIC
# MAGIC In the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) the `exercises/ex5` folder contains data about car accidents in USA. The same data is given in multiple formats and it is a subset of the dataset in [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents).
# MAGIC
# MAGIC In this task read the data into data frames from both CSV and Parquet source format. The CSV files use `|` as the column separator.
# MAGIC
# MAGIC Code for displaying the data and information about the source files is already included.
# MAGIC

# COMMAND ----------

# Fill in your name (or some other unique identifier). This will be used to identify your target folder for the exercise.
student_name = "Abdul Rehman"

source_path = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex5/"
target_path = f"abfss://students@tunics320f2023gen2.dfs.core.windows.net/{student_name}/ex5/"
data_file = "accidents"


# COMMAND ----------

source_csv_folder = source_path + f"{data_file}_csv"

# create and display the data from CSV source
df_csv = spark.read.option("header","true").option("inferSchema","true").option("sep","|").csv(source_csv_folder)

display(df_csv)


# COMMAND ----------

# Typically, a some more suitable file format would be used, like Parquet.
# With Parquet column format is stored in the file itself, so it does not need to be given.
source_parquet_folder = source_path + f"{data_file}_parquet"

# create and display the data from Parquet source
df_parquet = spark.read.parquet(source_parquet_folder)

display(df_parquet)


# COMMAND ----------

# print the list of files for the different file formats
print("CSV files:")
printStorage(source_csv_folder)

print("\nParquet files:")
printStorage(source_parquet_folder)

# The schemas for both data frames should be the same
print("\n== CSV types ==")
printColumnTypes(df_csv)
print("\n== Parquet types ==")
printColumnTypes(df_parquet)


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 1 (the same output for both displays, only the first few lines shown here when using `show` to view the data frame):
# MAGIC
# MAGIC ```text
# MAGIC +---------+-------------------+-------------------+--------------------+---------+------+-----+-------------+
# MAGIC |       ID|         Start_Time|           End_Time|         Description|     City|County|State|Temperature_F|
# MAGIC +---------+-------------------+-------------------+--------------------+---------+------+-----+-------------+
# MAGIC |A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullert...|Whitehall|Lehigh|   PA|         31.0|
# MAGIC |A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullert...|Whitehall|Lehigh|   PA|         31.0|
# MAGIC |A-3558713|2016-01-14 20:18:33|2017-01-30 13:55:44|Closed at Fullert...|Whitehall|Lehigh|   PA|         31.0|
# MAGIC |A-3572241|2016-01-14 20:18:33|2017-02-17 23:22:00|Closed at Fullert...|Whitehall|Lehigh|   PA|         31.0|
# MAGIC |A-3572395|2016-01-14 20:18:33|2017-02-19 00:38:00|Closed at Fullert...|Whitehall|Lehigh|   PA|         31.0|
# MAGIC +---------+-------------------+-------------------+--------------------+---------+------+-----+-------------+
# MAGIC only showing top 5 rows
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC CSV files:
# MAGIC 31.97 MB --- abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex5/accidents_csv/us_traffic_accidents.csv
# MAGIC Total size: 31.97 MB
# MAGIC
# MAGIC Parquet files:
# MAGIC 9.56 MB --- abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex5/accidents_parquet/us_traffic_accidents.parquet
# MAGIC Total size: 9.56 MB
# MAGIC
# MAGIC == CSV types ==
# MAGIC ID: string
# MAGIC Start_Time: timestamp
# MAGIC End_Time: timestamp
# MAGIC Description: string
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: double
# MAGIC
# MAGIC == Parquet types ==
# MAGIC ID: string
# MAGIC Start_Time: timestamp
# MAGIC End_Time: timestamp
# MAGIC Description: string
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: double
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Write the data to new storage
# MAGIC
# MAGIC Write the data from `df_csv` and `df_parquet` to the [student container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/students/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) in both CSV and Parquet formats.
# MAGIC
# MAGIC Note, since the data in both data frames should be the same, you should be able to use either one as the source when writing it to a new folder (regardless of the target format).
# MAGIC

# COMMAND ----------

# remove all previously written files from the target folder first
cleanTargetFolder(target_path)

# the target paths for both CSV and Parquet
target_file_csv = target_path + data_file + "_csv"
target_file_parquet = target_path + data_file + "_parquet"

# write the data from task 1 in CSV format to the path given by target_file_csv
df_csv.write.option("header",True).option("sep","|").csv(target_file_csv)

# write the data from task 1 in Parquet format to the path given by target_file_parquet
df_parquet.write.parquet(target_file_parquet)

# Check the written files:
printStorage(target_file_csv)
printStorage(target_file_parquet)

# Both with CSV and Parquet, the data can be divided into multiple files depending on how many workers were doing the writing.
# If a single file is needed, you can force the output into a single file with "coalesce(1)" before the write command
# This will make the writing less efficient, especially for larger datasets. (and is not needed in this exercise)
# There are some additional small metadata files (_SUCCESS, _committed, _started) that you can ignore in this exercise.
print("===== End of output =====")


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 2 (note that the exact filenames will be different for you):
# MAGIC
# MAGIC ```text
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/_SUCCESS
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/_committed_3113733412107253362
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/_started_3113733412107253362
# MAGIC 8.99 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/part-00000-tid-3113733412107253362-c2b4319b-cb7f-499b-95d4-87feef106b82-8254-1-c000.csv
# MAGIC 8.99 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/part-00001-tid-3113733412107253362-c2b4319b-cb7f-499b-95d4-87feef106b82-8255-1-c000.csv
# MAGIC 8.99 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/part-00002-tid-3113733412107253362-c2b4319b-cb7f-499b-95d4-87feef106b82-8256-1-c000.csv
# MAGIC 4.99 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_csv/part-00003-tid-3113733412107253362-c2b4319b-cb7f-499b-95d4-87feef106b82-8257-1-c000.csv
# MAGIC Total size: 31.97 MB
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_parquet/_SUCCESS
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_parquet/_committed_6192047308321304174
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_parquet/_started_6192047308321304174
# MAGIC 9.56 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_parquet/part-00000-tid-6192047308321304174-c238e085-402c-4378-b03b-83a3fe26db1f-8258-1-c000.snappy.parquet
# MAGIC Total size: 9.56 MB
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Add new rows to storage
# MAGIC
# MAGIC First create a new data frame based on the task 1 data that contains the `42` latest incidents (based on the starting time) in the city of `New York`. The ids of the incidents of this data frame should have an added postfix `_Z1`, e.g., `A-3877306` should be replaced with `A-3877306_Z1`.
# MAGIC
# MAGIC Then append these new 42 rows to both the CSV storage and Parquet storage. I.e., write the new rows in append mode in CSV format to folder given by `target_file_csv` and in Parquet format to folder given by `target_file_parquet`.
# MAGIC
# MAGIC Finally, read the data from the storages again to check that the appending was successful.
# MAGIC

# COMMAND ----------

more_rows_count = 42

# Create a data frame that holds some new rows that will be appended to the storage, As dataframe are immutable so new column is need to be made up, withColumn(old column name chnages same column, new column name create a new column)
#df_new_rows = df_csv.withColumn("ID",functions.concat(df_csv.ID,functions.lit("_Z1"))).where(df_csv.City=='New York').orderBy(functions.col("Start_Time").desc()).limit(42)
#This command is more benefical then above one as concatenation of withCol is applied to only result dataset, not whole rows as above
df_new_rows = df_csv.where(df_csv.City=='New York').orderBy(functions.col("Start_Time").desc()).limit(42).withColumn("ID",functions.concat(df_csv.ID,functions.lit("_Z1")))

df_new_rows.show(2)
print(df_new_rows.count())

# Append the new rows to CSV storage:
# important to consistenly use the same header and column separator options when using CSV storage
df_new_rows.write.option("header",True).option("sep","|").mode("append").csv(target_file_csv)

# Append the new rows to Parquet storage:
df_new_rows.write.mode("append").parquet(target_file_parquet)

# COMMAND ----------

# Read the merged data from the CSV files to check that the new rows have been stored
df_new_csv = spark.read.option("inferSchema","true").option("separator","|").option("header","true").csv(target_file_csv)

# Read the merged data from the Parquet files to check that the new rows have been stored
df_new_parquet = spark.read.parquet(target_file_parquet)


old_rows = df_parquet.count()
new_rows = df_new_rows.count()
print(f"Old DF had {old_rows} rows and we are adding {new_rows} rows => we should have {old_rows + new_rows} in the merged data.")

print(f"Old CSV DF had {df_csv.count()} rows and new DF has {df_new_csv.count()}.")
print(f"Old Parquet DF had {df_parquet.count()} rows and new DF has {df_new_parquet.count()}.")
print("===== End of output =====")


# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 3:
# MAGIC
# MAGIC ```text
# MAGIC +------------+-------------------+-------------------+--------------------+--------+--------+-----+-------------+
# MAGIC |          ID|         Start_Time|           End_Time|         Description|    City|  County|State|Temperature_F|
# MAGIC +------------+-------------------+-------------------+--------------------+--------+--------+-----+-------------+
# MAGIC |A-3877306_Z1|2023-02-16 20:48:16|2023-02-16 22:00:00|Closed road from ...|New York|New York|   NY|         58.0|
# MAGIC |A-4076884_Z1|2023-02-12 07:54:11|2023-02-12 10:05:18|Port Authority NY...|New York|New York|   NY|         40.0|
# MAGIC +------------+-------------------+-------------------+--------------------+--------+--------+-----+-------------+
# MAGIC only showing top 2 rows
# MAGIC ```
# MAGIC
# MAGIC and
# MAGIC
# MAGIC ```text
# MAGIC Old DF had 198082 rows and we are adding 42 rows => we should have 198124 in the merged data.
# MAGIC Old CSV DF had 198082 rows and new DF has 198124.
# MAGIC Old Parquet DF had 198082 rows and new DF has 198124.
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Append modified rows
# MAGIC
# MAGIC In the previous task, appending new rows was successful because the new data had the same schema as the original data.
# MAGIC
# MAGIC In this task we try to append data with a modified schema to the CSV and Parquet storages.
# MAGIC
# MAGIC First create a new data frame based on `df_new_rows` from task 3. The data frame should be modified in the following way:
# MAGIC
# MAGIC - The values in the `ID` column should have a postfix `_Z2` instead of `_Z1`. E.g., `A-3877306_Z1` should be replaced with `A-3877306_Z2`.
# MAGIC - A new column `AddedColumn1` should be added with values `"prefix-CITY"` where `CITY` is replaced by the city of the incident.
# MAGIC - A new column `AddedColumn2` should be added with a constant value `"New column"`.
# MAGIC - The column `Description` should be dropped.
# MAGIC
# MAGIC Then append these modified rows to both the CSV storage and Parquet storage.
# MAGIC

# COMMAND ----------

# Some new rows that have different columns
#I used regex for split instead of straight "_", and then got first element of list from split 
df_modified = df_new_rows.drop(df_new_rows.Description).withColumn("ID",functions.concat(functions.split(df_new_rows.ID,'[_]')[0],functions.lit("_Z2"))).withColumn("AddedColumn1",functions.concat(functions.lit("prefix-"),df_new_rows.City)).withColumn("AddedColumn2",functions.lit("New column"))

df_modified.show(2)


# Append the new modified rows to CSV storage:
df_modified.write.option("sep","|").option("header",True).mode("append").csv(target_file_csv)
# Append the new modified rows to Parquet storage:
df_modified.write.mode("append").parquet(target_file_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 4:
# MAGIC
# MAGIC ```text
# MAGIC +------------+-------------------+-------------------+--------+--------+-----+-------------+---------------+------------+
# MAGIC |          ID|         Start_Time|           End_Time|    City|  County|State|Temperature_F|   AddedColumn1|AddedColumn2|
# MAGIC +------------+-------------------+-------------------+--------+--------+-----+-------------+---------------+------------+
# MAGIC |A-3877306_Z2|2023-02-16 20:48:16|2023-02-16 22:00:00|New York|New York|   NY|         58.0|prefix-New York|  New column|
# MAGIC |A-4076884_Z2|2023-02-12 07:54:11|2023-02-12 10:05:18|New York|New York|   NY|         40.0|prefix-New York|  New column|
# MAGIC +------------+-------------------+-------------------+--------+--------+-----+-------------+---------------+------------+
# MAGIC only showing top 2 rows
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Check the merged data
# MAGIC
# MAGIC In this task we check the contents of the CSV and Parquet storages after the two data append operations, the new rows with the same schema in task 3, and the new rows with a modified schema in task 4.
# MAGIC
# MAGIC Your task is to first write the code that loads the data from the storages again. And then run the given test code that shows the number of rows, columns, schema, and some sample rows.
# MAGIC
# MAGIC Finally, answer the questions in the final cell of this task.
# MAGIC

# COMMAND ----------

# CSV should have been broken in some way
# Read in the CSV data again from the CSV storage: target_file_csv
modified_csv_df = spark.read.option("sep","|").option("header","true").option("inferSchema","true").csv(target_file_csv)

print("== CSV storage:")
print(f"The number of rows should be correct: {modified_csv_df.count()} (i.e., {df_csv.count()}+2*{more_rows_count})")
print(f"However, the original data had {len(df_csv.columns)} columns, inserted data had {len(df_modified.columns)} columns. Afterwards we have {len(modified_csv_df.columns)} columns while we should have {len(df_csv.columns) + 2} distinct columns.")

getTestDF(modified_csv_df).show()

printColumnTypes(modified_csv_df)


# COMMAND ----------

modified_parquet_df = spark.read.parquet(target_file_parquet)
print("== Parquet storage:")
print(f"The count for number of rows seems wrong: {df_parquet.count()} (should be: {df_parquet.count()}+2*{more_rows_count})")
print(f"Actually all {df_parquet.count()+2*more_rows_count} rows should be included but the 2 conflicting schemas can cause the count to be incorrect.")
print(f"The original data had {len(df_parquet.columns)} columns, inserted data had {len(df_modified.columns)} columns. Afterwards we have {len(modified_parquet_df.columns)} columns while we should have {len(df_parquet.columns) + 2} distinct columns.")

getTestDF(modified_parquet_df).show()

print("Unlike the CSV case, the data types for the columns have not been affected. But some columns are just ignored.")
printColumnTypes(modified_parquet_df)


# COMMAND ----------

# MAGIC %md
# MAGIC *Did you get similar output for the data in CSV storage? If not, what was the difference?*
# MAGIC Yes I got the similar output to CSV
# MAGIC
# MAGIC *What is your explanation/guess for why the CSV seems broken and the schema cannot be inferred anymore?*
# MAGIC There are 2 header lines in csv filr and there is different types of data in csv file, so data is inconsistent that is why schema cannot be inferred. 
# MAGIC
# MAGIC *Did you get similar output for the data in Parquet storage, and which of the 2 alternatives? If not, what was the difference?*
# MAGIC Yes I got similar ouput , mine was similar to paraquet 2
# MAGIC
# MAGIC *What is your explanation/guess for why not all 10 distinct columns are included in the data frame in the Parquet case?*
# MAGIC Not all distinct columns are not added, because my guess is header of paraquet has changed, so description column is removed, and for the rows which donot have data in new columns, null is assumed. Datatype remain consistent, this shows paraquet has so many advantages over csv

# COMMAND ----------

# MAGIC %md
# MAGIC Example output for task 5:
# MAGIC
# MAGIC For CSV:
# MAGIC
# MAGIC ```text
# MAGIC == CSV storage:
# MAGIC The number of rows should be correct: 198166 (i.e., 198082+2*42)
# MAGIC However, the original data had 8 columns, inserted data had 9 columns. Afterwards we have 8 columns while we should have 10 distinct columns.
# MAGIC +------------+--------------------+--------------------+--------------------+---------+--------+-----+---------------+
# MAGIC |          ID|          Start_Time|            End_Time|         Description|     City|  County|State|  Temperature_F|
# MAGIC +------------+--------------------+--------------------+--------------------+---------+--------+-----+---------------+
# MAGIC |   A-3558690|2016-01-14T20:18:...|2017-01-30T13:25:...|Closed at Fullert...|Whitehall|  Lehigh|   PA|           31.0|
# MAGIC |   A-3558700|2016-01-14T20:18:...|2017-01-30T13:34:...|Closed at Fullert...|Whitehall|  Lehigh|   PA|           31.0|
# MAGIC |A-3877306_Z1|2023-02-16T20:48:...|2023-02-16T22:00:...|Closed road from ...| New York|New York|   NY|           58.0|
# MAGIC |A-4076884_Z1|2023-02-12T07:54:...|2023-02-12T10:05:...|Port Authority NY...| New York|New York|   NY|           40.0|
# MAGIC |A-3877306_Z2|2023-02-16T20:48:...|2023-02-16T22:00:...|            New York| New York|      NY| 58.0|prefix-New York|
# MAGIC |A-4076884_Z2|2023-02-12T07:54:...|2023-02-12T10:05:...|            New York| New York|      NY| 40.0|prefix-New York|
# MAGIC +------------+--------------------+--------------------+--------------------+---------+--------+-----+---------------+
# MAGIC
# MAGIC Also the data types cannot be properly inferred anymore, for example temperature is now given as a string instead of a double.
# MAGIC (The reason should be visible in the sample output.):
# MAGIC ID: string
# MAGIC Start_Time: string
# MAGIC End_Time: string
# MAGIC Description: string
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: string
# MAGIC ```
# MAGIC
# MAGIC and for Parquet (alternative 1):
# MAGIC
# MAGIC ```text
# MAGIC == Parquet storage:
# MAGIC The count for number of rows seems wrong: 198082 (should be: 198082+2*42)
# MAGIC Actually all 198166 rows should be included but the 2 conflicting schemas can cause the count to be incorrect.
# MAGIC The original data had 8 columns, inserted data had 9 columns. Afterwards we have 8 columns while we should have 10 distinct columns.
# MAGIC +------------+-------------------+-------------------+--------------------+---------+--------+-----+-------------+
# MAGIC |          ID|         Start_Time|           End_Time|         Description|     City|  County|State|Temperature_F|
# MAGIC +------------+-------------------+-------------------+--------------------+---------+--------+-----+-------------+
# MAGIC |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullert...|Whitehall|  Lehigh|   PA|         31.0|
# MAGIC |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullert...|Whitehall|  Lehigh|   PA|         31.0|
# MAGIC |A-3877306_Z1|2023-02-16 20:48:16|2023-02-16 22:00:00|Closed road from ...| New York|New York|   NY|         58.0|
# MAGIC |A-4076884_Z1|2023-02-12 07:54:11|2023-02-12 10:05:18|Port Authority NY...| New York|New York|   NY|         40.0|
# MAGIC |A-3877306_Z2|2023-02-16 20:48:16|2023-02-16 22:00:00|                NULL| New York|New York|   NY|         58.0|
# MAGIC |A-4076884_Z2|2023-02-12 07:54:11|2023-02-12 10:05:18|                NULL| New York|New York|   NY|         40.0|
# MAGIC +------------+-------------------+-------------------+--------------------+---------+--------+-----+-------------+
# MAGIC
# MAGIC Unlike the CSV case, the data types for the columns have not been affected. The added columns are just ignored.
# MAGIC ID: string
# MAGIC Start_Time: timestamp
# MAGIC End_Time: timestamp
# MAGIC Description: string
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: double
# MAGIC ```
# MAGIC
# MAGIC Parquet (alternative 2):
# MAGIC
# MAGIC ```text
# MAGIC == Parquet storage:
# MAGIC The count for number of rows seems wrong: 198082 (should be: 198082+2*42)
# MAGIC Actually all 198166 rows should be included but the 2 conflicting schemas can cause the count to be incorrect.
# MAGIC The original data had 8 columns, inserted data had 9 columns. Afterwards we have 9 columns while we should have 10 distinct columns.
# MAGIC +------------+-------------------+-------------------+---------+--------+-----+-------------+---------------+------------+
# MAGIC |          ID|         Start_Time|           End_Time|     City|  County|State|Temperature_F|   AddedColumn1|AddedColumn2|
# MAGIC +------------+-------------------+-------------------+---------+--------+-----+-------------+---------------+------------+
# MAGIC |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Whitehall|  Lehigh|   PA|         31.0|           NULL|        NULL|
# MAGIC |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Whitehall|  Lehigh|   PA|         31.0|           NULL|        NULL|
# MAGIC |A-3877306_Z1|2023-02-16 20:48:16|2023-02-16 22:00:00| New York|New York|   NY|         58.0|           NULL|        NULL|
# MAGIC |A-4076884_Z1|2023-02-12 07:54:11|2023-02-12 10:05:18| New York|New York|   NY|         40.0|           NULL|        NULL|
# MAGIC |A-3877306_Z2|2023-02-16 20:48:16|2023-02-16 22:00:00| New York|New York|   NY|         58.0|prefix-New York|  New column|
# MAGIC |A-4076884_Z2|2023-02-12 07:54:11|2023-02-12 10:05:18| New York|New York|   NY|         40.0|prefix-New York|  New column|
# MAGIC +------------+-------------------+-------------------+---------+--------+-----+-------------+---------------+------------+
# MAGIC
# MAGIC Unlike the CSV case, the data types for the columns have not been affected. The added columns are just ignored.
# MAGIC ID: string
# MAGIC Start_Time: timestamp
# MAGIC End_Time: timestamp
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: double
# MAGIC AddedColumn1: string
# MAGIC AddedColumn2: string
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Delta - Reading and writing data
# MAGIC
# MAGIC [Delta](https://docs.databricks.com/en/delta/index.html) tables are a storage format that are more advanced. They can be used somewhat like databases.
# MAGIC
# MAGIC This is not native in Spark, but open format which is more and more commonly used.
# MAGIC
# MAGIC Delta is more strict with data. We cannot for example have whitespace in column names as you can have in Parquet and CSV. However, in this exercise the example data is given with column names where these additional requirements have already been fulfilled. And thus you don't have to worry about them in this exercise.
# MAGIC
# MAGIC Delta technically looks more or less like Parquet with some additional metadata files.
# MAGIC
# MAGIC In this this task read the source data given in Delta format into a data frame. And then write a copy of the data into the students container to allow modifications in the following tasks.
# MAGIC

# COMMAND ----------

source_delta_folder = source_path + f"{data_file}_delta"

# Read the original data in Delta format to a data frame
df_delta = spark.read.format("delta").load(source_delta_folder)

print(f"== Number or rows: {df_delta.count()}")
print("== Columns:")
printColumnTypes(df_delta)
print("== Storage files:")
printStorage(source_delta_folder)
print("===== End of output =====")


# COMMAND ----------

target_file_delta = target_path + data_file + "_delta"

print(target_file_delta)
# write the data from df_delta using the Delta format to the path given by target_file_delta
df_delta.write.format("delta").save(target_file_delta)

# Check the written files:
printStorage(target_file_delta)
print("===== End of output =====")


# COMMAND ----------

# MAGIC %md
# MAGIC Example output from task 6:
# MAGIC
# MAGIC ```text
# MAGIC == Number or rows: 198082
# MAGIC == Columns:
# MAGIC ID: string
# MAGIC Start_Time: timestamp
# MAGIC End_Time: timestamp
# MAGIC Description: string
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: double
# MAGIC == Storage files:
# MAGIC 0.0 MB --- abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex5/accidents_delta/_delta_log/00000000000000000000.crc
# MAGIC 0.0 MB --- abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex5/accidents_delta/_delta_log/00000000000000000000.json
# MAGIC 9.56 MB --- abfss://shared@tunics320f2023gen2.dfs.core.windows.net/exercises/ex5/accidents_delta/part-00000-35a096d7-a0ee-439a-85e0-aa78e2935f39-c000.snappy.parquet
# MAGIC Total size: 9.56 MB
# MAGIC ```
# MAGIC
# MAGIC and (the actual file names will be different)
# MAGIC
# MAGIC ```text
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_delta/_delta_log/00000000000000000000.crc
# MAGIC 0.0 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_delta/_delta_log/00000000000000000000.json
# MAGIC 9.56 MB --- abfss://students@tunics320f2023gen2.dfs.core.windows.net/some_unique_folder/ex5/accidents_delta/part-00000-576356db-5737-43ef-b11c-163e91229175-c000.snappy.parquet
# MAGIC Total size: 9.56 MB
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Task 7 - Delta - Appending data
# MAGIC
# MAGIC Add the new rows using the same schema, `df_new_rows` from task 3, and the new rows using the modified schema, `df_modified` from task 4, to the Delta storage.
# MAGIC
# MAGIC Then, read the merged data and study whether the result with Delta is correct without lost or invalid data.
# MAGIC

# COMMAND ----------

# Append the new rows using the same schema, df_new_rows, to the Delta storage:
df_new_rows.write.format("delta").mode("append").save(target_file_delta)
# On default, Delta is similar to Parquet. However, we can enable it to handle schema modifications.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Append the new rows using the modified schema, df_modified, to the Delta storage:
df_modified.write.format("delta").mode("append").save(target_file_delta)

# COMMAND ----------

# Read the merged data from Delta storage to check that the new rows have been stored
modified_delta_df = spark.read.format("delta").load(target_file_delta)

print(f"The number of rows should be correct: {modified_delta_df.count()} (i.e., {df_delta.count()}+2*{more_rows_count})")
print(f"The original data had {len(modified_delta_df.columns)} columns, inserted data had {len(df_modified.columns)} columns. Afterwards we have {len(modified_delta_df.columns)} columns while we should have ${len(df_delta.columns) + 2} distinct columns.")
print("Delta handles these perfectly. The columns which were not given values are available with NULL values.")

getTestDF(modified_delta_df).show()
printColumnTypes(modified_delta_df)
print("===== End of output =====")


# COMMAND ----------

# MAGIC %md
# MAGIC Example output from task 7:
# MAGIC
# MAGIC ```text
# MAGIC The number of rows should be correct: 198166 (i.e., 198082+2*42)
# MAGIC The original data had 10 columns, inserted data had 9 columns. Afterwards we have 10 columns while we should have 10 distinct columns.
# MAGIC Delta handles these perfectly. The columns which were not given values are available with NULL values.
# MAGIC +------------+-------------------+-------------------+--------------------+---------+--------+-----+-------------+---------------+------------+
# MAGIC |          ID|         Start_Time|           End_Time|         Description|     City|  County|State|Temperature_F|   AddedColumn1|AddedColumn2|
# MAGIC +------------+-------------------+-------------------+--------------------+---------+--------+-----+-------------+---------------+------------+
# MAGIC |   A-3558690|2016-01-14 20:18:33|2017-01-30 13:25:19|Closed at Fullert...|Whitehall|  Lehigh|   PA|         31.0|           NULL|        NULL|
# MAGIC |   A-3558700|2016-01-14 20:18:33|2017-01-30 13:34:02|Closed at Fullert...|Whitehall|  Lehigh|   PA|         31.0|           NULL|        NULL|
# MAGIC |A-3877306_Z1|2023-02-16 20:48:16|2023-02-16 22:00:00|Closed road from ...| New York|New York|   NY|         58.0|           NULL|        NULL|
# MAGIC |A-4076884_Z1|2023-02-12 07:54:11|2023-02-12 10:05:18|Port Authority NY...| New York|New York|   NY|         40.0|           NULL|        NULL|
# MAGIC |A-3877306_Z2|2023-02-16 20:48:16|2023-02-16 22:00:00|                NULL| New York|New York|   NY|         58.0|prefix-New York|  New column|
# MAGIC |A-4076884_Z2|2023-02-12 07:54:11|2023-02-12 10:05:18|                NULL| New York|New York|   NY|         40.0|prefix-New York|  New column|
# MAGIC +------------+-------------------+-------------------+--------------------+---------+--------+-----+-------------+---------------+------------+
# MAGIC
# MAGIC ID: string
# MAGIC Start_Time: timestamp
# MAGIC End_Time: timestamp
# MAGIC Description: string
# MAGIC City: string
# MAGIC County: string
# MAGIC State: string
# MAGIC Temperature_F: double
# MAGIC AddedColumn1: string
# MAGIC AddedColumn2: string
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - Delta - Full modifications
# MAGIC
# MAGIC Editing existing values is not possible without overwriting the entire dataset.
# MAGIC
# MAGIC Previously we only added new lines. This way of working only supports adding new data. It does **not** support modifying existing data: updating values or deleting rows.
# MAGIC (We could do that manually by adding a primary key and timestamp and always searching for the newest value.)
# MAGIC
# MAGIC Nevertheless, Delta tables take care of this and many more itself.
# MAGIC
# MAGIC This task is divided into multiple cells with separate instructions. In the following cell add the code to write the `df_delta_small` into Delta storage.
# MAGIC

# COMMAND ----------

# Let us first save smaller data so that it is easier to see what is happening
delta_table_file = target_path + data_file + "_deltatable_small"

# create a small 6 row data frame with only 5 columns
df_delta_small =  getTestDF(modified_delta_df, ["Z1"], 3) \
    .drop("Description", "End_Time", "County", "AddedColumn1", "AddedColumn2")

# Write the new small data frame to storage in Delta format to path based on delta_table_file
df_delta_small.write.format("delta").save(delta_table_file)


# Create Delta table based on your target folder
deltatable = DeltaTable.forPath(spark, delta_table_file)


# COMMAND ----------

# MAGIC %md
# MAGIC Delta tables are more like database type tables. We define them based on data and modify the table itself.
# MAGIC
# MAGIC We do this by telling Delta what is the primary key of the data. After this we tell it to "merge" new data to the old one. If primary key matches, we update the information. If primary key is new, add a row.
# MAGIC
# MAGIC In the following cell, add the code to update the `deltatable` with the given updates in `df_delta_update`.
# MAGIC
# MAGIC The rows should be updated when the `ID` columns match. And if the id from the update is a new one, a new row should be inserted into the `deltatable`.
# MAGIC
# MAGIC Documentation page [https://docs.databricks.com/en/delta/merge.html#modify-all-unmatched-rows-using-merge](https://docs.databricks.com/en/delta/merge.html#modify-all-unmatched-rows-using-merge) might be helpful in this task.
# MAGIC

# COMMAND ----------

# create a 5 row data frame with the same columns with updated values for the temperature
df_delta_update = df_new_rows \
    .limit(5) \
    .drop("Description", "End_Time", "County") \
    .withColumn("Temperature_F", functions.round(functions.rand(1) * 100, 1))

# Show the data before the merge
print(f"== Before merge, the size of Delta file is {sizeInKB(delta_table_file)} kB and contains {deltatable.toDF().count()} rows.")
deltatable.toDF().sort(functions.desc("Start_Time")).show()


# code for updating the deltatable with df_delta_update
deltatable.alias('deltaTableMain').merge(df_delta_update.alias('deltaTableUpdate'),'deltaTableMain.ID = deltaTableUpdate.ID') \
  .whenMatchedUpdate(set = 
    {
        "ID":"deltaTableUpdate.ID",
        "Start_Time":"deltaTableUpdate.Start_Time",
        "City":"deltaTableUpdate.City",
        "State":"deltaTableUpdate.State",
        "Temperature_F":"deltaTableUpdate.Temperature_F"
    }
  ).whenNotMatchedInsert(values =
    {
        "ID":"deltaTableUpdate.ID",
        "Start_Time":"deltaTableUpdate.Start_Time",
        "City":"deltaTableUpdate.City",
        "State":"deltaTableUpdate.State",
        "Temperature_F":"deltaTableUpdate.Temperature_F"
    }
  ).execute()


# Show the data after the merge
print(f"== After merge, the size of Delta file is {sizeInKB(delta_table_file)} kB and contains {deltatable.toDF().count()} rows.")
deltatable.toDF().sort(functions.desc("Start_Time")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC Note: If you execute the previous cell multiple times, you can notice that the file size increases every time. However, the amount of rows does not change.

# COMMAND ----------

# We can get rid of additional data by vacuuming
print(f"== Before vacuum, the size of Delta file is {sizeInKB(delta_table_file)} kB.")

# Typically we do not want to vacuum all the data, only data older than 30 days or so.
# We need to tell Delta that we really want to do something stupid
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
deltatable.vacuum(0)
print(f"== After vacuum, the size of Delta file is {sizeInKB(delta_table_file)} kB.")

# you can print the files after vacuuming by uncommenting the following
# printStorage(delta_table_file)

# the vacuuming should not change the actual data
# deltatable.toDF().sort(functions.desc("Start_Time")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC Example output from task 8:
# MAGIC
# MAGIC ```text
# MAGIC == Before merge, the size of Delta file is 3.44 kB and contains 6 rows.
# MAGIC +------------+-------------------+---------+-----+-------------+
# MAGIC |          ID|         Start_Time|     City|State|Temperature_F|
# MAGIC +------------+-------------------+---------+-----+-------------+
# MAGIC |A-3877306_Z1|2023-02-16 20:48:16| New York|   NY|         58.0|
# MAGIC |A-4076884_Z1|2023-02-12 07:54:11| New York|   NY|         40.0|
# MAGIC |A-5005008_Z1|2023-01-30 22:00:00| New York|   NY|         50.0|
# MAGIC |   A-3558690|2016-01-14 20:18:33|Whitehall|   PA|         31.0|
# MAGIC |   A-3558700|2016-01-14 20:18:33|Whitehall|   PA|         31.0|
# MAGIC |   A-3558713|2016-01-14 20:18:33|Whitehall|   PA|         31.0|
# MAGIC +------------+-------------------+---------+-----+-------------+
# MAGIC
# MAGIC == After merge, the size of Delta file is 5.2 kB and contains 8 rows.
# MAGIC +------------+-------------------+---------+-----+-------------+
# MAGIC |          ID|         Start_Time|     City|State|Temperature_F|
# MAGIC +------------+-------------------+---------+-----+-------------+
# MAGIC |A-3877306_Z1|2023-02-16 20:48:16| New York|   NY|         63.6|
# MAGIC |A-4076884_Z1|2023-02-12 07:54:11| New York|   NY|         59.9|
# MAGIC |A-5005008_Z1|2023-01-30 22:00:00| New York|   NY|         13.5|
# MAGIC |A-5284521_Z1|2023-01-29 22:05:45| New York|   NY|          7.7|
# MAGIC |A-4949927_Z1|2023-01-28 03:26:49| New York|   NY|         85.4|
# MAGIC |   A-3558690|2016-01-14 20:18:33|Whitehall|   PA|         31.0|
# MAGIC |   A-3558700|2016-01-14 20:18:33|Whitehall|   PA|         31.0|
# MAGIC |   A-3558713|2016-01-14 20:18:33|Whitehall|   PA|         31.0|
# MAGIC +------------+-------------------+---------+-----+-------------+
# MAGIC ```
# MAGIC
# MAGIC and (the numbers might not match exactly)
# MAGIC
# MAGIC ```text
# MAGIC == Before vacuum, the size of Delta file is 5.2 kB.
# MAGIC == After vacuum, the size of Delta file is 3.5 kB.
# MAGIC ```
# MAGIC
