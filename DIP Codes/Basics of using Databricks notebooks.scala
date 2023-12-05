// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Basic instructions to Databricks Notebooks

// COMMAND ----------

// MAGIC %md
// MAGIC Databricks notebooks are easy way to execute Apache Spark in cloud environments with browser.
// MAGIC Here are a few pointers on how to get started. 
// MAGIC
// MAGIC For more information, see [official instructions](https://learn.microsoft.com/en-gb/azure/databricks/notebooks/notebooks-code).
// MAGIC
// MAGIC ### Going around in the workspace
// MAGIC * Your code is, on default, stored in the Databricks Workspace path. You can get there by selecting "Workspace" in the left menu and then Workspace / Users / Your email.
// MAGIC * The workspace will be destroyed in the end of the course. Thus, all your code will disappear.
// MAGIC * The recommended way is to use Databricks git repository support. The instructions for this are shown on the first weekly assignment and in Moodle. You see the repositories by selecting "Workspace" in the left menu and then Repos / Your email.
// MAGIC
// MAGIC ### Executing code
// MAGIC * To do anything useful, you need to attach your notebook to a computational cluster. You can do this behind the top right "Connect" button in the notebook.
// MAGIC * If nobody else is using the cluster, the cluster might start for you. This might take ~5 minutes.
// MAGIC * All the students are using the same computational cluster. It scales up and down automatically depending on the usage. Scaling might also take ~5 minutes to take effect.
// MAGIC * Notebooks are divided to cells. You can execute cell code by clicking top right corner play button or by using "Shift"+"Enter" shortcut.
// MAGIC * You can see the other keyboard shortcuts via Help / Keyboard shortcuts
// MAGIC * As you hopefully remember, Apache Spark cluster consists of Driver and Worker nodes. All Spark commands are shared between workers, all Scala and Python code is executed on Driver node. Thus, do not execute heavy load without Spark.
// MAGIC
// MAGIC ### Selecting programming language
// MAGIC * Each notebook has a default language. You can select it on the top of the notebook, right side of the notebook name. In this notebook, the default language is "Scala"
// MAGIC * Cells can have different languages. However, you can not directly refer to variables in other languages.
// MAGIC * You can select the cell wise language from the top right corner of the cell. Alternatives are Scala, Python and Markdown (for documentation). Databricks also supports R and SQL cells, but they are not allowed in course assignments.
// MAGIC * You can also use "magic commands" on the first line of cell to set the language. The magic commands are `%scala`, `%python`and `%md`.
// MAGIC
// MAGIC ### Working with cells
// MAGIC * You can add and remove cells, and change their order.
// MAGIC * You can also execute cells in any order.
// MAGIC * This means, you will probably end up in very confusing ordering of cells. And notebooks that do not actually work when executing from the beginning to end.
// MAGIC * That is, your assignment might not work for us if you are not careful. Remember to try execution in the correct order.
// MAGIC * Using functional programming practices helps in this.
// MAGIC
// MAGIC ### Important
// MAGIC * Before submitting your code, **always execute notebook fully with clean state**. This is done by selecting Run / Clear state and run all.
// MAGIC * Due to permission restrictions, every student can restart the cluster. **Do NOT restart clusters**. Otherwise everyone's notebook executions will be cleared. Codes will not disappear.
// MAGIC * Avoid executing heavy loads on Spark Driver node. Instead, **distribute the computation with Spark framework**: in practice, use commands that start with `spark.` or `dataframe.`.

// COMMAND ----------

// MAGIC %md
// MAGIC **Let's check some practical code examples**
// MAGIC
// MAGIC This notebook is stored in the common folder `/shared_readonly/` so everyone is working on it at the same time.
// MAGIC You probably want to take a copy of it to your own folder. You can do it via File / Clone on the top menu of the notebook.

// COMMAND ----------

// Let us execute Scala
// Because Scala is the default notebook language, we do not need the magic command %scala on the first line
val string_list : List[String] = List("This", "is", "Scala")
val printed_string = string_list.mkString(" ")
println("** Let's start our first printing.")
println(printed_string + " code!")
println("** Successfully printed our first text. Rest is other cell output.")

// COMMAND ----------

// MAGIC %python
// MAGIC # Let's execute Python. Note the magic command above
// MAGIC string_list = ["This", "is", "Python"]
// MAGIC printed_string = " ".join(string_list)
// MAGIC print(printed_string + " code!")

// COMMAND ----------

// MAGIC %md
// MAGIC **Then some example Spark code.**
// MAGIC
// MAGIC This Markdown cell is created with magic command. Double click this cell to see the magic command `%md` on the first line.

// COMMAND ----------

// Let us work on Scala and Apache Spark

// Create a sample DataFrame
val data = Seq(
  ("Alice", 25),
  ("Bob", 30),
  ("Charlie", 35)
)

// Let us do our first Apache Spark call. You can recognize it on the next line because we use library "spark"
val df = spark.createDataFrame(data).toDF("Name", "Age")
// Above code is might now be divided between worker nodes

// Use Apache Spark print to show the result
df.show()

// COMMAND ----------

// However, Databricks also has a way to print DataFrames in nicer format
display(df)

// COMMAND ----------

// Let us do a bit of computation with Spark
import org.apache.spark.sql.functions._ // Very often DataFrame computations need this import

val avgAge = df.select(avg("Age")).head().getDouble(0)
val maxAge = df.select(max("Age")).head().getInt(0)
println(s"Average Age: $avgAge")
println(s"Maximum Age: $maxAge")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading files
// MAGIC
// MAGIC The environment is in the cloud. So, the accessed data is also stored in the cloud. In this case, we use Azure Storage Account and Azure Data Lake Storage Gen2. You can get there with your Microsoft account by installing [Microsoft Azure Storage Explorer](https://azure.microsoft.com/en-us/products/storage/storage-explorer) to your machine or with browser on following URLs:
// MAGIC * [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) for example data sets.
// MAGIC * [Student container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2023-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2023gen2/path/students/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) where you can store your own data. Please create a folder for yourself with your name.
// MAGIC * You also find all the relevant Azure resources by going to the [Azure portal](https://portal.azure.com) to Storage accounts and select [`tunics320f2023gen2` /  containers](https://portal.azure.com/#@tuni.onmicrosoft.com/resource/subscriptions/e0c78478-e7f8-429c-a25f-015eae9f54bb/resourceGroups/tuni-cs320-f2023-rg/providers/Microsoft.Storage/storageAccounts/tunics320f2023gen2/containersList).
// MAGIC
// MAGIC Now, let's go through some examples on how to read the data int the storage with Databricks Apache Spark. The address for the data is
// MAGIC `abfss://<container>@tunics320f2023gen2.dfs.core.windows.net/<path>/<to>/<file.csv>`

// COMMAND ----------

val file_csv = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/demo/kaggle/csv/10mb_imdb_anime.csv"
val df_csv = spark.read
  .option("header", "true")  // The first row has column names
  .option("inferSchema", "true")  // Try to determine the column types automatically from the data
  .option("sep", ",")
  .csv(file_csv)

display(df_csv)

// COMMAND ----------

// MAGIC %python
// MAGIC # The same with Python
// MAGIC file = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/demo/kaggle/csv/10mb_imdb_anime.csv"
// MAGIC df = spark.read  \
// MAGIC   .option("header", "true") \
// MAGIC   .option("inferSchema", "true") \
// MAGIC   .option("sep", ",") \
// MAGIC   .csv(file)
// MAGIC
// MAGIC display(df)

// COMMAND ----------

// Typically we use some more suitable file format, like Parquet.
// Column format is stored in the file itself, so we do not need to give it.
val file_parquet = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/demo/kaggle/parquet/10mb_imdb_anime.parquet"
val df_parquet = spark.read.parquet(file_parquet)
display(df_parquet)

// COMMAND ----------

// There is alsoa a possibility to use a more advanced Delta file format, also called "Delta lake table"
// We will cover this later in the course
val file_delta = "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/demo/kaggle/delta/10mb_imdb_anime_delta"
val df_delta = spark.read.format("delta").load(file_delta)
display(df_delta)

// COMMAND ----------

// MAGIC %md
// MAGIC # Writing files
// MAGIC
// MAGIC Reading files is enough for this course. However, if you want, you can also try writing the files

// COMMAND ----------

val student_name = "example_student" // Change this to your own name

// COMMAND ----------

val target_path = s"abfss://students@tunics320f2023gen2.dfs.core.windows.net/${student_name}/demo/"
val target_file = target_path + "10mb_imdb_anime.parquet"

// Write to Parquet
df_parquet.write
    .mode("overwrite")
    .parquet(target_file)

// COMMAND ----------

// Use Databricks specific utilities library to see the files. Spark has Hadoop filesystem library for the same, but it is not as simple
display(dbutils.fs.ls(target_path))

// COMMAND ----------

// Actually Parquet file is usually a folder with multiple files.
// Workers might write to multiple files at the same time. Let's check the actual files
display(dbutils.fs.ls(target_file))

// COMMAND ----------


