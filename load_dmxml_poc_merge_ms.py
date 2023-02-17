# Databricks notebook source
import dlt 
import pyspark.sql.functions as f

def load_to_bronze(folder_name):
    dataPath = f"dbfs:/mnt/stauresdwhrawprod/dbx-demo/ms_poc/{folder_name}"
    if folder_name == 'dm_xmls':
        
        @dlt.view(name=f"raw_{folder_name}",
                      comment=f"Raw version of table {folder_name}" 
                  )
        def incremental_raw():
            df = (spark
                  .readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "parquet")
                  .load(dataPath)
                  .withColumn("dwh_inserted", f.current_timestamp())
                  .withColumn("dwh_file_name", f.input_file_name())
                 )
            return df

        @dlt.table(
            name=f"bronze_{folder_name}",
            comment=f"data za 202301 k tabulce {folder_name}",
            partition_cols= ["server_id", "date_in_date"],
            table_properties= {
                               "quality":"bronze", 
                               "delta.minReaderVersion":"2", 
                               "delta.minWriterVersion":"5",
                               "pipelines.autoOptimize.managed": "true"
            }
        )
        def streaming_bronze():
            return dlt.read_stream("raw_dm_xmls").withColumn('date_in_date', f.to_date("date_in")) 
    else:
        @dlt.view(name=f"raw_{folder_name}",
                      comment=f"data za 202301 k tabulce {folder_name}" 
                  )
        def incremental_raw():
            df = (spark
                  .readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "parquet")
                  .load(dataPath)
                  .withColumn("dwh_inserted", f.current_timestamp())
                  .withColumn("dwh_file_name", f.input_file_name())
                 )
            return df

        @dlt.table(
            name=f"bronze_{folder_name}",
            comment=f"data za 202301 k tabulce {folder_name}",
            partition_cols= ["server_id", "inserted_date"],
            table_properties= {
                               "quality":"bronze", 
                               "delta.minReaderVersion":"2", 
                               "delta.minWriterVersion":"5",
                               "pipelines.autoOptimize.managed": "true"
            }
        )
        def streaming_bronze():
            return dlt.read_stream("raw_dm_xmls_wp").withColumn('inserted_date', f.to_date("inserted"))

# COMMAND ----------

lst = ["dm_xmls", "dm_xmls_wp"]
for i in lst:
    load_to_bronze(i)
