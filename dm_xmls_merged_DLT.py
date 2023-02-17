# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
import functools
import datetime
import pyspark.sql.utils
from typing import List

# COMMAND ----------

def repair_wrong_make(df):
  
  # make ktery je jinak napsany
  dict_make = {"Mini": "Mini (BMW)",
               "Ssang Yong" : "SsangYong",
               "Mclaren":"McLaren",
              }
  
  df = df.replace(dict_make,subset=['make'])
  
  return df
  
  # zkusit najit dle modelu - v pripade null nebo spatneho make - vetsinou davaji nazev bazaru
  # --> z analyzy vyplynulo, ze pokud je null make tak z 98% je null i model. 
  #unknown_make = [None,"Aixam", "Ligier", "Ssang Yong" ]
  #df = df.withColumn("make", f.when(f.col("make").isin(unknown_make), ))
  

# COMMAND ----------

def replace_null(df):
  df = df.fillna("UNKNOWN", subset=["body","make","model","fuel_type","drive"])
  return df

# COMMAND ----------

def run_preprocessing(df, source_type):
  
  df = repair_wrong_make(df)
  
  if source_type == "dm_xmls":
    df = replace_null(df)
  
  #df = add_loc_data(config,df)
  
  df = (df
        .withColumn("source",f.lit(source_type))
        .withColumn("source_id",f.col("id"))
       )
  
  return df

# COMMAND ----------

def get_list_of_values_of_col_from_sources(dm_xmls, dm_xmls_wp, column = "server_id"):

  list_values_xmls = dm_xmls.select(column).distinct().rdd.flatMap(lambda x: x).collect() 
  list_values_xmls_wp = dm_xmls_wp.select(column).distinct().rdd.flatMap(lambda x: x).collect()
  list_values = list(set(list_values_xmls+list_values_xmls_wp))
  list_values.sort()
   
  return list_values

# COMMAND ----------

def split_values_in_list_to_batches(list_values, batch_size = 50):
  
  list_values = [list_values[i * batch_size:(i + 1) * batch_size] for i in range((len(list_values) + batch_size - 1) // batch_size )] 
    
  return list_values

# COMMAND ----------

def get_history_df(server_id, output_tb_name, window_history_params,wp_priority_attributes):
    
    get_last_processed_server_stock_date = (spark.sql(f"show partitions {output_tb_name}")
                                        .filter(f.col("server_id")==server_id)
                                        .select("date_in_date")
                                        .distinct()
                                        .sort("date_in_date",ascending=False)
                                       )
    
    if get_last_processed_server_stock_date.rdd.isEmpty(): 
        df_history_window = spark.table(output_tb_name).filter(f.col("server_id")==server_id)
        return df_history_window
    
    get_last_processed_server_stock_date = get_last_processed_server_stock_date.first()[0]
    print(get_last_processed_server_stock_date)
        
    output_df_columns = spark.table(output_tb_name).columns 
    rem_cols = ["date_in_date"]+wp_priority_attributes
  
    df_history_window = (spark.table(output_tb_name)
                     .filter(f.col("server_id")==server_id)
                     .filter(f.col("date_in_date")>=get_last_processed_server_stock_date + datetime.timedelta(days=-1*window_history_params))
                     .select([c for c in output_df_columns if c not in rem_cols])
                     .dropDuplicates(subset = ["guid"])
                    )
  
    return df_history_window

# COMMAND ----------

def has_column(df, col):
        try:
            df[col]
            return True
        except pyspark.sql.utils.AnalysisException:
            return False

# COMMAND ----------

def get_columns_from_output(columns_list, df):

    existing_cols = []
    for column in columns_list:
        if has_column(df,column):
            existing_cols.append(column)
            
    return existing_cols

# COMMAND ----------

def new_guids_to_xmls_merged(dm_xmls:DataFrame, 
                             dm_xmls_wp:DataFrame, 
                             output_tb_name:str, 
                             window_history_params:int, 
                             wp_priority_attributes:List[str], 
                             batch_size:int
                            ):
  '''
  dm_xmls: bronz delta table source of dm_xmls
  dm_xmls_wp: bronz delta table source of dm_xmls_wp
  output_tb_name: string name of silver output table
  window_history_params: number of days from history of the server used for fill attributes of exising guids
  wp_priority_attributes: attributes which has to be selected from wp (are changing during time like price)
  batch_size: number of days which will be used for join (size of interval) if source data has more then 1 days.
  '''
  
  rem_cols = wp_priority_attributes
    
  output_df_columns = spark.table(output_tb_name).columns 
  dm_xmls_existing_cols = get_columns_from_output(output_df_columns, dm_xmls)

  if not has_column(dm_xmls_wp, "date_in_date"):
    dm_xmls_wp = dm_xmls_wp.withColumn("date_in_date", f.to_date(f.col("inserted")))

  dm_xmls = run_preprocessing(dm_xmls,source_type = "dm_xmls").select([c for c in dm_xmls_existing_cols if c not in rem_cols]).dropDuplicates(subset = ["guid","server_id"])
  dm_xmls_wp = (run_preprocessing(dm_xmls_wp, source_type = "dm_xmls_wp")
                 .withColumnRenamed("price_orig","price")
                .select(["guid","server_id","date_in_date"]+rem_cols)
                .dropDuplicates(subset=["guid","server_id","date_in_date"])
               )
  
  dm_xmls = dm_xmls.persist()
  dm_xmls_wp = dm_xmls_wp.persist()
  
  list_servers = get_list_of_values_of_col_from_sources(dm_xmls, dm_xmls_wp, column = "server_id")
    
  df_final_list = []
  for server_id in list_servers: 
    dm_xmls_server = dm_xmls.filter(f.col("server_id")==server_id)
    dm_xmls_wp_server = dm_xmls_wp.filter(f.col("server_id")==server_id)
    
    df_history_window_per_server = get_history_df(server_id = server_id, 
                                       output_tb_name = output_tb_name, 
                                       window_history_params = window_history_params,
                                       wp_priority_attributes=wp_priority_attributes)
    
    list_values_days = get_list_of_values_of_col_from_sources(dm_xmls_server, dm_xmls_wp_server, column = "date_in_date")
    list_days = split_values_in_list_to_batches(list_values_days, batch_size = batch_size)

    list_df_final_days = []
    for day_interval in list_days:
      
      min_day = min(day_interval)
      max_day = max(day_interval)

      dm_xmls_server_days = (dm_xmls_server
                              .filter(f.col("date_in_date")>=min_day)
                              .filter(f.col("date_in_date")<=max_day)
                            )

      dm_xmls_wp_server_days = (dm_xmls_wp_server
                              .filter(f.col("date_in_date")>=min_day)
                              .filter(f.col("date_in_date")<=max_day)
                            )
    
      df_final_days_merge1 = (dm_xmls_wp_server_days
                               .join(df_history_window_per_server.select([c for c in dm_xmls_server_days.columns if c not in {'date_in_date'}]), 
                                     on = ["server_id","guid"], 
                                     how = "left")
                              .join(dm_xmls_server_days.select("guid","server_id"),
                                    on = ["server_id","guid"], 
                                     how = "left_anti"
                                   )
                             )


      df_final_days_merge2  = (dm_xmls_wp_server_days
                               .join(dm_xmls_server_days.select([c for c in dm_xmls_server_days.columns if c not in {'date_in_date'}]), 
                                     on = ["server_id","guid"], 
                                     how = "inner"
                                    ))
    
      df_final_days = df_final_days_merge1.union(df_final_days_merge2)
      list_df_final_days.append(df_final_days)

    df_final_server = functools.reduce(DataFrame.union, list_df_final_days)
    df_final_list.append(df_final_server)

  df_final = functools.reduce(DataFrame.union, df_final_list)
  
  dm_xmls = dm_xmls.unpersist()
  dm_xmls_wp = dm_xmls_wp.unpersist()
  
  return df_final

# COMMAND ----------

#dm_xmls = spark.read.format("parquet").load("dbfs:/mnt/stauresdwhrawprod/dmeu/DataMiningEU/dbo_dm_xmls/dm_xmls_20211105_160735.parquet")
#dm_xmls_wp = spark.read.format("parquet").load("dbfs:/mnt/stauresdwhrawprod/dmeu/DataMiningEU/dbo_dm_xmls_wp_guid_wm3/dm_xmls_wp_guid_wm3_20211105_162258.parquet")
#dm_xmls = spark.table("hive_metastore.dwh_ms_poc_dmxmls_merge.bronze_dm_xmls")
#dm_xmls_wp = spark.table("hive_metastore.dwh_ms_poc_dmxmls_merge.bronze_dm_xmls_wp")
#wp_priority_attributes = ["price","tachometer"]
#window_history_params = 8
#batch_size = 50
#output_tb_name = "hive_metastore.dwh_ms_poc_dmxmls_merge.silver_dm_xmls_merge" 

# COMMAND ----------

#df_final = new_guids_to_xmls_merged(dm_xmls, dm_xmls_wp, output_tb_name, window_history_params, wp_priority_attributes, batch_size)

# COMMAND ----------

#df_final = df_final.cache()

# COMMAND ----------

#dm_xmls_wp.withColumn("stock_date",f.to_date(f.col("inserted"))).dropDuplicates(subset = ["guid","stock_date"]).count()

# COMMAND ----------

#dm_xmls.count()

# COMMAND ----------

#df_final.count()

# COMMAND ----------

#df_final.agg(*[f.count(f.when(f.isnull(c), c)).alias(c) for c in df_final.columns]).show()
