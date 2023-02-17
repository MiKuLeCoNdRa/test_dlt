# Databricks notebook source
import dlt
import new_guids_to_xmls_merged from lucka_ntb

# COMMAND ----------

# MAGIC %run /Users/martin.levy@aaaauto.cz/dm_xmls_merged_DLT

# COMMAND ----------

# Silver
@dlt.table(name="silver_dmxmls_wp_merge", 
        comment="Merge dmxmls and wp bronze tables",
        table_properties= {
                            "quality":"silver", 
                            "delta.minReaderVersion":"2", 
                            "delta.minWriterVersion":"5",
                            "pipelines.autoOptimize.managed": "true"
        },
        partition_cols=["server_id", "stock_date"] 
)
def silver_dmxmls_wp_merge():
    # input streams 
    df_dmxmls = dlt.read_stream("bronze_dm_xmls")  
    df_wp = dlt.read_stream("bronze_dm_xmls_wp") 
    
    #fce od lucky
    df_final = new_guids_to_xmls_merged(df_dmxmls, df_wp,"hive_metastore.dwh_ms_poc_dmxmls_merge.silver_dm_xmls_merge", 3, ["price","tachometer"], 31)
  
    
    # return final merged result
    return df_final 
