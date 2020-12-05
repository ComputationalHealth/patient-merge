from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType
import pandas as pd

lst_of_path = [
  '/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/',
  '/projects/cch/patient-merge/mimic_omop_tables/experiment/Day7/',
  '/projects/cch/patient-merge/mimic_omop_tables/experiment/Day14/'
]

#-- Initialize spark session
spark = SparkSession\
    .builder\
    .getOrCreate()

person_0_path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/"
df_person_base = spark.read.parquet(person_0_path + "person")
cnt_uniq_person_base = df_person_base.select('person_id').distinct().count()

columns = ['copy2copy', 'Num of Uniq Patient', 'AD', 'IC', 'IR', 'DL', 'DM']
vals = [(person_0_path, cnt_uniq_person_base, 0, 0, 0, 0, 0)]

df_matric = spark.createDataFrame(vals,columns)
#df_matric.printSchema()
    
for i in range(len(lst_of_path)-1):
    
    base_path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output_mimic/"
    
    ad_path = base_path + "AD_cp" + str(i+1) + "_to_cp" + str(i) + ".csv"
    ic_path = base_path + "IC_cp" + str(i+1) + "_to_cp" + str(i) + ".csv"
    ir_path = base_path + "IR_cp" + str(i+1) + "_to_cp" + str(i) + ".csv"
    dl_path = base_path + "DL_cp" + str(i+1) + "_to_cp" + str(i) + ".csv"
    dm_path = base_path + "DM_cp" + str(i+1) + "_to_cp" + str(i) + ".csv"
    
    
    #--read all result and count
    df_ad = spark.read.csv(ad_path)
    df_ad_count = df_ad.count()
    if df_ad_count > 0:
        df_ad_count = df_ad_count - 1
    
    df_ic = spark.read.csv(ic_path)
    df_ic_count = df_ic.count()
    if df_ic_count > 0:
        df_ic_count = df_ic_count - 1
    
    df_ir = spark.read.csv(ir_path)
    df_ir_count = df_ir.count()
    if df_ir_count > 0:
        df_ir_count = df_ir_count - 1
    
    df_dl = spark.read.csv(dl_path)
    df_dl_count = df_dl.count()
    if df_dl_count > 0:
        df_dl_count = df_dl_count - 1
    
    df_dm = spark.read.csv(dm_path)
    df_dm_count = df_dm.count()
    if df_dm_count > 0:
        df_dm_count = df_dm_count - 1
    
    copy2copy = lst_of_path[i+1]
    df_person_item = spark.read.parquet(copy2copy + "person")
    cnt_uniq_person_item = df_person_item.select('person_id').distinct().count()
    
    df_item = spark.createDataFrame([(copy2copy, cnt_uniq_person_item, df_ad_count, df_ic_count,\
                                      df_ir_count, df_dl_count, df_dm_count)],columns )
                                    
    df_matric = df_matric.union(df_item)
    
    

df_matric.show(30, truncate=False)
                                     
#-- Close spark session
spark.stop()