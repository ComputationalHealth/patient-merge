from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType
import pandas as pd

lst_of_path = [
  '/projects/cch/patient-merge/versioned_omop/2020-07-30/',
  '/projects/cch/patient-merge/versioned_omop/2020-08-02/',
  '/projects/cch/patient-merge/versioned_omop/2020-08-09/',
  '/projects/cch/patient-merge/versioned_omop/2020-08-16/',
  '/projects/cch/patient-merge/versioned_omop/2020-08-19/',
  '/projects/cch/patient-merge/versioned_omop/2020-08-23/',
  '/projects/cch/patient-merge/versioned_omop/2020-08-30/',
  ###'/projects/cch/patient-merge/versioned_omop/2020-09-04/',
  '/projects/cch/patient-merge/versioned_omop/2020-09-06/',
  '/projects/cch/patient-merge/versioned_omop/2020-09-20/',
  '/projects/cch/patient-merge/versioned_omop/2020-09-27/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-04/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-11/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-18/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-25/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-27/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-28/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-29/',
  '/projects/cch/patient-merge/versioned_omop/2020-10-30/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-02/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-03/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-04/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-05/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-06/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-07/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-08/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-09/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-10/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-11/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-12/',
  '/projects/cch/patient-merge/versioned_omop/2020-11-13/'
]

#-- Initialize spark session
spark = SparkSession\
    .builder\
    .getOrCreate()

person_0_path = "/projects/cch/patient-merge/versioned_omop/2020-07-30/"
df_person_base = spark.read.parquet(person_0_path + "person")
cnt_uniq_person_base = df_person_base.select('person_id').distinct().count()

columns = ['copy2copy', 'Num of Uniq Patient', 'AD', 'IC', 'IR', 'DL', 'DM']
vals = [(person_0_path, cnt_uniq_person_base, 0, 0, 0, 0, 0)]

df_matric = spark.createDataFrame(vals,columns)
#df_matric.printSchema()
    
for i in range(len(lst_of_path)-1):
    
    base_path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output/"
    
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