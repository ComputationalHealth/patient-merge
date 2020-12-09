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

columns = ['copy2copy', 'Num of Uniq Patient', 'Sex Change', 'DOB Change', 'Race Change', 'Eth Change']
vals = [(person_0_path, cnt_uniq_person_base, 0, 0, 0, 0)]

df_matric = spark.createDataFrame(vals,columns)
#df_matric.printSchema()
    
for i in range(len(lst_of_path)-1):
    
    if i+1 < len(lst_of_path):
      path_0 = lst_of_path[i]   #path_0 is baseline
      path_1 = lst_of_path[i+1]
    
      person_path_0 = path_0 + "person"
      person_path_1 = path_1 + "person"
    
      df_person_0 = spark.read.parquet(person_path_0)
      df_person_1 = spark.read.parquet(person_path_1)
    
      df_person_0 = df_person_0.alias("df_person_0")
      df_person_1 = df_person_1.alias("df_person_1")
      
      #-- same person id, but sex change
      df_sex_ch = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) &
                (
                  (col('df_person_0.gender_concept_id') != col('df_person_1.gender_concept_id'))
                )
              ),
              'inner'
      )
    
      df_sex_ch = df_sex_ch.select(col('df_person_0.person_id')).distinct()
      df_sex_ch_count = df_sex_ch.count()
      
      #-- same person id, but dob change
      df_dob_ch = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) &
                (
                  (col('df_person_0.date_of_birth') != col('df_person_1.date_of_birth'))
                )
              ),
              'inner'
      )
    
      df_dob_ch = df_dob_ch.select(col('df_person_0.person_id')).distinct()
      df_dob_ch_count = df_dob_ch.count()
      
      
      #-- same person id, but race change
      df_race_ch = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) &
                (
                  (col('df_person_0.race_concept_id') != col('df_person_1.race_concept_id'))
                )
              ),
              'inner'
      )
    
      df_race_ch = df_race_ch.select(col('df_person_0.person_id')).distinct()
      df_race_ch_count = df_race_ch.count()
      
      #-- same person id, but ethnicity change
      df_eth_ch = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) &
                (
                  (col('df_person_0.ethnicity_concept_id') != col('df_person_1.ethnicity_concept_id'))
                )
              ),
              'inner'
      )
    
      df_eth_ch = df_eth_ch.select(col('df_person_0.person_id')).distinct()
      df_eth_ch_count = df_eth_ch.count()
      
    
      df_person_item = spark.read.parquet(person_path_1)
      cnt_uniq_person_item = df_person_item.select('person_id').distinct().count()
    
      df_item = spark.createDataFrame([(person_path_1, cnt_uniq_person_item, df_sex_ch_count, df_dob_ch_count,\
                                      df_race_ch_count, df_eth_ch_count)],columns )
      
      df_item.show() 
      
      df_matric = df_matric.union(df_item)
    

df_matric.show(30, truncate=False)
                                     
#-- Close spark session
spark.stop()