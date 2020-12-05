from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType
import pandas as pd
import datetime

lst_of_path = [
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


baseline_visit_path = "/projects/cch/patient-merge/versioned_omop/2020-11-02/visit_occurrence"  
baseline_date_str = "2020-11-02"
baseline_datetime = datetime.datetime.strptime(baseline_date_str,'%Y-%m-%d') 

df_visit_baseline = spark.read.parquet(baseline_visit_path)
#-- exclude future encounters 
df_visit_baseline = df_visit_baseline.filter(df_visit_baseline.visit_start_datetime < baseline_datetime)


#-- Case 1 Patient with discharge time --
df_visit_case_1 = df_visit_baseline.where(col("visit_end_datetime").isNotNull())
df_visit_case_1 = df_visit_case_1.alias('df_base_1')

columns = ['copy#', 'discharge_status_change_only', 'discharge_time_change_only', 'both']
vals = [("2020-11-02", 0, 0, 0)]
df_case_1 = spark.createDataFrame(vals,columns)

for i in range(len(lst_of_path)):
    path = lst_of_path[i]
    visit_path = path + "visit_occurrence"
    
    dt_str = path.split('/')[len(path.split('/')) - 2]
    visit_datetime = datetime.datetime.strptime(dt_str,'%Y-%m-%d')
    
    df_visit = spark.read.parquet(visit_path)
    #-- exclude future encounters 
    df_visit = df_visit.filter(df_visit.visit_start_datetime < visit_datetime)
    df_visit = df_visit.alias('df_visit')
    
    #-- get the patients with discharge status change but not discharge time change
    df_inner_status_change = df_visit_case_1.join(df_visit,
              (
                (col('df_base_1.visit_occurrence_id') == col('df_visit.visit_occurrence_id')) &
                (col('df_base_1.visit_end_datetime') == col('df_visit.visit_end_datetime')) &
                (col('df_base_1.discharge_to_source_value') != col('df_visit.discharge_to_source_value'))
              ),
              'inner'
    )
    
    cnt_uniq_status_change = df_inner_status_change.select('df_base_1.person_id').distinct().count()
    
    #-- get the patients with discharge time change, but not discharge status change
    df_inner_time_change = df_visit_case_1.join(df_visit,
              (
                (col('df_base_1.visit_occurrence_id') == col('df_visit.visit_occurrence_id')) &
                (col('df_base_1.discharge_to_source_value') == col('df_visit.discharge_to_source_value')) &
                (col('df_base_1.visit_end_datetime') != col('df_visit.visit_end_datetime'))
              ),
              'inner'
    )
    
    cnt_uniq_time_change = df_inner_time_change.select('df_base_1.person_id').distinct().count()
    
    #-- get the patients with both discharge time and discharge status change
    df_inner_both_change = df_visit_case_1.join(df_visit,
              (
                (col('df_base_1.visit_occurrence_id') == col('df_visit.visit_occurrence_id')) &
                (col('df_base_1.discharge_to_source_value') != col('df_visit.discharge_to_source_value')) &
                (col('df_base_1.visit_end_datetime') != col('df_visit.visit_end_datetime'))
              ),
              'inner'
    )

    cnt_uniq_both_change = df_inner_both_change.select('df_base_1.person_id').distinct().count()
    
    #-- aggregate to display together later
    df_item = spark.createDataFrame([(dt_str, cnt_uniq_status_change, cnt_uniq_time_change,\
                                      cnt_uniq_both_change)],columns )
    
    #-- print each iteration
    df_item.show()
    
    df_case_1 = df_case_1.union(df_item)
    
    
    
#-- Case 2 Patient without discharge time, but has discharge status --
df_visit_case_2 = df_visit_baseline.where(col("visit_end_datetime").isNull() & col("discharge_to_source_value").isNotNull())
df_visit_case_2 = df_visit_case_2.alias('df_base_2')

df_case_2 = spark.createDataFrame(vals,columns)

for i in range(len(lst_of_path)):
    path = lst_of_path[i]
    visit_path = path + "visit_occurrence"
    
    dt_str = path.split('/')[len(path.split('/')) - 2]
    visit_datetime = datetime.datetime.strptime(dt_str,'%Y-%m-%d')
    
    df_visit = spark.read.parquet(visit_path)
    #-- exclude future encounters 
    df_visit = df_visit.filter(df_visit.visit_start_datetime < visit_datetime)
    df_visit = df_visit.alias('df_visit')
    
    #-- get the patients with discharge status change but not discharge time change
    df_inner_status_change = df_visit_case_2.join(df_visit,
              (
                (col('df_base_2.visit_occurrence_id') == col('df_visit.visit_occurrence_id')) &
                (col('df_base_2.visit_end_datetime') == col('df_visit.visit_end_datetime')) &
                (col('df_base_2.discharge_to_source_value') != col('df_visit.discharge_to_source_value'))
              ),
              'inner'
    )
    
    cnt_uniq_status_change = df_inner_status_change.select('df_base_2.person_id').distinct().count()
    
    #-- get the patients with discharge time change, but not discharge status change
    df_inner_time_change = df_visit_case_2.join(df_visit,
              (
                (col('df_base_2.visit_occurrence_id') == col('df_visit.visit_occurrence_id')) &
                (col('df_base_2.discharge_to_source_value') == col('df_visit.discharge_to_source_value')) &
                (col('df_base_2.visit_end_datetime') != col('df_visit.visit_end_datetime'))
              ),
              'inner'
    )
    
    cnt_uniq_time_change = df_inner_time_change.select('df_base_2.person_id').distinct().count()
    
    #-- get the patients with both discharge time and discharge status change
    df_inner_both_change = df_visit_case_2.join(df_visit,
              (
                (col('df_base_2.visit_occurrence_id') == col('df_visit.visit_occurrence_id')) &
                (col('df_base_2.discharge_to_source_value') != col('df_visit.discharge_to_source_value')) &
                (col('df_base_2.visit_end_datetime') != col('df_visit.visit_end_datetime'))
              ),
              'inner'
    )

    cnt_uniq_both_change = df_inner_both_change.select('df_base_2.person_id').distinct().count()
    
    #-- aggregate to display together later
    df_item = spark.createDataFrame([(dt_str, cnt_uniq_status_change, cnt_uniq_time_change,\
                                      cnt_uniq_both_change)],columns )
    
    #-- print each iteration
    df_item.show()
    
    df_case_2 = df_case_2.union(df_item)



#-- Print on the screen    
print("Case 1: Patient with discharge time. Unique number of patient at baseline (2020-11-02) is - ")
print(str(df_visit_case_1.select('person_id').distinct().count()))
df_case_1.show(15, truncate=False)    

print("Case 2: Patient without discharge time, but has discharge status. Unique number of patient at baseline (2020-11-02) is - ")
print(str(df_visit_case_2.select('person_id').distinct().count()))
df_case_2.show(15, truncate=False)


#-- Close spark session
spark.stop()