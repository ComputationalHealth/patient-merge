from modules.benchmark_mimic import report_new_add_patient
from modules.benchmark_mimic import report_id_change_patient
from modules.benchmark_mimic import report_id_reuse_patient
from modules.benchmark_mimic import report_delete_patient
from modules.benchmark_mimic import report_merge_patient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import datetime

lst_of_path = [
  '/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/',
  '/projects/cch/patient-merge/mimic_omop_tables/experiment/Day7/',
  '/projects/cch/patient-merge/mimic_omop_tables/experiment/Day14/'
]

#-- Initialize spark session
spark = SparkSession\
    .builder\
    .getOrCreate()
      
for i in range(len(lst_of_path)-1):
  #-- In each loop, compare two snapshots, i.e. Day7 compares to Day0, Day14 compares to Day7
  if i+1 < len(lst_of_path):
    path_0 = lst_of_path[i]   #path_0 is baseline
    path_1 = lst_of_path[i+1]
    
    person_path_0 = path_0 + "person"
    person_path_1 = path_1 + "person"
    
    df_person_0 = spark.read.parquet(person_path_0)
    df_person_1 = spark.read.parquet(person_path_1)
    
    enc_path_0 = path_0 + "visit_occurrence"
    enc_path_1 = path_1 + "visit_occurrence"
    
    df_visit_0 = spark.read.parquet(enc_path_0)
    df_visit_1 = spark.read.parquet(enc_path_1)
    
    #-- Exclude future encounters
    #start_date_0 = path_0.split('/')[len(path_0.split('/')) - 2]   #2020-11-02
    #start_date_1 = path_1.split('/')[len(path_1.split('/')) - 2]
    
    #start_datetime_0 = datetime.datetime.strptime(start_date_0,'%Y-%m-%d')   #convert to datetime
    #start_datetime_1 = datetime.datetime.strptime(start_date_1,'%Y-%m-%d')
    
    #df_visit_0 = df_visit_0.filter(df_visit_0.visit_start_datetime < start_datetime_0)
    #df_visit_1 = df_visit_1.filter(df_visit_1.visit_start_datetime < start_datetime_1)
    #-----------------------------
        
    print("New Added Patients: copy " + str(i+1) + " -> copy " + str(i) )
    report_new_add_patient(spark, df_person_0, df_person_1, i)
    
    print("ID Change Patients: copy " + str(i+1) + " -> copy " + str(i) )
    report_id_change_patient(spark, df_person_0, df_person_1, i, 1)
    
    print("ID Reuse Patients: copy " + str(i+1) + " -> copy " + str(i) )
    report_id_reuse_patient(spark, df_person_0, df_person_1, i)
    
    print("Deleted Patients: copy " + str(i+1) + " -> copy " + str(i) )
    report_delete_patient(spark, df_person_0, df_person_1, df_visit_0, df_visit_1, i)
    
    print("Merged Patients: copy " + str(i+1) + " -> copy " + str(i) )
    report_merge_patient(spark, df_person_0, df_person_1, df_visit_0, df_visit_1, i)

#-- Close spark session
spark.stop()