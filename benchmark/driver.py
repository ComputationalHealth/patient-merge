from modules.benchmark import report_new_add_patient
from modules.benchmark import report_id_change_patient
from modules.benchmark import report_id_reuse_patient
from modules.benchmark import report_delete_patient
from modules.benchmark import report_merge_patient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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