from modules.generator import delete_path
from modules.generator import create_path
from modules.generator import add_new_patient
from modules.generator import id_change
from modules.generator import id_reuse
from modules.generator import pure_delete_patient
from modules.generator import delete_merge_patient
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import getopt,sys

def main(argv):
  
  #-- Give instruction to run the code, get the input number of copies we want to create
  short_options = "hn:v"
  long_options = ["help", "numofcopy=", "verbose"]
  try:
    opts, args = getopt.getopt(argv,short_options,long_options)
  except getopt.GetoptError:
    print('driver.py -n <numofcopy>')
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
       print('driver.py -n <numofcopy>')
       sys.exit()
    elif opt in ("-n", "--numofcopy"):
       if int(arg) > 7:
          print('Max numofcopy is 7')
          sys.exit(2)
       else:
          numofcopy = arg
        
  print("Creating %s copies of dataset including Day0 ..." % numofcopy)
  
  #-- Initialize spark session
  spark = SparkSession\
      .builder\
      .getOrCreate()

  #-- Load the initial copy of data - Day0
  #-- Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day0
  df_person = spark.read.parquet("/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/person")
  df_visit = spark.read.parquet("/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/visit_occurrence")
  #df_person.show(5)

  #-- Load the playbook csv file to create mimic data (This file needs to be copied to hdfs prior)
  df_playbook = spark.read.csv("/projects/cch/patient-merge/mimic_omop_tables/experiment/src_sample_person_200.csv", header=True)
  #df_playbook.show(5)

  
  #-- Loop numofcopy and create each of the copies of dataset
  for i in range(int(numofcopy)-1):
    print("Day %s is being created ..." % str((i+1)*7) )
    
    #-- add new patient | New patient case
    df_new_person = add_new_patient(spark, df_person, df_playbook, i+1)
    
    #-- change id for couple patients | ID change case
    df_new_person, df_new_visit = id_change(spark, df_new_person, df_visit, df_playbook, i+1)
    
    #-- swap id for couple patients | ID reuse case
    df_new_person = id_reuse(spark, df_new_person, df_playbook, i+1)
    
    #-- purely delete couple patients and their associated visits
    df_new_person, df_new_visit = pure_delete_patient(spark, df_new_person, df_new_visit, df_playbook, i+1)
    
    #-- merge patients with their visits
    df_new_person, df_new_visit = delete_merge_patient(spark, df_new_person, df_new_visit, df_playbook, i+1)
    
    #-- copy df_new_person to df_person for the next loop, df_visit as well
    df_person = df_new_person
    df_visit = df_new_visit
    
    #-- Create data - add/change/delete
    #- Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/
    path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/Day" + str((i+1)*7)
    person_path = path + "/" + "person"
    visit_path = path + "/" + "visit_occurrence"
    df_new_person.write.parquet(person_path)
    df_new_visit.write.parquet(visit_path)
  
  
  print("Finish successfully!")

  #delete_path(spark,"/projects/cch/patient-merge/mimic_omop_tables/experiment/Day07")
  
  #-- Close spark session
  spark.stop()
  
if __name__ == "__main__":
   main(sys.argv[1:])