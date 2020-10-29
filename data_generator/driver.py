from modules.generator import delete_path
from modules.generator import create_path
from modules.generator import add_new_patient
from modules.generator import id_change
from modules.generator import id_reuse
from modules.generator import delete_merge_patient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
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
       numofcopy = arg
        
  print("Creating %s copies of dataset including Day0 ..." % numofcopy)
  
  #-- Initialize spark session
  spark = SparkSession\
      .builder\
      .getOrCreate()

  #-- Load the initial copy of data - Day0
  #-- Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day0
  df_person = spark.read.parquet("/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/person")
  #df_visit = spark.read.parquet("/projects/cch/patient-merge/mimic_omop_tables/experiment/Day0/visit_occurrence")
  #print(df_person.head(5))

  #-- Load the playbook csv file to create mimic data (This file needs to be copied to hdfs prior)
  df_playbook = spark.read.csv("/projects/cch/patient-merge/mimic_omop_tables/experiment/src_sample_person_200.csv", header=True)
  #print(df_playbook.head(5))

  
  #-- Loop numofcopy and create each of the copies of dataset
  for i in range(int(numofcopy)-1):
    print("Day %s is being created ..." % str((i+1)*7) )
    
    df_new_person = add_new_patient(spark, df_person, df_playbook, i+1)
        
    
    #-- Create data - add/change/delete
    #- Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/
    path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/Day" + str((i+1)*7)
    person_path = path + "/" + "person"
    visit_path = path + "/" + "visit_occurrence"
    #df_new_person.write.parquet(person_path)

  
  
  print("Finish successfully!")

  #delete_path(spark,"/projects/cch/patient-merge/mimic_omop_tables/experiment/Day07")

  spark.stop()
  
if __name__ == "__main__":
   main(sys.argv[1:])