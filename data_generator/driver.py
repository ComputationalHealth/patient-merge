from modules.generator import delete_path
from modules.generator import create_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import pandas as pd

spark = SparkSession\
    .builder\
    .getOrCreate()

# Get the initial copy of data - Day00
# Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day00
#df_person = spark.read.parquet("/projects/cch/patient-merge/mimic_omop_tables/experiment/Day00/person")
#df_visit = spark.read.parquet("/projects/cch/patient-merge/mimic_omop_tables/experiment/Day00/visit_occurrence")
#print(df_person.head(5))


# Create Day07 data - add/change/delete
# Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day07
create_path(spark, "/projects/cch/patient-merge/mimic_omop_tables/experiment/Day07")



# Create Day14 data - add/change/delete
# Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day14



# Create Day21 data - add/change/delete
# Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day21



# Create Day28 data - add/change/delete
# Path - /projects/cch/patient-merge/mimic_omop_tables/experiment/Day28




#delete_path(spark,"/projects/cch/patient-merge/mimic_omop_tables/experiment/Day07")

spark.stop()