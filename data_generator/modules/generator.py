from pyspark.sql.functions import *


def create_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.create(sc._jvm.org.apache.hadoop.fs.Path(path))

  
  
def delete_path(spark, path):
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)


def add_new_patient(spark, df_person, df_playbook, copy_num):
    
    df_new_person = df_playbook.filter((df_playbook.copy_num == copy_num) & (df_playbook.category == 'AD'))
    df_new_person = df_new_person.select(df_new_person.columns[:18])
    #df_new_person.show(truncate=False)
      
    df_person = df_person.union(df_new_person)
    
    return df_person
  

def id_change(spark, df_person, df_playbook, copy_num):
    
    df_id_change = df_playbook.filter((df_playbook.copy_num == copy_num) & (df_playbook.category == 'IC'))
    df_new_data = df_id_change.withColumn('person_id',df_id_change.target_person_id)
    df_new_data = df_new_data.select(df_new_data.columns[:18])
    
    #exclude the inner part
    df_person = df_person.join(df_id_change, on=['person_id'], how='left_anti')
    df_person = df_person.union(df_new_data)
    
    return df_person
  

def id_reuse(spark, df_person, df_playbook, copy_num):
    
    return df_person
  

def delete_merge_patient(spark, df_person, df_playbook, copy_num):
    
    return df_person