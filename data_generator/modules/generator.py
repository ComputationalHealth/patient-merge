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
    df_id_reuse = df_playbook.filter((df_playbook.copy_num == copy_num) & (df_playbook.category == 'IR'))
    df_new_data = df_id_reuse.withColumn('person_id',df_id_reuse.target_person_id)
    df_new_data = df_new_data.select(df_new_data.columns[:18])
    
    #exclude the inner part
    df_person = df_person.join(df_id_reuse, on=['person_id'], how='left_anti')
    df_person = df_person.union(df_new_data)
    
    return df_person
  
  
def pure_delete_patient(spark, df_person, df_visit, df_playbook, copy_num):
    df_id_delete = df_playbook.filter((df_playbook.copy_num == copy_num) & (df_playbook.category == 'DL'))
    
    #-- remove person record based on playbook
    df_person = df_person.join(df_id_delete, df_id_delete.person_id == df_person.person_id, 'left_anti')
    
    #-- also remove visit_occurrence record for the list of person
    df_visit_delete = df_visit.join(df_id_delete, df_visit.person_id == df_id_delete.person_id, 'inner')
    df_visit = df_visit.join(df_visit_delete, df_visit.visit_occurrence_id == df_visit_delete.visit_occurrence_id, 'left_anti')
    
    return df_person, df_visit
  

def delete_merge_patient(spark, df_person, df_visit, df_playbook, copy_num):
    df_id_merge = df_playbook.filter((df_playbook.copy_num == copy_num) & (df_playbook.category == 'DM'))
    df_id_mapping = df_id_merge.select(col('person_id'), col('target_person_id'))
    df_id_mapping = df_id_mapping.withColumnRenamed('person_id','pat_id')
    
    #update visit_occurrence table
    df_visit = df_visit.alias("df_0")
    df_id_mapping = df_id_mapping.alias("df_1")
    
    df_visit_to_update = df_visit.join(df_id_mapping, col('person_id') == col('pat_id'), 'inner')
    df_visit_to_update = df_visit_to_update.withColumn('person_id',df_visit_to_update.target_person_id)
    
    df_visit_new = df_visit_to_update.select(df_visit_to_update.columns[:19])
    df_visit = df_visit.join(df_id_mapping, col('person_id') == col('pat_id'), 'left_anti')
    df_visit = df_visit.union(df_visit_new)
    
    #update person table
    df_person = df_person.join(df_id_mapping, col('person_id') == col('pat_id'), 'left_anti')
    
    return df_person, df_visit