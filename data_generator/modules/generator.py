
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
    
    return df_person
  

def id_change(spark, df_person, df_playbook, copy_num):
    
    return df_person
  

def id_reuse(spark, df_person, df_playbook, copy_num):
    
    return df_person
  

def delete_merge_patient(spark, df_person, df_playbook, copy_num):
    
    return df_person