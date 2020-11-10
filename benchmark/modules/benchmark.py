from pyspark.sql.functions import *

def report_new_add_patient(spark, df_person_0, df_person_1):
    
    df_person_0 = df_person_0.alias("df_person_0")
    df_person_1 = df_person_1.alias("df_person_1")
    
    df_inner = df_person_0.join(df_person_1,
              (
                (col('df_person_0.gender_concept_id') == col('df_person_1.gender_concept_id')) &
                (col('df_person_0.year_of_birth') == col('df_person_1.year_of_birth')) &
                (col('df_person_0.month_of_birth') == col('df_person_1.month_of_birth')) &
                (col('df_person_0.day_of_birth') == col('df_person_1.day_of_birth')) &
                (col('df_person_0.race_concept_id') == col('df_person_1.race_concept_id')) &
                (col('df_person_0.ethnicity_concept_id') == col('df_person_1.ethnicity_concept_id'))
              ),
              'inner'
    )
    
    
    df_return = df_person_1.select('person_id').subtract(df_inner.select('df_person_1.person_id'))
    
    df_return.distinct().show(truncate=False)
    
    return None

  
def report_id_change_patient(spark, df_person_0, df_person_1):
  
    df_person_0 = df_person_0.alias("df_person_0")
    df_person_1 = df_person_1.alias("df_person_1")
    
    df_inner_without_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.gender_concept_id') == col('df_person_1.gender_concept_id')) &
                (col('df_person_0.year_of_birth') == col('df_person_1.year_of_birth')) &
                (col('df_person_0.month_of_birth') == col('df_person_1.month_of_birth')) &
                (col('df_person_0.day_of_birth') == col('df_person_1.day_of_birth')) &
                (col('df_person_0.race_concept_id') == col('df_person_1.race_concept_id')) &
                (col('df_person_0.ethnicity_concept_id') == col('df_person_1.ethnicity_concept_id'))
              ),
              'inner'
    )
    
    df_inner_with_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) 
              ),
              'inner'
    )
      
    
    df_new_and_id_change = df_person_1.select('person_id').subtract(df_inner_with_id.select('df_person_1.person_id'))
    df_new = df_person_1.select('person_id').subtract(df_inner_without_id.select('df_person_1.person_id'))
    
    df_id_change = df_new_and_id_change.select('person_id').subtract(df_new.select('person_id'))
    
    df_id_change.distinct().show(truncate=False)
    
    return None

  
def report_id_reuse_patient(spark, df_person_0, df_person_1):
  
    df_person_0 = df_person_0.alias("df_person_0")
    df_person_1 = df_person_1.alias("df_person_1")
    
    #-- will include list of ID reuse and ID change patient
    df_inner_with_diff_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.gender_concept_id') == col('df_person_1.gender_concept_id')) &
                (col('df_person_0.year_of_birth') == col('df_person_1.year_of_birth')) &
                (col('df_person_0.month_of_birth') == col('df_person_1.month_of_birth')) &
                (col('df_person_0.day_of_birth') == col('df_person_1.day_of_birth')) &
                (col('df_person_0.race_concept_id') == col('df_person_1.race_concept_id')) &
                (col('df_person_0.ethnicity_concept_id') == col('df_person_1.ethnicity_concept_id')) &
                #(col('df_person_0.person_source_value') == col('df_person_1.person_source_value')) &
                (col('df_person_0.person_id') != col('df_person_1.person_id')) 
              ),
              'inner'
    )
    
    #-- pure ID change patinet
    df_inner_without_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.gender_concept_id') == col('df_person_1.gender_concept_id')) &
                (col('df_person_0.year_of_birth') == col('df_person_1.year_of_birth')) &
                (col('df_person_0.month_of_birth') == col('df_person_1.month_of_birth')) &
                (col('df_person_0.day_of_birth') == col('df_person_1.day_of_birth')) &
                (col('df_person_0.race_concept_id') == col('df_person_1.race_concept_id')) &
                (col('df_person_0.ethnicity_concept_id') == col('df_person_1.ethnicity_concept_id'))
              ),
              'inner'
    )
    
    df_inner_with_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) 
              ),
              'inner'
    )
      
    
    df_new_and_id_change = df_person_1.select('person_id').subtract(df_inner_with_id.select('df_person_1.person_id'))
    df_new = df_person_1.select('person_id').subtract(df_inner_without_id.select('df_person_1.person_id'))
    
    df_id_change = df_new_and_id_change.select('person_id').subtract(df_new.select('person_id'))
    
    #-- get ID reuse patient
    df_id_reuse = df_inner_with_diff_id.select(col('df_person_1.person_id')).subtract(df_id_change)
    df_id_reuse.distinct().show(truncate=False)
    
    return None
  
  
def report_delete_patient(spark, df_person_0, df_person_1, df_visit_0, df_visit_1):
    
    df_person_0 = df_person_0.alias("df_person_0")
    df_person_1 = df_person_1.alias("df_person_1")
    df_visit_0 = df_visit_0.alias("df_0")
    df_visti_1 = df_visit_1.alias("df_1")
    
    df_inner_with_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) 
              ),
              'inner'
    )
    
    #-- id list includes: deleted patient, merged patient, and id change patients
    df_deleted_merged_changed_id = df_person_0.select('person_id').subtract(df_inner_with_id.select('df_person_1.person_id'))
    
    df_visit_d_m_c = df_visit_0.join(df_deleted_merged_changed_id, \
                                     df_visit_0.person_id == df_deleted_merged_changed_id.person_id, 'inner')
    
    df_visit_d_m_c = df_visit_d_m_c.select(col('visit_occurrence_id'),col('df_person_0.person_id'))
    df_person_d_m_c = df_visit_d_m_c.select(col('df_person_0.person_id'))
    df_visit_d_m_c = df_visit_d_m_c.withColumnRenamed('visit_occurrence_id','visit_id')
    df_visit_d_m_c = df_visit_d_m_c.withColumnRenamed('person_id','pat_id')
    
    #-- get deleted person id
    df_person_deleted = df_visit_d_m_c.join(df_visit_1, \
                                  df_visit_1.visit_occurrence_id == df_visit_d_m_c.visit_id, 'left_anti')
    df_person_deleted = df_person_deleted.select(col('pat_id'))
    df_person_deleted = df_person_deleted.withColumnRenamed('pat_id','person_id')
    
    df_person_deleted.distinct().show()
    
    return None

  
def report_merge_patient(spark, df_person_0, df_person_1, df_visit_0, df_visit_1):
  
    
    
    return None