from pyspark.sql.functions import *

def report_new_add_patient(spark, df_person_0, df_person_1, copy_num):
    
    df_person_0 = df_person_0.alias("df_person_0")
    df_person_1 = df_person_1.alias("df_person_1")
    
    df_new_diff_id =  df_person_1.join(df_person_0,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) 
              ),
              'left_anti'
    )
    
    df_new_diff_id = df_new_diff_id.alias("df_new_id")
    
    df_old_diff_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.person_id')) 
              ),
              'left_anti'
    )
    
    df_old_diff_id = df_old_diff_id.alias("df_old_id")
    
    df_new_person = df_new_diff_id.join(df_old_diff_id,
                    (
                      (col('df_old_id.gender_concept_id') == col('df_new_id.gender_concept_id')) &
                      (col('df_old_id.year_of_birth') == col('df_new_id.year_of_birth')) &
                      (col('df_old_id.month_of_birth') == col('df_new_id.month_of_birth')) &
                      (col('df_old_id.day_of_birth') == col('df_new_id.day_of_birth')) &
                      #(col('df_old_id.date_of_birth') == col('df_new_id.date_of_birth')) &
                      (col('df_old_id.race_concept_id') == col('df_new_id.race_concept_id')) &
                      (col('df_old_id.ethnicity_concept_id') == col('df_new_id.ethnicity_concept_id'))
                    ),
                    'left_anti'   
                    )
    
    df_return = df_new_person.select('person_id').distinct()
        
    path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output_mimic/AD_cp" + str(copy_num+1) + "_to_cp" + str(copy_num) + ".csv" 
    df_return.coalesce(1).write.csv(path, header=True)
    
    df_return.show(truncate=False)
    
    return None

  
def report_id_change_patient(spark, df_person_0, df_person_1, copy_num, print_on_scr):
  
    df_person_0 = df_person_0.alias("df_person_0")
    
    df_person_1 = df_person_1.withColumnRenamed('person_id','new_person_id')
    df_person_1 = df_person_1.alias("df_person_1")
    
    df_new_diff_id =  df_person_1.join(df_person_0,
              (
                (col('df_person_0.person_id') == col('df_person_1.new_person_id')) 
              ),
              'left_anti'
    )
    
    df_new_diff_id = df_new_diff_id.alias("df_new_id")
    
    df_old_diff_id = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.new_person_id')) 
              ),
              'left_anti'
    )
    
    df_old_diff_id = df_old_diff_id.alias("df_old_id")
    
    df_id_change_with_same_demo = df_new_diff_id.join(df_old_diff_id,
                    (
                      (col('df_old_id.gender_concept_id') == col('df_new_id.gender_concept_id')) &
                      (col('df_old_id.year_of_birth') == col('df_new_id.year_of_birth')) &
                      (col('df_old_id.month_of_birth') == col('df_new_id.month_of_birth')) &
                      (col('df_old_id.day_of_birth') == col('df_new_id.day_of_birth')) &
                      #(col('df_old_id.date_of_birth') == col('df_new_id.date_of_birth')) &
                      (col('df_old_id.race_concept_id') == col('df_new_id.race_concept_id')) &
                      (col('df_old_id.ethnicity_concept_id') == col('df_new_id.ethnicity_concept_id'))
                    ),
                    'inner'   
                    )
    
    df_id_change_map = df_id_change_with_same_demo.select(col('df_old_id.person_id'), col('df_new_id.new_person_id'))
    
      
    if (print_on_scr):
      
      path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output_mimic/IC_cp" + str(copy_num+1) + "_to_cp" + str(copy_num) + ".csv" 
      df_id_change_map.coalesce(1).write.csv(path, header=True)
    
      df_id_change_map.show(truncate=False)
      
    return df_id_change_map

  
def report_id_reuse_patient(spark, df_person_0, df_person_1, copy_num):
  
    df_person_0 = df_person_0.alias("df_person_0")
    
    df_person_1 = df_person_1.withColumnRenamed('person_id','new_person_id')
    df_person_1 = df_person_1.alias("df_person_1")
    
    df_id_reuse = df_person_0.join(df_person_1,
              (
                (col('df_person_0.person_id') == col('df_person_1.new_person_id')) &
                (
                  (col('df_person_0.gender_concept_id') != col('df_person_1.gender_concept_id')) |
                  (col('df_person_0.year_of_birth') != col('df_person_1.year_of_birth')) |
                  (col('df_person_0.month_of_birth') != col('df_person_1.month_of_birth')) |
                  (col('df_person_0.day_of_birth') != col('df_person_1.day_of_birth')) |
                  (col('df_person_0.race_concept_id') != col('df_person_1.race_concept_id')) |
                  (col('df_person_0.ethnicity_concept_id') != col('df_person_1.ethnicity_concept_id'))
                )
              ),
              'inner'
    )
    
    df_id_reuse_map = df_id_reuse.select(col('df_person_0.person_id'), col('df_person_1.new_person_id'))
    
    
    path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output_mimic/IR_cp" + str(copy_num+1) + "_to_cp" + str(copy_num) + ".csv" 
    df_id_reuse_map.coalesce(1).write.csv(path, header=True)
    
    df_id_reuse_map.show(truncate=False)
    
    return None
  
  
def report_delete_patient(spark, df_person_0, df_person_1, df_visit_0, df_visit_1, copy_num):
    
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
    
    df_person_deleted = df_person_deleted.distinct()
    
    path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output_mimic/DL_cp" + str(copy_num+1) + "_to_cp" + str(copy_num) + ".csv" 
    df_person_deleted.coalesce(1).write.csv(path, header=True)
    
    df_person_deleted.show()
    
    return None

  
def report_merge_patient(spark, df_person_0, df_person_1, df_visit_0, df_visit_1, copy_num):
    
    
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
    
    #-- get merged and id changed person id
    df_person_m_c = df_visit_d_m_c.join(df_visit_1, \
                                  df_visit_1.visit_occurrence_id == df_visit_d_m_c.visit_id, 'inner')
    df_person_m_c = df_person_m_c.select(col('pat_id'), col('person_id'))
    
    #-- get id changed person id
    df_person_c = report_id_change_patient(spark, df_person_0, df_person_1,copy_num, 0)
    
    df_person_m = df_person_m_c.subtract(df_person_c)
    
    #-- change column names
    df_person_m = df_person_m.withColumnRenamed('pat_id','source_person_id')
    df_person_m = df_person_m.withColumnRenamed('person_id','target_person_id')
    
    df_person_m = df_person_m.distinct()
    
    path = "/projects/cch/patient-merge/mimic_omop_tables/experiment/output_mimic/DM_cp" + str(copy_num+1) + "_to_cp" + str(copy_num) + ".csv" 
    df_person_m.coalesce(1).write.csv(path, header=True)
    
    df_person_m.show()
    
    return None