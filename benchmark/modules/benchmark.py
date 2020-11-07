def report_new_add_patient(spark, df_person_0, df_person_1):
    
    df_inner = df_person_0.join(df_person_1, on=['person_id', \
                                                 'gender_concept_id', \
                                                 'birth_datetime', \
                                                 'race_concept_id', \
                                                 'ethnicity_concept_id'
                                                ], how='inner')
      
    #df_new_patient = df_person_1.select('person_id').subtract(df_inner.select('person_id'))
    df_person_1.select('person_id').subtract(df_inner.select('person_id')).show(truncate=False)
    
    return Nonec