SELECT 'J_CN_PATIENT_HEME_TREATMENT_REGIMEN'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM  EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG