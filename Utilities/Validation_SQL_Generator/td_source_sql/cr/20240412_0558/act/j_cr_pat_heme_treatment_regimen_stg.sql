select 'J_CR_PAT_HEME_TREATMENT_REGIMEN_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG