select 'J_CN_PATIENT_HEME_TRANSPLANT_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.stg_PatientHemeTransplant