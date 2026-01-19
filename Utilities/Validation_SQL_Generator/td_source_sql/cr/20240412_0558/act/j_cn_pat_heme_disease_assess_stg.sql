select 'J_CN_PAT_HEME_DISEASE_ASSESS_STG' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' 
AS SOURCE_STRING from EDWCR_STAGING.Patient_Heme_Disease_Assessment_STG