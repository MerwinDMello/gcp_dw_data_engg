SELECT 'J_CR_PATIENT_TUMOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM  edwcr_staging.CR_System_User_Stg