SELECT 'J_REF_RAD_ONC_TREATMENT_TYPE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.stg_SC_Modalities