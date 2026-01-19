SELECT 'J_CR_REF_PATHWAY_VAR_REASON'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM  EDWCR_STAGING.Patient_Heme_Treatment_Regimen_STG