SELECT 'J_CR_RO_FACT_RAD_ONC_PATIENT'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.STG_FactPatient