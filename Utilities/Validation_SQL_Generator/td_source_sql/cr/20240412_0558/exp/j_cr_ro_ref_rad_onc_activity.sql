SELECT 'J_CR_RO_REF_RAD_ONC_ACTIVITY'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.STG_DimActivity DHD