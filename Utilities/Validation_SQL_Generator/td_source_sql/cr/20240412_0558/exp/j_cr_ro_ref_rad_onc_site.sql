SELECT 'J_CR_RO_REF_RAD_ONC_SITE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_STAGING.CR_RO_DimSite_stg