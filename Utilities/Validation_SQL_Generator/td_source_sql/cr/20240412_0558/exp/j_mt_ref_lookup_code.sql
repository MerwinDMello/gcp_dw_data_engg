SELECT 'J_MT_REF_LOOKUP_CODE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  edwcr_staging.REF_LOOKUP_CODE_STG