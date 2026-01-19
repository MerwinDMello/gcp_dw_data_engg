SELECT 'J_MT_REF_TREATMENT_TYPE_GRP'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  edwcr_staging.REF_TREATMENT_TYPE_GROUP_STG