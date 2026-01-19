SELECT 'J_MT_REF_HOSPITAL'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  edwcr_staging.REF_HOSPITAL_STG