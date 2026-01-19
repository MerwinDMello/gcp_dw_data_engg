SELECT 'J_CR_REF_TREATMENT_TYPE'||','|| cast(count(*) as varchar(20))||',	' as SOURCE_STRING 
FROM  edwcr_staging.Ref_CR_Treatment_Type_Stg