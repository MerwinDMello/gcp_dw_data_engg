select 'J_CR_REF_TREATMENT_PHASE'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Treatment_Phase