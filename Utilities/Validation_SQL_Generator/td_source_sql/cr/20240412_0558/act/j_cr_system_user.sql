select 'J_CR_PATIENT_TUMOR'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM edwcr.CR_System_User