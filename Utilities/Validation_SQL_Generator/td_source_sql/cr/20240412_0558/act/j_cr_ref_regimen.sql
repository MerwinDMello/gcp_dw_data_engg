select 'J_CR_REF_REGIMEN'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Regimen