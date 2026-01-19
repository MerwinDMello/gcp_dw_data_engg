select 'J_CR_REF_PATHWAY_VAR_REASON'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR.Ref_Pathway_Var_Reason