SELECT 'J_CR_RO_Fact_Cancer_Patient_Financial'||','|| cast(count(*) as varchar(20))||',' as SOURCE_STRING 
FROM EDWCR_BASE_Views.Consolidated_Patient_Encounter