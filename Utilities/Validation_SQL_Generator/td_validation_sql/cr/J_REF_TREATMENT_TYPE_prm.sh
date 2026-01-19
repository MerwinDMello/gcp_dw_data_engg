export JOBNAME='J_REF_TREATMENT_TYPE'
export AC_EXP_SQL_STATEMENT="select 'J_REF_TREATMENT_TYPE'+ ',' + 
cast(count(*) as varchar(20)) + ',' AS SOURCE_STRING from [Navadhoc].[dbo].[RefTreatmentType]" 

export AC_ACT_SQL_STATEMENT="select 'J_REF_TREATMENT_TYPE' || ',' || CAST(COUNT(*) AS VARCHAR(20)) || ',' AS SOURCE_STRING from EDWCR.Ref_Treatment_Type"


